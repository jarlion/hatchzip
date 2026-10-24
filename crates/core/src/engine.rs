//! In-place extraction, independent of the archive format.
//!
//! Units are processed from the highest offset down. After each batch the
//! archive is truncated to where the batch began, which every filesystem
//! supports, FAT32 and exFAT included, so this path needs no hole punching.
//!
//! A unit too large for the free space would need room for all of its
//! output before any of its archive bytes could go. Where the filesystem can
//! punch holes and the format can resume decoding mid-file, such a unit is
//! streamed instead: every [`stream_step`] of output it is flushed, a resume
//! point is journaled, and only then are the archive bytes before that point
//! punched out. The archive's length stays the same until the unit commits
//! through the ordinary path.
//!
//! A compressed tar is one stream that decodes only from the front, so it is
//! one unit holding every entry, always streamed, front to back, with resume
//! points between the files as well as inside them (see
//! [`Run::stream_solid`]). Without hole punching it still streams, so a
//! crash resumes from the last point, but nothing is freed until the end.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, anyhow, bail};
use rayon::prelude::*;

use crate::archive::resumable::{self, Decoder};
use crate::archive::storage::{Storage, Tail};
use crate::archive::targz::{Event, Profile, Walker};
use crate::archive::{self, Codec, Format, Item, Kind, Sink, Stream, Unit};
use crate::journal::{Journal, Link, Paths, Resume, TarAt};
use crate::{fsutil, platform};

/// Larger batches extract in parallel better, smaller ones redo less work
/// after a crash and keep the archive shrinking in fine steps.
const BATCH_MAX_UNITS: usize = 512;
const BATCH_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const BUF_SIZE: usize = 1024 * 1024;
const MAX_SYMLINK: u64 = 64 * 1024;

/// Output written between two resume points of a streamed unit. Each point
/// costs a flush and a journal write; the space check counts one step as the
/// most a streamed unit holds that its archive bytes don't offset.
const STREAM_STEP: u64 = 256 * 1024 * 1024;

fn stream_step() -> u64 {
    test_knob("HATCHZIP_STREAM_STEP").map_or(STREAM_STEP, u64::from)
}

/// Units smaller than this are always extracted whole: streaming one would
/// save less than a couple of steps.
fn stream_threshold() -> u64 {
    2 * stream_step()
}

/// The test suite streams every unit it can, whether it fits or not, so the
/// streaming path runs on small archives.
fn always_stream() -> bool {
    test_knob("HATCHZIP_STREAM_STEP").is_some()
}

fn on_disk(u: &Unit) -> u64 {
    u.items.iter().map(|i| i.size.next_multiple_of(4096)).sum()
}

pub struct Job {
    pub storage: Arc<Storage>,
    pub format_name: &'static str,
    format: Box<dyn Format>,
    /// Units still to do, highest offset first.
    pub units: Vec<Unit>,
    pub tail_start: u64,
    /// Where the archive currently ends: its original length, or wherever
    /// the last commit truncated it.
    pub top: u64,
    /// Whether the archive's filesystem can free bytes inside a file, and
    /// how much of a punched range it may keep allocated at each end. Probed
    /// once, on first use.
    punch: OnceLock<Option<u64>>,
    /// The streamed unit a previous run left half done, if any.
    resume: Option<Resume>,
}

impl Job {
    /// `password` is only needed for encrypted archives. A missing one shows
    /// up as [`archive::PasswordNeeded`], from here or from
    /// [`Job::check_password`].
    pub fn open(
        path: &Path,
        writable: bool,
        password: Option<&str>,
        // Skip the full measuring pass for a compressed tar (see `archive::open`).
        scan: bool,
    ) -> Result<Self> {
        let storage = Arc::new(Storage::open(path, writable)?);
        let opened = archive::open(&storage, None, password, scan)?;
        let top = storage.original_len();
        Self::build(storage, opened, top)
    }

    /// Reopens an archive a previous run already started on. A read-only
    /// open leaves a pending truncation for the next writable one. The
    /// password is never stored, so an encrypted archive needs it again.
    pub fn resume(
        journal: &Journal,
        tail: Arc<Tail>,
        writable: bool,
        password: Option<&str>,
    ) -> Result<Self> {
        let storage = Arc::new(Storage::from_parts(&journal.parts, writable)?);
        let current = storage.current_len()?;
        if current < journal.committed_len {
            bail!(
                "{} is shorter than HatchZip left it and will not be modified",
                storage.primary().display()
            );
        }
        if current > journal.committed_len && writable {
            // The last run committed a batch but stopped before truncating.
            storage.set_len(journal.committed_len)?;
        }
        let point = journal.resume.as_ref();
        let opened = archive::open(
            &storage,
            Some((&journal.format, tail, point)),
            password,
            true,
        )?;
        let mut job = Self::build(storage, opened, journal.committed_len)?;
        if let Some(r) = &journal.resume {
            // Only the highest uncommitted unit is ever streamed, so a point
            // for any other one means the journal and archive disagree.
            if job.units.first().map(|u| u.start) != Some(r.unit) {
                bail!(
                    "the journal of {} records a partially extracted file that is not the next one \
                     in the archive. The archive will not be modified",
                    job.storage.primary().display()
                );
            }
            job.resume = Some(r.clone());
        }
        Ok(job)
    }

    fn build(storage: Arc<Storage>, opened: archive::Opened, top: u64) -> Result<Self> {
        let mut units = opened.units;
        units.retain(|u| u.start < top);
        units.sort_by_key(|u| std::cmp::Reverse(u.start));

        let mut seen = HashSet::new();
        for item in units.iter().flat_map(|u| &u.items) {
            for part in item.path.iter() {
                if let Some(why) = platform::name_problem(&part.to_string_lossy()) {
                    bail!(
                        "'{}' cannot be created at this location: {why}. The archive was not modified",
                        item.path.display()
                    );
                }
            }
            // Case-insensitive filesystems would let two items race for one
            // file inside a parallel batch.
            if item.kind != Kind::Dir && !seen.insert(item.path.to_string_lossy().to_lowercase()) {
                bail!(
                    "'{}' appears more than once in the archive (ignoring case); extraction refused",
                    item.path.display()
                );
            }
        }
        Ok(Self {
            storage,
            format_name: opened.name,
            format: opened.format,
            units,
            tail_start: opened.tail_start,
            top,
            punch: OnceLock::new(),
            resume: None,
        })
    }

    /// `Some(granule)` if holes punched in the archive give its space back,
    /// where `granule` is how much may stay allocated at each end of a
    /// punched range. `None` on filesystems that can only truncate, such as
    /// FAT32 and exFAT, which keep today's whole-unit behavior.
    pub fn punch_support(&self) -> Option<u64> {
        *self.punch.get_or_init(|| {
            if test_knob("HATCHZIP_NO_PUNCH").is_some() {
                return None;
            }
            let dir = match self.storage.primary().parent() {
                Some(d) if !d.as_os_str().is_empty() => d,
                _ => Path::new("."),
            };
            fsutil::can_punch(dir).then(|| platform::punch_granule(dir))
        })
    }

    pub fn encrypted(&self) -> bool {
        self.units.iter().any(|u| u.encrypted)
    }

    /// The archive is one compressed stream, extracted front to back.
    pub fn is_stream(&self) -> bool {
        self.format.solid().is_some()
    }

    /// The archive is one compressed stream that stays whole until the run
    /// ends, because the filesystem can't free space inside it: the disk
    /// has to hold the archive and every extracted file at once.
    pub fn kept_whole(&self, reclaims: bool) -> bool {
        self.is_stream() && reclaims && self.punch_support().is_none()
    }

    /// Proves the password before anything is deleted: decrypts the smallest
    /// encrypted unit, which checks its CRC, up to a limit so a huge solid
    /// block doesn't stall. ZipCrypto accepts about one wrong password in 256
    /// at first sight; reading the data catches those.
    pub fn check_password(&self) -> Result<()> {
        const ENOUGH: u64 = 64 * 1024 * 1024;
        let Some(unit) = self
            .units
            .iter()
            .filter(|u| u.encrypted)
            .min_by_key(|u| u.size())
        else {
            return Ok(());
        };
        let mut worker = self.format.worker(1)?;
        let mut sink = ProbeSink {
            left: ENOUGH,
            buf: vec![0; BUF_SIZE],
        };
        match worker.extract(unit, &mut sink) {
            Ok(()) => Ok(()),
            Err(e) if e.chain().any(|c| c.is::<ProbeDone>()) => Ok(()),
            Err(e) if e.chain().any(|c| c.is::<archive::PasswordNeeded>()) => Err(e),
            Err(_) => Err(archive::WrongPassword.into()),
        }
    }

    pub fn item_count(&self) -> usize {
        self.units.iter().map(|u| u.items.len()).sum()
    }

    pub fn remaining_size(&self) -> u64 {
        let done = self.resume.as_ref().map_or(0, |r| r.written);
        self.units.iter().map(Unit::size).sum::<u64>() - done
    }

    /// The resume point recorded for `units[i]`, if it is half streamed.
    fn resumed(&self, i: usize) -> Option<&Resume> {
        self.resume
            .as_ref()
            .filter(|r| r.unit == self.units[i].start)
    }

    /// Where the data of `units[i]` lies, if it is one file whose decoding
    /// can be resumed from the middle.
    fn stream_of(&self, i: usize) -> Option<Stream> {
        let unit = &self.units[i];
        match unit.items.as_slice() {
            [item] if item.kind == Kind::File && !unit.encrypted => {
                self.format.stream(unit).ok().flatten()
            }
            _ => None,
        }
    }

    /// Whether `units[i]` may free its own archive bytes while it is
    /// extracted. A half-streamed unit always continues streaming: the bytes
    /// before its resume point may be gone.
    fn streams(&self, i: usize, reclaims: bool) -> Option<Stream> {
        if self.resumed(i).is_some() {
            return self.stream_of(i);
        }
        if !reclaims || self.units[i].size() < stream_threshold() {
            return None;
        }
        self.punch_support()?;
        self.stream_of(i)
    }

    /// Bytes freed once `units[i]` is committed: everything from its start up
    /// to whatever came after it.
    fn span(&self, i: usize) -> u64 {
        let end = if i == 0 {
            self.top
        } else {
            self.units[i - 1].start
        };
        end - self.units[i].start
    }

    /// Replays the extraction one unit at a time and returns `(peak, end)`:
    /// the most free space it ever consumes beyond what is free now, and how
    /// much it consumes once finished. `fresh` means the tail hasn't been
    /// saved yet; `reclaims` that freed archive space lands on the output's
    /// disk.
    pub fn space_needed(&self, fresh: bool, reclaims: bool) -> (u64, i128) {
        let tail = (self.storage.original_len() - self.tail_start) as i128;
        let mut extra: i128 = if fresh { tail } else { 0 };
        let mut peak = extra;
        for (i, u) in self.units.iter().enumerate() {
            let span = self.span(i) as i128;
            if let Some(solid) = self.format.solid() {
                let (rise, net) = self.solid_cost(i, &solid.profile, span, reclaims);
                peak = peak.max(extra + rise);
                extra += net;
                continue;
            }
            if let Some(s) = self.streams(i, reclaims) {
                let (rise, net) = self.stream_cost(i, &s, span, reclaims);
                peak = peak.max(extra + rise);
                extra += net;
                continue;
            }
            let on_disk = on_disk(u) as i128;
            peak = peak.max(extra + on_disk);
            extra += on_disk;
            if reclaims {
                extra -= span;
            }
        }
        if reclaims {
            // Whatever precedes the first unit is deleted at the very end,
            // along with the saved tail.
            let stub = self.units.last().map_or(self.top, |u| u.start);
            extra -= stub as i128 + tail;
        }
        (peak.max(0) as u64, extra)
    }

    /// What streaming `units[i]` costs in free space: `(rise, net)`, the most
    /// it consumes at any moment and what it consumes once committed.
    ///
    /// At any moment the unit holds the output written since the last resume
    /// point (at most a step, plus one read buffer or deflate block), the
    /// archive bytes at either end of the punched range the filesystem keeps
    /// (a granule each), and however far the output so far has outgrown the
    /// input it came from. For stored data that last part is zero. For
    /// deflate it only grows as decoding goes on, since encoders fall back to
    /// stored blocks rather than expand data, so the end state bounds it; a
    /// 1/1024 margin covers block headers.
    ///
    /// Without punching (the filesystem stopped supporting it after a run
    /// began), the unit holds all of its remaining output until it commits.
    fn stream_cost(&self, i: usize, s: &Stream, span: i128, reclaims: bool) -> (i128, i128) {
        let at = self.resumed(i);
        // What a crashed run already wrote is on disk, so the free space
        // measured now has paid for it.
        let done = at.map_or(0, |r| r.written / 4096 * 4096);
        let out_left = (on_disk(&self.units[i]) - done) as i128;
        let read_to = at.map_or(s.data_start, |r| r.bit / 8);
        let data_left = (s.data_start + s.packed).saturating_sub(read_to) as i128;
        // At most this much was punched already. It is free now, so the
        // final truncation gives back that much less; assuming the most
        // keeps the estimate on the safe side.
        let freed = read_to.saturating_sub(s.data_start) as i128;
        let net = if reclaims {
            out_left - (span - freed)
        } else {
            out_left
        };
        let rise = match self.punch_support() {
            Some(granule) if reclaims => {
                let slack = match s.codec {
                    Codec::Stored => 0,
                    Codec::Deflate => out_left / 1024,
                };
                let step = (stream_step() + BUF_SIZE as u64 + 2 * granule) as i128;
                ((out_left - data_left).max(0) + slack + step).min(out_left)
            }
            _ => out_left,
        };
        (rise, net)
    }

    /// What streaming a compressed tar, `units[i]`, costs in free space:
    /// `(rise, net)` as for [`stream_cost`](Self::stream_cost).
    ///
    /// Before each resume point is taken, the files hold what they held at
    /// the point before, `lead` at most ahead of the compressed bytes freed
    /// up to it, plus what was written since: under a step, plus the output
    /// to the next point after the step is full, at most `gap`. The scan
    /// measured both at the very points the run will meet. A read buffer
    /// and a granule at each end of the punched range come on top. Links are
    /// made at the end, once the archive is gone.
    fn solid_cost(&self, i: usize, p: &Profile, span: i128, reclaims: bool) -> (i128, i128) {
        let out_left = (p.files + p.links) as i128;
        // Assumed punched already, as for a single streamed file.
        let freed = self.resumed(i).map_or(0, |r| r.bit / 8) as i128;
        let net = if reclaims {
            out_left - (span - freed)
        } else {
            out_left
        };
        let rise = match self.punch_support() {
            Some(granule) if reclaims => {
                let ahead = stream_step() + p.gap + BUF_SIZE as u64 + 2 * granule;
                (p.lead.max(0) + ahead as i128).min(p.files as i128)
            }
            // Nothing is freed until the run commits, so every file is on
            // disk next to the whole archive.
            _ => out_left,
        };
        (rise.max(net), net)
    }

    /// The entry a resume point stopped inside of, if any: the first item of
    /// the first unit, whose part file holds `written` bytes.
    fn open_entry(&self) -> Option<&Item> {
        let r = self.resume.as_ref()?;
        let inside = match &r.stream {
            Some(s) => matches!(s.at, TarAt::File { .. }),
            None => true,
        };
        if !inside {
            return None;
        }
        self.units.first().and_then(|u| u.items.first())
    }

    /// Output a crashed run wrote for units it never committed. The output
    /// folder started empty, so anything at those paths is ours. A
    /// half-streamed file is not in the list: its archive bytes may be gone,
    /// so it is the only copy of its first part. For a compressed tar that
    /// is the entry its point is in; the files after the point are still in
    /// the archive.
    pub fn uncommitted(&self, out: &Path) -> Vec<(PathBuf, u64)> {
        let open = self.open_entry().is_some();
        let mut found = Vec::new();
        for (i, u) in self.units.iter().enumerate() {
            for (k, item) in u.items.iter().enumerate() {
                if item.kind != Kind::File || (open && i == 0 && k == 0) {
                    continue;
                }
                let dest = out.join(&item.path);
                for p in [part_path(&dest), dest] {
                    if let Ok(m) = fs::symlink_metadata(&p)
                        && m.is_file()
                    {
                        found.push((p, m.len().next_multiple_of(4096)));
                    }
                }
            }
        }
        found
    }

    /// Output a crashed stream wrote past its last resume point. It is cut
    /// off before the stream continues, so it counts as free.
    pub fn past_resume_point(&self, out: &Path) -> u64 {
        let (Some(r), Some(item)) = (self.resume.as_ref(), self.open_entry()) else {
            return 0;
        };
        let dest = out.join(&item.path);
        [part_path(&dest), dest]
            .iter()
            .find_map(|p| fs::metadata(p).ok())
            .map_or(0, |m| {
                (m.len().next_multiple_of(4096)).saturating_sub(r.written.next_multiple_of(4096))
            })
    }

    /// Reads every unit and lets the decoders check their checksums,
    /// writing nothing.
    pub fn verify(&self, on_bytes: &(dyn Fn(u64) + Sync)) -> Result<()> {
        let threads = threads_per_unit(self.units.len());
        let failures: Vec<String> = self
            .units
            .par_iter()
            .map_init(
                || {
                    (
                        self.format.worker(threads),
                        VerifySink {
                            on_bytes,
                            buf: vec![0; BUF_SIZE],
                        },
                    )
                },
                |(worker, sink), unit| {
                    let mut run = || -> Result<()> {
                        let w = worker.as_mut().map_err(|e| anyhow!("{e:#}"))?;
                        w.extract(unit, sink)
                    };
                    run().err().map(|e| format!("{e:#}"))
                },
            )
            .flatten()
            .collect();
        if failures.is_empty() {
            return Ok(());
        }
        let mut msg = format!("{} damaged:", failures.len());
        for f in failures.iter().take(10) {
            msg.push_str("\n  ");
            msg.push_str(f);
        }
        Err(anyhow!(msg))
    }
}

fn threads_per_unit(units: usize) -> u32 {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    (cores / units.max(1)).max(1) as u32
}

fn part_path(dest: &Path) -> PathBuf {
    let mut part = dest.as_os_str().to_owned();
    part.push(".hatchzip-part");
    PathBuf::from(part)
}

/// Opens a streamed file's part file positioned at `written`, cutting off
/// anything written after that point.
fn open_part(dest: &Path, part: &Path, written: u64) -> Result<File> {
    if written == 0 {
        return OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(part)
            .with_context(|| format!("creating {}", part.display()));
    }
    if !part.exists() && dest.exists() {
        // The previous run finished the file and renamed it, then stopped
        // before committing. The point it journaled still holds.
        fs::rename(dest, part).with_context(|| format!("reopening {}", dest.display()))?;
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(part)
        .with_context(|| {
            format!(
                "{} is missing, and the archive data it was extracted from is already \
                 released, so this file cannot be completed",
                part.display()
            )
        })?;
    if file.metadata()?.len() < written {
        bail!(
            "{} is shorter than when its progress was saved; the extraction cannot continue",
            part.display()
        );
    }
    file.set_len(written)?;
    file.seek(SeekFrom::Start(written))?;
    Ok(file)
}

/// A file of a compressed tar being written to its part file.
struct OpenFile {
    item: Item,
    dest: PathBuf,
    part: PathBuf,
    file: File,
    written: u64,
}

impl OpenFile {
    /// Gives the finished file its metadata and its real name.
    fn finish(self) -> Result<()> {
        if self.written != self.item.size {
            bail!("it is smaller than the archive records");
        }
        if let Some(t) = self.item.mtime {
            self.file.set_modified(t)?;
        }
        platform::apply_mode(&self.file, self.item.mode)?;
        if platform::SYNC_EACH_FILE {
            self.file.sync_all()?;
        }
        drop(self.file);
        fs::rename(&self.part, &self.dest)?;
        Ok(())
    }
}

/// The output just before `written`, which a deflate stream may refer back
/// to. It is on disk, flushed before the point was journaled, so the
/// journal doesn't have to carry it.
fn read_window(file: &mut File, written: u64) -> Result<Vec<u8>> {
    let n = written.min(resumable::WINDOW as u64);
    let mut window = vec![0; n as usize];
    file.seek(SeekFrom::Start(written - n))?;
    file.read_exact(&mut window)?;
    file.seek(SeekFrom::Start(written))?;
    Ok(window)
}

pub struct Progress {
    pub on_bytes: Box<dyn Fn(u64) + Sync>,
    pub on_commit: Box<dyn Fn(u64) + Sync>,
    /// Publishes the total output size once it is known. A run that skipped
    /// the measuring scan only learns it when the one decode pass has listed
    /// every entry; for scanned runs it repeats the planned total.
    pub on_total: Box<dyn Fn(u64) + Sync>,
}

pub enum Outcome {
    Finished,
    Paused,
}

pub struct Run<'a> {
    pub job: Job,
    pub journal: Journal,
    pub paths: Paths,
    pub stop: &'a AtomicBool,
    pub progress: Progress,
    /// Free space the run must never eat into.
    pub keep_free: u64,
}

impl Run<'_> {
    pub fn execute(mut self) -> Result<(Outcome, usize)> {
        let out = self.journal.output.clone();
        fs::create_dir_all(&out)
            .with_context(|| format!("creating output folder {}", out.display()))?;

        let reclaims = fsutil::same_filesystem(self.job.storage.primary(), &out);
        let mut next = 0;
        while next < self.job.units.len() {
            if self.stop.load(Ordering::Relaxed) {
                return Ok((Outcome::Paused, self.job.item_count()));
            }
            if let Some(solid) = self.job.format.solid() {
                let walk = solid.walk(self.job.resumed(next))?;
                let Some(links) = self.stream_solid(next, walk, reclaims, &out)? else {
                    return Ok((Outcome::Paused, self.job.item_count()));
                };
                self.commit(next, next + 1, links, &out)?;
                // The whole stream is decoded, so every entry is listed: the
                // total is now known even when the scan was skipped (it then
                // repeats the planned total, which changes nothing).
                let total = self.job.units[next].items.iter().map(|i| i.size).sum();
                (self.progress.on_total)(total);
                next += 1;
                continue;
            }
            if let Some(stream) = self.stream_now(next, reclaims, &out)? {
                if !self.stream_unit(next, &stream, &out)? {
                    return Ok((Outcome::Paused, self.job.item_count()));
                }
                self.commit(next, next + 1, Vec::new(), &out)?;
                next += 1;
                continue;
            }
            let end = self.next_batch(next, &out, reclaims)?;
            let batch = &self.job.units[next..end];

            let Some(links) = self.extract_batch(batch, &out)? else {
                return Ok((Outcome::Paused, self.job.item_count()));
            };
            self.commit(next, end, links, &out)?;
            next = end;
        }

        // Count before finish(): it consumes the job (and its storage handle).
        let count = self.job.item_count();
        self.finish(&out)?;
        Ok((Outcome::Finished, count))
    }

    /// Commits `units[start..end]`, all extracted, and truncates the archive
    /// behind them.
    fn commit(&mut self, start: usize, end: usize, links: Vec<Link>, out: &Path) -> Result<()> {
        let batch = &self.job.units[start..end];
        // The journal must never claim a file that a power cut could still
        // take back, since its archive bytes are about to be truncated.
        platform::sync_batch(out).context("flushing extracted files to disk")?;
        if platform::SYNC_EACH_FILE {
            // Links are only recorded here and made at the end, so their
            // folders may not exist yet.
            let dirs: HashSet<PathBuf> = batch
                .iter()
                .flat_map(|u| &u.items)
                .filter(|it| matches!(it.kind, Kind::File | Kind::Dir))
                .filter_map(|it| out.join(&it.path).parent().map(Path::to_path_buf))
                .collect();
            for d in &dirs {
                fsutil::sync_dir(d)?;
            }
        }
        crash_point("after-extract");

        let new_len = batch.last().expect("batches are never empty").start;
        self.journal.committed_len = new_len;
        self.journal.links.extend(links);
        // A streamed unit's point is spent once the unit itself is claimed;
        // both change in the same atomic write.
        self.journal.resume = None;
        self.job.resume = None;
        self.journal.save(&self.paths)?;
        crash_point("after-journal");

        self.job
            .storage
            .set_len(new_len)
            .context("shrinking the archive")?;
        (self.progress.on_commit)(new_len);
        Ok(())
    }

    /// Whether `units[i]` is extracted by streaming: always if it is half
    /// streamed already, otherwise if it can be and its output wouldn't fit
    /// whole in the space there is right now. When it fits, the ordinary
    /// path is faster, since it extracts many units in parallel.
    fn stream_now(&self, i: usize, reclaims: bool, out: &Path) -> Result<Option<Stream>> {
        if self.job.resumed(i).is_some() {
            return match self.job.stream_of(i) {
                Some(s) => Ok(Some(s)),
                None => bail!(
                    "the journal records '{}' as partially extracted, but it cannot be resumed",
                    self.job.units[i].items[0].path.display()
                ),
            };
        }
        let Some(stream) = self.job.streams(i, reclaims) else {
            return Ok(None);
        };
        if always_stream() {
            return Ok(Some(stream));
        }
        let avail = fsutil::available_space(out)?.saturating_sub(self.keep_free);
        Ok((on_disk(&self.job.units[i]) > avail).then_some(stream))
    }

    /// Extracts the one file of `units[i]` while freeing the archive bytes
    /// behind it. Returns false if a stop request interrupted it; the last
    /// resume point stays in the journal and the next run continues there.
    ///
    /// The order at every point is what makes a crash harmless: the output
    /// up to the point is flushed, then the point is journaled, and only then
    /// is the archive punched, never past the byte the point restarts in. A
    /// crash before the journal write leaves the previous point in force,
    /// and everything it needs is still in the archive.
    fn stream_unit(&mut self, i: usize, stream: &Stream, out: &Path) -> Result<bool> {
        let unit_start = self.job.units[i].start;
        let item = self.job.units[i].items[0].clone();
        let dest = out.join(&item.path);
        let part = part_path(&dest);
        let at = self.job.resumed(i).cloned().unwrap_or(Resume {
            unit: unit_start,
            written: 0,
            bit: stream.data_start * 8,
            crc: 0,
            stream: None,
        });
        let started_here = at.written == 0;
        let result = self
            .stream_file(&item, stream, at, &dest, &part, out)
            .with_context(|| format!("extracting '{}'", item.path.display()));
        if result.is_err() && started_here && self.journal.resume.is_none() {
            // Nothing was punched yet, so the next run starts this file over.
            let _ = fs::remove_file(&part);
        }
        result
    }

    fn stream_file(
        &mut self,
        item: &Item,
        stream: &Stream,
        at: Resume,
        dest: &Path,
        part: &Path,
        out: &Path,
    ) -> Result<bool> {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = open_part(dest, part, at.written)?;
        let window = match stream.codec {
            Codec::Stored => Vec::new(),
            Codec::Deflate => read_window(&mut file, at.written)?,
        };
        let mut decoder = Decoder::new(self.job.storage.reader(None), stream, at.bit, &window)?;
        let mut crc = crc32fast::Hasher::new_with_initial(at.crc);
        let mut written = at.written;
        let mut since_point = 0;
        let step = stream_step();
        let mut punching = self.job.punch_support();
        let what = format!("'{}'", item.path.display());
        self.ensure_room(out, &what, item.size - written, punching)?;

        let mut buf = vec![0; BUF_SIZE];
        loop {
            if self.stop.load(Ordering::Relaxed) {
                return Ok(false);
            }
            let n = decoder.read(&mut buf)?;
            if n == 0 {
                break;
            }
            written += n as u64;
            if written > item.size {
                bail!("it is larger than the archive records");
            }
            file.write_all(&buf[..n])?;
            crc.update(&buf[..n]);
            (self.progress.on_bytes)(n as u64);
            since_point += n as u64;

            if since_point < step || written == item.size {
                continue;
            }
            match decoder.restart_point() {
                Some(bit) => {
                    let point = Resume {
                        unit: at.unit,
                        written,
                        bit,
                        crc: crc.clone().finalize(),
                        stream: None,
                    };
                    punching = self.save_point(&file, part, out, point, stream, punching)?;
                    since_point = 0;
                    self.ensure_room(out, &what, item.size - written, punching)?;
                }
                // A deflate block runs as long as its encoder likes; zlib-ng
                // and zlib-rs write level 1 as a single block. Until it ends
                // there is nowhere to resume from, so nothing can be freed,
                // and the run has to stop before the disk fills.
                None => {
                    let avail = fsutil::available_space(out)?.saturating_sub(self.keep_free);
                    if avail < 2 * BUF_SIZE as u64 {
                        bail!(
                            "not enough disk space to continue: the compressed data of this entry has \
                             no resume point in the last {}, so its archive space cannot be \
                             released yet. {} more is required (the remainder of this file). \
                             Progress is saved; free up space and resume the extraction.",
                            human(since_point),
                            human(item.size - written),
                        );
                    }
                }
            }
        }
        if written != item.size {
            bail!("it is smaller than the archive records");
        }
        if let Some(expected) = stream.crc
            && crc.finalize() != expected
        {
            bail!("its data is damaged (CRC mismatch)");
        }
        if let Some(t) = item.mtime {
            file.set_modified(t)?;
        }
        platform::apply_mode(&file, item.mode)?;
        file.sync_all()?;
        drop(file);
        // From here a crash leaves the finished file under its real name
        // while the journal still holds the last point; `open_part` takes it
        // back as the part file.
        fs::rename(part, dest)?;
        Ok(true)
    }

    /// Makes `point` the one a crash resumes from, then frees the archive
    /// bytes it no longer needs. Returns whether to keep punching.
    fn save_point(
        &mut self,
        file: &File,
        part: &Path,
        out: &Path,
        point: Resume,
        stream: &Stream,
        punching: Option<u64>,
    ) -> Result<Option<u64>> {
        file.sync_all()
            .context("flushing the extracted file to disk")?;
        // The part file and the folders above it may be new; a point that
        // outlives its file would resume into nothing.
        let mut dir = part.parent();
        while let Some(d) = dir {
            fsutil::sync_dir(d)?;
            if d == out {
                break;
            }
            dir = d.parent();
        }
        crash_point("stream-after-sync");

        self.journal.resume = Some(point.clone());
        self.journal.save(&self.paths)?;
        self.job.resume = Some(point.clone());
        crash_point("stream-after-journal");

        // Decoding restarts inside the byte holding `point.bit`, so that byte
        // and everything after it must stay.
        if punching.is_some()
            && !self
                .job
                .storage
                .punch(stream.data_start, point.bit / 8)
                .context("freeing extracted archive bytes")?
        {
            // The probe said this works; if the filesystem disagrees after
            // all, the run continues without freeing anything mid-file.
            return Ok(None);
        }
        crash_point("stream-after-punch");
        Ok(punching)
    }

    /// Extracts a compressed tar, `units[i]`, front to back from `walk`,
    /// freeing the compressed bytes behind the decoder. Returns the links
    /// found since the last resume point, for the commit, or `None` if a
    /// stop request interrupted it; the last point stays in the journal and
    /// the next run continues there.
    ///
    /// Each point follows the order of [`stream_unit`](Self::stream_unit):
    /// every file written before it is flushed, then the point is journaled,
    /// with the links found so far, and only then is the archive punched up
    /// to the byte it restarts in. Files finished after the last point are
    /// written again on resume, the half-written one is cut back to the
    /// point.
    fn stream_solid(
        &mut self,
        i: usize,
        mut walk: Walker,
        reclaims: bool,
        out: &Path,
    ) -> Result<Option<Vec<Link>>> {
        let unit = self.job.units[i].start;
        let step = stream_step();
        // With the output on another disk, freeing the archive's space
        // doesn't help it, and the space check didn't count on it.
        let mut punching = self.job.punch_support().filter(|_| reclaims);
        let profile = match self.job.format.solid() {
            Some(s) => s.profile.clone(),
            None => bail!("the archive is not a compressed stream"),
        };
        // A skipped scan (--no-space-check) leaves `profile.files` at 0; treat
        // that as "remaining output unknown" with `u64::MAX` so the per-point
        // guard still enforces a safe step-sized bound instead of demanding room
        // for all output, or bailing outright.
        //
        // It likewise leaves `units[i].items` empty; rebuild it as we decode the
        // one stream, so the final entry count and `commit`'s dir sync stay
        // correct. A scanned open already filled it, so this is a no-op then,
        // and for a resumed run, which always scans.
        let rebuild = self.job.units[i].items.is_empty();
        let mut left = if profile.files > 0 { profile.files } else { u64::MAX };
        // This run hasn't punched anything yet. Its first punch starts at 0,
        // in case the last run stopped between journaling a point and
        // punching up to it.
        let mut punched = 0;
        let mut since_point = 0u64;
        let mut checked = 0u64;
        let mut open: Option<OpenFile> = None;
        let mut links = Vec::new();
        let mut dirs = HashSet::new();
        self.ensure_room(out, "the archive", left, punching)?;

        loop {
            if self.stop.load(Ordering::Relaxed) {
                return Ok(None);
            }
            match walk.next(BUF_SIZE)? {
                Event::Entry { item, link, done } => {
                    if rebuild {
                        self.job.units[i].items.push(item.clone());
                    }
                    let dest = out.join(&item.path);
                    let what = || format!("extracting '{}'", item.path.display());
                    match &item.kind {
                        Kind::Dir => {
                            fs::create_dir_all(&dest).with_context(what)?;
                            dirs.insert(dest);
                        }
                        Kind::Symlink => {
                            let target = String::from_utf8(link.unwrap_or_default())
                                .map_err(|_| anyhow!("the link target is not valid UTF-8"))
                                .with_context(what)?;
                            links.push(Link {
                                path: item.path.clone(),
                                target: PathBuf::from(target),
                                hard: false,
                            });
                        }
                        Kind::HardLink(target) => links.push(Link {
                            path: item.path.clone(),
                            target: target.clone(),
                            hard: true,
                        }),
                        Kind::File => {
                            if let Some(parent) = dest.parent() {
                                fs::create_dir_all(parent).with_context(what)?;
                            }
                            let part = part_path(&dest);
                            let file = open_part(&dest, &part, done).with_context(what)?;
                            open = Some(OpenFile {
                                item,
                                dest,
                                part,
                                file,
                                written: done,
                            });
                        }
                    }
                }
                Event::Data(bytes) => {
                    let f = open
                        .as_mut()
                        .ok_or_else(|| anyhow!("the tar stream holds data outside a file"))?;
                    f.file
                        .write_all(bytes)
                        .with_context(|| format!("extracting '{}'", f.item.path.display()))?;
                    let n = bytes.len() as u64;
                    f.written += n;
                    since_point += n;
                    left = left.saturating_sub(n);
                    (self.progress.on_bytes)(n);
                    // A deflate block runs as long as its encoder likes, and
                    // until it ends there is nowhere to resume from, so nothing
                    // can be freed: stop before the disk fills.
                    if punching.is_some()
                        && since_point >= step
                        && since_point - checked >= BUF_SIZE as u64
                    {
                        checked = since_point;
                        let avail = fsutil::available_space(out)?.saturating_sub(self.keep_free);
                        if avail < 2 * BUF_SIZE as u64 {
                            bail!(
                                "not enough disk space to continue: the compressed data has no \
                                 resume point in the last {}, so its space cannot be released \
                                 yet. {} more is required (the remainder of the archive). \
                                 Progress is saved; free up space and resume the extraction.",
                                human(since_point),
                                human(left),
                            );
                        }
                    }
                }
                Event::FileEnd => {
                    let f = open
                        .take()
                        .ok_or_else(|| anyhow!("the tar stream ends a file it never began"))?;
                    let pad = f.item.size.next_multiple_of(4096) - f.item.size;
                    let path = f.item.path.clone();
                    let parent = f.dest.parent().map(Path::to_path_buf);
                    f.finish()
                        .with_context(|| format!("extracting '{}'", path.display()))?;
                    dirs.extend(parent);
                    since_point += pad;
                    left = left.saturating_sub(pad);
                }
                Event::Point => {
                    if since_point < step {
                        continue;
                    }
                    let mut point = walk.point()?;
                    point.unit = unit;
                    punching = self.solid_point(
                        point,
                        open.as_ref(),
                        &mut links,
                        &mut dirs,
                        out,
                        punching,
                        &mut punched,
                    )?;
                    since_point = 0;
                    checked = 0;
                    self.ensure_room(out, "the archive", left, punching)?;
                }
                Event::End => break,
            }
        }
        if open.is_some() {
            bail!("the tar stream ended inside a file");
        }
        Ok(Some(links))
    }

    /// Makes `point` the one a crash resumes from, then frees the archive
    /// bytes before it. Returns whether to keep punching.
    #[allow(clippy::too_many_arguments)]
    fn solid_point(
        &mut self,
        point: Resume,
        open: Option<&OpenFile>,
        links: &mut Vec<Link>,
        dirs: &mut HashSet<PathBuf>,
        out: &Path,
        punching: Option<u64>,
        punched: &mut u64,
    ) -> Result<Option<u64>> {
        if let Some(f) = open {
            f.file
                .sync_all()
                .context("flushing the extracted file to disk")?;
            dirs.extend(f.part.parent().map(Path::to_path_buf));
        }
        platform::sync_batch(out).context("flushing extracted files to disk")?;
        if platform::SYNC_EACH_FILE {
            // Files were flushed as they were finished; their names live in
            // the folders, which may be new themselves.
            let mut done = HashSet::new();
            for d in dirs.drain() {
                let mut dir = Some(d.as_path());
                while let Some(d) = dir {
                    if !done.insert(d.to_path_buf()) {
                        break;
                    }
                    fsutil::sync_dir(d)?;
                    if d == out {
                        break;
                    }
                    dir = d.parent();
                }
            }
        }
        dirs.clear();
        crash_point("stream-after-sync");

        let byte = point.bit / 8;
        self.journal.links.append(links);
        self.journal.resume = Some(point.clone());
        self.journal.save(&self.paths)?;
        self.job.resume = Some(point);
        crash_point("stream-after-journal");

        if punching.is_some() {
            if !self
                .job
                .storage
                .punch(*punched, byte)
                .context("freeing extracted archive bytes")?
            {
                return Ok(None);
            }
            *punched = byte;
            (self.progress.on_commit)(self.job.top - byte);
        }
        crash_point("stream-after-punch");
        Ok(punching)
    }

    /// Stops a stream at a resume point, before it writes what doesn't fit.
    /// `left` is the output still to write.
    fn ensure_room(&self, out: &Path, what: &str, left: u64, punching: Option<u64>) -> Result<()> {
        let avail = fsutil::available_space(out)?.saturating_sub(self.keep_free);
        // `left == u64::MAX` marks "remaining output unknown" (skipped scan):
        // clamp it so `next_multiple_of` cannot overflow, then fall back to a
        // safe per-step bound instead of the full-output requirement.
        let unknown = left == u64::MAX;
        let left = if unknown { u64::MAX - 4096 } else { left };
        let left = left.next_multiple_of(4096);
        let need = match punching {
            Some(granule) => (stream_step() + BUF_SIZE as u64 + 2 * granule).min(left),
            None if unknown => stream_step() + 2 * BUF_SIZE as u64,
            None => left,
        };
        if need > avail {
            bail!(
                "not enough disk space to continue: the next part of {} requires {} and {} is \
                 available ({} reserved).\n{} more is required. Progress is saved; free up space \
                 and resume the extraction.",
                what,
                human(need),
                human(avail),
                human(self.keep_free),
                human(need - avail),
            );
        }
        Ok(())
    }

    /// Pick units starting at `start` whose output fits in the space there is
    /// right now. Always takes at least one, or explains why it can't.
    fn next_batch(&self, start: usize, out: &Path, reclaims: bool) -> Result<usize> {
        let avail = fsutil::available_space(out)?.saturating_sub(self.keep_free);
        let units = &self.job.units;
        let need = on_disk;
        let first = need(&units[start]);
        if first > avail {
            // The upfront check makes this rare: it takes something else on
            // the machine filling the disk while we run.
            let what = match units[start].items.as_slice() {
                [one] => format!("'{}'", one.path.display()),
                many => format!("the next block of {} files", many.len()),
            };
            bail!(
                "not enough disk space to continue: {what} requires {} and {} is available \
                 ({} reserved).\n{} more is required. Progress is saved; free up space and \
                 resume the extraction.",
                human(first),
                human(avail),
                human(self.keep_free),
                human(first - avail),
            );
        }
        let budget = avail.min(BATCH_MAX_BYTES.max(first));
        let max_units = test_knob("HATCHZIP_BATCH_ENTRIES").map_or(BATCH_MAX_UNITS, |n| n as usize);
        let mut used = 0;
        let mut end = start;
        while end < units.len() && end - start < max_units {
            let size = need(&units[end]);
            if end > start && used + size > budget {
                break;
            }
            if end > start && always_stream() && self.job.streams(end, reclaims).is_some() {
                break;
            }
            used += size;
            end += 1;
        }
        Ok(end)
    }

    /// Returns `None` if a stop request interrupted the batch. Nothing in the
    /// archive has been touched at that point, so the batch simply reruns.
    fn extract_batch(&self, batch: &[Unit], out: &Path) -> Result<Option<Vec<Link>>> {
        let threads = threads_per_unit(batch.len());
        let on_bytes = &*self.progress.on_bytes;
        let results: Vec<Result<Vec<Link>>> = batch
            .par_iter()
            .map_init(
                || {
                    let sink = DiskSink {
                        out,
                        stop: self.stop,
                        on_bytes,
                        buf: vec![0; BUF_SIZE],
                        links: Vec::new(),
                    };
                    (self.job.format.worker(threads), sink)
                },
                |(worker, sink), unit| {
                    let w = worker
                        .as_mut()
                        .map_err(|e| anyhow!("opening the archive: {e:#}"))?;
                    w.extract(unit, sink)?;
                    Ok(std::mem::take(&mut sink.links))
                },
            )
            .collect();

        let mut links = Vec::new();
        let mut interrupted = false;
        for r in results {
            match r {
                Ok(l) => links.extend(l),
                Err(e) if is_interrupt(&e) => interrupted = true,
                Err(e) => return Err(e),
            }
        }
        Ok((!interrupted).then_some(links))
    }

    fn finish(self, out: &Path) -> Result<()> {
        make_links(&self.journal.links, out)?;
        let Self { job, paths, .. } = self;
        let Job {
            storage, format, ..
        } = job;
        // The format holds its own handle on the storage; it has to go first.
        drop(format);
        let storage = Arc::try_unwrap(storage)
            .map_err(|_| anyhow!("archive still in use at the end of extraction"))?;
        // What is left is at most a self-extractor stub in front of the
        // first entry. The saved index goes with it.
        storage.remove()?;
        paths.remove()
    }
}

/// A run that crashed after deleting the archive but before cleaning up only
/// has its links and side files left to deal with.
pub fn finish_orphaned(journal: &Journal, paths: &Paths) -> Result<()> {
    make_links(&journal.links, &journal.output)?;
    paths.remove()
}

pub(crate) fn make_links(links: &[Link], out: &Path) -> Result<()> {
    for link in links {
        let path = out.join(&link.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let made = if link.hard {
            let target = out.join(&link.target);
            let _ = fs::remove_file(&path);
            // FAT32 and exFAT have no hard links; a copy is the next best thing.
            fs::hard_link(&target, &path).or_else(|_| fs::copy(&target, &path).map(|_| ()))
        } else {
            platform::create_symlink(&path, &link.target)
        };
        made.with_context(|| format!("creating link '{}'", link.path.display()))?;
    }
    Ok(())
}

struct DiskSink<'a> {
    out: &'a Path,
    stop: &'a AtomicBool,
    on_bytes: &'a (dyn Fn(u64) + Sync),
    buf: Vec<u8>,
    links: Vec<Link>,
}

impl Sink for DiskSink<'_> {
    fn item(&mut self, item: &Item, data: &mut dyn Read) -> Result<()> {
        self.write(item, data)
            .with_context(|| format!("extracting '{}'", item.path.display()))
    }
}

impl DiskSink<'_> {
    fn write(&mut self, item: &Item, data: &mut dyn Read) -> Result<()> {
        let dest = self.out.join(&item.path);
        match &item.kind {
            Kind::Dir => {
                fs::create_dir_all(&dest)?;
                return Ok(());
            }
            Kind::Symlink => {
                let mut target = Vec::new();
                data.take(MAX_SYMLINK).read_to_end(&mut target)?;
                let target = PathBuf::from(String::from_utf8(target)?);
                self.links.push(Link {
                    path: item.path.clone(),
                    target,
                    hard: false,
                });
                return Ok(());
            }
            Kind::HardLink(target) => {
                self.links.push(Link {
                    path: item.path.clone(),
                    target: target.clone(),
                    hard: true,
                });
                return Ok(());
            }
            Kind::File => {}
        }

        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        let part = part_path(&dest);
        let result = self.copy(item, data, &part);
        if result.is_err() {
            let _ = fs::remove_file(&part);
            return result;
        }
        fs::rename(&part, &dest)?;
        Ok(())
    }

    fn copy(&mut self, item: &Item, data: &mut dyn Read, part: &Path) -> Result<()> {
        let mut dst = File::create(part)?;
        let mut written = 0u64;
        loop {
            if self.stop.load(Ordering::Relaxed) {
                return Err(io::Error::from(io::ErrorKind::Interrupted).into());
            }
            // Decoders check their CRC when the stream ends, so a read error
            // here is how corruption surfaces.
            let n = data.read(&mut self.buf)?;
            if n == 0 {
                break;
            }
            written += n as u64;
            if written > item.size {
                bail!("it is larger than the archive records");
            }
            dst.write_all(&self.buf[..n])?;
            (self.on_bytes)(n as u64);
        }
        if written != item.size {
            bail!("it is smaller than the archive records");
        }
        if let Some(t) = item.mtime {
            dst.set_modified(t)?;
        }
        platform::apply_mode(&dst, item.mode)?;
        if platform::SYNC_EACH_FILE {
            dst.sync_all()?;
        }
        Ok(())
    }
}

/// Reads at most `left` bytes, then stops the decoder with [`ProbeDone`].
struct ProbeSink {
    left: u64,
    buf: Vec<u8>,
}

#[derive(Debug)]
struct ProbeDone;

impl std::fmt::Display for ProbeDone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("read enough to prove the password")
    }
}

impl std::error::Error for ProbeDone {}

impl Sink for ProbeSink {
    fn item(&mut self, _item: &Item, data: &mut dyn Read) -> Result<()> {
        loop {
            let n = data.read(&mut self.buf)?;
            if n == 0 {
                return Ok(());
            }
            self.left = self.left.saturating_sub(n as u64);
            if self.left == 0 {
                return Err(ProbeDone.into());
            }
        }
    }
}

struct VerifySink<'a> {
    on_bytes: &'a (dyn Fn(u64) + Sync),
    buf: Vec<u8>,
}

impl Sink for VerifySink<'_> {
    fn item(&mut self, item: &Item, data: &mut dyn Read) -> Result<()> {
        let mut read = 0;
        loop {
            let n = data
                .read(&mut self.buf)
                .with_context(|| format!("'{}'", item.path.display()))?;
            if n == 0 {
                break;
            }
            read += n as u64;
            (self.on_bytes)(n as u64);
        }
        if item.kind == Kind::File && read != item.size {
            bail!(
                "'{}' is shorter than the archive records",
                item.path.display()
            );
        }
        Ok(())
    }
}

fn is_interrupt(e: &anyhow::Error) -> bool {
    e.chain()
        .filter_map(|c| c.downcast_ref::<io::Error>())
        .any(|io| io.kind() == io::ErrorKind::Interrupted)
}

/// Settings only the test suite uses. Release builds ignore them.
fn test_knob(name: &str) -> Option<u32> {
    if !cfg!(debug_assertions) {
        return None;
    }
    std::env::var(name).ok()?.parse().ok()
}

/// Lets the test suite kill the process at the two moments that matter for
/// crash safety: after a batch is on disk, and after it is journaled.
pub(crate) fn crash_point(name: &str) {
    if !cfg!(debug_assertions) || std::env::var("HATCHZIP_CRASH_AT").map_or(true, |v| v != name) {
        return;
    }
    static PASSED: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    if PASSED.fetch_add(1, Ordering::Relaxed) >= test_knob("HATCHZIP_CRASH_AFTER").unwrap_or(0) {
        std::process::abort();
    }
}

pub fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1000.0 && unit < UNITS.len() - 1 {
        v /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}
