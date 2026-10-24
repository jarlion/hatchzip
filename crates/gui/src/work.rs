//! What runs on the worker thread: the same steps as `hatchzip extract` and
//! `hatchzip test`, reporting through [`Update`]s instead of printing.
//!
//! Byte counts go through atomics that the UI samples on a timer, so a fast
//! extraction never floods the event loop with one message per write.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use hatchzip_core::archive::{PasswordNeeded, WrongPassword};
use hatchzip_core::engine::{self, Job, Outcome, Progress, Run};
use hatchzip_core::journal::Paths;
use hatchzip_core::session::{self, SpacePlan, State};

#[derive(Default)]
pub struct Counters {
    pub done: AtomicU64,
    pub total: AtomicU64,
    /// What is left of the archive on disk; zero while only testing.
    pub archive: AtomicU64,
    pub stop: AtomicBool,
}

impl Counters {
    pub fn reset(&self) {
        self.done.store(0, Ordering::Relaxed);
        self.total.store(0, Ordering::Relaxed);
        self.archive.store(0, Ordering::Relaxed);
        self.stop.store(false, Ordering::Relaxed);
    }
}

/// No `Debug`: a request can carry a password.
#[derive(Clone)]
pub enum Request {
    Extract {
        archive: PathBuf,
        /// `None` keeps what the archive's journal or name says.
        output: Option<PathBuf>,
        keep_free: u64,
        verify_first: bool,
        /// When true, skip the pre-flight space check and start extracting
        /// immediately; the runtime guards still pause on a full disk.
        no_space_check: bool,
        password: Option<String>,
    },
    Test {
        archive: PathBuf,
        password: Option<String>,
    },
    /// Stops for good: turns a half-extracted archive back into a normal one.
    Restore { archive: PathBuf },
}

impl Request {
    pub fn archive(&self) -> &Path {
        match self {
            Self::Extract { archive, .. }
            | Self::Test { archive, .. }
            | Self::Restore { archive } => archive,
        }
    }

    pub fn password(&self) -> Option<&str> {
        match self {
            Self::Extract { password, .. } | Self::Test { password, .. } => password.as_deref(),
            Self::Restore { .. } => None,
        }
    }

    /// Remembers a password that worked, so a rerun after a pause doesn't
    /// ask again. It lives only in this process.
    pub fn set_password(&mut self, p: String) {
        match self {
            Self::Extract { password, .. } | Self::Test { password, .. } => *password = Some(p),
            Self::Restore { .. } => {}
        }
    }
}

/// Asks the person for a password; the flag says the last one was wrong.
/// `None` means they gave up.
pub type Ask<'a> = &'a mut dyn FnMut(bool) -> Option<String>;

pub enum Update {
    /// Opening the archive and checking the space.
    Checking,
    /// A password the person typed opened the archive.
    Unlocked(String),
    /// It fits; shown while the run starts.
    Planned(SpacePlan),
    NoSpace(SpacePlan),
    /// Reading every file to check it; nothing is written.
    Verifying,
    /// Extraction has started, into this folder.
    Working(PathBuf),
    Paused,
    Extracted(PathBuf),
    Tested(usize),
    /// The archive is whole again, holding this many entries; the ones
    /// already extracted stay in the folder.
    Restored {
        entries: usize,
        output: PathBuf,
    },
    Failed(Failure),
}

pub struct Failure {
    pub message: String,
    /// A journal exists, so running again continues instead of starting over.
    pub resumable: bool,
}

/// Runs a request to the end, a pause or an error. Errors are reported as an
/// update, never returned, so the UI hears about every way this ends.
pub fn run(request: &Request, counters: &Arc<Counters>, send: &dyn Fn(Update), ask: Ask) {
    let result = match request {
        Request::Extract {
            archive,
            output,
            keep_free,
            verify_first,
            password,
            no_space_check,
        } => extract(
            archive,
            output.clone(),
            *keep_free,
            *verify_first,
            *no_space_check,
            Secret {
                password: password.clone(),
                ask,
            },
            counters,
            send,
        ),
        Request::Test { archive, password } => test(
            archive,
            Secret {
                password: password.clone(),
                ask,
            },
            counters,
            send,
        ),
        Request::Restore { archive } => restore(archive, send),
    };
    if let Err(e) = result {
        let resumable = matches!(request, Request::Extract { .. })
            && session::locate(request.archive()).is_ok_and(|(_, paths)| paths.exists());
        let message = if e.is::<PasswordNeeded>() || e.is::<WrongPassword>() {
            // Only reached when the person closed the password prompt.
            match request {
                Request::Test { .. } => "Test cancelled. The archive was not modified.",
                _ => "Extraction cancelled. The archive was not modified.",
            }
            .to_string()
        } else {
            sentence(&format!("{e:#}"))
        };
        send(Update::Failed(Failure { message, resumable }));
    }
}

struct Secret<'a> {
    password: Option<String>,
    ask: Ask<'a>,
}

/// `session::open_encrypted`, reporting a password the person typed so a
/// rerun can use it.
fn open(
    primary: &Path,
    paths: &Paths,
    output: Option<PathBuf>,
    writable: bool,
    secret: Secret,
    scan: bool,
    send: &dyn Fn(Update),
) -> Result<State> {
    let given = secret.password.clone();
    let (state, used) = session::open_encrypted(
        primary,
        paths,
        output,
        writable,
        secret.password,
        secret.ask,
        scan,
    )?;
    if let Some(p) = used
        && Some(&p) != given.as_ref()
    {
        send(Update::Unlocked(p));
    }
    Ok(state)
}

fn extract(
    archive: &Path,
    output: Option<PathBuf>,
    keep_free: u64,
    verify_first: bool,
    no_space_check: bool,
    secret: Secret,
    counters: &Arc<Counters>,
    send: &dyn Fn(Update),
) -> Result<()> {
    send(Update::Checking);
    let (primary, paths) = session::locate(archive)?;
    let (job, journal) = match open(&primary, &paths, output, true, secret, !no_space_check, send)? {
        State::Orphan(journal) => {
            engine::finish_orphaned(&journal, &paths)?;
            send(Update::Extracted(journal.output));
            return Ok(());
        }
        State::Resume(job, journal) => {
            if !no_space_check {
                let plan = session::plan(&job, &journal.output, true, keep_free)?;
                if !plan.fits {
                    send(Update::NoSpace(plan));
                    return Ok(());
                }
                send(Update::Planned(plan));
            }
            session::clear_uncommitted(&job, &journal.output)?;
            (job, journal)
        }
        State::Fresh(job, out) => {
            if !no_space_check {
                let plan = session::plan(&job, &out, false, keep_free)?;
                if !plan.fits {
                    send(Update::NoSpace(plan));
                    return Ok(());
                }
                send(Update::Planned(plan));
            }
            if verify_first {
                verify(&job, counters, send)
                    .context("the archive is damaged; no files were extracted")?;
                if counters.stop.load(Ordering::Relaxed) {
                    send(Update::Paused);
                    return Ok(());
                }
            }
            let journal = session::begin(&job, &paths, out)?;
            (job, journal)
        }
    };

    let output = journal.output.clone();
    counters.total.store(journal.total_size, Ordering::Relaxed);
    counters
        .done
        .store(journal.total_size - job.remaining_size(), Ordering::Relaxed);
    counters
        .archive
        .store(journal.committed_len, Ordering::Relaxed);
    send(Update::Working(output.clone()));

    let (c1, c2) = (counters.clone(), counters.clone());
    let run = Run {
        job,
        journal,
        paths,
        stop: &counters.stop,
        progress: Progress {
            on_bytes: Box::new(move |n| {
                c1.done.fetch_add(n, Ordering::Relaxed);
            }),
            on_commit: Box::new(move |len| c2.archive.store(len, Ordering::Relaxed)),
        },
        keep_free,
    };
    let (outcome, _entries) = run.execute()?;
    match outcome {
        Outcome::Finished => {
            counters.archive.store(0, Ordering::Relaxed);
            send(Update::Extracted(output));
        }
        Outcome::Paused => send(Update::Paused),
    }
    Ok(())
}

fn test(
    archive: &Path,
    secret: Secret,
    counters: &Arc<Counters>,
    send: &dyn Fn(Update),
) -> Result<()> {
    send(Update::Checking);
    let (primary, paths) = session::locate(archive)?;
    let job = match open(&primary, &paths, None, false, secret, true, send)? {
        State::Fresh(job, _) => job,
        _ => bail!(
            "this archive is partially extracted and cannot be tested. Resume or restore it first"
        ),
    };
    verify(&job, counters, send).context("the archive is damaged")?;
    send(Update::Tested(job.item_count()));
    Ok(())
}

fn restore(archive: &Path, send: &dyn Fn(Update)) -> Result<()> {
    send(Update::Checking);
    let r = hatchzip_core::restore::restore(archive)?;
    send(Update::Restored {
        entries: r.entries,
        output: r.output,
    });
    Ok(())
}

fn verify(job: &Job, counters: &Arc<Counters>, send: &dyn Fn(Update)) -> Result<()> {
    counters
        .total
        .store(job.remaining_size(), Ordering::Relaxed);
    counters.done.store(0, Ordering::Relaxed);
    send(Update::Verifying);
    job.verify(&|n| {
        counters.done.fetch_add(n, Ordering::Relaxed);
    })
}

/// The engine's errors are written to follow "error:" on a terminal; on
/// their own they read better as a sentence.
pub fn sentence(message: &str) -> String {
    let mut chars = message.trim_end().chars();
    let mut out: String = match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => return String::new(),
    };
    if out.ends_with(|c: char| c.is_alphanumeric() || c == ')') {
        out.push('.');
    }
    out
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn errors_read_as_sentences() {
        assert_eq!(sentence("not a zip"), "Not a zip.");
        assert_eq!(
            sentence("pick another folder; or not."),
            "Pick another folder; or not."
        );
        assert_eq!(sentence(""), "");
    }

    fn make_zip(dir: &Path, password: Option<&str>) -> PathBuf {
        let path = dir.join("game.zip");
        let mut w = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
        let mut opts = zip::write::SimpleFileOptions::default();
        if let Some(p) = password {
            opts = opts.with_aes_encryption(zip::AesMode::Aes256, p);
        }
        w.add_directory("data/", zip::write::SimpleFileOptions::default())
            .unwrap();
        for i in 0..20 {
            w.start_file(format!("data/file{i}.bin"), opts).unwrap();
            w.write_all(&vec![i as u8; 10_000 + i * 100]).unwrap();
        }
        w.finish().unwrap();
        path
    }

    fn name(u: Update) -> String {
        match u {
            Update::Checking => "checking".to_string(),
            Update::Unlocked(p) => format!("unlocked {p}"),
            Update::Planned(_) => "planned".to_string(),
            Update::NoSpace(_) => "no space".to_string(),
            Update::Verifying => "verifying".to_string(),
            Update::Working(_) => "working".to_string(),
            Update::Paused => "paused".to_string(),
            Update::Extracted(_) => "extracted".to_string(),
            Update::Tested(n) => format!("tested {n}"),
            Update::Restored { entries, .. } => format!("restored {entries}"),
            Update::Failed(f) => format!("failed: {}", f.message),
        }
    }

    #[test]
    fn a_paused_run_gives_the_archive_back() {
        let dir = tempfile::tempdir().unwrap();
        let archive = make_zip(dir.path(), None);
        let counters = Arc::new(Counters::default());
        // Paused before the first batch: the journal exists, so the archive
        // counts as half extracted.
        let request = extract(&archive, Some(dir.path().join("out")), false);
        counters.stop.store(true, Ordering::Relaxed);
        collect(&request, &counters);
        assert!(session::locate(&archive).unwrap().1.exists());

        let restore = Request::Restore {
            archive: archive.clone(),
        };
        assert_eq!(collect(&restore, &counters), ["checking", "restored 21"]);
        assert!(!session::locate(&archive).unwrap().1.exists());
        // A normal archive again: testing it needs no journal.
        counters.reset();
        let test = Request::Test {
            archive,
            password: None,
        };
        assert_eq!(
            collect(&test, &counters),
            ["checking", "verifying", "tested 21"]
        );
    }

    /// Runs `request`, answering password prompts from `answers` in order
    /// and recording whether each prompt said the last one was wrong.
    fn collect_asking(
        request: &Request,
        counters: &Arc<Counters>,
        answers: &[Option<&str>],
    ) -> (Vec<String>, Vec<bool>) {
        let seen = Mutex::new(Vec::new());
        let mut asked = Vec::new();
        let mut answers = answers.iter();
        run(
            request,
            counters,
            &|u| seen.lock().unwrap().push(name(u)),
            &mut |wrong| {
                asked.push(wrong);
                answers.next().copied().flatten().map(str::to_string)
            },
        );
        (seen.into_inner().unwrap(), asked)
    }

    fn collect(request: &Request, counters: &Arc<Counters>) -> Vec<String> {
        let (updates, asked) = collect_asking(request, counters, &[]);
        assert!(asked.is_empty(), "a plain archive never asks");
        updates
    }

    fn extract(archive: &Path, out: Option<PathBuf>, verify_first: bool) -> Request {
        Request::Extract {
            archive: archive.to_path_buf(),
            output: out,
            keep_free: 0,
            verify_first,
            no_space_check: false,
            password: None,
        }
    }

    #[test]
    fn test_then_extract_a_real_archive() {
        let dir = tempfile::tempdir().unwrap();
        let archive = make_zip(dir.path(), None);
        let counters = Arc::new(Counters::default());

        let updates = collect(
            &Request::Test {
                archive: archive.clone(),
                password: None,
            },
            &counters,
        );
        assert_eq!(updates, ["checking", "verifying", "tested 21"]);
        assert_eq!(
            counters.done.load(Ordering::Relaxed),
            counters.total.load(Ordering::Relaxed)
        );

        counters.reset();
        let out = dir.path().join("out");
        let updates = collect(&extract(&archive, Some(out.clone()), true), &counters);
        assert_eq!(
            updates,
            ["checking", "planned", "verifying", "working", "extracted"]
        );
        assert!(!archive.exists());
        assert_eq!(
            std::fs::read(out.join("data/file3.bin")).unwrap(),
            vec![3u8; 10_300]
        );
        assert_eq!(
            counters.done.load(Ordering::Relaxed),
            counters.total.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn extracts_next_to_other_files_but_never_over_them() {
        let dir = tempfile::tempdir().unwrap();
        let archive = make_zip(dir.path(), None);
        std::fs::write(dir.path().join("notes.txt"), b"mine").unwrap();
        let counters = Arc::new(Counters::default());
        // The folder holds the archive and a file of its own: extract here.
        let here = extract(&archive, Some(dir.path().to_path_buf()), false);
        assert_eq!(
            collect(&here, &counters),
            ["checking", "planned", "working", "extracted"]
        );
        assert_eq!(
            std::fs::read(dir.path().join("notes.txt")).unwrap(),
            b"mine"
        );
        assert!(dir.path().join("data/file19.bin").exists());

        // Again, now every file would land on one that is there.
        let again = make_zip(dir.path(), None);
        counters.reset();
        let updates = collect(
            &extract(&again, Some(dir.path().to_path_buf()), false),
            &counters,
        );
        assert_eq!(updates.len(), 2, "{updates:?}");
        assert!(updates[1].starts_with("failed: "), "{updates:?}");
        assert!(again.exists(), "a refused run leaves the archive alone");
    }

    #[test]
    fn a_stop_before_the_first_batch_pauses_and_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let archive = make_zip(dir.path(), None);
        let out = dir.path().join("out");
        let counters = Arc::new(Counters::default());
        let request = extract(&archive, Some(out.clone()), false);

        counters.stop.store(true, Ordering::Relaxed);
        assert_eq!(
            collect(&request, &counters),
            ["checking", "planned", "working", "paused"]
        );
        // Paused before anything was truncated: the journal is there to
        // continue from.
        assert!(archive.exists());
        assert!(session::locate(&archive).unwrap().1.exists());

        counters.reset();
        assert_eq!(
            collect(&request, &counters),
            ["checking", "planned", "working", "extracted"]
        );
        assert!(!archive.exists());
        assert_eq!(std::fs::read_dir(out.join("data")).unwrap().count(), 20);
    }

    #[test]
    fn not_enough_space_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let archive = make_zip(dir.path(), None);
        let counters = Arc::new(Counters::default());
        let request = Request::Extract {
            archive: archive.clone(),
            output: None,
            keep_free: u64::MAX / 4,
            verify_first: false,
            no_space_check: false,
            password: None,
        };
        assert_eq!(collect(&request, &counters), ["checking", "no space"]);
        assert!(!session::locate(&archive).unwrap().1.exists());
    }

    #[test]
    fn failures_say_what_happened() {
        let dir = tempfile::tempdir().unwrap();
        let bogus = dir.path().join("notes.zip");
        std::fs::write(&bogus, b"not an archive").unwrap();
        let counters = Arc::new(Counters::default());
        let request = Request::Test {
            archive: bogus,
            password: None,
        };
        let updates = collect(&request, &counters);
        assert_eq!(updates.len(), 2);
        assert!(updates[1].starts_with("failed: "), "{updates:?}");
    }

    #[test]
    fn asks_for_a_password_until_it_is_right() {
        let dir = tempfile::tempdir().unwrap();
        let archive = make_zip(dir.path(), Some("hunter2"));
        let counters = Arc::new(Counters::default());
        let test = Request::Test {
            archive: archive.clone(),
            password: None,
        };
        let (updates, asked) = collect_asking(&test, &counters, &[Some("nope"), Some("hunter2")]);
        assert_eq!(asked, [false, true], "first needed, then wrong");
        assert_eq!(
            updates,
            ["checking", "unlocked hunter2", "verifying", "tested 21"]
        );

        // Closing the prompt gives up without touching anything.
        let mut request = extract(&archive, Some(dir.path().join("out")), false);
        let (updates, _) = collect_asking(&request, &counters, &[None]);
        assert_eq!(
            updates,
            [
                "checking",
                "failed: Extraction cancelled. The archive was not modified."
            ]
        );
        assert!(archive.exists());

        // A remembered password isn't asked for again.
        request.set_password("hunter2".into());
        counters.reset();
        let (updates, asked) = collect_asking(&request, &counters, &[]);
        assert!(asked.is_empty());
        assert_eq!(updates, ["checking", "planned", "working", "extracted"]);
        assert_eq!(
            std::fs::read(dir.path().join("out/data/file7.bin")).unwrap(),
            vec![7u8; 10_700]
        );
    }
}
