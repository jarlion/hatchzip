# Changelog

All notable changes to HatchZip are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and the project adheres to
[Semantic Versioning](https://semver.org/).

## [0.2.0]

### Added
- `--no-space-check` for the CLI and an **Extract here (skip check)** option in
  the GUI and right-click menu. It skips the pre-extraction space gate, so
  HatchZip starts extracting at once and simply pauses if the disk fills.
- For `.tar.gz` / `.tgz`, when the space check is skipped the compressed stream
  is now decoded **only once**: the measuring scan that the space plan needed is
  merged into the single extraction pass. This roughly halves the wait before
  extraction begins on a large compressed tar. Disk-full protection during
  extraction is still enforced step by step.

### Fixed
- In `--no-space-check` mode on a compressed tar, the final "N entries
  extracted" count and the per-batch directory fsync on resume are now correct:
  the entry list is rebuilt as the one stream is decoded, instead of being left
  empty by the skipped scan.

### Changed
- Bumped the version to `0.2.0` so builds are easy to tell apart. The installer
  and release artifacts pick this up automatically from `hatchzip --version`.
- The progress total is published as soon as the one decode pass has listed
  every entry, so a skipped-check extraction of a compressed tar no longer
  reports an unknown (zero) total while it runs.
