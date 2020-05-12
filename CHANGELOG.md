# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2020-05-12
### Added

- Add a flag (`-n / --no-header`) to treat the input as if there is no header row (i.e. to avoid sending the first row to each split / chunk).

### Changed

- Changed the defaults to **not** decompress inputs / compress outputs. This is a breaking change but should be a less surprising default.
- Explicitly add jemalloc as the global allocator.

## [0.3.0] - 2019-09-24
### Added

- Add a flag (`--csv`) to parse input as CSV rather than just treating as newline delimited. This is only really needed if files contain embedded newlines, and will impact performance, so should be used sparingly!
- Add a short flag for uncompressed output (`-U`).

### Fixed

- Allow proportions of 1.0 to be specified.

## [0.2.2] - 2018-11-14
### Fixed

- Fix an off-by-one error when there are unknown total rows.
- Fix a bug where the header wasn't sent to additional chunks.

### Added

- Added examples to README.

## [0.2.1] - 2018-11-09
### Changed

- Updated dependencies ready for first crates.io release.
- Internal crate modifications.

## [0.2.0] - 2018-10-30
### Fixed

- Improve errors if proportion is less than 0.0 or greater than 1.0.

### Changed

- Don't try to infer compression from input.

## [0.1.0] - 2018-10-18
### Added

- First version of the crate.

[Unreleased]: https://github.com/sd2k/ttv/compare/v0.2.2...HEAD
[0.4.0]: https://github.com/sd2k/ttv/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/sd2k/ttv/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/sd2k/ttv/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/sd2k/ttv/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/sd2k/ttv/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/sd2k/ttv/releases/tag/v0.1.0
