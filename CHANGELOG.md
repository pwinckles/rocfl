# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

### Changed

- Invalid `fixity` block validation code changed to `E111`
- Inconsistent object id validation code changed to `E110`
- Invalid `manifest` block validation code changed to `E106`
- Invalid version number validation code changed to `E104`

### Added

- Validate object spec versions are monotonically increasing
- Validate only one object/root version declaration
- Validate all manifest entries are used at least once
- `info` command for display object/repo details
- Create new objects/repos using OCFL `1.0` or `1.1`
- `upgrade` command for upgrading the spec version of objects/repos

### Fixed

- Resetting staged updates now correctly removes the reset file from
  the manifest

## [1.6.6] - 2022-03-01

### Fixed

- Ensure `null` fields are never serialized
- Fix bug where the incorrect object storage path was being calculated
  when updating objects in S3

## [1.6.5] - 2022-01-28

### Fixed

- Corrected how the max zero-padded version is calculated

## [1.6.4] - 2022-01-24

### Changed

- Updated dependencies

### Fixed

- Corrected print order of `show` command
- Corrected argument value names, which fixes a panic that was
  introduced in the `show` command

## [1.6.3] - 2022-01-03

### Added

- Support for layout extension [0007-n-tuple-omit-prefix-storage-layout](https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.html)

### Changed

- Upgraded to [digest](https://crates.io/crates/digest) `0.10.0`, which
  now allows for blake2b to be supported the same as the other
  algorithms
- Upgraded to [clap](https://crates.io/crates/clap) `3.0.0`

## [1.6.2] - 2021-11-09

### Added

- Validation warnings and errors can now be suppressed using
  `--suppress-warning` and `--suppress-error`

### Changed

- `validate` now uses `--level` to determine what is printed

## [1.6.1] - 2021-10-21

### Fixed

- Purge confirmation was not being displayed

## [1.6.0] - 2021-10-12

### Added

- `validate` command for validating repositories and objects 
- Disable output styling when there isn't a TTY
- Custom deserialization implementation to reduce memory usage

### Changed

- `ls` now does _not_ sort object listings by default, and will
  immediately display results so long as they do not need to be
  displayed in a table, that is `-l` or `-p` are not specified or `-t`
  is specified
- `ls` now returns `1` if it encounters any errors while listing objects

## [1.5.2] - 2021-07-15

### Added

- AWS [credential
  profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
  support though the `-p` option
  
### Fixed

- Objects were not listed correctly in OCFL repositories in S3 where
  the repository root was in the root of the bucket
  
## [1.5.1] - 2021-07-14

### Added

- Support for storage layout extension
  [0006-flat-omit-prefix-storage-layout](https://ocfl.github.io/extensions/0006-flat-omit-prefix-storage-layout.html)

## [1.5.0] - 2021-04-08

### Added

- `ls` supports `-D`that makes it interpret logical path parts as
  directories
- `config` command for setting up a `rocfl` config file
- `init` command for creating new OCFL repositories
- `new` command for creating new OCFL objects
- `cp` command for copying files into objects
- `mv` command for moving files into objects
- `rm` command for removing files from objects
- `reset` command for undoing staged changes
- `status` command for displaying staged changes
- `commit` command for committing changes to an object
- `purge` command for permanently deleting objects

## [1.4.0] - 2021-02-08

### Added

- [Storage layout extension](https://ocfl.github.io/extensions/)
  support
- Verbose logging with the `-V` flag
- A build that does not include the S3 dependencies

### Fixed

- A bug `cat`ting files when the repository root is not the current
  directory

## [1.3.3] - 2020-10-20

### Changed

- Modified dependencies to produce a more portable linux build

## [1.3.2] - 2020-10-20

### Changed

- Use rustls instead of openssl

## [1.3.1] - 2020-08-06

### Fixed

- A bug `cat`ting files from S3

## [1.3.0] - 2020-08-05

### Added

- `cat` command for printing files to stdout

## [1.2.0] - 2020-08-04

### Changed

- Improved output table formatting

## [1.1.2] - 2020-07-27

### Changed

- S3 client library

## [1.1.1] - 2020-07-27

### Fixed

- `clippy` warnings

## [1.1.0] - 2020-07-27

### Added

- Support for S3 based repositories

## [1.0.3] - 2020-07-24

### Changed

- `log` output formatting

## [1.0.2] - 2020-07-24

### Fixed

- Windows paths

## [1.0.1] - 2020-07-21

### Fixed

- Pipe interrupt no longer causes an error

## [1.0.0] - 2020-07-21

### Added

- Initial release

<!-- next-url -->
[Unreleased]: https://github.com/pwinckles/rocfl/compare/v1.6.6...HEAD
[1.6.6]: https://github.com/pwinckles/rocfl/compare/v1.6.5...v1.6.6
[1.6.5]: https://github.com/pwinckles/rocfl/compare/v1.6.4...v1.6.5
[1.6.4]: https://github.com/pwinckles/rocfl/compare/v1.6.3...v1.6.4
[1.6.3]: https://github.com/pwinckles/rocfl/compare/v1.6.2...v1.6.3
[1.6.2]: https://github.com/pwinckles/rocfl/compare/v1.6.1...v1.6.2
[1.6.1]: https://github.com/pwinckles/rocfl/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/pwinckles/rocfl/compare/v1.5.2...v1.6.0
[1.5.2]: https://github.com/pwinckles/rocfl/compare/v1.5.1...v1.5.2
[1.5.1]: https://github.com/pwinckles/rocfl/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/pwinckles/rocfl/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/pwinckles/rocfl/compare/v1.3.1...v1.4.0
[1.3.3]: https://github.com/pwinckles/rocfl/compare/v1.3.1...v1.3.3
[1.3.2]: https://github.com/pwinckles/rocfl/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/pwinckles/rocfl/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/pwinckles/rocfl/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/pwinckles/rocfl/compare/v1.1.2...v1.2.0
[1.1.2]: https://github.com/pwinckles/rocfl/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/pwinckles/rocfl/compare/v1.1.1...v1.1.1
[1.1.0]: https://github.com/pwinckles/rocfl/compare/v1.0.3...v1.1.0
[1.0.3]: https://github.com/pwinckles/rocfl/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/pwinckles/rocfl/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/pwinckles/rocfl/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/pwinckles/rocfl/releases/tag/v1.0.0
