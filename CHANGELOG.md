# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `validate` command for validating repositories and objects 
- Disable output styling when there isn't a TTY
- Custom deserialization implementation to reduce memory usage

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
