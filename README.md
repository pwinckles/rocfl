# rocfl

[![Build Status](https://travis-ci.com/pwinckles/rocfl.svg?branch=master)](https://travis-ci.com/pwinckles/rocfl)

`rocfl` is a command line utility for working with [OCFL](https://ocfl.io/) repositories.
It currently only supports read operations.

`rocfl` is able to interact with OCFL repositories located on the local filesystem or in S3. However, the
S3 support is currently very slow. Listing all of the objects in an S3 repository will always be slow, but
performance when interacting with individual objects will improve once it's possible to navigate directly to
an object rather than having to crawl the entire repository looking for it.

## Installation

You can either download a pre-built binary from the [releases page](https://github.com/pwinckles/rocfl/releases),
or build your own copy locally.

### Local Build

1. Install [Rust](https://www.rust-lang.org/tools/install), and make sure `cargo` is on your `PATH`
1. Execute: `cargo install rocfl`
1. Verify the install: `rocfl help`

## Usage

It is intended to be run from within an OCFL repository's storage root. I can be run outside
of a storage root by specifying the repository root using the `--root` option.

Objects are identified by crawling the directories under the storage root. It does not presently
map object IDs directly to storage directories.

The following is an overview of the features that `rocfl` supports. For a detailed description of
all of the options available, consult the builtin help by executing `rocfl help` or
`rocfl help <COMMAND>`.

### List

The `ls` operation can be used to either list all of the objects in a repository or list all of
the files in an OCFL object. When listing files, only files in the HEAD object state are returned.
Previous versions can be queried with the `-v` option.

#### Examples

##### Listing Objects

The following command lists all of the object IDs in a repository that's rooted in the current
working directory:

```console
rocfl ls
```

This lists the same objects but with additional details, current version and updated date:

```console
rocfl ls -l
```

Adding the `-p` flag additionally provides the path from the storage root to the object:

```console
rocfl ls -lp
```

A subset of objects can be listed by providing a glob pattern to match on:

```console
rocfl ls -lo foo*
```

##### Listing Object Contents

The contents of an object's current state are displayed by invoking `ls` on a specific object ID:

```console
rocfl ls foobar
```

With the `-l` flag, additional details are displayed. In this case, the version and date indicate
when the individual file was last updated:

```console
rocfl ls -l foobar
```

The `-p` flag can also be used here to display the paths to the physical files on disk:

```console
rocfl ls -p foobar
```

The contents of previous versions are displayed by using the `-v` option. The following command
displays the files that were in the first version of the object:

```console
rocfl ls -v1 foobar
```

An object's contents can be filtered by specifying a glob pattern to match on:

```console
rocfl ls foobar '*.txt'
```

The output is sorted by name by default, but can also be sorted version or updated date:

```console
rocfl ls -lsversion foobar
```

### Log

The `log` operation displays the version metadata for all versions of an object. It can also be
executed on a file within an object, in which case only versions that affected the specified
file are displayed.

#### Examples

Show all of the versions of an object in ascending order:

```console
rocfl log foobar
```

Only display the five most recent versions:

```console
rocfl log -rn5 foobar
```

Show all of the versions, but formatted so each version is on a single line:

```console
rocfl log -c foobar
```

Show all of the versions that affected a specific file:

```console
rocfl log foobar file1.txt
```

### Show

The `show` operation displays everything that changed in an object within a specific version.
If no version is specified, the most recent changes are shown.

#### Examples

Show the changes in the most recent version:

```console
rocfl show foobar
```

Show the changes in the first version:

```console
rocfl show foobar v1
```

Don't show the version metadata; only show the files that changed:

```console
rocfl show -m foobar
```

### Diff

The `diff` operation displays the files that changed between two specific versions.

#### Example

Show the changes between the second and fourth versions:

```console
rocfl diff v2 v4
```


## S3

In order to interrogate a repository located in S3, you first need to create an IAM user with access to S3, and then
setup a local `~/.aws/credentials` file as [described here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).
Then, when you invoke `rocfl` you must specify the bucket the repository is in as well as the bucket region. For example:

```console
rocfl -R us-east-2 -b example-ocfl-repo ls
```

You can specify a sub directory, or prefix, that the repository is rooted in within the bucket like this:

```console
rocfl -R us-east-2 -b example-ocfl-repo -r ocfl-root ls
```