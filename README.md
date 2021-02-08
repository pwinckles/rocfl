# rocfl

![build](https://github.com/pwinckles/rocfl/workflows/build/badge.svg)

`rocfl` is a command line utility for interrogating
[OCFL](https://ocfl.io/) repositories. It is able to interact with
OCFL repositories on the local filesystem or in S3, but only supports
read operations.

You can either download a pre-built binary from the [releases
page](https://github.com/pwinckles/rocfl/releases), or build your own
copy locally.

### Local Build

1. Install [Rust](https://www.rust-lang.org/tools/install), and make
   sure `cargo` is on your `PATH`
1. Execute: `cargo install rocfl`
1. Verify the install: `rocfl help`

If you want to build a binary that does not include the code for
integrating with S3, which adds a large number of dependencies, then
you can do so by running: `cargo install rocfl --no-default-features`.

## Usage

`rocfl` is intended to be run from within an OCFL repository's storage
root. It can be run outside of a storage root by specifying the
repository root using the `--root` option.

Listing all of the objects in large repositories is a slow operation
because the entire repository must be crawled to identify objects.
However, listing the contents of individual objects is fast, so long
as the OCFL repository contains an
[ocfl_layout.json](https://ocfl.io/1.0/spec/#root-structure) file that
defines the layout of the repository using an implemented [OCFL
storage layout extension](https://ocfl.github.io/extensions/). `rocfl`
supports the following layout extensions:

- [0002-flat-direct-storage-layout](https://ocfl.github.io/extensions/0002-flat-direct-storage-layout.html)
- [0003-hash-and-id-n-tuple-storage-layout](https://ocfl.github.io/extensions/0003-hash-and-id-n-tuple-storage-layout.html)
- [0004-hashed-n-tuple-storage-layout](https://ocfl.github.io/extensions/0004-hashed-n-tuple-storage-layout.html)

If a repository does not define a storage layout, or it uses an
unimplemented layout, then `rocfl` must scan the repository to locate
a request object. While the scan does take significantly longer than
accessing the object directly, it is still fairly fast in small to
medium sized repositories.

The following is an overview of the features that `rocfl` supports.
For a detailed description of all of the options available, consult
the builtin help by executing `rocfl help` or `rocfl help <COMMAND>`.

### List

The `ls` operation can be used to either list all of the objects in a
repository or list all of the files in an OCFL object. When listing
files, only files in the HEAD object state are returned. Previous
versions can be queried with the `-v` option.

#### Examples

##### Listing Objects

The following command lists all of the object IDs in a repository
that's rooted in the current working directory:

```console
rocfl ls
```

This lists the same objects but with additional details, current
version and updated date:

```console
rocfl ls -l
```

Adding the `-p` flag additionally provides the path from the storage
root to the object:

```console
rocfl ls -lp
```

A subset of objects can be listed by providing a glob pattern to match
on:

```console
rocfl ls -lo foo*
```

##### Listing Object Contents

The contents of an object's current state are displayed by invoking
`ls` on a specific object ID:

```console
rocfl ls foobar
```

With the `-l` flag, additional details are displayed. In this case,
the version and date indicate when the individual file was last
updated:

```console
rocfl ls -l foobar
```

The `-p` flag can also be used here to display the paths to the
physical files on disk relative the storage root:

```console
rocfl ls -p foobar
```

The contents of previous versions are displayed by using the `-v`
option. The following command displays the files that were in the
first version of the object:

```console
rocfl ls -v1 foobar
```

An object's contents can be filtered by specifying a glob pattern to
match on:

```console
rocfl ls foobar '*.txt'
```

The output is sorted by name by default, but can also be sorted
version or updated date:

```console
rocfl ls -lsversion foobar
```

### Log

The `log` operation displays the version metadata for all versions of
an object. It can also be executed on a file within an object, in
which case only versions that affected the specified file are
displayed.

#### Examples

Show all of the versions of an object in ascending order:

```console
rocfl log foobar
```

Only display the five most recent versions:

```console
rocfl log -rn5 foobar
```

Show all of the versions, but formatted so each version is on a single
line:

```console
rocfl log -c foobar
```

Show all of the versions that affected a specific file:

```console
rocfl log foobar file1.txt
```

### Show

The `show` operation displays everything that changed in an object
within a specific version. If no version is specified, the most recent
changes are shown.

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

The `diff` operation displays the files that changed between two
specific versions.

#### Example

Show the changes between the second and fourth versions:

```console
rocfl diff v2 v4
```

### Cat

The `cat` operation writes the contents of a file to `stdout`.

#### Examples

Display the contents of the head version of a file:

```console
rocfl cat foobar file1.txt
```

Display the contents of a file from a specific version of the object:

```console
rocfl cat -v1 foobar file1.txt
```

## S3

In order to interrogate a repository located in S3, you first need to
create an IAM user with access to S3, and then setup a local
`~/.aws/credentials` file as [described
here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).
Then, when you invoke `rocfl` you must specify the bucket the
repository is in as well as the bucket region. For example:

```console
rocfl -R us-east-2 -b example-ocfl-repo ls
```

You can specify a sub directory, or prefix, that the repository is
rooted in within the bucket like this:

```console
rocfl -R us-east-2 -b example-ocfl-repo -r ocfl-root ls
```
