# rocfl

`rocfl` is a command line utility for working with [OCFL](https://ocfl.io/) repositories.
It currently only supports read operations.

## Installation

1. Install [Rust](https://www.rust-lang.org/tools/install), and make sure `cargo` is on your `PATH`
1. Execute: `cargo install rocfl`
1. Verify the install: `rocfl help`

Theoretically `rocfl` works on Windows, but I haven't tried it.

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

```
rocfl ls
```

This lists the same objects but with additional details, current version and updated date:

```
rocfl ls -l
```

Adding the `-p` flag additionally provides the path from the storage root to the object:

```
rocfl ls -lp
```

A subset of objects can be listed by providing a glob pattern to match on:

```
rocfl ls -lo foo*
```

##### Listing Object Contents

The contents of an object's current state are displayed by invoking `ls` on a specific object ID:

```
rocfl ls foobar
```

With the `-l` flag, additional details are displayed. In this case, the version and date indicate
when the individual file was last updated:

```
rocfl ls -l foobar
```

The `-p` flag can also be used here to display the paths to the physical files on disk:

```
rocfl ls -p foobar
```

The contents of previous versions are displayed by using the `-v` option. The following command
displays the files that were in the first version of the object:

```
rocfl ls -v1 foobar
```

An object's contents can be filtered by specifying a glob pattern to match on:

```
rocfl ls foobar '*.txt'
```

The output is sorted by name by default, but can also be sorted version or updated date:

```
rocfl ls -lsversion foobar
```

### Log

The `log` operation displays the version metadata for all versions of an object. It can also be
executed on a file within an object, in which case only versions that affected the specified
file are displayed.

#### Examples

Show all of the versions of an object in ascending order:

```
rocfl log foobar
```

Only display the five most recent versions:

```
rocfl log -rn5 foobar
```

Show all of the versions, but formatted so each version is on a single line:

```
rocfl log -c foobar
```

Show all of the versions that affected a specific file:

```
rocfl log foobar file1.txt
```

### Show

The `show` operation displays everything that changed in an object within a specific version.
If no version is specified, the most recent changes are shown.

#### Examples

Show the changes in the most recent version:

```
rocfl show foobar
```

Show the changes in the first version:

```
rocfl show foobar v1
```

Don't show the version metadata; only show the files that changed:

```
rocfl show -m foobar
```

### Diff

The `diff` operation displays the files that changed between two specific versions.

#### Example

Show the changes between the second and fourth versions:

```
rocfl diff v2 v4
```
