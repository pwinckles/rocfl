# OCFL Community Extension 0004: Hashed N-tuple Storage Layout

* **Extension Name:** 0004-hashed-n-tuple-storage-layout
* **Authors:** Peter Winckles
* **Minimum OCFL Version:** 1.0
* **OCFL Community Extensions Version:** 1.0
* **Obsoletes:** n/a
* **Obsoleted by:** n/a

## Overview

This storage root extension describes how to safely map OCFL object identifiers
of any length, containing any characters to OCFL object root directories with
the primary goals of ensuring portability and filesystem performance at the cost
of directory name transparency.

Using this extension, OCFL object identifiers are hashed and encoded
as lowercase hex strings. These digests are then divided into _N_
n-tuple segments, which are used to create nested paths under the OCFL
storage root.

This approach allows OCFL object identifiers of any composition to be evenly
distributed across the storage hierarchy. The maximum number of files under any
given directory is controlled by the number of characters in each n-tuple, and
the tree depth is controlled by the number of n-tuple segments each digest is
divided into. Additionally, it obviates the need to handle special characters in
OCFL object identifiers because the mapped directory names will only ever
contain the characters `0-9a-f`.

However, this comes at the cost of not being able to identify the OCFL object
identifier of an object simply by browsing the OCFL storage hierarchy. The ID of
an object may only be found within its `inventory.json`.

## Parameters

### Summary

* **Name:** `digestAlgorithm`
    * **Description:** The digest algorithm to apply on the OCFL object
      identifier; MUST be an algorithm that is allowed in the OCFL fixity block
    * **Type:** string
    * **Constraints:** Must not be empty
    * **Default:** sha256
* **Name**: `tupleSize`
    * **Description:** Indicates the segment size (in characters) to split the
      digest is split into
    * **Type:** number
    * **Constraints:** An integer between 0 and 32 inclusive
    * **Default:** 3
* **Name:** `numberOfTuples`
    * **Description:** Indicates the number of segments to use for path generation
    * **Type:** number
    * **Constraints:** An integer between 0 and 32 inclusive
    * **Default:** 3
* **Name:** `shortObjectRoot`
    * **Description:** When true, indicates that the OCFL object root directory
      name should contain the remainder of the digest not used in the n-tuple
      segments
    * **Type:** boolean
    * **Default:** false

### Details

#### digestAlgorithm

`digestAlgorithm` is defaulted to `sha256`, and it MUST either contain a digest
algorithm that's [officially supported by the OCFL
specification](https://ocfl.io/1.0/spec/#digest-algorithms) or defined in a community
extension. The specified algorithm is applied to OCFL object identifiers to
produce hex encoded digest values that are then mapped to OCFL object root
paths.

#### tupleSize

`tupleSize` determines the number of digest characters to include in
each tuple. The tuples are used as directory names. The default value
is `3`, which means that each intermediate directory in the OCFL
storage hierarchy could contain up to 4096 sub-directories. Increasing
this value increases the maximum number of sub-directories per
directory.

If `tupleSize` is set to `0`, then no tuples are created and `numberOfTuples`
MUST also equal `0`.

The product of `tupleSize` and `numberOfTuples` MUST be less than or equal to
the number of characters in the hex encoded digest.

#### numberOfTuples

`numberOfTuples` determines how many tuples to create from the digest. The
tuples are used as directory names, and each successive directory is nested
within the previous. The default value is `3`, which means that every OCFL
object root will be 4 directories removed from the OCFL storage root, 3 tuple
directories plus 1 encapsulation directory. Increasing this value increases the
depth of the OCFL storage hierarchy.

If `numberOfTuples` is set to `0`, then no tuples are created and `tupleSize`
MUST also equal `0`.

The product of `numberOfTuples` and `tupleSize` MUST be less than or equal to
the number of characters in the hex encoded digest.

#### shortObjectRoot

The directory that immediately encapsulates an OCFL object MUST either be named
using the entire digest or the remainder of the digest that was not used in a
tuple. When `shortObjectRoot` is set to `false`, the default, the entire digest
is used, and, when it's `true` only the previously unused remainder is used.

If the product of `tupleSize` and `numberOfTuples` is equal to the number of
characters in the hex encoded digest, then `shortObjectRoot` MUST be `false`.

## Procedure

The following is an outline of the steps to map an OCFL object identifier to an
OCFL object root path:

1. The OCFL object identifier, UTF-8 encoded, is hashed using the specified
   `digestAlgorithm`.
2. The digest is encoded as a lowercase hex string.
3. Starting at the beginning of the digest and working forwards, the digest is
   divided into `numberOfTuples` tuples each containing `tupleSize` characters.
4. The tuples are joined, in order, using the filesystem path separator.
5. If `shortObjectRoot` is `true`, the remaining, unused portion of the digest
   is joined on the end of this path. Otherwise, the entire digest is joined on
   the end.

## Examples

### Example 1

This example demonstrates what the OCFL storage hierarchy looks like when using
the default configuration.

#### Parameters

It is not necessary to specify any parameters to use the default configuration.
However, if you were to do so, it would look like the following:

```json
{
    "extensionName": "0004-hashed-n-tuple-storage-layout",
    "digestAlgorithm": "sha256",
    "tupleSize": 3,
    "numberOfTuples": 3,
    "shortObjectRoot": false
}
```

#### Mappings

| Object ID | Digest | Object Root Path |
| --------- | ------ | ---------------- |
| object-01 | 3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4 | `3c0/ff4/240/3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4` |
| ..hor/rib:le-$id | 487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d | `487/326/d8c/487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── extensions/
│   └── 0004-hashed-n-tuple-storage-layout/
│       └── config.json
├── 3c0/
│   └── ff4/
│       └── 240/
│           └── 3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4/
│               ├── 0=ocfl_object_1.0
│               ├── inventory.json
│               ├── inventory.json.sha512
│               └── v1 [...]
└── 487/
    └── 326/
        └── d8c/
            └── 487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d/
                ├── 0=ocfl_object_1.0
                ├── inventory.json
                ├── inventory.json.sha512
                └── v1 [...]
```

### Example 2

This example demonstrates the effects of modifying the default parameters to use
a different `digestAlgoirthm`, smaller `tupleSize`, and a larger
`numberOfTuples`.

#### Parameters

```json
{
    "extensionName": "0004-hashed-n-tuple-storage-layout",
    "digestAlgorithm": "md5",
    "tupleSize": 2,
    "numberOfTuples": 15,
    "shortObjectRoot": true
}
```

#### Mappings

| Object ID | Digest | Object Root Path |
| --------- | ------ | ---------------- |
| object-01 | ff75534492485eabb39f86356728884e | `ff/75/53/44/92/48/5e/ab/b3/9f/86/35/67/28/88/4e` |
| ..hor/rib:le-$id | 08319766fb6c2935dd175b94267717e0 | `08/31/97/66/fb/6c/29/35/dd/17/5b/94/26/77/17/e0` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── extensions/
│   └── 0004-hashed-n-tuple-storage-layout/
│       └── config.json
├── 08/
│   └── 31/
│       └── 97/
│           └── 66/
│               └── fb/
│                   └── 6c/
│                       └── 29/
│                           └── 35/
│                               └── dd/
│                                   └── 17/
│                                       └── 5b/
│                                           └── 94/
│                                               └── 26/
│                                                   └── 77/
│                                                       └── 17/
│                                                           └── e0/
│                                                               ├── 0=ocfl_object_1.0
│                                                               ├── inventory.json
│                                                               ├── inventory.json.sha512
│                                                               └── v1 [...]
└── ff/
    └── 75/
        └── 53/
            └── 44/
                └── 92/
                    └── 48/
                        └── 5e/
                            └── ab/
                                └── b3/
                                    └── 9f/
                                        └── 86/
                                            └── 35/
                                                └── 67/
                                                    └── 28/
                                                        └── 88/
                                                            └── 4e/
                                                                ├── 0=ocfl_object_1.0
                                                                ├── inventory.json
                                                                ├── inventory.json.sha512
                                                                └── v1 [...]
```

### Example 3

This example demonstrates what happens when `tupleSize` and `numberOfTuples` are
set to `0`. This is an edge case and not a recommended configuration.

#### Parameters

```json
{
    "extensionName": "0004-hashed-n-tuple-storage-layout",
    "digestAlgorithm": "sha256",
    "tupleSize": 0,
    "numberOfTuples": 0,
    "shortObjectRoot": false
}
```

#### Mappings

| Object ID | Digest | Object Root Path |
| --------- | ------ | ---------------- |
| object-01 | 3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4 | `3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4` |
| ..hor/rib:le-$id | 487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d | `487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── extensions/
│   └── 0004-hashed-n-tuple-storage-layout/
│       └── config.json
├── 3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4/
│   ├── 0=ocfl_object_1.0
│   ├── inventory.json
│   ├── inventory.json.sha512
│   └── v1 [...]
└── 487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d/
    ├── 0=ocfl_object_1.0
    ├── inventory.json
    ├── inventory.json.sha512
    └── v1 [...]
```
