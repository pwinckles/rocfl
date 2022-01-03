# OCFL Community Extension 0007: N Tuple Omit Prefix Storage Layout

* **Extension Name:** 0007-n-tuple-omit-prefix-storage-layout
* **Authors:** Michael Vandermillen and Andrew Woods
* **Minimum OCFL Version:** 1.0
* **OCFL Community Extensions Version:** 1.0
* **Obsoletes:** n/a
* **Obsoleted by:** n/a

## Overview

This storage root extension describes an OCFL storage layout combining a pairtree-like root directory structure derived from prefix-omitted object identifiers, followed by the prefix-omitted object identifier themselves.
The OCFL object identifiers are expected to contain prefixes which are removed in the mapping to directory names.
The OCFL object identifier prefix is defined as all characters before and including a configurable delimiter.
Where the prefix-omitted identifier length is less than tuple size * number of tuples, the remaining object id (prefix omitted) is left or right-side, zero-padded (configurable, left default), and optionally reversed (default false).
The object id is then divided into N n-tuple segments, and used to create nested paths under the OCFL storage root, followed by the prefix-omitted object id directory.

This layout combines the advantages of 0006-flat-omit-prefix-storage-layout (directory name transparency) and the 0004-hashed-n-tuple-storage-layout (enhanced file system/bucket performance).

The limitations of this layout are filesystem dependent (with one exception), and are generally as follows:

* The size of object identifiers, minus the length of the prefix, cannot exceed the maximum allowed directory name size
  (eg. 255 characters)
* Object identifiers cannot include characters that are illegal in directory names
* This extension is defined over the ASCII subset of UTF-8 (code points 0x20 to 0x7F). Any character outside of this range in either an identifier or a path is an error.

## Parameters

### Summary

* **Name:** `delimiter`
    * **Description:** The case-insensitive, delimiter marking the end of the OCFL object identifier prefix; MUST consist
      of a character string of length one or greater. If the delimiter is found multiple times in the OCFL object
      identifier, its last occurence (right-most) will be used to select the termination of the prefix.
    * **Type:** string
    * **Constraints:** Must not be empty
    * **Default:** :
* **Name**: `tupleSize`
    * **Description:** Indicates the segment size (in characters) to split the
      identifier into
    * **Type:** number
    * **Constraints:** An integer between 1 and 32 inclusive
    * **Default:** 3
* **Name:** `numberOfTuples`
    * **Description:** Indicates the number of segments to use for path generation
    * **Type:** number
    * **Constraints:** An integer between 1 and 32 inclusive
    * **Default:** 3
* **Name:** `zeroPadding`
    * **Description:** Indicates whether to use left or right zero padding for ids less than tupleSize * numberOfTuples
    * **Type:** string
    * **Constraints:** Must be either "left" or "right"
    * **Default:** left
* **Name:** `reverseObjectRoot`
    * **Description:** When true, indicates that the prefix-omitted, padded object identifier should be reversed
    * **Type:** boolean
    * **Default:** false

## Procedure

The following is an outline of the steps to map an OCFL object identifier to an
OCFL object root path:

1. Remove the prefix, which is everything to the left of the right-most instance of the delimiter, as well as the delimiter. If there is no delimiter, the whole id is used; if the delimiter is found at the end, an error is thrown.
2. Optionally, add zero-padding to the left or right of the remaining id, depending on `zeroPadding` configuration.
3. Optionally reverse the remaining id, depending on `reverseObjectRoot`
4. Starting at the leftmost character of the resulting id and working right, divide the id into `numberOfTuples` each containing `tupleSize` characters.
5. Create the start of the object root path by joining the tuples, in order, using the filesystem path separator.
6. Complete the object root path by joining the prefix-omitted id (from step 1) onto the end.

## Examples

### Example 1

This example demonstrates mappings where the single-character delimiter is found one or more times in the OCFL object
identifier, with default `zeroPadding`, modified `tupleSize` and `numberOfTuples`, and `reverseObjectRoot` true.

#### Parameters

```json
{
    "extensionName": "0007-n-tuple-omit-prefix-storage-layout",
    "delimiter": ":",
    "tupleSize": 4,
    "numberOfTuples": 2,
    "zeroPadding": "left",
    "reverseObjectRoot": true
}
```

#### Mappings

| Object ID | Object Root Path |
| --- | --- |
| namespace:12887296 | `6927/8821/12887296` |
| urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66 | `66a9/c002/6e8bc430-9c3a-11d9-9669-0800200c9a66` |
| abc123 | `321c/ba00/abc123`

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── extensions/
│   └── 0007-n-tuple-omit-prefix-storage-layout/
│       └── config.json
├── 6927/
│   └── 8821/
│       └── 12887296/
│           ├── 0=ocfl_object_1.0
│           ├── inventory.json
│           ├── inventory.json.sha512
│           └── v1 [...]
├── 66a9/
│   └── c002/
│       └── 6e8bc430-9c3a-11d9-9669-0800200c9a66/
│           ├── 0=ocfl_object_1.0
│           ├── inventory.json
│           ├── inventory.json.sha512
│           └── v1 [...]
└── 321c/
    └── ba00/
        └── abc123/
            ├── 0=ocfl_object_1.0
            ├── inventory.json
            ├── inventory.json.sha512
            └── v1 [...]
            
```

### Example 2

This example demonstrates mappings where the multi-character delimiter is found one or more times in the OCFL object
identifier, with default `tupleSize`, `numberOfTuples`, and `reverseObjectRoot`, and modified `zeroPadding`.

#### Parameters

```json
{
    "extensionName": "0007-n-tuple-omit-prefix-storage-layout",
    "delimiter": "edu/",
    "tupleSize": 3,
    "numberOfTuples": 3,
    "zeroPadding": "right",
    "reverseObjectRoot": false
}
```

#### Mappings

| Object ID | Object Root Path |
| --- | --- |
| https://institution.edu/3448793 | `344/879/300/3448793` |
| https://institution.edu/abc/edu/f8.05v | `f8./05v/000/f8.05v` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── extensions/
│   └── 0007-n-tuple-omit-prefix-storage-layout/
│       └── config.json
├── 344/
│   └── 879/
│       └── 300/
│           └── 3448793/
│               ├── 0=ocfl_object_1.0
│               ├── inventory.json
│               ├── inventory.json.sha512
│               └── v1 [...]
└── f8./
    └── 05v/
        └── 000/
            └── f8.05v/
                ├── 0=ocfl_object_1.0
                ├── inventory.json
                ├── inventory.json.sha512
                └── v1 [...]
```