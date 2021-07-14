# OCFL Community Extension 0006: Flat Omit Prefix Storage Layout

* **Extension Name:** 0006-flat-omit-prefix-storage-layout
* **Authors:** Andrew Woods
* **Minimum OCFL Version:** 1.0
* **OCFL Community Extensions Version:** 1.0
* **Obsoletes:** n/a
* **Obsoleted by:** n/a

## Overview

This storage root extension describes a flat OCFL storage layout. The OCFL object directories are direct children of
the OCFL storage root directory.
The OCFL object identifiers are expected to contain prefixes which are removed in the mapping to directory names. The
OCFL object identifier prefix is defined as all characters before and including a configurable delimiter.

The limitations of this layout are filesystem dependent, but are generally as follows:

* The size of object identifiers, minus the length of the prefix, cannot exceed the maximum allowed directory name size
  (eg. 255 characters)
* Object identifiers cannot include characters that are illegal in directory names
* Performance may degrade as the size of a repository increases because every object is a direct child of the storage root

## Parameters

### Summary

* **Name:** `delimiter`
    * **Description:** The case-insensitive, delimiter marking the end of the OCFL object identifier prefix; MUST consist
      of a character string of length one or greater. If the delimiter is found multiple times in the OCFL object
      identifier, its last occurence (right-most) will be used to select the termination of the prefix.
    * **Type:** string
    * **Constraints:** Must not be empty
    * **Default:**

## Examples

### Example 1

This example demonstrates mappings where the single-character delimiter is found one or more times in the OCFL object
identifier.

#### Parameters

There is no default configuration; therefore, configuration parameters must be provided.

```json
{
    "extensionName": "0006-flat-omit-prefix-storage-layout",
    "delimiter": ":"
}
```

#### Mappings

| Object ID | Object Root Path |
| --- | --- |
| namespace:12887296 | `12887296` |
| urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66 | `6e8bc430-9c3a-11d9-9669-0800200c9a66` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── 12887296/
│   ├── 0=ocfl_object_1.0
│   ├── inventory.json
│   ├── inventory.json.sha512
│   └── v1 [...]
└── 6e8bc430-9c3a-11d9-9669-0800200c9a66/
    ├── 0=ocfl_object_1.0
    ├── inventory.json
    ├── inventory.json.sha512
    └── v1 [...]
```

### Example 2

This example demonstrates mappings where the multi-character delimiter is found one or more times in the OCFL object
identifier.

#### Parameters

There is no default configuration; therefore, configuration parameters must be provided.

```json
{
    "extensionName": "0006-flat-omit-prefix-storage-layout",
    "delimiter": "edu/"
}
```

#### Mappings

| Object ID | Object Root Path |
| --- | --- |
| https://institution.edu/3448793 | `3448793` |
| https://institution.edu/abc/edu/f8.05v | `f8.05v` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── 3448793/
│   ├── 0=ocfl_object_1.0
│   ├── inventory.json
│   ├── inventory.json.sha512
│   └── v1 [...]
└── f8.05v/
    ├── 0=ocfl_object_1.0
    ├── inventory.json
    ├── inventory.json.sha512
    └── v1 [...]
```

### Example 3

This example demonstrates mappings that produce directory names that are invalid on unix filesystems; therefore this
layout cannot be used in a repository that needs to be able to store objects with identifiers like these.

#### Parameters

There is no default configuration; therefore, configuration parameters must be provided.

```json
{
    "extensionName": "0006-flat-omit-prefix-storage-layout",
    "delimiter": "info:"
}
```

#### Mappings

| Object ID | Object Root Path |
| --- | --- |
| info:fedora/object-01 | `fedora/object-01` |
| https://example.org/info:/12345/x54xz321/s3/f8.05v | `/12345/x54xz321/s3/f8.05v` |
