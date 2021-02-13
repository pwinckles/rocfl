# OCFL Community Extension 0002: Flat Direct Storage Layout

* **Extension Name:** 0002-flat-direct-storage-layout
* **Authors:** Peter Winckles
* **Minimum OCFL Version:** 1.0
* **OCFL Community Extensions Version:** 1.0
* **Obsoletes:** n/a
* **Obsoleted by:** n/a

## Overview

This storage root extension describes a simple flat OCFL storage layout. OCFL object identifiers are mapped directly to directory names that are direct children of the OCFL storage root directory.

The limitations of this layout are filesystem dependent, but are generally as follows:

* The size of object IDs cannot exceed the maximum allowed directory name size (eg. 255 characters)
* Object IDs cannot include characters that are illegal in directory names
* Performance may degrade as the size of a repository increases because every object is a direct child of the storage root

## Parameters

This extension has no parameters.

## Procedure

The OCFL object identifier is used, without any changes, as the object's root path within the OCFL storage root.

## Examples

### Example 1

This example demonstrates some mappings that produce directory names that are valid on unix filesystems.

#### Mappings

| Object ID | Object Root Path |
| --- | --- |
| object-01 | `object-01` |
| ..hor\_rib:lé-$id | `..hor_rib:lé-$id` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── object-01/
│   ├── 0=ocfl_object_1.0
│   ├── inventory.json
│   ├── inventory.json.sha512
│   └── v1 [...]
└── ..hor_rib:lé-$id/
    ├── 0=ocfl_object_1.0
    ├── inventory.json
    ├── inventory.json.sha512
    └── v1 [...]
```

### Example 2

This example demonstrates some mappings that produce directory names that are invalid on unix filesystems; therefore this layout cannot be used in a repository that needs to be able to store objects with IDs like these.

#### Mappings

| Object ID | Object Root Path |
| --- | --- |
| info:fedora/object-01 | `info:fedora/object-01` |
| abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij | `abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij` |

