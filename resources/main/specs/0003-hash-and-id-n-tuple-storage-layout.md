# OCFL Community Extension 0003: Hashed Truncated N-tuple Trees with Object ID Encapsulating Directory for OCFL Storage Hierarchies

* **Extension Name:** 0003-hash-and-id-n-tuple-storage-layout
* **Authors:** Ben Cail
* **Minimum OCFL Version:** 1.0
* **OCFL Community Extensions Version:** 1.0
* **Obsoletes:** n/a
* **Obsoleted by:** n/a

## Overview

This storage root extension describes how to safely map OCFL object identifiers
of any length, containing any characters, to OCFL object root directories.

Using this extension, OCFL object identifiers are hashed and encoded as hex
strings (all letters lower-case). These digests are then divided into _N_
n-tuple segments, which are used to create nested paths under the OCFL storage
root. Finally, the OCFL object identifier is percent-encoded to create a
directory name for the OCFL object root (see ["Encapsulation
Directory"](#encapsulation-directory) section below).

The n-tuple segments approach allows OCFL object identifiers to be evenly
distributed across the storage hierarchy. The maximum number of files under any
given directory is controlled by the number of characters in each n-tuple, and
the tree depth is controlled by the number of n-tuple segments each digest is
divided into. The encoded encapsulation directory name provides visibility into
the object identifier from the file path (see ["Encapsulation
Directory"](#encapsulation-directory) section below for details).

## Encapsulation Directory

For basic OCFL object identifiers, the object identifier is used as the name of
the encapsulation directory (ie. the object root directory).

Some object identifiers could contain characters that are not safe for directory
names on all filesystems. Safe characters are defined as A-Z, a-z, 0-9, '-' and
'\_'. When an unsafe character is encountered in an object identifier, it is
percent-encoded using the lower-case hex characters of its UTF-8 encoding.

Some object identifiers could also result in an encoded string that is longer
than can be supported as a directory name. To handle that scenario, if the
percent-encoded object identifier is longer than 100 characters, it is truncated
to 100 characters, and then the digest of the original object identifier is
appended to the encoded object identifier like this:
\<encoded-object-identifier-first-100-chars>-\<digest>. Note: this means that it
is no longer possible to determine the full object identifier from the
encapsulation directory name - some characters have been removed, and even the
first 100 characters of the encoded object identifier cannot be fully, reliably
decoded, because the truncation may leave a partial encoding at the end of the
100 characters.

| Object ID | Encapsulation Directory Name |
| --- | --- |
| object-01 | object-01 |
| ..Hor/rib:lè-$id | %2e%2eHor%2frib%3al%c3%a8-%24id |
| abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghija | abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij-5cc73e648fbcff136510e330871180922ddacf193b68fdeff855683a01464220 |

## Parameters

### Summary

* **Name:** `digestAlgorithm`
    * **Description:** The digest algorithm to apply to the OCFL object identifier; MUST be an algorithm that is allowed in the OCFL fixity block
    * **Type:** string
    * **Constraints:** Must not be empty
    * **Default:** sha256
* **Name**: `tupleSize`
    * **Description:** Indicates the size of the segments (in characters) that the digest is split into
    * **Type:** number
    * **Constraints:** An integer between 0 and 32 inclusive
    * **Default:** 3
* **Name:** `numberOfTuples`
    * **Description:** Indicates how many segments are used for path generation
    * **Type:** number
    * **Constraints:** An integer between 0 and 32 inclusive
    * **Default:** 3

### Details

#### digestAlgorithm

`digestAlgorithm` is defaulted to `sha256`, and it MUST either contain a digest
algorithm that's [officially supported by the OCFL
spec](https://ocfl.io/draft/spec/#digest-algorithms) or defined in a community
extension. The specified algorithm is applied to OCFL object identifiers to
produce hex encoded digest values that are then mapped to OCFL object root
paths.

#### tupleSize

`tupleSize` determines the number of digest characters to include in each tuple.
The tuples are used as directory names. The default value is `3`, which means
that each intermediate directory in the OCFL storage hierarchy could contain up
to 4096 directories. Increasing this value increases the maximum number of
sub-directories per directory.

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

## Procedure

The following is an outline of the steps to follow to map an OCFL object
identifier to an OCFL object root path using this extension (also see the
["Python Code"](#python-code) section):

1. The OCFL object identifier is encoded as UTF-8 and hashed using the specified
   `digestAlgorithm`.
2. The digest is encoded as a lower-case hex string.
3. Starting at the beginning of the digest and working forwards, the digest is
   divided into `numberOfTuples` tuples each containing `tupleSize` characters.
4. The tuples are joined, in order, using the filesystem path separator.
5. The OCFL object identifier is percent-encoded to create the encapsulation
   directory name (see ["Encapsulation Directory"](#encapsulation-directory)
   section above for details).
6. The encapsulation directory name is joined to the end of the path.

## Examples

### Example 1

This example demonstrates what the OCFL storage hierarchy looks like when using
this extension's default configuration.

#### Parameters

It is not necessary to specify any parameters to use the default configuration.
However, if you were to do so, it would look like the following:

```json
{
    "extensionName": "0003-hash-and-id-n-tuple-storage-layout",
    "digestAlgorithm": "sha256",
    "tupleSize": 3,
    "numberOfTuples": 3
}
```

#### Mappings

| Object ID | Digest | Object Root Path |
| --- | --- | --- |
| object-01 | 3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4 | `3c0/ff4/240/object-01` |
| ..hor/rib:le-$id | 487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d | `487/326/d8c/%2e%2ehor%2frib%3ale-%24id` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── extensions/0003-hash-and-id-n-tuple-storage-layout/config.json
├── 3c0/
│   └── ff4/
│       └── 240/
│           └── object-01/
│               ├── 0=ocfl_object_1.0
│               ├── inventory.json
│               ├── inventory.json.sha512
│               └── v1 [...]
└── 487/
    └── 326/
        └── d8c/
            └── %2e%2ehor%2frib%3ale-%24id/
                ├── 0=ocfl_object_1.0
                ├── inventory.json
                ├── inventory.json.sha512
                └── v1 [...]
```

### Example 2

This example demonstrates the effects of modifying the default parameters to use
a different `digestAlgorithm`, smaller `tupleSize`, and a larger
`numberOfTuples`.

#### Parameters

```json
{
    "extensionName": "0003-hash-and-id-n-tuple-storage-layout",
    "digestAlgorithm": "md5",
    "tupleSize": 2,
    "numberOfTuples": 15
}
```

#### Mappings

| Object ID | Digest | Object Root Path |
| --- | --- | --- |
| object-01 | ff75534492485eabb39f86356728884e | `ff/75/53/44/92/48/5e/ab/b3/9f/86/35/67/28/88/object-01` |
| ..hor/rib:le-$id | 08319766fb6c2935dd175b94267717e0 | `08/31/97/66/fb/6c/29/35/dd/17/5b/94/26/77/17/%2e%2ehor%2frib%3ale-%24id` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── extensions/0003-hash-and-id-n-tuple-storage-layout/config.json
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
│                                                           └── %2e%2ehor%2frib%3ale-%24id/
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
                                                            └── object-01/
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
    "extensionName": "0003-hash-and-id-n-tuple-storage-layout",
    "digestAlgorithm": "sha256",
    "tupleSize": 0,
    "numberOfTuples": 0
}
```

#### Mappings

| Object ID | Digest | Object Root Path |
| --- | --- | --- |
| object-01 | 3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4 | `object-id` |
| ..hor/rib:le-$id | 487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d | `%2e%2ehor%2frib%3ale-%24id` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── extensions/0003-hash-and-id-n-tuple-storage-layout/config.json
├── object-id/
│   ├── 0=ocfl_object_1.0
│   ├── inventory.json
│   ├── inventory.json.sha512
│   └── v1 [...]
└── %2e%2ehor%2frib%3ale-%24id/
    ├── 0=ocfl_object_1.0
    ├── inventory.json
    ├── inventory.json.sha512
    └── v1 [...]
```

## Python Code

Here is some python code that implements the algorithm:

```
import codecs
import hashlib
import os
import re


def _percent_encode(c):
    c_bytes = c.encode('utf8')
    s = ''
    for b in c_bytes:
        s += '%' + codecs.encode(bytes([b]), encoding='hex_codec').decode('utf8')
    return s


def _get_encapsulation_directory(object_id, digest):
    d = ''
    for c in object_id:
        if re.match(r'[A-Za-z0-9-_]{1}', c):
            d += c
        else:
            d += _percent_encode(c)
    if len(d) > 100:
        return f'{d[:100]}-{digest}'
    return d


def ocfl_path(object_id, algorithm='sha256', tuple_size=3, number_of_tuples=3):
    object_id_utf8 = object_id.encode('utf8')
    if algorithm == 'md5':
        digest = hashlib.md5(object_id_utf8).hexdigest()
    elif algorithm == 'sha256':
        digest = hashlib.sha256(object_id_utf8).hexdigest()
    elif algorithm == 'sha512':
        digest = hashlib.sha512(object_id_utf8).hexdigest()
    digest = digest.lower()
    path = ''
    for i in range(number_of_tuples):
        part = digest[i*tuple_size:i*tuple_size+tuple_size]
        path = os.path.join(path, part)
    encapsulation_directory = _get_encapsulation_directory(object_id, digest=digest)
    path = os.path.join(path, encapsulation_directory)
    return path


def _check_path(object_id, correct_path, algorithm='sha256', tuple_size=3, number_of_tuples=3):
    p = ocfl_path(object_id, algorithm=algorithm, tuple_size=tuple_size, number_of_tuples=number_of_tuples)
    assert p == correct_path, f'{p} != {correct_path}'
    print(f'  "{object_id}" {algorithm} => {p}')


def run_tests():
    print('running tests...')
    assert _percent_encode('.') == '%2e'
    assert _percent_encode('ç') == '%c3%a7'
    _check_path(object_id='object-01', correct_path='3c0/ff4/240/object-01')
    _check_path(object_id='object-01', correct_path='ff7/553/449/object-01', algorithm='md5')
    _check_path(object_id='object-01', correct_path='ff755/34492/object-01', algorithm='md5', tuple_size=5, number_of_tuples=2)
    _check_path(object_id='object-01', correct_path='object-01', algorithm='md5', tuple_size=0, number_of_tuples=0)
    _check_path(object_id='object-01', correct_path='ff/75/53/44/92/48/5e/ab/b3/9f/86/35/67/28/88/object-01', algorithm='md5', tuple_size=2, number_of_tuples=15)
    _check_path(object_id='..hor/rib:le-$id', correct_path='487/326/d8c/%2e%2ehor%2frib%3ale-%24id')
    _check_path(object_id='..hor/rib:le-$id', correct_path='083/197/66f/%2e%2ehor%2frib%3ale-%24id', algorithm='md5') #08319766fb6c2935dd175b94267717e0
    _check_path(object_id='..Hor/rib:lè-$id', correct_path='373/529/21a/%2e%2eHor%2frib%3al%c3%a8-%24id')
    long_object_id = 'abcdefghij' * 26
    long_object_id_digest = '55b432806f4e270da0cf23815ed338742179002153cd8d896f23b3e2d8a14359'
    _check_path(object_id=long_object_id, correct_path=f'55b/432/806/abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij-{long_object_id_digest}')
    long_object_id_101 = 'abcdefghij' * 10 + 'a'
    long_object_id_101_digest = '5cc73e648fbcff136510e330871180922ddacf193b68fdeff855683a01464220'
    _check_path(object_id=long_object_id_101, correct_path=f'5cc/73e/648/abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij-{long_object_id_101_digest}')


if __name__ == '__main__':
    run_tests()
```
