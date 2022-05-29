# OCFL Community Extensions

**Version**: 1.0

This repository contains community extensions to the [OCFL Specification and Implementation Notes](https://ocfl.io/). Extensions are a means of adding new functionality and documenting standards outside of the main OCFL specification process. For example, storage layout extensions define how OCFL object IDs are mapped to OCFL object root directories within an OCFL storage root. This mapping is outside of the scope of the OCFL specification, but is valuable information to capture so that repositories are self-describing and easily accessible using generic OCFL tooling.

This is a community driven repository. Community members are encouraged to contribute by submitting new extensions and reviewing others' submissions. For more details, see the [review/merge policy](#review--merge-policy) below.

See the current set of [adopted extensions](https://ocfl.github.io/extensions/) and [extensions open for review and discussion](https://github.com/OCFL/extensions/pulls).

## Using Community Extensions

To use OCFL extensions you first need an OCFL client that supports the desired extensions. OCFL clients are not required to support extensions to be compliant with the OCFL specification, and the extensions that any given client supports will vary. The idea behind this repository is to encourage the development and implementation of common extensions so that there can be interoperability between OCFL clients.

## Implementing Community Extensions

Reference the OCFL specification's description of [object extensions](https://ocfl.io/1.0/spec/#object-extensions) and [storage root extensions](https://ocfl.io/1.0/spec/#storage-root-extensions).

The OCFL storage root MAY contain a copy of an extension's specification.

Each extension specification details how it should be implemented, but there are a few rules that apply to every extension.

A *root extension directory* refers to the directory named `extensions` that is located in either the storage root or an object root. An *extension directory* is an extension specific directory that is the child of a root extension directory and MUST be named using the extension's *Registered Name*, or `initial` (see [Optional Initial Extension](#optional-initial-extension)). For example, `extensions/0000-example-extension` is the extension directory for the extension [0000-example-extension](docs/0000-example-extension.md).

### Configuration Files

An extension's parameters are serialized as a JSON object and written to a configuration file named `config.json` within the extension's extension directory.

If an extension includes a configuration file, one of the properties in that file MUST be `extensionName`, where the value is the *Registered Name* of the extension.

For example, the extension [0000-example-extension](docs/0000-example-extension.md) could be parameterized as follows:

```json
{
  "extensionName": "0000-example-extension",
  "firstExampleParameter": 12,
  "secondExampleParameter": "Hello",
  "thirdExampleParameter": "Green"
}
```

Based on how the extension is used, its configuration file is written to one of the following locations, relative the storage root:

* `extensions/0000-example-extension/config.json`, if it is a [storage root extension](https://ocfl.io/1.0/spec/#storage-root-extensions)
* `OBJECT_ROOT/extensions/0000-example-extension/config.json`, if it is an [object extension](https://ocfl.io/1.0/spec/#object-extensions)

### Undefined Behavior

It is conceivable that some extensions may not be compatible with other extensions, or may be rendered incompatible based on how they're implemented in a client. For example, suppose that there are multiple extensions that define how logs should be written to an object's log directory. You could declare that your objects are using multiple log extensions, but the result is undefined and up to the implementing client. It may only write one log format or the other, it may write all of them, or it may reject the configuration entirely.

Because OCFL clients are not required to implement any or all extensions, it is also possible that a client may encounter an extension that it does not implement. In these cases, it is up to the client to decide how to proceed. A client may fail on unsupported extensions, or it may choose to ignore the extensions and carry on.

### Optional Initial Extension

A _root extension directory_ MAY optionally contain an _initial_ extension that, if it exists, SHOULD be applied before all other extensions in the directory.
An _initial extension_ is identified by the extension directory name "initial".

An _initial extension_ could be used to address some of the [undefined behaviors](#undefined-behavior), define how extensions are applied, and answer questions such as:

- Is an extension deactivated, only applying to earlier versions of the object?
- Should extensions be applied in a specific order?
- Does one extension depend on another?

## Specifying Community Extensions

### Layout

Community extensions MUST be written as GitHub flavored markdown files in the `docs` directory of this repository. The
filename of an extension is based on its *Registered Name* with a `.md` extension.

Extensions are numbered sequentially, and the *Registered Name* of an extension is prefixed with this 4-digit, zero-padded
decimal number. The *Registered Name* should be descriptive, use hyphens to separate words, and have a maximum of 250
characters in total.

New extensions should use `NNNN` as a place-holder for the next available prefix number at the time of merging. New extension pull-requests should not update the index document (`docs/index.md`), this will be done post-approval.

Extensions are intended to be mostly static once published. Substantial revisions of content beyond simple fixes warrants publishing a new extension, and marking the old extension obsolete by updating the *Obsoletes/Obsoleted by* sections in each extension respectively.

An example/template is available in this repository as "[OCFL Community Extension 0000: Example Extension](docs/0000-example-extension.md)" and is rendered
via GitHub pages as https://ocfl.github.io/extensions/0000-example-extension

### Headers

Extension definitions MUST contain a header section that defines the following fields:

* **Extension Name**: The extension's unique *Registered Name*
* **Authors**: The names of the individuals who authored the extension
* **Minimum OCFL Version**: The minimum OCFL version that the extension requires, eg. *1.0*
* **OCFL Community Extensions Version**: The version of the OCFL Extensions Specification that the extension conforms to, eg. *1.0*
* **Obsoletes**: The *Registered Name* of the extension that this extension obsoletes, or *n/a*
* **Obsoleted by**: The *Registered Name* of the extension that obsoletes this extension, or *n/a*

### Parameters

Extension definitions MAY define parameters to enable configuration as needed. Extension parameters are serialized as JSON values, and therefore must conform to the [JSON specification](https://tools.ietf.org/html/rfc8259). Parameters MUST be defined in the following structure:

* **Name**: A short, descriptive name for the parameter. The name is used as the parameter's key within its JSON representation.
    * **Description**: A brief description of the function of the parameter. This should be expanded on in the main description of the extension which MUST reference all the parameters.
    * **Type**: The JSON data type of the parameter value. One of `string`, `number`, `boolean`, `array`, or `object`. The structure of complex types MUST be further described.
    * **Constraints**: A description of any constraints to apply to parameter values. Constraints may be plain text, regular expressions, [JSON Schema](https://www.ietf.org/archive/id/draft-handrews-json-schema-02.txt), or whatever makes the most sense for the extension.
    * **Default**: The default value of parameter. If no default is specified, then the parameter is mandatory.

### Body

Each specification MUST thoroughly document how it is intended to be implemented and used, including detailed examples is helpful. If the extension uses parameters, the parameters MUST be described in detail in the body of the specification.

## Review / Merge Policy

1. A pull-request is submitted per the guidelines described in the "[Organization of this repository](https://github.com/OCFL/extensions#organization-of-this-repository)" section of this document
1. Authors of (legitimate) pull-requests will be added by an owner of the OCFL GitHub organization to the [extension-authors](https://github.com/orgs/OCFL/teams/extension-authors) team
    - The purpose of being added to this team is to enable adding `labels` to their pull-request(s)
1. If a pull-request is submitted in order to facilitate discussion, the `draft` label should be applied by the author
1. If a pull-request is ready for review, it should have a title that is suitable for merge (i.e. not have a title indicating "draft"), and optionally have the `in-review` label applied by the author
1. A pull-request must be merged by an OCFL Editor if the following criteria are met:
    1. At least two OCFL Editors have "[Approved](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/approving-a-pull-request-with-required-reviews)" the pull-request
    1. At least one other community member has "[Approved](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/approving-a-pull-request-with-required-reviews)" the pull-request
    1. The approvers represent three distinct organizations
1. After the pull-request has been merged with `NNNN` as a placeholder for the extension number in the _Registered Name_, an OCFL Editor will determine the extension number based on the next sequentially available number. They will create an additional administrative pull-request to change `NNNN` to the appropriate number in the extension file name and the extension document itself, as well as adding an entry to the index page entry (`docs/index.md`).