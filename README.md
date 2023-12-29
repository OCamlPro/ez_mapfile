
[![Actions Status](https://github.com/ocamlpro/ez_mapfile/workflows/Main%20Workflow/badge.svg)](https://github.com/ocamlpro/ez_mapfile/actions)
[![Release](https://img.shields.io/github/release/ocamlpro/ez_mapfile.svg)](https://github.com/ocamlpro/ez_mapfile/releases)

# ez_mapfile

* Website: https://ocamlpro.github.io/ez_mapfile
* General Documentation: https://ocamlpro.github.io/ez_mapfile/sphinx
* API Documentation: https://ocamlpro.github.io/ez_mapfile/doc
* Sources: https://github.com/ocamlpro/ez_mapfile

## Short Description

This library implements an efficient storing of data in memory-mapped
file, and several data structures over files (arrays,
hashtbl). Everything is in the `EzMapfile` namespace.

* `MapFile`: the raw memory-mapped files with functions to store
  and retrieve strings and integers

* the `FileEncoding` module provides functions to create encoders and
  decoders that will be used to marshal/unmarshal types in the data
  structures.

* Data structures over files: there are `FileSequence` (a file storing
  a sequence of items), `FileArray` (a set of files storing items with
  random access) and `FileHashtbl` (a set of files storing key-values
  bindings in a hash-table structure).

* All the data structure are implemented over an abstract file
  interface, that uses memory-mapped files by default
  (`FileEngine.mapfile_config`) but can be configured to use
  more expensive standard files (using `FileEngine.openfile_config`)

## Supported Platforms

The library has been successfully compiled and tested on:
* Linux
* Windows/Mingw

Though not tested, it should work on Macosx and Windows/Cygwin.

## Testing

The library comes with an executable `ez-mapfile-test` that can be
used to test that the library is working::

```
ez-mapfile-test [--list] [-k KEYWORD]* [TEST-NAME]*
```

The `--list` option can be used to print selected tests. The `-k
KEYWORD` option can be used to add a filter, each test name is a list
of keywords separated by dots, and this option will only select the
tests with the corresponding keyword.

The following keywords are useful:

* Selecting a data structure: use `sequence`, `array` or `hashtbl`
* Selecting the size of the test: use `min`, `average` or `max`
* Selecting the engine: use `mapfile` or `openfile`

For example:

```
ez-mapfile-test -k min
```

will run all the tests with the smallest size, which is the good way to run
the first time.




