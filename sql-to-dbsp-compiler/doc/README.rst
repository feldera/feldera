SQL to DBSP Compiler documentation
==================================

This directory contains documentation for the SQL to DBSP compiler.

Build the Documentation
-----------------------

The documentation is written using ReStructured Text (RST).  The build
system has been adapted from <https://github.com/systemsapproach/book.git>.

The build process is stored in the `Makefile` and requires Python be
installed. The `Makefile` will create a virtualenv (`venv-docs`) which
installs the documentation generation toolset.  You may also need to
install the `enchant` C library using your system’s package manager
for the spelling checker to function properly.

To generate HTML in `_build/html`,  run `make html`.

To check the formatting of the book, run `make lint`.

To check spelling, run `make spelling`. If there are additional
words, names, or acronyms that are correctly spelled but not in the dictionary,
please add them to the `dict.txt` file.

To see the other available output formats, run `make`.

On Apple silicon, if you get the exception ''The 'enchant' C library
was not found and maybe needs to be installed'' when running the
`make` commands, set the following environment variable first after
installing enchant via `brew install enchant`:

```
PYENCHANT_LIBRARY_PATH=/opt/homebrew/lib/libenchant-2.2.dylib
```
