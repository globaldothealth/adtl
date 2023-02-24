# adtl – another data transformation language

[![](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![tests](https://github.com/globaldothealth/adtl/actions/workflows/tests.yml/badge.svg)](https://github.com/globaldothealth/adtl/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/globaldothealth/adtl/branch/main/graph/badge.svg?token=QTD7HRR3TO)](https://codecov.io/gh/globaldothealth/adtl)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


adtl is a data transformation language (DTL) used by some applications in
[Global.health](https://global.health), notably for the ISARIC clinical data pipeline at
[globaldothealth/isaric](https://github.com/globaldothealth/isaric). This package
was previously within the isaric repository and has been split out to enable
easier use in other applications.

## Installation

You can install this package using either [`pipx`](https://pypa.github.io/pipx/)
or `pip`. Installing via `pipx` offers advantages if you want to just use the
`adtl` tool standalone from the command line, as it isolates the Python
package dependencies in a virtual environment. On the other hand, `pip` installs
packages to the global environment which is generally not recommended as it
can interfere with other packages on your system.

* Installation via `pipx`: `pipx install git+https://github.com/globaldothealth/adtl`
* Installation via `pip`: `python3 -m pip install git+https://github.com/globaldothealth/adtl`

If you are writing code which depends on adtl (instead of using
the command-line program), then it is best to add a dependency on
`git+https://github.com/globaldothealth/adtl` to your Python build tool of
choice.

## Rationale

Most existing data transformation languages are usually in a XML dialect, though
there are recent variations in other file formats. In addition, many DTLs use a
custom domain specific language. The primary utility of this DTL is to provide a
easy to use library in Python for basic data transformations, which are
specified in a JSON file. It is not meant to be a comprehensive, and adtl can
be used as a step within a larger data processing pipeline.

## Usage

adtl can be used from the command line or as a Python library

As a CLI:
```bash
adtl specification-file input-file -o output
```

Python library:
```python
import adtl

parser = adtl.Parser(specification)
print(parser.tables) # list of tables created

for row in parser.parse().read_table(table):
    print(row)
```

## Development

Install [pre-commit](https://pre-commit.com) and setup pre-commit hooks (`pre-commit install`) which will do linting checks before commit.
