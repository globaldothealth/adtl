# adtl – another data transformation language

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

[![tests](https://github.com/globaldothealth/adtl/actions/workflows/tests.yml/badge.svg)](https://github.com/globaldothealth/adtl/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/globaldothealth/adtl/branch/main/graph/badge.svg?token=QTD7HRR3TO)](https://codecov.io/gh/globaldothealth/adtl)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


adtl is a data transformation language (DTL) used by some applications in
[Global.health](https://global.health), notably for the ISARIC clinical data pipeline at
[globaldothealth/isaric](https://github.com/globaldothealth/isaric) and the InsightBoard
project dashboard at [globaldothealth/InsightBoard](https://github.com/globaldothealth/InsightBoard)

Documentation: [ReadTheDocs](https://adtl.readthedocs.io/en/latest/index.html)

## Installation

You can install this package using either [`pipx`](https://pypa.github.io/pipx/)
or `pip`. Installing via `pipx` offers advantages if you want to just use the
`adtl` tool standalone from the command line, as it isolates the Python
package dependencies in a virtual environment. On the other hand, `pip` installs
packages to the global environment which is generally not recommended as it
can interfere with other packages on your system.

* Installation via `pipx`:

  ```shell
  pipx install adtl
  ```

* Installation via `pip`:

  ```shell
  python3 -m pip install adtl
  ```

If you are writing code which depends on adtl (instead of using the
command-line program), then it is best to add a dependency on `adtl` to your
Python build tool of choice.

To use the development version, replace `adtl` with the full GitHub URL:

```shell
pip install git+https://github.com/globaldothealth/adtl
```

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
adtl parse specification-file input-file
```

Here *specification-file* is the parser specification (as TOML or JSON)
and *input-file* is the data file (not the data dictionary) that adtl
will transform using the instructions in the specification.

If adtl is not in your PATH, this may give an error. Either add the location
where the adtl script is installed to your PATH, or try running adtl as a module

```shell
python3 -m adtl parse specification-file input-file
```

Running adtl will create output files with the name of the parser, suffixed with
table names in the current working directory.

Before trying to transform your data, you can check that your specification file matches
the format adtl expects, and for fields which may have been either misspelled or missed out
during the mapping, by using:
```bash
adtl check specification-file input-file
```

Python library:
```python
import adtl

parser = adtl.Parser(specification)
print(parser.tables) # list of tables created

for row in parser.parse().read_table(table):
    print(row)
```
alternatively to get an output file as a CSV, similarly to the CLI:
```python
import adtl

data = adtl.parse("specification-file", "input-file")
```
where `data` is returned as a dictionary of pandas dataframes, one for each table.

## Development

Install [pre-commit](https://pre-commit.com) and setup pre-commit hooks
(`pre-commit install`) which will do linting checks before commit.
