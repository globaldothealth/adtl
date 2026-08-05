---
title: Usage
---
# Usage

adtl can be used from the command line or as a Python library.

## Parsing data

**As a CLI**:
```bash
adtl parse specification-file input-file
```

Here *specification-file* is the [parser specification](../specification) (as TOML or JSON)
and *input-file* is the data file (not the data dictionary) that adtl
will transform using the instructions in the specification.

If adtl is not in your PATH, this may give an error. Either add the location
where the adtl script is installed to your PATH, or try running adtl as a module:

```shell
python3 -m adtl parse specification-file input-file
```

**Output files** are created in the current working directory, named after the parser
and suffixed with each table name defined in the specification. For example, a parser
named `study` with tables `subject` and `observation` will produce `study-subject.csv`
and `study-observation.csv`.

**As a Python library**:
```python
import adtl

parser = adtl.Parser(specification)
print(parser.tables)  # list of tables created

for row in parser.parse().read_table(table):
    print(row)
```

Alternatively, to get output as a dictionary of pandas DataFrames (one per table):
```python
import adtl

data = adtl.parse("specification-file", "input-file")
```

See {py:func}`adtl.parse` in the [module reference](../api/adtl) for the full list of
options, and {py:class}`adtl.Parser` for the lower-level interface.

## Validation columns

When a table has an associated JSON schema (set via the `schema` key in the specification),
adtl adds two extra columns to the output:

* `adtl_valid` (boolean): `True` if the row is valid according to the schema, `False` otherwise
* `adtl_error` (string): validation error message when `adtl_valid` is `False`

These columns are always present if a schema is configured for the table — even for valid rows,
where `adtl_valid` will be `True` and `adtl_error` will be empty.

## Checking a specification

Before running a full parse, you can check that a specification file is valid and
that its field names match those in a data file:

```bash
adtl check specification-file
adtl check specification-file data-file  # also cross-checks field names against data
```

This will:
- Validate the specification structure
- Report fields referenced in the spec but absent from the data file (error)
- Report fields present in the data file but not mapped in the spec (warning)

The same checks are available in Python as {py:func}`adtl.check_mapping`, with
{py:func}`adtl.validate_specification` covering specification validation alone.

## CLI options

Key options for `adtl parse`:

| Option | Description |
|--------|-------------|
| `-o FILE, --output FILE` | Write output to a specific file (single table only) |
| `--parquet` | Save outputs as Parquet files instead of CSV |
| `--encoding ENC` | Source file encoding (default: UTF-8) |
| `--include-def FILE` | Include an additional definitions file at runtime |
| `--include-transform FILE` | Include a Python file with custom transformation functions |
| `-p, --parallel` | Process data in parallel (recommended for large datasets) |
| `-q, --quiet` | Suppress progress bar and summary output |
| `--save-report FILE` | Save a validation summary as JSON |

Run `adtl parse --help` for the full list.

### Parallel processing

For large source files, pass `-p` / `--parallel` to process rows across multiple CPU
cores simultaneously:

```bash
adtl parse specification-file large-data.csv -p
```

This can give a significant speed improvement when parsing files with many rows.
It is not recommended for small datasets, where the overhead of spawning workers
outweighs the benefit.

The same flag is available in the Python interface:

```python
data = adtl.parse("specification-file", "large-data.csv", parallel=True)
```
