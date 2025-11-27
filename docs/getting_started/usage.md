---
title: Usage
---
# Usage

adtl can be used from the command line or as a Python library

**As a CLI**:
```bash
adtl parse specification-file input-file
```

Here *specification-file* is the [parser specification](/specification) (as TOML or JSON)
and *input-file* is the data file (not the data dictionary) that adtl
will transform using the instructions in the specification.

If adtl is not in your PATH, this may give an error. Either add the location
where the adtl script is installed to your PATH, or try running adtl as a module

```shell
python3 -m adtl parse specification-file input-file
```

Running adtl will create output files with the name of the parser, suffixed with
table names in the current working directory.

**As a Python library**:
```python
import adtl

parser = adtl.Parser(specification)
print(parser.tables) # list of tables created

for row in parser.parse().read_table(table):
    print(row)
```
alternatively to get an output file as a data table, similarly to the CLI:
```python
import adtl

data = adtl.parse("specification-file", "input-file")
```
where `data` is returned as a dictionary of pandas dataframes, one for each table.

# Specification tools
