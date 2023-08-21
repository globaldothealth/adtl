# adtl – another data transformation language

[![](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![tests](https://github.com/globaldothealth/adtl/actions/workflows/tests.yml/badge.svg)](https://github.com/globaldothealth/adtl/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/globaldothealth/adtl/branch/main/graph/badge.svg?token=QTD7HRR3TO)](https://codecov.io/gh/globaldothealth/adtl)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


adtl is a data transformation language (DTL) used by some applications in
[Global.health](https://global.health), notably for the ISARIC clinical data pipeline at
[globaldothealth/isaric](https://github.com/globaldothealth/isaric).

**adtl is currently a prototype and is subject to major revisions**

## Motivation

Most existing data transformation languages are usually in a XML dialect, though
there are recent variations in other file formats. In addition, many DTLs use a
custom domain specific language. The primary utility of this DTL is to provide a
easy to use library in Python for basic data transformations, which are
specified in a JSON file. It is not meant to be a comprehensive, and adtl can
be used as a step within a larger data processing pipeline.

```{toctree}
---
caption: Getting started
maxdepth: 1
---

getting_started/installation
getting_started/usage
```

```{toctree}
---
caption: Specification
maxdepth: 1
---
specification
```

```{toctree}
---
caption: Module reference
maxdepth: 1
---

adtl
transformations
```