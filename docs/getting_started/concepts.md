---
title: Concepts
---
# Concepts

This page explains the core ideas behind adtl before you dive into the specification
or start writing parsers.

## What adtl does

adtl transforms a **source data file** (CSV, etc.) into one or more **output tables**
by following rules written in a **parser specification** (TOML or JSON).

```
source data file  +  parser specification  →  output table(s)
   (input.csv)         (parser.toml)           (parser-subject.csv, ...)
```

The specification does not contain data — it contains *rules* describing how to map
fields from the source format to the target format. The same parser can be run against
many data files, as long as they share the same structure.

## The three things you work with

| Thing | What it is | Example |
|-------|-----------|---------|
| **Source data** | The raw input CSV you want to transform | A hospital admission dataset |
| **Parser specification** | TOML/JSON file describing the field mappings | `study.toml` |
| **Target schema** | JSON Schema describing what valid output looks like | `subject.schema.json` |

The target schema is optional but *strongly* recommended — it lets adtl flag rows that don't
conform to the expected output format via the `adtl_valid` and `adtl_error` columns.

## Table kinds

A single parser can produce multiple output tables from the same source row.
Each table has a `kind` that controls how source rows map to output rows:

```
Source row
    │
    ├─── oneToOne  ──► one output row (e.g. one subject record per admission)
    │
    ├─── oneToMany ──► many output rows (e.g. one row per symptom observed)
    │
    └─── groupBy   ──► one output row per group (e.g. collapse repeated visits
                       into a single subject record)
```

**`oneToOne`** is the default and simplest kind. Each row in the source produces
exactly one row in the output.

**`oneToMany`** is used when one source row should fan out into multiple output rows —
for example, when a source row records many symptoms and you want one row per symptom.
Each variant is written as a `[[table]]` block (double brackets in TOML) with an optional
`if` condition controlling when it is emitted.

**`groupBy`** is used when the same subject appears across multiple source rows (e.g. a
longitudinal dataset) and you want to collapse those rows into one. You specify which
field to group by and how to aggregate conflicting values.

**`constant`** produces a fixed table whose rows do not depend on the source data at all — useful for embedding metadata.

## A minimal parser

Here is a complete parser that maps a one-row-per-patient CSV to a `subject` table:

```toml
[adtl]
name = "study"
description = "Maps study data to subject table"

[adtl.tables.subject]
kind = "oneToOne"
schema = "schemas/subject.schema.json"  # optional

[subject]
subject_id  = { field = "patient_id" }
age         = { field = "age_years" }
sex_at_birth = { field = "sex", values = { 1 = "male", 2 = "female" } }
enrolment_date = { field = "enrol_dt", source_date = "%d/%m/%Y" }
```

This would produce `study-subject.csv`.

## Definitions and reuse

Mappings that repeat across many fields (e.g. a yes/no/unknown coding) can be
extracted into a `[adtl.defs]` block and referenced with `ref = "name"`:

```toml
[adtl.defs.yn]
values = { 1 = true, 2 = false }

[subject.has_diabetes]
field = "diab_yn"
ref = "yn"
```

This keeps parsers concise and avoids copy-paste errors.

## Next steps

- [Usage](usage) — how to run adtl from the CLI or Python
- [Specification](../specification) — full reference for all mapping rules
- [Troubleshooting](troubleshooting) — common errors and how to fix them
