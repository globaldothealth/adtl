---
title: Examples
---
# Examples

Each example contains source data, a
fully annotated parser, JSON schemas, and a walkthrough covering every mapping
pattern used.

```{toctree}
:maxdepth: 2
:hidden:

examples/wildlife_surveillance/surveillance_example
```

---

## Wildlife disease surveillance

A synthetic animal disease surveillance dataset designed to demonstrate a broad
set of ADTL mapping patterns in one place.

**Features covered:**

| Feature | What it demonstrates |
|---------|---------------------|
| `groupBy` + `lastNotNull` | Collapsing duplicate source rows (follow-up updates) |
| Value mappings + `[adtl.defs]` | Decoding numeric codes; reusing definitions |
| `combinedType = "firstNonNull"` | Preferring one field and falling back to another |
| `unit` / `source_unit` | Converting age months → years and weight lbs → kg |
| `combinedType = "any"` | Deriving a boolean flag from multiple symptom columns |
| `combinedType = "set"` | Collecting comorbidities from several boolean columns |
| Field-level `if` | Suppressing `outcome_date` for unresolved cases |
| `oneToMany` + `for` loop | Expanding repeated test columns into one row per test |
| Row-level `if` | Suppressing empty test slots |
| JSON schema validation | Reading `adtl_valid` / `adtl_error`; correcting failures |

**Run it:**

```bash
adtl parse examples/wildlife_surveillance/surveillance_parser.toml \
           examples/wildlife_surveillance/surveillance_data.csv
```
