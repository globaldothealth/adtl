# ADTL feature walkthrough — wildlife disease surveillance

This example uses a fictional wildlife disease surveillance dataset to demonstrate
a broad set of ADTL mapping patterns in one place.

The files in this folder are:

| File | Description |
|------|-------------|
| `surveillance_data.csv` | Source dataset (9 rows, 8 cases, some duplicates) |
| `surveillance_parser.toml` | Worked parser with annotations |
| `cases.schema.json` | JSON schema for the cases output table |
| `tests.schema.json` | JSON schema for the tests output table |

To generate the output data, from the repository root run:

```bash
adtl parse examples/wildlife_surveillance/surveillance_parser.toml examples/wildlife_surveillance/surveillance_data.csv
```

This dataset is small so running in parallel is not necessary, however for large datasets
you should use the `-p` / `--parallel` flag.

---

## The source data

The synthetic source data records animal disease cases reported by field workers. Some cases
are reported more than once as updates arrive (C001 appears in two rows). Numeric
codes stand in for species and sex. Age and weight are recorded in different units
by different field partners.

```
case_id  species  sex  age_years  age_months  weight_kg  weight_lbs  ...
C001     1        M    3                       12.5
C001     (update row — only outcome and outcome_date filled in)
C002     2        F               18                      8.8
C003     1        M    5                                  34.0
...
```

---

## Parser structure

A parser file (like `surveillance_parser.toml`) is a TOML document with three kinds of section.

### 1. The `[adtl]` header

Every parser starts with a metadata block that names the parser, sets global
options, and declares the output tables:

```toml
[adtl]
  name        = "surveillance"
  description = "Wildlife disease surveillance example"
  defaultDateFormat = "%d/%m/%Y"   # source date format, applied globally

  [adtl.tables.cases]              # declares an output table called "cases"
    kind    = "groupBy"
    groupBy = "case_id"
    aggregation = "lastNotNull"

  [adtl.tables.tests]              # declares a second output table called "tests"
    kind          = "oneToMany"
    discriminator = "test_name"
    schema        = "tests.schema.json"
    common        = { case_id = { field = "case_id" } }
```

`kind` is required for every table. The two most common kinds are:

- **`groupBy`** — Wide format. One output row per unique key value; duplicate source rows are
  merged by keeping the last non-null value for each field
- **`oneToMany`** — Long format. One source row expands out into multiple output rows (one per `[[tests]]`-style block); requires a `discriminator` field

### 2. Field mappings — `[cases]`

A single-bracket section (e.g. `[cases]`) lists the fields for a `groupBy` (or
`oneToOne`) table. **The TOML key becomes the output column name; the value
describes where to get it from.**

```toml
[cases]
  case_id     = { field = "case_id" }       # copy from source column "case_id"
  report_date = { field = "report_date", source_date = "%d/%m/%Y" }
  country     = "COL"                       # constant: same value for every row
```

A minimal field mapping needs only `field`:

```toml
species = { field = "animal" }
```
This creates a column `species` in the output `cases` table, and fills it with a copy of the data from the 'animal' column in the source file.

Additional keys modify how the value is read or transformed:

| Key | Purpose |
|-----|---------|
| `field` | Source column name to read from |
| `values` | Map source values to output values: `{ 1 = "dog", 2 = "cat" }` |
| `ref` | Use a reusable definition from `[adtl.defs]` |
| `unit` / `source_unit` | Convert between units (e.g. `"kg"` from `"lbs"`) |
| `apply` | Apply a named transformation function |
| `source_date` | Parse the source value as a date in this format |
| `if` | Only populate this field when a condition is met |
| `can_skip` | Don't error if this column is absent from the source file |

A field mapping can also be a plain string or number (a **constant**), which
writes that value to every row:

```toml
dataset_id = "surveillance-2024"   # same value in every output row
```

### 3. `[[tests]]` blocks — rows for `oneToMany` tables

Double-bracket sections define the output rows for `oneToMany` tables. Each
`[[tests]]` block produces one output row per source row (subject to any `if`
condition). Field mappings inside the block follow exactly the same syntax as
in a single-bracket section:

```toml
[[tests]]
  test_name   = { field = "test1_name" }
  test_result = { field = "test1_result" }
  if.test1_name = { "!=" = "" }    # suppress this row if the test name is blank
```

Multiple `[[tests]]` blocks are all processed for each source row — this is how
one source row becomes many output rows.

---

## Feature walkthrough

### groupBy: collapsing duplicate source rows

The cases table uses `kind = "groupBy"`:

```toml
[adtl.tables.cases]
  kind        = "groupBy"
  groupBy     = "case_id"
  aggregation = "lastNotNull"
```

C001 appears in two source rows — an initial report and a follow-up update:

```
C001, 12/03/2024, dog, male, 3yr, 12.5kg, ..., outcome=2, (no outcome_date)
C001, 15/03/2024, (all other fields blank), outcome=1, outcome_date=15/03/2024
```

With `aggregation = "lastNotNull"`, the output keeps the last non-null value for
each field individually:
- Demographics (`species`, `sex`, `age_years`, …): from row 1 (row 2 is blank)
- `outcome`: from row 2 (recovered)
- `outcome_date`: from row 2 (2024-03-15)
- `report_date`: from row 2 (2024-03-15, the date of the latest update)

The result is one clean, up-to-date row per case:

```
case_id  species  outcome    outcome_date  report_date
C001     dog      recovered  2024-03-15    2024-03-15
```

---

### Reusable definitions

Mappings shared across many fields can be extracted to `[adtl.defs]` and referenced
with `ref = "name"`:

```toml
[adtl.defs]
  yn = { values = { 1 = true, 0 = false } }

```

All 3 symptom fields, e.g. `[cases.has_respiratory_symptom]` then just say `ref = "yn"` rather than repeating the mapping.

---

### combinedType firstNonNull: prefer one field, fall back to another

Some field workers record age in years, others in months. `firstNonNull` tries each
field in order and returns the first non-null result. `unit`/`source_unit` performs
the conversion using [pint](https://pint.readthedocs.io) before the result is returned:

```toml
[cases.age_years]
  combinedType = "firstNonNull"
  fields = [
    { field = "age_years" },
    { field = "age_months", unit = "years", source_unit = "months" },
  ]
```

The same pattern handles weight, where some files have kg and others have lbs:

```toml
[cases.weight_kg]
  combinedType = "firstNonNull"
  fields = [
    { field = "weight_kg" },
    { field = "weight_lbs", unit = "kg", source_unit = "lbs" },
  ]
```

C002 has `weight_lbs = 8.8` and no `weight_kg` → output is `weight_kg = 3.99`.
C003 has `weight_lbs = 34.0` → output is `weight_kg = 15.42`, while all other records
have `weight_kg` filled and therefore do not need conversion.

---

### combinedType any: true if any field is true

`any` is useful when a concept is captured across multiple source fields and you want
to know if any of them are positive:

```toml
[cases.has_respiratory_symptom]
  combinedType = "any"
  fields = [
    { field = "fever_yn",   ref = "yn" },
    { field = "cough_yn",   ref = "yn" },
    { field = "dyspnea_yn", ref = "yn" },
  ]
```

C006 has `fever_yn = 0`, `cough_yn = 0`, `dyspnea_yn = 0` → `has_respiratory_symptom = False`.
All other cases have at least one symptom present → `True`.

---

### combinedType set: collect unique values from multiple columns

`set` collects non-null results from multiple fields into a deduplicated list.
This is useful when a concept (e.g. "comorbidities") is spread across several
boolean columns:

```toml
[cases.conditions]
  combinedType = "set"
  excludeWhen  = "none"
  fields = [
    { field = "cond_diabetes", values = { 1 = "diabetes mellitus" } },
    { field = "cond_obesity",  values = { 1 = "obesity" } },
    { field = "cond_cardiac",  values = { 1 = "cardiac disease" } },
  ]
```

`excludeWhen = "none"` drops null values, so a case with no conditions produces
`[]` rather than `[null, null, null]`.

C001 has `cond_diabetes = 1`, `cond_obesity = 1` → `['diabetes mellitus', 'obesity']`.
C004 has only `cond_obesity = 1` → `['obesity']`.

Use `list` instead of `set` if you want to preserve duplicates (rare in practice).

---

### if (field-level): only populate a field when a condition is met

A field-level `if` returns null for that field when the condition is not met, while
the row itself is still emitted.

Here, `outcome_date` should only be populated for resolved cases (recovered or
euthanised). Cases still under observation have no date yet, and returning null
is correct rather than letting empty source values through:

```toml
[cases.outcome_date]
  field       = "outcome_date"
  source_date = "%d/%m/%Y"
  if.any      = [{ outcome = 1 }, { outcome = 3 }]
```

The `if.any` condition is evaluated against the **raw source row values** (`outcome = 1`
means "source field `outcome` equals `1`"), not the parsed output.

C002 has `outcome = 2` (still under observation) → `outcome_date = null` in the output,
even though the source column exists (it's just empty).
C001 and C003 have `outcome = 1` → `outcome_date` is set.
C008 has `outcome = 3` → `outcome_date` is set.

---

### oneToMany + for loop: expand repeated columns into rows

The source data stores up to three test results per case as parallel column sets:
`test1_name`/`test1_result`, `test2_name`/`test2_result`, `test3_name`/`test3_result`.

Rather than writing three identical `[[tests]]` blocks, a `for` loop expands one
block by substituting the variable `{n}` into field names:

```toml
[adtl.tables.tests]
  kind          = "oneToMany"
  discriminator = "test_name"
  schema        = "tests.schema.json"
  common        = { case_id = { field = "case_id" }, report_date = { field = "report_date" } }

[[tests]]
  test_name   = { field = "test{n}_name" }
  test_result = { field = "test{n}_result" }
  if."test{n}_name" = { "!=" = "" }
  for.n.range = [1, 3]
```

This produces three blocks (n = 1, 2, 3), each mapping the corresponding column pair.

`common` adds `case_id` and `report_date` to every row without repeating them.

### if (row-level): suppress empty test slots

The row-level `if` condition prevents the block from emitting a row at all when the
condition fails. Here it stops entries being created if the test name is not present:

```toml
if."test{n}_name" = { "!=" = "" }
```

C004 has no tests at all → no rows in the tests output.
C002 has one test → one row (n=1 only).
C005 has two test results, but only one is named (test 1, PCR). test 2 has a positive result recorded but no name associated, so is not included in the transformed data.

**Note:** the `!=` operator checks for a non-empty string. Other comparison operators
are `<`, `>`, `<=`, `>=`. Logical combinations use `if.all`, `if.any`, `if.not`.
You must provide a schema (as done here with `tests.schema.json`) to make use of this function.

---

## Schema validation and correcting errors

Both output tables have a JSON schema (`cases.schema.json` and `tests.schema.json`).
When a row fails validation, adtl adds `adtl_valid = False` and a description of
the problem in `adtl_error` to that row.

### The deliberate errors in this example

The `severity` field is passed through from the source without any value mapping:

```toml
[cases.severity]
  field = "severity"
```

The schema allows only `"mild"`, `"moderate"`, or `"severe"`. Three source rows
contain values that don't conform:

| Case | Source value | Problem |
|------|-------------|---------|
| C004 | `MILD` | Wrong capitalisation |
| C006 | `mod` | Abbreviated word |
| C008 | `critical` | Not a valid enum value |

Running the parser shows this immediately in the terminal summary:

```
|table  |valid|total|percentage_valid|
|-------|-----|-----|----------------|
|cases  |5    |8    |62.500000%      |
|tests  |14   |14   |100.000000%     |
```

### Inspecting which rows failed

Load the output and filter for invalid rows to see the exact errors:

```python
import pandas as pd

cases = pd.read_csv("surveillance-cases.csv")
print(cases[~cases.adtl_valid][["case_id", "severity", "adtl_error"]])
```

Output:
```
 case_id  severity                                             adtl_error
    C004      MILD  data.severity must be one of ['mild', 'moderate', 'severe']
    C006       mod  data.severity must be one of ['mild', 'moderate', 'severe']
    C008  critical  data.severity must be one of ['mild', 'moderate', 'severe']
```

A useful summary across many rows:

```python
print(cases[~cases.adtl_valid]["adtl_error"].value_counts())
```

### Fixing the parser

The root cause is inconsistent source data: different field workers recorded
severity using different capitalisation and terminology. As an exercise, using the information in this example and in the extended [specification](../../specification.md) file, see if you can edit the `severity` field to get a 100% validation rate.

<details>

<summary>Solution tips</summary>

**Option 1 — `caseInsensitive`**: handles capitalisation differences automatically.
Add it to the field mapping:

```toml
[cases.severity]
  field           = "severity"
  caseInsensitive = true
```

This fixes C004 (`MILD` → `mild`), but not C006 and C008 — `caseInsensitive` only changes how source keys are
matched; it doesn't remap an unrecognised term to a valid one.

**Option 2 — explicit `values` mapping**: handles both problems at once and makes
the parser's intent explicit:

```toml
[cases.severity]
  field  = "severity"
  values = {
    mild     = "mild",
    MILD     = "mild",
    Mild     = "mild",
    moderate = "moderate",
    Moderate = "moderate",
    severe   = "severe",
    critical = "severe",
  }
```

This approach is more verbose, but fixes all the errors present. Every variant seen in the
source is explicitly mapped to a canonical output value. However, if your source is updated and a severity variant is added which **isn't** in the `severity` mapping, the new variant will silently be ignored and passed through as `null`. The schema requires the field to be filled so an error will be raised in this
case, but it's not as robust as it could be.

**Solution - mix and match**
The best option is to mix both options 1 & 2, and use an additional [`ignoreMissingKey`](../../specification.md#field-with-value-mapping) flag.
```toml
[cases.severity]
  field = "severity"
  caseInsensitive = true
  ignoreMissingKey = true
  values = { mod = "moderate", critical = "severe" }
```
This is less verbose that option 2, but still covers all the errors while defending against new ones.

`caseInsensitive` will allow `MILD` to be matched to `mild`; the mapped values `mod` and `critical` will be transformed to the appropriate enums, and any values not mapped in the `values` section (which we know already match the schema) will be added as-is by the `ignoreMissingKey` option.
</details>

---

## Further reading

- [ADTL specification](../../specification.md) — full reference for all mapping rules
- [Concepts](../../getting_started/concepts.md) — mental model: source → parser → schema
- [Troubleshooting](../../getting_started/troubleshooting.md) — common errors
