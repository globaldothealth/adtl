---
title: Troubleshooting
---
# Troubleshooting

## Checking your parser before a full run

Use `adtl check` to catch common problems early:

```bash
adtl check parser.toml              # validates spec structure only
adtl check parser.toml data.csv     # also cross-checks field names against data
```

This reports:
- **Errors** — fields referenced in the spec that don't exist in the data file (the parse will fail or silently skip)
- **Warnings** — fields in the data that have no mapping in the spec (data you might be missing)

---

## Common errors

### `KeyError` or missing field at parse time

**Symptom:** adtl raises an error about a field not being found, or produces empty columns.

**Cause:** A `field = "name"` in the spec refers to a column that doesn't exist in the source CSV,
either because of a typo or because the column has a different name in this data file.

**Fix:** Run `adtl check parser.toml data.csv`. Fix any fields listed as absent from the data.
If a field is legitimately absent in some data files (e.g. follow-up data), mark it with
`can_skip = true` or use `skipFieldPattern` in the `[adtl]` block.

---

### Validation failures (`adtl_valid = False`)

**Symptom:** Output rows have `adtl_valid = False` and an error message in `adtl_error`.

**Cause:** The parsed output doesn't match the target JSON schema — e.g. a required field is
null, a value is outside the allowed enum, or a type mismatch.

**Fix:**
1. Look at the `adtl_error` column for the specific validation message.
2. Common causes:
   - A value mapping (`values = { ... }`) doesn't cover all values in the data — add the missing
     entries or set `ignoreMissingKey = true` to pass unmatched values through.
   - A required field is always null because the source field name is wrong — check the mapping.
   - A numeric field contains non-numeric strings — check whether `returnUnmatched = true` in
     the spec is causing type mixing.
3. To investigate interactively, load the output CSV and filter for `adtl_valid == False`:
   ```python
   import pandas as pd
   df = pd.read_csv("parser-table.csv")
   print(df[df.adtl_valid == False][["adtl_error"]].value_counts())
   ```

---

### Value mapping returns null unexpectedly

**Symptom:** A field that should have a value is null in the output, but the source data
has a non-empty value.

**Cause:** The source value doesn't match any key in the `values` mapping.

**Fix:**
- Check for case or whitespace differences. Add `caseInsensitive = true` to the rule if the
  data has inconsistent capitalisation.
- Set `ignoreMissingKey = true` if you want unmatched values passed through as-is.
- Use `returnUnmatched = true` at the top-level `[adtl]` block to return all unmatched values
  across the whole parser (note: this may cause type inconsistencies in columns).

---

### `oneToMany` table produces no rows

**Symptom:** A `oneToMany` table is empty even though the source data has values.

**Cause:** adtl automatically suppresses `oneToMany` rows where all mapped fields are null
(e.g. no value mapped successfully). If an `if` condition is also specified, it overrides
the automatic logic entirely — so if the condition is wrong, no rows will pass.

**Fix:**
- Check that at least one field in each `[[table]]` block has a non-null value for a typical
  source row.
- If using a custom `if` condition, verify it matches the actual values in the data (use
  `adtl check` or inspect the source CSV directly).
- Temporarily remove the `if` condition to confirm rows are produced without it.

---

### Date fields produce null or incorrect output

**Symptom:** Date columns are null or in the wrong format.

**Cause:** The `source_date` format string doesn't match the format in the data.

**Fix:** Set `source_date` to the correct [strftime format](http://man.openbsd.org/strftime)
for your data. For example, `%d/%m/%Y` for `31/01/2023` or `%Y%m%d` for `20230131`.
You can also set a `defaultDateFormat` in the `[adtl]` block to apply to all date fields.

---

### `adtl` command not found

**Symptom:** `adtl: command not found` when running from the terminal.

**Fix:** The adtl script was not added to your PATH during installation.
Either install with `pipx` (which handles PATH automatically) or run as a module:
```bash
python3 -m adtl parse parser.toml data.csv
```

---

### Parquet output fails with type errors

**Symptom:** `adtl parse --parquet` raises an error about inconsistent column types.

**Cause:** Parquet requires a consistent type down each column. If `returnUnmatched = true`
is set in the spec, columns that normally hold mapped values may also contain raw strings,
causing a type conflict.

**Fix:** Remove `returnUnmatched = true` from the spec, or don't use `--parquet`.
Identify which columns have mixed types and fix their mappings so values are consistently
typed.
