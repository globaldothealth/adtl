# Specification format

The specification file describes the field mappings from the source file to the
target schema. The format is under development and expected to change.

Specification files can be in TOML or JSON, with TOML preferred due to readability.

Each specification file can refer to one or more tables, which are
created in parallel from one source file.

## metadata

**Required fields**. These metadata fields are defined under a header key `adtl`.

* **name**: Name of the specification, usually the source data name in
  lowercase and hyphenated. By convention, this is the same name as the
  specification file.
* **description**: Description of the specification
* **tables**: Dictionary with keys as names of tables that are
  mapped from the source file. Each table key contains a dictionary
  with the following optional keys:

  * *kind*: If this is set to *groupBy* the parser will group
    rows together according to the *groupBy* key. The other
    allowed value is *oneToMany* when multiple rows are
    generated from the same row.
  * *groupBy*: Attribute(s) to group by
  * *aggregation*: Aggregation type. Currently only one
    type*lastNotNull* is supported which sets a particular
    attribute to the last non-null value in the grouped dataset.

* **defs**: Definitions that can be referred to elsewhere in the schema

## references

Often, a part of the schema is repeated, and it is better to
[avoid repeated code](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself). adtl
supports references anywhere a dictionary or object is allowed using `ref = "someReference"`.

This would require a `someReference` key within the top-level definitions section:

```ini
[adtl]
name = "parser"

[adtl.tables]
someTable = { groupBy = "subjid", aggregation = "lastNotNull" }

[adtl.defs]
someReference = { values = { 1 = true, 2 = false } }
```

## table mappings

Each table has its associated field mappings under a key of the same
name, so there should be a top level `[table]` section.
Within the table dictionary, keys are **fields / attributes** in the schema. Values are **rules**
that describe the mapping from the source data format. There are several valid
rule patterns, listed below. Each rule will either have a `field` attribute
that is the corresponding field in the source format, or a `combinedField`
attribute which links multiple fields in the source format, and specifies how
the fields should be combined. Fields can be marked as privacy sensitive using
`sensitive = true`, which can be used by the parser to take additional steps,
such as hashing the field.

* **Constant**: Every value in the table is the same constant value

  ```ini
  country_iso3 = "GBR"
  ```

* **Single field**: Maps to a single field from the source format

  ```ini
  [table.date_death]  # specifies that date_death is under table named 'table'
  field = "flw_date_death"
  description = "Date of death"
  ```

* **Single field with conditional**: Maps to a single field from the source format
  only if condition(s) are met. The value is set to *null* if the condition fails.

  ```ini
  field = "foobar"
  if = { foobar_type = 4 }
  ```

  Operations other than equals can be specified as `{ field_name = {op = value} }`
  where *op* is one of `< | > | <= | >= | !=`. Logical operations (and, or) are
  supported with `any = [ condition-list ]` (or) and `all = [ condition-list ]` (and).
  In the above example, if we wanted to set from field *foobar* only if
  *foobar_type* is 4 and *bazbar* < 5. For simplicity, the equals operation is optional,
  and adtl allows conditions of the form `{ field_name = value }`:

  ```ini
  field = "foobar"
  if.all = [  # in TOML this is a nested key, like { "if": { "all": [ ... ] } }
    { foobar_type = 4 },
    { bazbar = { "<" = 5 }}
  ]
  ```

* **Single field with unit**: Often values need to be normalised to a particular unit.
  This can be done by setting `source_unit` and `unit` attributes on a field. The
  [pint](https://pint.readthedocs.io) library is used, so the units should be in a format
  that pint understands. Generally pint works well with
  [most common units](https://github.com/hgrecco/pint/blob/master/pint/default_en.txt).
  The `source_unit` field can also be a rule, but `unit` must be a string. For example,
  to set the age based on a field called `age_unit` which can be months or years:

  ```ini
  field = "age_estimate"
  source_unit = { field = "age_estimateunit", values = { 1 = "months", 2 = "years" }}
  unit = "years"
  ```

* **Single field with date**: Normalising date formats is a common transformation.
  The date format in the source file is indicated in the `source_date` key (which
  can itself refer to a field, like `source_unit`), and the date format to be
  transformed to is indicated in the `date` field. By default, if `date` is not
  specified, it defaults to ISO 8601 date format `%Y-%m-%d`.

  Date formats are specified in [strftime(3)](http://man.openbsd.org/strftime) format.

  ```ini
  field = "outcome_date"
  source_date = "%d/%m/%Y"
  date = "%Y-%m-%d"
  ```

* **Single field with mapping**: Same as **Single field**, but with an extra
  `values` key that describes the mapping from the values to the ones in the
  schema. This covers boolean fields, with the mappings being to `true` | `false` | `null`.

  ```ini
  [table.sex_at_birth]
  field = "sex"
  values = { 1 = "male", 2 = "female", 3 = "non_binary" }
  description = "Sex at Birth"
  ```

  ```ini
  [table.has_dementia]
  field = "dementia_mhyn"
  values = { 1 = true, 2 = false }
  description = "Dementia"
  ```

* **Combined type**: Refers to multiple fields in the source format. Requires
  a `combinedType` attribute specifying the combination criteria, and
  a `fields` attribute which a list of fields that will be combined.
  Accepted values for `combinedType` are:

  * *any* - Whether any of the fields are non-null (truthy)
  * *all* - Whether all of the fields are non-null (truthy)
  * *firstNonNull* - First in the list of fields that has a non-null value
  * *list* - List of various fields

  A combinedType can have multiple fields within a `fields` key, or can specify
  multiple fields with a `fieldPattern` key which is a regex that is matched to the
  list of fields:

  ```ini
  [table.has_liver_disease]
  combinedType = "list"
  fields = [
    { fieldPattern = ".*liv.*", values = { 1 = true, 0 = false }}
  ]
  ```

  Example of a `combinedType = "any"` mapping:

  ```ini
  [table.has_liver_disease]
  combinedType = "any"
  fields =  [
    { field = "modliv", description = "Moderate liver disease", values = { 1 = true, 0 = false }},
    { field = "mildliver", description = "Mild liver disease", values = { 1 = true, 0 = false }},
  ]
  ```

  **excludeWhen**: List fields can have an optional *excludeWhen* key which can either be a list of values or `none` or `false-like`. When it is `none` we drop the null values (None in Python) or it can be `false-like` in which case false-like values (`bool(x) == False` in Python) are excluded (empty lists, boolean False, 0). Alternatively a list of values to be excluded can be provided.

  If *excludeWhen* is not set, no exclusions take place and all values are returned as-is.

* **Conditional rows**: For the *oneToMany* case, each row in the source file generates
  multiple rows for the target. This is expressed in the specification by making the
  value corresponding to the table key a list instead of an object. Additionally
  an `if` key sets the condition under which the row is emitted.

  ```ini
  [[table]]
  date = { field = "dsstdtc" }
  name = "headache"
  if = { headache_cmyn = 1 }

  [[table]]
  date = { field = "dsstdtc" }
  name = "cough"
  if = { cough_cmyn = 1 }
  ```

* **Data transformations using `apply`**: Arbitrary functions can be applied to source fields. adtl ships with a
  library found in the `transformations.py` file, but users may add their own. Parameters other than the source field
  which need to be parsed into the transformation function must be listed as `apply_params`, in the same order as they
  should be passed to the transformation function.

  ```ini
  [[table]]
    field = "icu_admitted"
    apply = { function = "isNotNull" }
  
  [[table]]
    field = "brthdtc"
    apply = { function = "yearsElapsed", params = ["dsstdat"] }

  ```
