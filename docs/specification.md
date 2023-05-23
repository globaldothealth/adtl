# Specification format

The specification file describes the field mappings from the source file to the
target schema. The format is under development and expected to change.

Specification files can be in TOML or JSON, with TOML preferred due to readability.

Each specification file can refer to one or more tables, which are
created in parallel from one source file.

[JSON schema](../schemas/dev.schema.json): This is a partial JSON schema to validate
adtl parser files.

## Metadata

These metadata fields are defined under a header key `adtl`.

### Required fields

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
  * *schema* (optional): Specifies JSON schema to use for validation,
    can be a relative path, or a URL
  * *common* (optional): Specifies common mappings that are applied to every if-block
    in a *kind*=*oneToMany* table.
  * *optional-fields* (optional): Specifies list of fields that are ordinarily required
    under the schema, but are considered optional for this parser.

### Optional fields

* **defs**: Definitions that can be referred to elsewhere in the schema
* **include-def** (list): List of additional TOML or JSON files to import as
  definitions
* **skipFieldPattern** : Regex string matching field names which may be skipped
if not present in a datafile, following the same syntax as `fieldPattern` key.
* **defaultDateFormat**: Default source date format, applied to all fields
  with either "date_" / "_date" in the field name or that have format date
  set in the JSON schema

## Validation

adtl supports validation using [JSON
Schema](https://json-schema.org/draft-07/json-schema-core.html), upto draft-07
of the specification. Validation is performed using
[fastjsonschema](https://github.com/horejsek/python-fastjsonschema).

adtl does not raise errors on validation issues. Instead two special columns are
added to each table that has an associated schema:

* `adtl_valid` (boolean): True if row is valid according to JSON schema, False otherwise
* `adtl_error` (string): Validation error message returned by fastjsonschema

## References

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

Often some definitions are repeated across files. adtl supports including
definitions from external files using the *include-def* keyword under the
`[adtl]` section. As an example, a mapping of country codes to country names
could be stored in `countries.toml`:

```toml
[countryMap.values]
1 = "ALB"
2 = "ZZZ"
# and so on
```

This could be included in adtl, and used as a reference just as if it was
included in the TOML file directly:

```toml
[adtl]
include-def = ["countries.toml"]

# ...

[cases.country_iso3]
field = "country"
ref = "countryMap"
```

Definition files can also be included from the command line by passing the
`--include-def` flag to adtl. This is useful when the included file can change
from one run to another, or in cases where the definitions/mappings are located
externally. The following would produce an equivalent result to the
`include-def` assignment in the above example, assuming `data.csv` is the source
data file:

```shell
adtl parser.toml data.csv --include-def countries.toml
```

## Table mappings

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

### Constant

Every value in the table is the same constant value

```ini
country_iso3 = "GBR"
```

### Field

Maps to a single field from the source format

```ini
[table.date_death]  # specifies that date_death is under table named 'table'
field = "flw_date_death"
description = "Date of death"
```

### Field with conditional

Maps to a single field from the source format only if condition(s) are met. The
value is set to *null* if the condition fails.

```ini
field = "foobar"
if = { foobar_type = 4 }
```

Operations other than equals can be specified as `{ field_name = {op = value} }`
where *op* is one of `< | > | <= | >= | !=`. Logical operations (and, or, not) are
supported with `any = [ condition-list ]` (or), `all = [ condition-list ]` (and),
`not = { condition }` (not).
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

The **oneToMany** table has default conditional behaviour so that rows are only shown
if the row is not empty, and contains values which can be mapped correctly if maps are
provided. For example, an observation recording the presence/absence of vomiting should only
be shown if values map to True/False:

```ini
[[table]]
  name = "vomiting_nausea"
  is_present = { field = "Admission Symptoms.Vomiting", values = {1 = True, 0 = False} } # values = ['0', 'Unknown', '1', 'UNKNOWN', '']
  # if.any = [{ "Admission Symptoms.Vomiting" = '1'}, { "Admission Symptoms.Vomiting" = '0'}] <- rule assumed by adtl
```

If a different/more specific conditional statement is required, e.g. if a row should only be displayed
based on the condition of a different field, this behaviour can be overridden by writing an
if condition into the parser; note that this will *stop any automated generation*, you should
specify all conditions under which the row should be displayed, for example:

```ini
[[observation]]
  name = "transfer_from_other_facility"
  phase = "study"
  date = { field = "rpt_date" }
  if = { rpt_date = { "!=" = "" } } # This is dependent on a date rather than an is_present field, so requires specifying.
  is_present = true
```

### Field with unit

Often values need to be normalised to a particular unit.
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

### Field with date

Normalising date formats is a common transformation.
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

### Field with value mapping

Same as **Single field**, but with an extra `values` key that describes the
mapping from the values to the ones in the schema. This covers boolean fields,
with the mappings being to `true` | `false` | `null`.

```ini
[table.sex_at_birth]
field = "sex"
values = { 1 = "male", 2 = "female", 3 = "non_binary" }
description = "Sex at Birth"
```

Example with boolean values

```ini
[table.has_dementia]
field = "dementia_mhyn"
values = { 1 = true, 2 = false }
description = "Dementia"
```

### Combined type

Refers to multiple fields in the source format. Requires
a `combinedType` attribute specifying the combination criteria, and
a `fields` attribute which a list of fields that will be combined.
Accepted values for `combinedType` are:

* *any* - Whether any of the fields are non-null (truthy)
* *all* - Whether all of the fields are non-null (truthy)
* *min* - Minimum of non-null fields
* *max* - Minimum of non-null fields
* *firstNonNull* - First in the list of fields that has a non-null value
* *list* - List of various fields
* *set* - List of various fields, with duplicates removed

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

**excludeWhen**: List and Set fields can have an optional *excludeWhen* key which can either be a list of values or `none` or `false-like`. When it is `none` we drop the null values (None in Python) or it can be `false-like` in which case false-like values (`bool(x) == False` in Python) are excluded (empty lists, boolean False, 0). Alternatively a list of values to be excluded can be provided.

If *excludeWhen* is not set, no exclusions take place and all values are returned as-is.

### Skippable fields

In some cases, a study will be assocaited with multiple data files, all of which have been
filled in to varying degrees. For example, one study site may not provide any follow-up data.

Rather than writing a new parser for every data file with minor differences, parsers can be made
robust to a certain amount of missing data by tagging applicable fields with `can_skip = True`,
for example:

```ini
[[observation]]
  name = "cough"
  phase = "admission"
  date = { field = "admit_date" }
  is_present = { field = "cough_ceoccur_v2", description = "Cough", ref = "Y/N/NK", "can_skip" = true }
```

In this case, if adtl does not find `cough_ceoccur_v2` in the data it will skip over the field
and continue, rather than throwing an error.

If there are lots of fields missing all with similar field names, for example if followup data
has been omitted and all the followup fields are labelled with a `flw` prefix e.g., `flw_cough`,
`flw2_fatigue`, this can be specified at the top of the file:

```ini
[adtl]
  name = "isaric-core"
  description = "isaric-core"
  skipFieldPattern = "flw.*"

[table.sex_at_birth]
  combinedType = "firstNonNull"
  excludeWhen = "none"
  fields = [
    { field = "sex", values = { 1 = "male", 2 = "female" } },
    { field = "flw_sex_at_birth", values = { 1 = "male", 2 = "female", 3 = "non_binary" } },
    { field = "flw2_sex_at_birth", values = { 1 = "male", 2 = "female", 3 = "non_binary" } },
  ]
```

Notice that in this case `can_skip` does not need to be added to the fields with a `flw` prefix.

### Data transformations (apply)

Arbitrary functions can be applied to source fields. adtl ships with a library
found in the `transformations.py` file, but users may add their own. Parameters
other than the source field which need to be parsed into the transformation
function must be listed as `params`, in the same order as they should be
passed to the transformation function.

If the parameter is a field attribute value from the source data, the field name
should be prefixed with a `$` to distinguish it from constant strings.

```ini
[[table]]
  field = "icu_admitted"
  apply = { function = "isNotNull" }

[[table]]
  field = "brthdtc"
  apply = { function = "yearsElapsed", params = ["$dsstdat"] }

```

### Conditional rows

For the *oneToMany* case, each row in the source file generates
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

### Repeated rows

Often, oneToMany tables (such as ISARIC observation table) have repeated blocks,
with only the field name and condition changing. Add a `for` keyword that will
add looping through variable(s). In the case of multiple variables being
provided, the cartesian product of the variables will be used to repeat the
block.

Field names within the block use the Python f-string syntax to represent the
variable, which is expanded out by using Python's `str.format`.

Example from an ISARIC dataset that contains five followup surveys that ask
about observed symptoms after discharge:

```toml
[[observation]]
  name = "history_of_fever"
  phase = "followup"
  date = { field = "flw2_survey_date_{n}" }
  is_present = { field = "flw2_fever_{n}", values = { 0 = false, 1 = true } }
  if.not."flw2_fever_{n}" = 2
  for.n.range = [1, 5]  # n goes from 1--5 inclusive
  # for.n = [1, 3, 5]  # can also specify a list
```

Note that **unlike** Python ranges, adtl ranges include both start and end of
the range.

Variable interpolations in braces can be anywhere in the block. So a `if.any`
condition could look like

```toml
[[observation]]
  name = "history_of_fever"
  phase = "followup"
  date = { field = "flw2_survey_date_{n}" }
  is_present = { field = "flw2_fever_{n}", values = { 0 = false, 1 = true } }
  if.any = [ { "flw2_fever_{n}" = 1 }, { "flw2_fever_{n}" = 0 } ]
  for.n.range = [1, 5]  # n goes from 1--5 inclusive
```

Multiple variables are supported in the for loop. If multiple variables are
specified, then the block is repeated for as many instances as the [Cartesian
product](https://en.wikipedia.org/wiki/Cartesian_product) of the values the
variables correspond to. As an example the for expression

```toml
for = { x = [1, 2], y = [3, 4] }
```

will loop over the values `x, y = [(1, 3), (1, 4), (1, 3), (1, 4)]`, and a block
with such a loop referring to both variables will get repeated four times:

```toml
[[observation]]
  field = "field_{x}_{y}"
  if."field_{x}_{y}" = 1
  for = { x = [1, 2], y = [3, 4] }
```

will get expanded as

```toml
[[observation]]
  field = "field_1_3"
  if."field_1_3" = 1

[[observation]]
  field = "field_1_4"
  if."field_1_4" = 1

[[observation]]
  field = "field_2_3"
  if."field_2_3" = 1

[[observation]]
  field = "field_2_4"
  if."field_2_4" = 1
```
