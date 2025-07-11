# Write a Data Parser

ADTL requires a [TOML](https://toml.io/en/) specification file which describes how raw
data should be converted into a new format, on a field-by-field basis. Every unique data
file format (i.e. unique sets of fields and data types) should have a corresponding
parser file.

AutoParser exists to semi-automate the process of writing new parser files. This requires
a data dictionary (which can be created if it does not already exist, see '[Create Data dictionary](data_dict)'),
and the JSON schema(s) for the target table format(s).

Parser generation is a 2-step process.

## Generate intermedaite mappings (CSV)
First, an intermediate mapping file is created which can look like this, for a wide-format table:

| target_field      | source_description | source_field     | common_values                                            | target_values                                              | value_mapping                                                                            |
|-------------------|--------------------|------------------|----------------------------------------------------------|------------------------------------------------------------|------------------------------------------------------------------------------------------|
| identity          | Identity           | Identité         |                                                          |                                                            |                                                                                          |
| name              | Full Name          | Nom complet      |                                                          |                                                            |                                                                                          |
| loc_admin_1       | Province           | Province         | Equateur, Orientale, Katanga, Kinshasa                   |                                                            |                                                                                          |
| country_iso3      |                    |                  |                                                          |                                                            |                                                                                          |
| notification_date | Notification Date  | DateNotification |                                                          |                                                            |                                                                                          |
| classification    | Classification     | Classicfication  | FISH, amphibie, oiseau, Mammifère, poisson, REPT, OISEAU | mammal, bird, reptile, amphibian, fish, invertebrate, None | mammifère=mammal, rept=reptile, fish=fish, oiseau=bird, amphibie=amphibian, poisson=fish |
| case_status       | Case Status        | StatusCas        | Vivant, Décédé                                           | alive, dead, unknown, None                                 | décédé=dead, vivant=alive                                                                |

`target_x` refers to the desired output format, while `source_x` refers to the raw data.
In this example, the final row shows that the `case_status` field in the desired output
format should be filled using data from the `StatusCas` field in the raw data. The `value_mapping`
column indicated that all instances of `décédé` in the raw data should be mapped to `dead`
in the converted file, and `vivant` should map to `alive`.

:::{warning}
**LLM's are prone to errors and hallucinations**. These intermediate mappings
should be manually curated, as the LLM may generate incorrect matches for either
the field, or the values within that field.
:::

If your desired format has multiple tables, one mapping file should be produced for each
table. A similar process is followed for long-format targets, but instead of 'target_field'
as the table index, 'source_field' is used and any source fields which cannot be mapped
to a provided variable will be left blank for the user to either map manually, or delete if
that data is not required.

Currently, all long-format schemas *must* provide a list of enums for the field denoted as the
'variable' column.

## Generate TOML

This step is automated and should produce a TOML file that conforms to the adtl parser
schema, ready for use transforming data.

## API

```{eval-rst}
.. autofunction:: adtl.autoparser.create_mapping
    :noindex:

.. autofunction:: adtl.autoparser.create_parser
    :noindex:
```
