# Creating a Data Dictionary

## Motivation

A data dictionary is a structured guide which contains the details of a data file.
It should contain, at minimum, a list of field/column names, and some kind of description
of what data each field holds. This often takes the form of a textual description, plus
a note of the data type (text, decimals, date, boolean...) and/or a set of expected values.

A data dictionary is required by AutoParser for [parser generation](parser_generation).
This is to avoid having to send potentially sensitive or confidential data to an external
body (in this case an externally hosted LLM hosted); instead a *decription* of what the
data looks like from the dictionary can be sent to the LLM, which allows for mapping to
occur without risking the unintentional release of data.

Many data capture services such as [REDCap](https://projectredcap.org/) will generate
a data dictionary automatically when surveys are set up. AutoParser can read in these
dictionaries, and format them for onward use.

However, where data is being
captured either rapidly, or by individuals/small teams, a formal data dictionary may not
have been created for a corresponding dataset. For this scenario, AutoParser provides
functionality to generate a simple dictionary based on your data. This dictionary can
then be used in other AutoParser modules.

## Create a basic data dictionary
AutoParser will take your raw data file and create a basic data dictionary. For an example
dataset of animals, a generated data dictionary looks like this:

| source_field      | source_description | source_type | common_values                                            |
|-------------------|--------------------|-------------|----------------------------------------------------------|
| Identité          |                    | string      |                                                          |
| Province          |                    | choice      | Equateur, Orientale, Katanga, Kinshasa                   |
| DateNotification  |                    | string      |                                                          |
| Classicfication   |                    | choice      | FISH, amphibie, oiseau, Mammifère, poisson, REPT, OISEAU |
| Nom complet       |                    | string      |                                                          |
| Date de naissance |                    | string      |                                                          |
| AgeAns            |                    | number      |                                                          |
| AgeMois           |                    | number      |                                                          |
| Sexe              |                    | choice      | F, M,   f, m, f, m     , inconnu                         |

`source_field` contains each column header from the source data, and `source_type` shows the
data type in each column. 'common_values' denotes where a small set of strings have been frequently detected,
so AutoParser assumes that specific terminology is being used, and lists them in `common_values`.

Notice that the `source_description` column is empty. This is done by default, so the
user can add in a short text description *in English* (as this column is read by the LLM
in later steps and assumes the text is written in English). For example, the description
for the `AgeMois` column might be 'Age in Months'.

If instead you would like to auto-generate these descriptions, AutoParser can use an LLM
to automate this step. Note, we strongly encourage all users to check the results of the
auto-generated descriptions for accuracy before proceeding to use the described data dictionary
to generate a data parser.

## Validating existing data dictionaries

AutoParser will attempt to convert and validate your data dictionary when provided at
the mapping and parser validation stage. To enable this to function, you must edit the
`config.toml` file you provide to match the columns in your existing data dictionary to
those AutoParser expects. For example, data provided like this:

```toml
[column_mappings]
  source_field = "Variable / Field Name"
  source_type = "Field Type"
  source_description = "Field Label"
  choices = "Choices, Calculations, OR Slider Labels"
```
indicates that your data dictionary contains columns 'Variable / Field Name' etc.

You can indicated the data format in each column in one of two ways:
1. With a `common_values` column like the one in the example above. This assumes all your
data is stored as human-readable direct values in the dataset you wish to convert. E.g.
in the example above, the 'Classification' column contains the animal's class expressed
literally. This is more common in small datasets which have been manually curated.
2. With a `choices` column. This assumes you have a coded dataset, where data is stored in
numerical or symbolic codes and comes with an accompanying lookup-table (usually a column
in the data dictionary) which defines what each code means. An example of this style of data dictionary:

| source_field      | ... | choices                                                  |
|-------------------| ... |----------------------------------------------------------|
| Identité          | ... |                                                          |
| Province          | ... | 1 = Equateur, 2 = Orientale, 3 Kinshasa                  |
| DateNotification  | ... |                                                          |
| Classicfication   | ... | 1 = Amphibie, 2 = Oiseau, 3 = Mammifère, 4 = Poisson, 5 = Reptile |
| Nom complet       | ... |                                                          |
| Date de naissance | ... |                                                          |
| AgeAns            | ... |                                                          |
| AgeMois           | ... |                                                          |
| Sexe              | ... | 1 = female, 2 = male, 3 = inconnu                        |

This is more common with large datasets and those generated by online surveys such as REDCap.

You should have exactly ONE of these two columns in your data dictionary for use with AutoParser.

## API

```{eval-rst}
.. autofunction:: adtl.autoparser.create_dict

.. autofunction:: adtl.autoparser.generate_descriptions
```
