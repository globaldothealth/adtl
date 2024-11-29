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

Many data capture services such as [REDCaP](https://projectredcap.org/) will generate
a data dictionary automatically when surveys are set up. However, where data is being
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
data type in each column. 'choice' denotes where a small set of strings have been detected,
so AutoParser assumes that specified terms are being used, and lists them in `common values`.

Notice that the `source_description` column is empty. This is done by default, so the
user can add in a short text description *in English* (as this column is read by the LLM
in later steps and assumes the text is written in English). For example, the description
for the `AgeMois` column might be 'Age in Months'.

If instead you would like to auto-generate these descriptions, AutoParser can use an LLM
to automate this step. Note, we strongly encourage all users to check the results of the
auto-generated descriptions for accuracy before proceeding to use the described data dictionary
to generate a data parser.

## API

```{eval-rst}
.. autofunction:: adtl.autoparser.create_dict

.. autofunction:: adtl.autoparser.generate_descriptions
```
