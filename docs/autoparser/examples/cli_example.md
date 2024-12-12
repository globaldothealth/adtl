# CLI Parser construction

This file describes how to run the same parser generation pipeline as described in the
[parser construction](example) notebook, but using the command line interface. It
constructs a parser file for an `animals.csv` file of test data, and assumes all commands
are run from the root of the `autoparser` package.

Note: As a reminder, you will need an API key for OpenAI or Google. This example uses the OpenAI LLM.

## Generate a data dictionary
In this example, we will generate a data dictionary with descriptions already added in one step. The CLI command follows this syntax:


```bash
adtl-autoparser create-dict data language [-d] [-k api_key] [-l llm_choice] [-c config_file] [-o output_name]
```
where the `-d` flag is used to request the LLM-generated descriptions. For the
`animal_data.csv` data we will run this command to generate a data dictionary
with descriptions

```bash
adtl-autoparser create-dict tests/test_autoparser/sources/animal_data.csv "fr" -d -k $OPENAI_API_KEY -c tests/test_autoparser/test_config.toml -o "animal_dd"
```
This creates an `animals_dd.csv` data dictionary to use in the next step.

## Create intermediate mapping file
The next step is to create an intermediate CSV for you to inspect, mapping the fields and values in the raw data to the target schema. This is the CLI syntax:

```bash
adtl-autoparser create-mapping dictionary schema language api_key [-l llm_choice] [-c config_file] [-o output_name]
```
so we can run
```bash
adtl-autoparser create-mapping animal_dd.csv tests/test_autoparser/schemas/animals.schema.json "fr" $OPENAI_API_KEY -c tests/test_autoparser/test_config.toml -o animal_mapping
```
to create the intermediate mapping file `animal_mapping.csv` for you to inspect for any errors.

## Write the parser file
Finally, the parser file for ADTL should be written out based on the contents of `animal_mapping.csv`. Once you've mande any changes to the mapping you want, we can use the `create_parser` command

```bash
adtl-autoparser create-parser mapping schema_path [-o output_parser_name] [--description parser_description] [-c config_file]
```
as
```bash
adtl-autoparser create-parser animal_mapping.csv tests/test_autoparser/schemas -o animal_parser -c tests/test_autoparser/test_config.toml
```
which writes out the TOML parser as `animal_parser.toml` ready for use in ADTL.
