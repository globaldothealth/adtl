# Getting started

## Installation

AutoParser is a Python package that can either be built into your code or run as a
command-line interface (CLI). You can install AutoParser using pip:

```bash
  python3 -m pip install adtl[autoparser]
```

Note that it is usually recommended to install into a virtual environment. We recommend using [uv](https://github.com/astral-sh/uv) to manage the virtual environment. To create and active a virtual environment for AutoParser using `uv` run the following commands:

```bash
uv sync
. .venv/bin/activate
```

To view and use the CLI, you can type `adtl-autoparser` into the command line to view the
options available.

## Other requirements

AutoParser relies on LLMs to automatically map raw data fields to a target schema.
In order to use this tool, you will need an API key for either [OpenAI](https://platform.openai.com/docs/quickstart/create-and-export-an-api-key)
or Google's [Gemini](https://aistudio.google.com/apikey).
You can select which model to use, or keep to the defaults which are OpenAI's `gpt-4o-mini`,
or Google's `gemini-1.5-flash`. Your model choice should support Structured Outputs (for [OpenAI](https://platform.openai.com/docs/guides/structured-outputs#supported-models)) or Controlled Generation (for [Gemini](https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/control-generated-output)). Please be aware that more high-powered models like OpenAI's
'O' series and Gemini 2.0 will cost more per API call.

These choices can be specified using the [config class](../../api/autoparser/config.md).

The LLM should *never* see your raw data; only the data dictionary which contains
column headers, text descriptions of what each field contains, and a list of frequently
occuring values if present.

### Supported file formats
Autoparser supports CSV, XLSX and parquet formats for raw data and data dictionary files, and either
JSON or TOML for the target schema.

## Quickstart

See the example notebook [here](../examples/example.ipynb) for a basic walk through the
functionality of AutoParser.

If you already have a data dictionary associated with your data, follow
[this example](../examples/example_with_dict.ipynb) instead.

## Troubleshooting
