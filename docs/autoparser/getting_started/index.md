# Getting started

## Installation

AutoParser is a Python package that can either be built into your code or run as a
command-line interface (CLI). You can install AutoParser using pip:

```bash
  python3 -m pip install git+https://github.com/globaldothealth/autoparser
```

Note that it is usually recommended to install into a virtual environment. We recommend using [uv](https://github.com/astral-sh/uv) to manage the virtual environment. To create and active a virtual environment for AutoParser using `uv` run the following commands:

```bash
uv sync
. .venv/bin/activate
```

To view and use the CLI, you can type `autoparser` into the command line to view the
options available.

## Other requirements

AutoParser relies on LLMs to automatically map raw data fields to a target schema.
In order to use this tool, you will need an API key for either [OpenAI](https://platform.openai.com/docs/quickstart/create-and-export-an-api-key)
or Google's [Gemini](https://aistudio.google.com/apikey).
AutoParser will use either OpenAI's `gpt-4-mini`, or Google's `gemini-1.5-flash`.

The LLM should *never* see your raw data; only the data dictionary which contains
column headers, and text descriptions of what each field shoud contain.

### Supported file formats
Autoparser supports CSV and XLSX formats for raw data and data dictionary files, and either
JSON or TOML for the target schema.

## Quickstart

See the example notebook [here](../examples/example.ipynb) for a basic walk through the
functionality of AutoParser.
