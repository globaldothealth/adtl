from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal, Union

import pandas as pd

from ..util import DEFAULT_CONFIG
from .long_mapper import LongMapper
from .wide_mapper import WideMapper

CONFIG = "../" + DEFAULT_CONFIG


def create_mapping(
    data_dictionary: Union[str, pd.DataFrame],
    table_name: str,
    api_key: str,
    config: Union[Path, None] = None,
    save: bool = True,
    file_name: str = "mapping_file",
    table_format: Literal["wide", "long"] = "wide",
    language: Union[str, None] = None,
    llm_provider: Union[str, None] = None,
    llm_model: Union[str, None] = None,
) -> pd.DataFrame:
    """
    Creates a csv containing the mapping between a data dictionary and a schema.

    Takes a data dictionary and matches both the source fields, and any common values
    to the schema. Uses an LLM to first match the source fields to appropriate schema
    targets, and then to match the common values to appropriate enum or boolean options.

    Parameters
    ----------
    data_dictionary
        Path to a CSV or XLSX file, or a DataFrame, containing the data dictionary.
    schema
        Path to a JSON schema file.
    language
        Language of the source data (e.g. french, english, spanish).
    api_key
        API key for the API defined in `llm_provider`
    llm_provider
        Which LLM to use, currently 'openai' and 'gemini' are supported.
    llm_model
        Specify an LLM model to use. If not provided, a default for the given provider
        will be used.
    config
        Path to a JSON file containing the configuration for autoparser.
    save
        Whether to save the mapping to a CSV file.
    file_name
        Name of the file to save the mapping to.

    Returns
    -------
    pd.DataFrame
        Dataframe containing the mapping between the data dictionary and the schema.
    """
    if table_format == "long":
        MapperClass = LongMapper
    elif table_format == "wide":
        MapperClass = WideMapper
    else:
        raise ValueError(
            f"Invalid table format: {table_format}. Must be either 'wide' or 'long'."
        )
    df = MapperClass(
        data_dictionary,
        table_name,
        language=language,
        api_key=api_key,
        llm_provider=llm_provider,
        llm_model=llm_model,
        config=config,
    ).create_mapping(save=save, file_name=file_name)

    return df


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Generate intermediate CSV used by make_toml.py (create-parser) "
            "to create TOML"
        ),
        prog="autoparser create-mapping",
    )
    parser.add_argument("dictionary", help="Data dictionary to use")
    parser.add_argument("table_name", help="Name of the table being mapped")
    parser.add_argument("language", help="Language of the original data")
    parser.add_argument("api_key", help="OpenAI API key to use")
    parser.add_argument(
        "-l",
        "--llm-provider",
        help="LLM API to use, either 'openai' or 'gemini'",
        default="openai",
    )
    parser.add_argument(
        "-m", "--llm-model", help="LLM model to use, e.g. 'gpt-4o-mini'"
    )
    parser.add_argument(
        "-c",
        "--config",
        help=f"Configuration file to use (default={CONFIG})",
        type=Path,
    )
    parser.add_argument(
        "-o", "--output", help="Name to use for output files", default="mapping_file"
    )
    parser.add_argument(
        "--long-table", help="The target table has a long format", action="store_true"
    )
    args = parser.parse_args(argv)
    create_mapping(
        data_dictionary=args.dictionary,
        table_name=args.table_name,
        language=args.language,
        api_key=args.api_key,
        llm_provider=args.llm_provider,
        llm_model=args.llm_model,
        config=args.config,
        save=True,
        file_name=args.output,
        table_format="long" if args.long_table else "wide",
    )


if __name__ == "__main__":
    main()
