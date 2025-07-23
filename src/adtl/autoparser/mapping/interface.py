"""
Easy access interface for creating mappings between data dictionaries and schemas.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal, Union

import pandas as pd

from ..config.config import setup_config
from ..util import DEFAULT_CONFIG
from .long_mapper import LongMapper
from .wide_mapper import WideMapper

CONFIG = "../" + DEFAULT_CONFIG


def create_mapping(
    data_dictionary: Union[str, pd.DataFrame],
    table_name: str,
    save: bool = True,
    file_name: str = "mapping_file",
    table_format: Literal["wide", "long"] = "wide",
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
    table_name
        Name of the table being mapped.
    save
        Whether to save the mapping to a CSV file.
    file_name
        Name of the file to save the mapping to.
    table_format
        Format of the table to create, either 'wide' or 'long'.

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
    df = MapperClass(data_dictionary, table_name).create_mapping(
        save=save, file_name=file_name
    )

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

    setup_config(args.config or CONFIG)

    create_mapping(
        data_dictionary=args.dictionary,
        table_name=args.table_name,
        save=True,
        file_name=args.output,
        table_format="long" if args.long_table else "wide",
    )


if __name__ == "__main__":
    main()  # pragma: no cover
