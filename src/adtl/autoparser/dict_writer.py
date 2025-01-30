"""
Infer a data dictionary from a dataset.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from .util import (
    DEFAULT_CONFIG,
    check_matches,
    load_data_dict,
    read_config_schema,
    read_data,
    setup_llm,
)


class DictWriter:
    """
    Class for inferring a data dictionary based on a dataset. Will not store the data,
    only the created data dictionary.

    Use `create_dict()` to create a data dictionary, as the function equivalent
    of the command line `create-dict` script.

    `generate_descriptions()` will use an LLM to generate descriptions for the
    data dictionary, using only the column headers, NOT the data itself.

    Parameters
    ----------
    config
        The path to the configuration file to use if not using the default configuration
    """

    def __init__(
        self,
        config: Path | str | None = None,
        llm: str | None = None,
        api_key: str | None = None,
    ):
        if isinstance(config, str):
            config = Path(config)
        self.config = read_config_schema(
            config or Path(Path(__file__).parent, DEFAULT_CONFIG)
        )

        if llm and api_key:
            self.model = setup_llm(llm, api_key)
        else:
            self.model = None

    def create_dict(self, data: pd.DataFrame | str) -> pd.DataFrame:
        """
        Create a basic data dictionary from a dataset.

        Creates a data dictionary from a dataset, including the field name, field type,
        and common values (defined as occuring more than 25 times in the columns).
        Also creates an empty column for field decriptions, which can either be added by
        hand later, or auto-generated with an LLM using `generate_descriptions()`.

        Parameters
        ----------
        data
            Path to a CSV or XLSX file, or a DataFrame, containing the raw data.

        Returns
        -------
        pd.DataFrame
            Data dictionary containing field names, field types, and common values.
        """

        df = read_data(data, "Data")

        names = df.columns
        types = [str(t) for t in df.dtypes]
        value_opts = {}

        for i in df.columns:
            values = df[i].value_counts()
            if len(values) <= self.config["num_choices"]:
                try:
                    value_opts[i] = f"{self.config['choice_delimiter']} ".join(
                        list(values.index.values)
                    )
                except TypeError:
                    value_opts[i] = np.nan
            else:
                value_opts[i] = np.nan

        dd = pd.DataFrame(
            {
                "Field Name": names,
                "Description": [np.nan] * len(names),
                "Field Type": types,
                "Common Values": value_opts.values(),
            }
        )
        dd["Field Type"] = dd["Field Type"].map(
            {
                "object": None,
                "int64": "number",
                "float64": "number",
                "datetime64[ns]": "date",
                "boolean": "bool",
            }
        )
        dd["Field Type"] = dd.apply(
            lambda x: (
                "choice" if isinstance(x["Common Values"], str) else x["Field Type"]
            ),
            axis=1,
        )
        dd["Field Type"] = dd["Field Type"].fillna("string")

        self.data_dictionary = dd

        return dd

    def generate_descriptions(
        self,
        language: str,
        data_dict: pd.DataFrame | str | None = None,
        key: str | None = None,
        llm: str | None = "openai",
    ) -> pd.DataFrame:
        """
        Generate descriptions for the columns in the dataset.

        Uses an LLM to auto-generate descriptions for a data dictionary based on the
        column headers.

        Parameters
        ----------
        language
            Language the column headers are in (e.g. french, spanish).
        data_dict
            Data dictionary containing the column headers, either as a dataframe or a
            path to the dictionary as a csv/xlsx file. Can be None if the data dict
            has already been created using `create_dict()`.
        key
            OpenAI API key.
        llm
            LLM API to call (currently only OpenAI is supported)

        Returns
        -------
        pd.DataFrame
            Data dictionary with descriptions added
        """
        if data_dict is None:
            try:
                data_dict = self.data_dictionary
            except AttributeError:
                raise ValueError(
                    "No data dictionary found. Please create a data dictionary first."
                )

        df = load_data_dict(self.config, data_dict)

        if not self.model:
            self.model = setup_llm(llm, key)

        headers = df.source_field

        descriptions = self.model.get_definitions(list(headers), language)

        descriptions = {d.field_name: d.translation for d in descriptions}
        df_descriptions = pd.DataFrame(
            descriptions.items(), columns=["source_field_gpt", "description"]
        )

        descrip = pd.concat([headers, df_descriptions], axis=1)

        # check ordering is correct even if the return field names aren't quite the same
        # e.g. numbering has been stripped
        assert all(
            descrip.apply(
                lambda x: check_matches(x["source_field_gpt"], [x.source_field]),
                axis=1,
            )
        ), "Field names from the LLM don't match the originals."

        descrip.drop(columns=["source_field_gpt"], inplace=True)

        new_dd = pd.merge(df, descrip, on="source_field")
        new_dd["source_description"] = new_dd["description"]
        new_dd.drop(columns=["description"], inplace=True)

        return new_dd


def create_dict(data: pd.DataFrame | str, config: Path | None = None) -> pd.DataFrame:
    """
    Create a basic data dictionary from a dataset.

    Creates a data dictionary from a dataset, including the field name, field type,
    and common values (defined as occuring more than 25 times in the columns).
    Also creates an empty column for field decriptions, which can either be added by
    hand later, or auto-generated with an LLM using `generate_descriptions()`.

    Parameters
    ----------
    data
        Path to a CSV or XLSX file, or a DataFrame, containing the raw data.
    config
        Path to the configuration file to use if not using the default configuration

    Returns
    -------
    pd.DataFrame
        Data dictionary containing field names, field types, and common values.
    """

    dd = DictWriter(config).create_dict(data)
    return dd


def generate_descriptions(
    data_dict: pd.DataFrame | str,
    language: str,
    key: str | None = None,
    llm: str | None = "openai",
    config: Path | None = None,
) -> pd.DataFrame:
    """
    Generate descriptions for the columns in the dataset.

    Uses an LLM to auto-generate descriptions for a data dictionary based on the
    column headers.

    Parameters
    ----------
    data_dict
        Data dictionary containing the column headers, either as a dataframe or a path
        to the dictionary as a csv/xlsx file.
    language
        Language the column headers are in (e.g. french, spanish).
    key
        OpenAI API key.
    llm
        LLM API to call (currently only OpenAI is supported)
    config
        Path to the configuration file to use if not using the default configuration

    Returns
    -------
    pd.DataFrame
        Data dictionary with descriptions added
    """

    dd = DictWriter(config=config).generate_descriptions(language, data_dict, key, llm)

    return dd


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate a basic data dictionary from a dataset",
        prog="autoparser create-dict",
    )
    parser.add_argument("data", help="Data to create dictionary from")
    parser.add_argument(
        "language", help="Language of the raw data (e.g. 'fr', 'en', 'es')"
    )
    parser.add_argument(
        "-d",
        "--descriptions",
        help="Use an LLM to generate descriptions from file headers",
        action="store_true",
    )
    parser.add_argument(
        "-k", "--api-key", help="OpenAI API key to generate descriptions"
    )
    parser.add_argument("-l", "--llm", help="LLM API to use", default="openai")
    parser.add_argument(
        "-c",
        "--config",
        help=f"Configuration file to use (default={DEFAULT_CONFIG})",
        type=Path,
    )
    parser.add_argument(
        "-o", "--output", help="Name to use for output files", default="datadict"
    )

    args = parser.parse_args(argv)

    if args.descriptions and not args.api_key:
        raise ValueError("API key required for generating descriptions")

    df = create_dict(args.data, args.config)
    if args.descriptions:
        df = generate_descriptions(
            df, args.language, args.api_key, args.llm, args.config
        )

    df.to_csv(f"{args.output}.csv", index=False)


if __name__ == "__main__":
    main()
