"""
Infer a data dictionary from a dataset.
"""

from __future__ import annotations

import argparse
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from pandera.errors import SchemaError

from .data_dict_schema import GeneratedDict, GeneratedDictDescribed
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
    llm_provider
        The LLM API to use (currently only OpenAI & Google Gemini are supported)
    llm_model
        The name of the LLM model to use (must support Structured Outputs for OpenAI, or
        the equivalent responseSchema for Gemini)
    api_key
        API key corresponsing to the chosen LLM provider/model
    """

    def __init__(
        self,
        config: Path | str | None = None,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        api_key: str | None = None,
    ):
        if isinstance(config, str):
            config = Path(config)
        self.config = read_config_schema(
            config or Path(Path(__file__).parent, DEFAULT_CONFIG)
        )

        try:
            self.config["max_common_count"]
        except KeyError:
            raise ValueError("'max_common_count' not found in config file.")

        if (llm_provider or llm_model) and api_key:
            self.model = setup_llm(api_key, provider=llm_provider, model=llm_model)
        else:
            self.model = None

    def _reset_headers_and_validate(self, data_dict: pd.DataFrame) -> pd.DataFrame:
        """
        Reset the headers of a data dictionary to those provided.
        Validate the data dictionary against the generated dictionary schema.


        Returns
        -------
        pd.DataFrame
            Data dictionary with the headers reset
        """

        column_mappings = self.config["column_mappings"]
        data_dict.rename(columns=column_mappings, inplace=True)

        # Validate the new data dictionary
        try:
            GeneratedDictDescribed.validate(data_dict)
        except SchemaError as e:
            if "'Description' contains duplicate values" in str(e):
                warnings.warn(
                    "Duplicate descriptions found in the data dictionary. "
                    "Before proceeding to mapping and parser generation, each description must be unique."
                )
            else:
                raise e

        return data_dict

    def _load_dict(self, data_dict: pd.DataFrame) -> pd.DataFrame:
        dd = load_data_dict(data_dict, schema=GeneratedDict)
        column_mappings = {v: k for k, v in self.config["column_mappings"].items()}
        dd.rename(columns=column_mappings, inplace=True)

        return dd

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

        # Get common value thresholds
        max_common_count = self.config["max_common_count"]
        min_common_freq = self.config.get("min_common_freq")

        # check the max count isn't > than 30% of the dataset
        calced_max_common_count = min(max_common_count, len(df) * 0.3)
        if calced_max_common_count < max_common_count:
            warnings.warn(
                f"Small Dataset: max_common_count of '{max_common_count}' is"
                f" too high for a dataset with {len(df)} rows.\n"
                f"Reducing to {calced_max_common_count} to avoid data "
                "identification issues.\n"
                "Setting the minimum frequency to 5% of the dataset."
            )
            max_common_count = calced_max_common_count
            min_common_freq = 0.05

        for j, i in enumerate(df.columns):
            values = df[i].value_counts(sort=True)

            # check for lists in the data
            if types[j] == "object" and any(
                "[" in x or "," in x for x in values.index.values if isinstance(x, str)
            ):
                # the values might be lists.
                list_col = df[i].apply(
                    lambda x: (
                        [v.lstrip(" ").rstrip(" ") for v in x.strip("[]").split(",")]
                        if isinstance(x, str)
                        else x
                    )
                )
                flat_col = [item for sublist in list_col.dropna() for item in sublist]
                values = pd.Series(flat_col).value_counts(sort=True)
                types[j] = "list"

            if min_common_freq:
                values = values[values > max(1, len(df) * min_common_freq)]
            value_opts[i] = np.nan
            if not values.empty and len(values) <= max_common_count:
                # drop any values with a frequency of 1
                values = values[values > 1]
                if not values.empty:
                    try:
                        value_opts[i] = f"{self.config['choice_delimiter']} ".join(
                            list(values.index.values)
                        )
                    except TypeError:
                        # This stops float values being given as 'common values'.
                        continue

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
                "list": "list",
            }
        )
        dd["Field Type"] = dd["Field Type"].fillna("string")

        self.data_dictionary = dd

        return dd

    def generate_descriptions(
        self,
        language: str,
        data_dict: pd.DataFrame | str | None = None,
        key: str | None = None,
        llm_provider: str = "openai",
        llm_model: str | None = None,
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
        llm_provider
            LLM API to call (currently only OpenAI & Google Gemini is supported)
        llm_model
            Name of the LLM model to use (must support Structured Outputs for OpenAI, or
            the equivalent responseSchema for Gemini). If not provided, the default for
            each provider will be used.

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

        df = self._load_dict(data_dict)

        if not self.model:
            self.model = setup_llm(key, provider=llm_provider, model=llm_model)

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

        new_dd = self._reset_headers_and_validate(new_dd)

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
    llm_provider: str | None = "openai",
    llm_model: str | None = None,
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
    llm_provider
        LLM API to call (currently OpenAI & Google Gemini are supported)
    llm_model
        Name of the LLM model to use (must support Structured Outputs for OpenAI, or the
        equivalent responseSchema for Gemini). If not provided, the default for each
        provider will be used.
    config
        Path to the configuration file to use if not using the default configuration

    Returns
    -------
    pd.DataFrame
        Data dictionary with descriptions added
    """

    dd = DictWriter(config=config).generate_descriptions(
        language, data_dict, key, llm_provider, llm_model
    )

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
    parser.add_argument("-k", "--api-key", help="LLM API key to generate descriptions")
    parser.add_argument(
        "-l",
        "--llm-provider",
        help="LLM API to use, either 'openai' or 'gemini'",
        default="openai",
    )
    parser.add_argument(
        "-m",
        "--llm-model",
        help="Select a specific model from the llm provider, e.g. 'gpt-4o-mini'",
    )
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
            df,
            args.language,
            args.api_key,
            args.llm_provider,
            args.llm_model,
            args.config,
        )

    df.to_csv(f"{args.output}.csv", index=False)


if __name__ == "__main__":
    main()
