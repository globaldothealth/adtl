"""
Load and validate a data dictionary for use with autoparser.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pandera.pandas as pa
from pandera.typing.pandas import DataFrame

from .config.config import get_config
from .data_dict_schema import DataDictionaryEntry, DataDictionaryProcessed
from .util import DEFAULT_CONFIG, read_data


class DictReader:
    """
    Class for reading in and converting data dictionaries provided by users into a
    format usable by autoparser.

    Validates the final data dictionary against the schema defined in
    `adtl.autoparser.data_dict_schema.DataDictionaryProcessed`.

    Parameters
    ----------
    data_dict
        Path to a CSV, XLSX or parquet file, or a DataFrame, containing the data dictionary.

    config
        The path to the configuration file to use if not using the default configuration

    """

    def __init__(
        self,
        data_dict: pd.DataFrame | str,
    ):
        self.config = get_config()

        self.data_dict = read_data(data_dict, "Data Dictionary")

    def _parse_choices(self, choice_col: pd.Series) -> pd.Series:
        sep = self.config.choice_delimiter
        link = self.config.choice_delimiter_map

        def parse(x):
            if not isinstance(x, str):
                return None
            options = {}
            for i in x.split(sep):
                try:
                    k, v = i.split(link)
                except ValueError:
                    return None
                k = k.strip()
                v = v.strip()
                options[k] = v
            return options if options else None

        return choice_col.apply(lambda x: parse(x))

    def _process_dict(self, dd):
        if "common_values" in dd.columns:

            def _lower_string(x):
                if isinstance(x, str):
                    return list(
                        {
                            y.lower().strip()
                            for y in x.split(self.config.choice_delimiter)
                        }
                    )

            dd["common_values"] = dd["common_values"].apply(_lower_string)

        elif "choices" in dd.columns:
            dd.loc[:, "choices"] = self._parse_choices(dd["choices"])

        return dd

    def validate_dictionary(
        self, save: bool | str = True
    ) -> DataFrame[DataDictionaryProcessed]:
        try:
            # check if the data dictionary is already in the correct format
            # e.g. if loaded from a parquet file
            DataDictionaryProcessed.validate(self.data_dict)
            return self.data_dict
        except pa.errors.SchemaError:
            pass

        # Assumes an unformatted data dictionary, so processes it
        try:
            dd = self.data_dict
            column_mappings = {
                v: k for k, v in self.config.column_mappings.model_dump().items()
            }
            dd.rename(columns=column_mappings, inplace=True)

            dd = dd.loc[
                :,
                dd.columns.isin(list(DataDictionaryProcessed.to_schema().columns)),
            ]

            DataDictionaryEntry.validate(dd, lazy=True)
        except pa.errors.SchemaErrors as exc:
            failure_cases = exc.failure_cases
            failure_cases["field_name"] = exc.failure_cases["index"].map(
                self.data_dict["source_field"]
            )

            with pd.option_context("display.max_rows", None):
                # Order the failure cases by the index of the first occurrence of each failure case
                # and then group by failure case to make clearer
                fc = failure_cases.assign(
                    failure_case=pd.Categorical(
                        exc.failure_cases["failure_case"],
                        categories=exc.failure_cases.groupby("failure_case")["index"]
                        .min()
                        .sort_values()
                        .index,
                        ordered=True,
                    )
                ).sort_values(["failure_case", "index"])

                raise pa.errors.SchemaError(
                    schema=exc.schema,
                    data=exc.data,
                    message=(
                        f"Data dictionary validation failed with {len(exc.failure_cases)} error(s). See below for details.  \n"
                        + f"{fc}"
                    ),
                ) from None

        self.data_dict = self._process_dict(dd)

        try:
            DataDictionaryProcessed.validate(self.data_dict, lazy=True)
        except pa.errors.SchemaErrors as exc:
            raise pa.errors.SchemaError(
                schema=exc.schema,
                data=exc.data,
                message=(
                    f"Processed data dictionary validation failed with {len(exc.failure_cases)} error(s). See below for details.  \n"
                    + f"{exc.failure_cases}"
                ),
            ) from None  # pragma: no cover

        if save:
            self.save_formatted_dictionary(name=save if isinstance(save, str) else None)

        return self.data_dict

    def save_formatted_dictionary(self, name: str | Path | None = None) -> None:
        """
        Save the formatted data dictionary to a parquet file.

        The file will be saved in the same directory as the input data dictionary,
        either with the name provided or as 'formatted_data_dict.parquet'
        """
        if name:
            output_path = Path(name)
            if not output_path.suffix == "parquet":
                output_path = output_path.with_suffix(".parquet")
        else:
            output_path = Path("formatted_data_dict.parquet")

        self.data_dict.to_parquet(output_path, index=False, engine="fastparquet")
        print(f"Formatted data dictionary saved to '{output_path}'")


def format_dict(data_dict: pd.DataFrame | str, save=False) -> pd.DataFrame:
    """
    Formats a pre-existing data dictionary to use with autoparser, or checks one
    that is already pre-formatted.

    Parameters
    ----------
    data_dict
        Path to a CSV, XLSX or parquet file, or a DataFrame, containing the data dictionary.
    config
        Path to the configuration file to use if not using the default configuration
    save
        If True, saves the formatted data dictionary to a parquet file in the same directory as the
        input data dictionary, as 'formatted_data_dictionary.parquet'.

    Returns
    -------
    pd.DataFrame
        Data dictionary containing field names, field types, and common values.
    """

    dr = DictReader(data_dict)
    dd = dr.validate_dictionary(save=save)

    return dd


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Check the formatting of your data dictionary and save a version in the correct format.",
        prog="autoparser format-dict",
    )
    parser.add_argument("data_dict", help="Data to create dictionary from")

    parser.add_argument(
        "-c",
        "--config",
        help=f"Configuration file to use (default={DEFAULT_CONFIG})",
        type=Path,
    )

    parser.add_argument("-o", "--output", help="new output data dictionary name")

    args = parser.parse_args(argv)

    dict_reader = DictReader(data_dict=args.data_dict)

    dict_reader.validate_dictionary(save=args.output if args.output else True)


if __name__ == "__main__":
    main()  # pragma: no cover
