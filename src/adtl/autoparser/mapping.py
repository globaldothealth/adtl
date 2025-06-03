"""
Create draft intermediate mapping in CSV from source dataset to target dataset
"""

from __future__ import annotations

import argparse
import warnings
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from .dict_reader import format_dict
from .util import (
    DEFAULT_CONFIG,
    check_matches,
    read_config_schema,
    read_json,
    setup_llm,
)


class Mapper:
    """
    Class for creating an intermediate mapping file linking the data dictionary to
    schema fields and values.

    Use `create_mapping()` to write out the mapping file, as the function equivalent
    of the command line `create-mapping` script.

    Parameters
    ----------
    data_dictionary
        The data dictionary to use
    schema
        The path to the schema file to map to
    language
        The language of the raw data (e.g. 'fr', 'en', 'es')
    api_key
        The API key to use for the LLM
    llm_provider
        The LLM API to use, currently only 'openai' and 'gemini' are supported
    llm_model
        The LLM model to use. If not provided, a default for the given provider will be
        used.
    config
        The path to the configuration file to use if not using the default configuration
    """

    def __init__(
        self,
        data_dictionary: str | pd.DataFrame,
        schema: Path,
        language: str,
        api_key: str | None = None,
        llm_provider: Literal["openai", "gemini"] | None = "openai",
        llm_model: str | None = None,
        config: Path | None = None,
    ):
        self.schema = read_json(schema)
        self.schema_properties = self.schema["properties"]
        self.language = language
        if llm_provider is None and llm_model is None:
            self.model = None
        else:
            self.model = setup_llm(api_key, provider=llm_provider, model=llm_model)

        self.config = read_config_schema(
            config or Path(Path(__file__).parent, DEFAULT_CONFIG)
        )

        self.data_dictionary = format_dict(data_dictionary, config=self.config)

    @property
    def target_fields(self) -> list[str]:
        """Returns a list of fields in the target schema"""
        try:
            return self._target_fields
        except AttributeError:
            self._target_fields = list(self.schema_properties.keys())
            return self._target_fields

    @property
    def target_types(self) -> dict[str, list[str]]:
        """Returns the field types of the target schema"""
        try:
            return self._target_types
        except AttributeError:
            self._target_types = {
                f: self.schema_properties[f].get("type", ["string", "null"])
                for f in self.target_fields
            }
            return self._target_types

    @property
    def target_values(self) -> pd.Series:
        """Returns the enum values or boolean options for the target schema"""
        try:
            return self._target_values
        except AttributeError:

            def _value_options(f):
                if "boolean" in self.target_types[f]:
                    return ["True", "False", "None"]
                elif "string" in self.target_types[f]:
                    return self.schema_properties[f].get("enum", np.nan)
                elif "array" in self.target_types[f]:
                    return (
                        self.schema_properties[f].get("items", {}).get("enum", np.nan)
                    )
                else:
                    return np.nan

            self._target_values = pd.Series(
                {f: _value_options(f) for f in self.target_fields}, self.target_fields
            )

            return self._target_values

    @property
    def common_values(self) -> pd.Series:
        """
        Returns the commonly repeated values in the source data
        Usually this indicates that the source field is an enum or boolean
        """
        try:
            return self._common_values
        except AttributeError:
            if "common_values" in self.data_dictionary.columns:
                cv = self.data_dictionary.common_values
                cv.index = self.data_dictionary.source_field
                self._common_values = cv
            elif "choices" in self.data_dictionary.columns:
                choices = self.data_dictionary.choices
                cv = choices.apply(
                    lambda x: list(x.values()) if isinstance(x, dict) else None
                )
                cv.index = self.data_dictionary.source_field
                self._common_values = cv
            else:
                raise ValueError(
                    "No common values or choices column found in data dictionary"
                )
            return self._common_values

    @property
    def common_values_mapped(self) -> pd.Series:
        try:
            return self._common_values_mapped
        except AttributeError:
            try:
                filtered_dict = self.filtered_data_dict
            except AttributeError:
                raise AttributeError(
                    "fields have to be mapped using the `match_fields_to_schema` method"
                    " first"
                )
            cv = self.common_values
            # cv_mapped = filtered_dict.source_field.dropna().apply(lambda x: cv.loc[x])
            cv_mapped = filtered_dict.source_field.apply(
                lambda x: cv.loc[x] if isinstance(x, str) else None
            )
            self._common_values_mapped = cv_mapped
            return self._common_values_mapped

    @property
    def mapped_fields(self):
        try:
            return self._mapped_fields
        except AttributeError:
            raise AttributeError(
                "mapped_fields have to be created using `match_fields_to_schema` method"
            )

    @mapped_fields.setter
    def mapped_fields(self, value: pd.Series):
        self._mapped_fields = value

    def match_fields_to_schema(self) -> pd.DataFrame:
        """
        Use the LLM to match the target (schema) fields to the descriptions of the
        source data fields from the data dictionary.
        """

        # english translated descriptions rather than names.
        source_fields = list(self.data_dictionary.source_description)

        mappings = self.model.map_fields(source_fields, self.target_fields)

        mapping_dict = pd.DataFrame(
            {
                "target_field": [f.target_field for f in mappings.targets_descriptions],
                "source_description": [
                    f.source_description for f in mappings.targets_descriptions
                ],
            }
        )

        df_merged = pd.merge(
            mapping_dict,
            self.data_dictionary,
            how="left",
            on="source_description",
        ).drop_duplicates(subset="target_field")
        df_merged.set_index("target_field", inplace=True, drop=True)

        # Check to see if any fields with mapped descriptions are missing after merge
        missed_merge = df_merged[
            (df_merged["source_description"].notna())
            & (df_merged["source_field"].isna())
        ]

        if not missed_merge.empty:
            descriptions_list = (
                self.data_dictionary["source_description"].dropna().tolist()
            )
            df_merged.loc[
                missed_merge.index,
                "source_description",
            ] = missed_merge["source_description"].apply(
                lambda x: check_matches(x, descriptions_list)
            )

            df_merged = (
                df_merged["source_description"]
                .reset_index()
                .merge(self.data_dictionary, how="left")
                .drop_duplicates(subset="target_field")
                .set_index("target_field")
            )

        self.mapped_fields = df_merged.source_field
        self.filtered_data_dict = df_merged
        return df_merged

    def match_values_to_schema(self) -> pd.DataFrame:
        """
        Use the LLM to match the common values from the data dictionary to the target
        values in the schema - i.e. enum or boolean options.
        """

        values_tuples = []
        for f in self.target_fields:
            s = self.common_values_mapped.get(f)
            t = self.target_values[f]
            if s is not None and t is not None:
                values_tuples.append((f, s, t))

        # to LLM
        value_pairs = self.model.map_values(values_tuples, self.language)

        value_mapping = {}

        for p in value_pairs.values:
            f = p.field_name
            value_dict = {
                pair.source_value: pair.target_value for pair in p.mapped_values
            }
            value_mapping[f] = value_dict

        self.mapped_values = pd.Series(value_mapping, name="mapped_values")

        return self.mapped_values

    def create_mapping(self, save=True, file_name="mapping_file") -> pd.DataFrame:
        """
        Creates an intermediate mapping dataframe linking the data dictionary to schema
        fields. The index contains the target (schema) field names, and the columns are:
        source_description
        source_field
        common_values OR choices (depending on the data dictionary)
        target_values
        value_mapping

        Raises a warning if any fields are present in the schema where a
        corresponding source field in the data dictionary has not been found.

        Parameters
        ----------
        save
            Whether to save the mapping to a CSV file. If True, lists in `target_values`
            dicts in `value_mapping` are converted to strings before saving.
        name
            The name to use for the CSV file
        """

        mapping_dict = self.match_fields_to_schema()
        mapped_vals = self.match_values_to_schema()

        mapping_dict.drop(columns=["source_type"], inplace=True)

        # reindex to add in any schema fields that weren't returned by the LLM
        mapping_dict = mapping_dict.reindex(self.target_fields)

        mapping_dict["target_values"] = mapping_dict.index.map(self.target_values)
        mapping_dict["value_mapping"] = mapping_dict.index.map(mapped_vals)

        unmapped = mapping_dict[mapping_dict["source_field"].isna()].index
        if any(unmapped):
            warnings.warn(
                f"The following schema fields have not been mapped: {list(unmapped)}",
                UserWarning,
            )

        if "choices" in self.data_dictionary:
            # If the data dictionary has a choices column,
            # the keys need to be mapped rather than the text values
            filtered_dict = self.data_dictionary[
                self.data_dictionary["source_field"].isin(mapping_dict["source_field"])
            ][["source_field", "choices"]]

            mapping_choices = mapping_dict[["source_field", "value_mapping"]]

            choices = filtered_dict.merge(mapping_choices, on="source_field")
            choices["combined_choices"] = choices.apply(
                lambda x: (
                    {k: x["value_mapping"].get(v) for k, v in x["choices"].items()}
                    if isinstance(x["choices"], dict)
                    else None
                ),
                axis=1,
            )

            merged_mapping_dict = (
                mapping_dict.reset_index()
                .merge(
                    choices[["source_field", "combined_choices"]],
                    on="source_field",
                    how="left",
                )
                .drop_duplicates(subset="target_field")
                .set_index("target_field")
            )
            mmd = merged_mapping_dict.drop(columns=["value_mapping"]).rename(
                columns={"combined_choices": "value_mapping"}
            )

            mapping_dict = mmd

        # turn lists & dicts into strings for consistancy with saved CSV
        for col in ["common_values", "target_values"]:
            if col in mapping_dict.columns:
                mapping_dict[col] = mapping_dict[col].apply(
                    lambda x: (
                        ", ".join(str(item) for item in x)
                        if isinstance(x, (list, np.ndarray))
                        else x
                    )
                )
        for col in ["choices", "value_mapping"]:
            if col in mapping_dict.columns:
                mapping_dict[col] = mapping_dict[col].apply(
                    lambda x: (
                        ", ".join(f"{k}={v}" for k, v in x.items())
                        if isinstance(x, dict)
                        else x
                    )
                )

        if save is False:
            return mapping_dict
        else:
            # Write to CSV
            if not file_name.endswith(".csv"):
                file_name += ".csv"
            mapping_dict.to_csv(file_name)
            return mapping_dict


def create_mapping(
    data_dictionary: str | pd.DataFrame,
    schema: Path,
    language: str,
    api_key: str,
    llm_provider: str | None = "openai",
    llm_model: str | None = None,
    config: Path | None = None,
    save: bool = True,
    file_name: str = "mapping_file",
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
    df = Mapper(
        data_dictionary, schema, language, api_key, llm_provider, llm_model, config
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
    parser.add_argument("schema", help="Schema to use")
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
        help=f"Configuration file to use (default={DEFAULT_CONFIG})",
        type=Path,
    )
    parser.add_argument(
        "-o", "--output", help="Name to use for output files", default="mapping_file"
    )
    args = parser.parse_args(argv)
    Mapper(
        args.dictionary,
        Path(args.schema),
        args.language,
        args.api_key,
        args.llm_provider,
        args.llm_model,
        args.config,
    ).create_mapping(save=True, file_name=args.output)


if __name__ == "__main__":
    main()
