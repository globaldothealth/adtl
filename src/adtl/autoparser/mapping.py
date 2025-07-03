"""
Create draft intermediate mapping in CSV from source dataset to target dataset
"""

from __future__ import annotations

import abc
import argparse
import warnings
from enum import Enum
from pathlib import Path
from typing import Literal, Optional, Union

import numpy as np
import pandas as pd
from pydantic import create_model

from .dict_reader import format_dict
from .util import (
    DEFAULT_CONFIG,
    check_matches,
    read_config_schema,
    read_json,
    setup_llm,
)


class BaseMapper(abc.ABC):
    """
    Abstract class for creating an intermediate mapping file linking the data dictionary to
    a schema's fields and values. Will be used to create Wide and Long mapping formats.

    Use `create_mapping()` to write out the mapping file, as the function equivalent
    of the command line `create-mapping` script.

    Parameters
    ----------
    data_dictionary
        The data dictionary to use
    table_name
        The name of the table to map to
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
        data_dictionary: Union[str, pd.DataFrame],
        table_name: str,
        *,
        api_key: str,
        language: Union[str, None] = None,
        llm_provider: Union[Literal["openai", "gemini"], None] = None,
        llm_model: Union[str, None] = None,
        config: Union[Path, None] = None,
    ):
        self.name = table_name

        self.config = read_config_schema(
            config or Path(Path(__file__).parent, DEFAULT_CONFIG)
        )

        self.language = language or self.config.get("language", None)
        self.llm_provider = llm_provider or self.config.get("llm_provider", None)
        self.llm_model = llm_model or self.config.get("llm_model", None)

        if self.language is None:
            raise ValueError(
                "Language must be specified either in the config file or as an argument"
            )

        if self.llm_provider is None and self.llm_model is None:
            self.model = None
        else:
            self.model = setup_llm(
                api_key, provider=self.llm_provider, model=self.llm_model
            )

        self.schema = read_json(self.config["schemas"][table_name])
        self.schema_properties = self.schema["properties"]

        self.data_dictionary = format_dict(data_dictionary, config=self.config)

    @abc.abstractmethod
    def create_mapping(self, save=True, file_name="mapping_file") -> pd.DataFrame:
        pass


class WideMapper(BaseMapper):
    """
    Class for creating an intermediate mapping file linking the data dictionary to
    a wide schema fields and values.
    """

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
                # pandera schema validation means this should never be reached
                raise ValueError(
                    "No common values or choices column found in data dictionary"
                )  # pragma: no cover
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

    def _relabel_choices(self, map_df):
        """
        If 'choices' are present in the data dictionary, relabel the choices
        in the mapping dataframe to match the source field names.
        e.g.
        if the data dictionary indicates {1: Man, 2:Female, 3:Unknown}, and the LLM maps
        {Man:male, Female:female, Unknown:unknown}, then `relabel_choices` should give
        {1:male, 2:female, 3:unknown}
        """

        filtered_dict = self.data_dictionary[
            self.data_dictionary["source_field"].isin(map_df["source_field"])
        ][["source_field", "choices"]]

        mapping_choices = map_df[["source_field", "value_mapping"]]

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
            map_df.reset_index()
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

        return mmd

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
            df_merged.loc[missed_merge.index, "source_description"] = missed_merge[
                "source_description"
            ].apply(lambda x: check_matches(x, descriptions_list))

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
            Whether to save the mapping to a CSV file. If True, lists and dicts are
            converted to strings before saving.
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
            mapping_dict = self._relabel_choices(mapping_dict)

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


class LongMapper(BaseMapper):
    """
    Class for creating an intermediate mapping file linking the data dictionary to
    long-format schema's fields and values.
    """

    @property
    def common_cols(self) -> str:
        """Returns the common columns for the long table"""
        if not hasattr(self, "_common_cols"):
            ccs = self.config["long_tables"][self.name].get("common_cols", None)
            if ccs is None:
                ccs = (
                    self.config["long_tables"][self.name]
                    .get("common_fields", {})
                    .keys()
                )

            self._common_cols = ccs
        return self._common_cols

    @property
    def common_fields(self) -> pd.Series:
        if not hasattr(self, "_common_fields"):
            self._common_fields = self.config["long_tables"][self.name].get(
                "common_fields", {}
            )
        return self._common_fields

    @property
    def schema_variable_col(self) -> str:
        """Returns the variable column for the long table"""
        return self.config["long_tables"][self.name]["variable_col"]

    @property
    def schema_value_cols(self) -> list[str]:
        """Returns the value columns for the long table"""
        return self.config["long_tables"][self.name]["value_cols"]

    @property
    def other_fields(self) -> list[str]:
        """Returns the other fields in the schema that are not target fields"""
        return [
            f
            for f in self.schema_properties.keys()
            if f
            not in [
                *self.common_fields.keys(),
                self.schema_variable_col,
                *self.schema_value_cols,
            ]
        ]

    def _create_data_structure(self) -> pd.DataFrame:
        def _enum_creator(name: str, enums: list[str]) -> dict:
            """
            Creates a dictionary for a single entry in the long table format.
            """
            return Enum(name, {v.upper(): v for v in enums})

        fields = {
            "source_description": (str, ...),
            "variable_name": (str, ...),
            "value_col": (
                Optional[_enum_creator("ValueColEnum", self.schema_value_cols)],
                None,
            ),
        }

        if self.schema_properties[self.schema_variable_col].get("enum", []):
            VarColEnum = _enum_creator(
                "VarColEnum", self.schema_properties[self.schema_variable_col]["enum"]
            )
            fields["variable_name"] = (Optional[VarColEnum], None)

        # Add arbitrary fields from CSV headers
        for field in self.other_fields:
            if self.schema_properties[field].get("enum", []):
                # If the field has an enum, create an Enum type for it
                EnumType = _enum_creator(
                    f"{field}Enum", self.schema_properties[field]["enum"]
                )
                fields[field] = (Optional[EnumType], None)
            else:
                fields[field] = (Optional[str], None)

        SingleEntry = create_model("SingleEntry", **fields)
        return SingleEntry

    def _check_config(self):
        """
        Check that the config file has the correct fields for the long table.
        """
        if "long_tables" not in self.config:
            raise ValueError(
                "No long tables defined in config file. Please set 'long_tables' in the config file."
            )
        if self.name not in self.config["long_tables"]:
            raise ValueError(
                f"Long table {self.name} not defined in config file. "
                "Please set 'long_tables' in the config file."
            )

        if "variable_col" not in self.config["long_tables"][self.name]:
            raise ValueError(
                f"Variable column not set in config for long table {self.name}. "
                "Please set 'variable_col' in the config file."
            )

    def set_common_fields(self, common_fields: dict[str, str]):
        """
        Function to assign fields to the common fields of the long table - i.e. fields
        which should be filled by the same text or source field in every row of the long
        table.
        """
        if self.common_cols != list(common_fields.keys()):
            raise ValueError(
                f"Common columns {self.common_cols} set in the config file do not"
                f" match provided common fields {common_fields.keys()}"
            )

        self._common_fields = common_fields
        self._common_cols = list(common_fields.keys())

    def match_fields_to_schema(self) -> pd.DataFrame:
        """
        Use the LLM to match the target (schema) fields to the descriptions of the
        source data fields from the data dictionary.
        """

        data_format = self._create_data_structure()

        source_descriptions = self.uncommon_data_dict.source_description

        mappings = self.model.map_long_table(
            data_format,
            source_descriptions.tolist(),
            self.schema_properties[self.schema_variable_col].get("enum", []),
            self.schema,
        )

        mapping_dict = pd.DataFrame(mappings.model_dump()["long_table"])

        missed_descrips = len(source_descriptions) != len(mapping_dict)

        if missed_descrips:
            mapping_dict = pd.merge(
                pd.DataFrame({"source_description": source_descriptions}),
                mapping_dict,
                how="left",
                on="source_description",
            )

            assert len(source_descriptions) == len(mapping_dict), (
                "malformed descriptions!"
            )

        df_merged = pd.merge(
            self.uncommon_data_dict,
            mapping_dict,
            how="left",
            on="source_description",
        )

        df_merged.set_index("source_field", inplace=True, drop=True)

        self.mapped_fields = df_merged.index
        return df_merged

    def create_mapping(
        self,
        save=True,
        file_name="mapping_file",
    ) -> pd.DataFrame:
        """
        Creates an intermediate mapping dataframe linking the data dictionary to schema
        fields. The index contains the source field names, and the columns are:
        source_description
        common_values OR choices (depending on the data dictionary)
        <variable_name> (the name of the column identified in the config file)
        value_col
        * any other fields in the long schema.

        Raises a warning if any fields are present in the schema where a
        corresponding source field in the data dictionary has not been found.

        Parameters
        ----------
        save
            Whether to save the mapping to a CSV file. If True, lists and dicts are
            converted to strings before saving.
        name
            The name to use for the CSV file
        """

        self._check_config()

        if self.common_cols and not self.common_fields:
            raise ValueError(
                "Common fields must be set in the config file or set using the"
                " `set_common_fields` method before mapping."
            )

        if self.common_cols:
            self.uncommon_data_dict = self.data_dictionary[
                ~self.data_dictionary.source_field.isin(self.common_cols)
            ].drop(columns=["source_type"])
        else:
            self.uncommon_data_dict = self.data_dictionary.drop(columns=["source_type"])

        mapping_dict = self.match_fields_to_schema()

        unmapped = mapping_dict[mapping_dict["variable_name"].isna()].index
        if any(unmapped):
            warnings.warn(
                f"The following fields have not been mapped to the new schema: {list(unmapped)}",
                UserWarning,
            )

        # Add in the common columns to the file
        for col, value in self.common_fields.items():
            mapping_dict[col] = value

        mapping_dict.rename(
            columns={"variable_name": self.schema_variable_col}, inplace=True
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
        data_dictionary, table_name, language, api_key, llm_provider, llm_model, config
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
        help=f"Configuration file to use (default={DEFAULT_CONFIG})",
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
