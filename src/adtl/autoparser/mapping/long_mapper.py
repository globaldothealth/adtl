from __future__ import annotations

import warnings
from enum import Enum
from functools import cached_property
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel, create_model

from ..mixin import LongTableMixin
from .base_mapper import BaseMapper


class LongMapper(BaseMapper, LongTableMixin):
    """
    Class for creating an intermediate mapping file linking the data dictionary to
    long-format schema's fields and values.
    """

    INDEX_FIELD = "source_field"

    @cached_property
    def common_values_mapped(self) -> pd.Series:
        try:
            filtered_dict = self.filtered_data_dict
        except AttributeError:
            raise AttributeError(
                "Fields have to be mapped using the `match_fields_to_schema` method"
                " first"
            )
        cv = self.common_values
        return cv.loc[filtered_dict[filtered_dict["variable_name"].notna()].index]

    @cached_property
    def target_values(self) -> pd.Series:
        """Returns the enum values or boolean options for the target schema"""

        def _value_options(f):
            if "boolean" in self.schema_fields[f].get("type", ["string", "null"]):
                return ["True", "False", "None"]
            elif "string" in self.schema_fields[f].get("type", ["string", "null"]):
                return self.schema_fields[f].get("enum", np.nan)
            elif "array" in self.schema_fields[f].get("type", ["string", "null"]):
                return self.schema_fields[f].get("items", {}).get("enum", np.nan)
            else:
                return np.nan

        return pd.Series(
            {f: _value_options(f) for f in self.value_cols},
            self.value_cols,
        )

    def _iter_value_tuples(self):
        for f in self.mapped_fields:
            s = self.common_values_mapped.get(f)
            t = self.target_values.get(self.filtered_data_dict.loc[f, "value_col"])
            if s is not None and t is not None:
                yield (f, s, t)

    def _create_data_model(self) -> BaseModel:
        def _enum_creator(name: str, enums: list[str]) -> dict:
            """
            Creates a dictionary for a single entry in the long table format.
            """
            return Enum(name, {v.upper(): v for v in enums})

        fields = {
            "source_description": (str, ...),
            "variable_name": (str, ...),
            "value_col": (
                Optional[_enum_creator("ValueColEnum", self.value_cols)],
                None,
            ),
        }

        if self.schema_fields[self.variable_col].get("enum", []):
            VarColEnum = _enum_creator(
                "VarColEnum", self.schema_fields[self.variable_col]["enum"]
            )
            fields["variable_name"] = (Optional[VarColEnum], None)

        # Add arbitrary fields from CSV headers
        for field in self.other_fields:
            if self.schema_fields[field].get("enum", []):
                # If the field has an enum, create an Enum type for it
                EnumType = _enum_creator(
                    f"{field}Enum", self.schema_fields[field]["enum"]
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
        if self.config.long_tables is None:
            raise ValueError(
                "No long tables defined in config file. Please set 'long_tables' in the config file."
            )

        if self.schema_fields[self.variable_col].get("enum") is None:
            raise ValueError(
                f"Variable column '{self.variable_col}' in schema does not have an enum set. "
                "Please set 'enum' in the schema for this field."
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

        data_format = self._create_data_model()

        if self.common_cols:
            self.uncommon_data_dict = self.data_dictionary[
                ~self.data_dictionary.source_field.isin(self.common_cols)
            ].drop(columns=["source_type"])
        else:
            self.uncommon_data_dict = self.data_dictionary.drop(columns=["source_type"])

        source_descriptions = self.uncommon_data_dict.source_description

        mappings = self.model.map_long_table(
            data_format,
            source_descriptions.tolist(),
            self.schema_fields[self.variable_col]["enum"],
        )

        mapping_dict = pd.DataFrame(mappings.model_dump(mode="json")["long_table"])

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
        self.filtered_data_dict = df_merged
        return df_merged

    def create_mapping(
        self,
        save=True,
        file_name="mapping_file",
    ) -> pd.DataFrame:
        """
        Creates an intermediate mapping dataframe linking the data dictionary to schema
        fields. The index contains the source field names, and the columns are:
        * source_description
        * common_values OR choices (depending on the data dictionary)
        * <variable_name> (the name of the column identified in the config file)
        * value_col
        * any other fields in the long schema.

        Raises a warning if any fields are present in the schema where a
        corresponding source field in the data dictionary has not been found.

        Parameters
        ----------
        save
            Whether to save the mapping to a CSV file. If True, lists and dicts are
            converted to strings before saving.
        file_name
            The name to use for the CSV file
        """

        self._check_config()

        if self.common_cols and not self.common_fields:
            raise ValueError(
                "Common fields must be set in the config file or set using the"
                " `set_common_fields` method before mapping."
            )

        mapping_dict = self.match_fields_to_schema()

        unmapped = mapping_dict[mapping_dict["variable_name"].isna()].index
        if any(unmapped):
            warnings.warn(
                f"The following fields have not been mapped to the new schema: {list(unmapped)}",
                UserWarning,
            )

        mapped_vals = self.match_values_to_schema()

        mapping_dict = pd.concat([mapping_dict, mapped_vals], axis=1)

        # Add in the common columns to the file
        for col, value in self.common_fields.items():
            mapping_dict[col] = value

        mapping_dict.rename(columns={"variable_name": self.variable_col}, inplace=True)

        return self.post_process_mapping(mapping_dict, save=save, file_name=file_name)
