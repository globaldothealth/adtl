from __future__ import annotations

import warnings
from functools import cached_property
from typing import Generator

import numpy as np
import pandas as pd

from ..util import check_matches
from .base_mapper import BaseMapper


class WideMapper(BaseMapper):
    """
    Class for creating an intermediate mapping file linking the data dictionary to
    a wide schema fields and values.
    """

    @cached_property
    def target_fields(self) -> list[str]:
        """Returns a list of fields in the target schema"""
        return list(self.schema_fields.keys())

    @cached_property
    def target_types(self) -> dict[str, list[str]]:
        """Returns the field types of the target schema"""
        return {
            f: self.schema_fields[f].get("type", ["string", "null"])
            for f in self.target_fields
        }

    @cached_property
    def target_values(self) -> pd.Series:
        """Returns the enum values or boolean options for the target schema"""

        def _value_options(f):
            if "boolean" in self.target_types[f]:
                return ["True", "False", "None"]
            elif "string" in self.target_types[f]:
                return self.schema_fields[f].get("enum", np.nan)
            elif "array" in self.target_types[f]:
                return self.schema_fields[f].get("items", {}).get("enum", np.nan)
            else:
                return np.nan

        target_vals = pd.Series(
            {f: _value_options(f) for f in self.target_fields},
            self.target_fields,
            name="target_values",
        )
        target_vals.index.name = "target_field"
        return target_vals

    def _iter_value_tuples(self) -> Generator[tuple[str, list[str], list[str]]]:
        for f in self.target_fields:
            s = self.common_values_mapped.get(f)
            t = self.target_values[f]
            if s is not None and t is not None:
                yield (f, s, t)

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

    def create_mapping(self, save=True, file_name="mapping_file") -> pd.DataFrame:
        """
        Creates an intermediate mapping dataframe linking the data dictionary to schema
        fields. The index contains the target (schema) field names, and the columns are:
        * source_description
        * source_field
        * common_values OR choices (depending on the data dictionary)
        * target_values
        * value_mapping

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

        mapping_dict = self.match_fields_to_schema()
        mapped_vals = self.match_values_to_schema()

        mapping_dict.drop(columns=["source_type"], inplace=True)

        # reindex to add in any schema fields that weren't returned by the LLM
        mapping_dict = mapping_dict.reindex(self.target_fields)

        mapping_dict = pd.concat(
            [mapping_dict, self.target_values, mapped_vals], axis=1
        )

        unmapped = mapping_dict[mapping_dict["source_field"].isna()].index
        if any(unmapped):
            warnings.warn(
                f"The following schema fields have not been mapped: {list(unmapped)}",
                UserWarning,
            )

        return self.post_process_mapping(mapping_dict, save=save, file_name=file_name)
