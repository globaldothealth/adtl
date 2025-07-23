from __future__ import annotations

import abc
from functools import cached_property
from typing import Union

import numpy as np
import pandas as pd

from ..config.config import get_config
from ..dict_reader import format_dict
from ..util import read_json


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
    """

    INDEX_FIELD = "target_field"

    def __init__(self, data_dictionary: Union[str, pd.DataFrame], table_name: str):
        self.name = table_name

        self.config = get_config()

        self.language = self.config.language
        self.model = self.config._llm

        if self.model is None:
            self.config.check_llm_setup()

        self.schema = read_json(self.config.schemas[table_name])
        self.schema_fields = self.schema["properties"]

        self.data_dictionary = format_dict(data_dictionary)

    # Abstract methods ---------------------------------------

    @abc.abstractmethod
    def _iter_value_tuples(self) -> list[tuple[str, list[str], list[str]]]: ...

    @abc.abstractmethod
    def create_mapping(self, save=True, file_name="mapping_file") -> pd.DataFrame: ...

    # Common properties ---------------------------------------

    @cached_property
    def common_values(self) -> pd.Series:
        """
        Returns the commonly repeated values in the source data
        Usually this indicates that the source field is an enum or boolean
        """
        if "common_values" in self.data_dictionary.columns:
            cv = self.data_dictionary.common_values
            cv.index = self.data_dictionary.source_field
            return cv
        elif "choices" in self.data_dictionary.columns:
            choices = self.data_dictionary.choices
            cv = choices.apply(
                lambda x: list(x.values()) if isinstance(x, dict) else None
            )
            cv.index = self.data_dictionary.source_field
            return cv
        else:
            # pandera schema validation means this should never be reached
            raise ValueError(
                "No common values or choices column found in data dictionary"
            )  # pragma: no cover

    @cached_property
    def common_values_mapped(self) -> pd.Series:
        try:
            filtered_dict = self.filtered_data_dict
        except AttributeError:
            raise AttributeError(
                "fields have to be mapped using the `match_fields_to_schema` method"
                " first"
            )
        cv = self.common_values
        return filtered_dict.source_field.apply(
            lambda x: cv.loc[x] if isinstance(x, str) else None
        )

    @property
    def mapped_fields(self) -> pd.Series:
        try:
            return self._mapped_fields
        except AttributeError:
            raise AttributeError(
                "mapped_fields have to be created using `match_fields_to_schema` method"
            )

    @mapped_fields.setter
    def mapped_fields(self, value: pd.Series):
        self._mapped_fields = value

    def _relabel_choices(self, map_df) -> pd.DataFrame:
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

    def post_process_mapping(self, mapping_dict, save, file_name) -> pd.DataFrame:
        """
        Turn lists & dicts into strings for consistency with saved CSV, then save.
        """

        if "choices" in self.data_dictionary:
            mapping_dict = self._relabel_choices(mapping_dict)

        for col in ["common_values", "target_values"]:
            if col in mapping_dict.columns:
                mapping_dict[col] = mapping_dict[col].apply(
                    lambda x: (
                        " | ".join(str(item) for item in x)
                        if isinstance(x, (list, np.ndarray))
                        else x
                    )
                )
        for col in ["choices", "value_mapping"]:
            if col in mapping_dict.columns:
                mapping_dict[col] = mapping_dict[col].apply(
                    lambda x: (
                        " | ".join(f"{k}={v}" for k, v in x.items())
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

    def match_values_to_schema(self) -> pd.DataFrame:
        """
        Use the LLM to match the common values from the data dictionary to the target
        values in the schema - i.e. enum or boolean options.
        """

        values_tuples = list(self._iter_value_tuples())

        # to LLM
        value_pairs = self.model.map_values(values_tuples, self.language)

        value_mapping = {}

        for p in value_pairs.values:
            f = p.field_name
            value_dict = {
                pair.source_value: pair.target_value for pair in p.mapped_values
            }
            value_mapping[f] = value_dict

        self.mapped_values = pd.Series(value_mapping, name="value_mapping")
        self.mapped_values.index.name = self.INDEX_FIELD

        return self.mapped_values
