from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandera.pandas as pa
from pandera.typing import Series


class GeneratedDict(pa.DataFrameModel):
    field_name: Series[str] = pa.Field(unique=True, alias="Field Name")
    description: Series[str] = pa.Field(nullable=True, alias="Description")
    field_type: Series[str] = pa.Field(alias="Field Type")
    common_values: Optional[Series[str]] = pa.Field(
        nullable=True, alias="Common Values"
    )


class GeneratedDictDescribed(GeneratedDict):
    description: Series[str] = pa.Field(unique=True, alias="Description")


class DataDictionaryEntry(pa.DataFrameModel):
    source_field: Series[str] = pa.Field(unique=True)
    source_description: Series[str] = pa.Field(unique=True)
    source_type: Series[str]
    common_values: Optional[Series[str]] = pa.Field(nullable=True)
    choices: Optional[Series[str]] = pa.Field(nullable=True)

    @pa.dataframe_check(error="Require exactly one: 'common_values' or 'choices'")
    def one_value_column(cls, df: pa.DataFrameModel):
        cols_present = [
            col for col in ["common_values", "choices"] if col in df.columns
        ]
        return len(cols_present) == 1


class DataDictionaryProcessed(DataDictionaryEntry):
    common_values: Optional[Series[Any]] = pa.Field(nullable=True)
    choices: Optional[Series[dict[str, str]]] = pa.Field(nullable=True)

    @pa.check("common_values")
    def check_list_of_strings(cls, s: Series[Any]) -> Series[bool]:
        return s.apply(
            lambda x: isinstance(x, (list, np.ndarray))
            and all(isinstance(i, str) for i in x)
        )
