"""
Mixin class for handling long table mappings.
"""

from __future__ import annotations

from functools import cached_property
from typing import Protocol

import pandas as pd

from .config.config import Config


class LongTableMixin(Protocol):
    """
    Mixin providing access to common long-table config fields.

    Requires:
        - self.config: dict-like config with a "long_tables" section
        - self.name: str, the name of the current table
        - self.schema_fields: The schema 'properties' list for the current table
    """

    config: Config
    name: str
    schema_fields: list[dict]

    @cached_property
    def common_cols(self) -> list[str]:
        """Returns the common columns for the long table"""
        ccs = self.config.long_tables[self.name].common_cols
        if not ccs:
            ccs = list(self.config.long_tables[self.name].common_fields.keys())

        return ccs

    @property
    def common_fields(self) -> pd.Series:
        if not hasattr(self, "_common_fields"):
            self._common_fields = self.config.long_tables[self.name].common_fields

        return self._common_fields

    @cached_property
    def variable_col(self) -> str:
        """Returns the variable column for the long table"""
        return self.config.long_tables[self.name].variable_col

    @cached_property
    def value_cols(self) -> list[str]:
        """Returns the value columns for the long table"""
        return self.config.long_tables[self.name].value_cols

    @cached_property
    def other_fields(self) -> list[str]:
        """Returns the other fields in the schema that are not target fields"""
        return [
            f
            for f in self.schema_fields
            if f
            not in [
                *self.common_fields.keys(),
                self.variable_col,
                *self.value_cols,
            ]
        ]
