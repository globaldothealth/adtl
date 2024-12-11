"""Stores the data structures for using with LLM API's"""

from __future__ import annotations

from pydantic import BaseModel

# target classes for generating descriptions


class SingleField(BaseModel):
    field_name: str
    translation: str


class ColumnDescriptionRequest(BaseModel):
    field_descriptions: list[SingleField]


# target classes for matching fields
class SingleMapping(BaseModel):
    target_field: str
    source_description: str | None


class MappingRequest(BaseModel):
    targets_descriptions: list[SingleMapping]


# target classes for matching values to enum/boolean options
class ValueMapping(BaseModel):
    source_value: str
    target_value: str | None


class FieldMapping(BaseModel):
    field_name: str
    mapped_values: list[ValueMapping]


class ValuesRequest(BaseModel):
    values: list[FieldMapping]
