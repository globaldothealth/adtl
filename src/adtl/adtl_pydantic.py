from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Apply(BaseModel):
    function: str
    params: Optional[List[Union[str, int, float, List[Any]]]] = None

    model_config = ConfigDict(extra="forbid")


class Generate(BaseModel):
    type: Literal["uuid5", "timestamp"]
    values: List[str]

    model_config = ConfigDict(extra="forbid")


class ConditionalFields(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ls: Optional[str | int] = Field(default=None, alias="<")
    gr: Optional[str | int] = Field(default=None, alias=">")
    leq: Optional[str | int] = Field(default=None, alias="<=")
    geq: Optional[str | int] = Field(default=None, alias=">=")
    eq: Optional[str | int] = Field(default=None, alias="==")
    neq: Optional[str | int] = Field(default=None, alias="!=")
    reg: Optional[str] = Field(default=None, alias="=~")


class IfField(ConditionalFields):
    all: Optional[list[dict[str, ConditionalFields]]] = Field(default=None, alias="all")
    any: Optional[list[dict[str, ConditionalFields]]] = Field(default=None, alias="any")
    not_: Optional[dict[str, ConditionalFields]] = Field(default=None, alias="not")


class FieldMappingObject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: Optional[str] = None
    description: Optional[str] = None
    values: Optional[Dict[str, Union[str, bool, int, List[Any]]]] = None
    source_unit: Optional[FieldMapping] = None
    unit: Optional[str] = None
    source_date: Optional[str] = None
    date: Optional[str] = None
    apply: Optional[Apply] = None
    generate: Optional[Generate] = None
    fieldPattern: Optional[str] = None
    if_: Optional[Dict[str, str | int | IfField]] = Field(default=None, alias="if")
    sensitive: Optional[Literal[True]] = None
    ref: Optional[str] = None
    ignoreMissingKey: Optional[Literal[True]] = None
    can_skip: Optional[Literal[True]] = None
    caseInsensitive: Optional[Literal[True]] = None


class CombinedMappingObject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: Optional[str] = None
    combinedType: Literal["list", "any", "all", "min", "max", "firstNonNull", "set"]
    fields: List[FieldMapping]
    excludeWhen: Optional[Union[Literal["none"], Literal["false-like"], List[str]]] = (
        None
    )


FieldMapping = Union[str, bool, FieldMappingObject]
Mapping = Union[FieldMapping, CombinedMappingObject]


class ForField(BaseModel):
    range: Optional[list[int]] = None


class LongEntry(BaseModel):
    __pydantic_extra__: dict[str, Mapping] = Field(init=False)
    model_config = ConfigDict(extra="allow")

    ref: Optional[str] = None
    if_: Optional[Dict[str, int | IfField | str]] = Field(default=None, alias="if")
    for_: Optional[Dict[str, list[int] | ForField]] = Field(default=None, alias="for")


# ---------- Table metadata ----------
class TableDef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["constant", "groupBy", "oneToMany", "oneToOne"]
    schema_: Optional[str] = Field(default=None, alias="schema")
    groupBy: Optional[str] = None
    aggregation: Optional[Literal["lastNotNull", "applyCombinedType"]] = None
    common: Optional[Dict[str, FieldMapping]] = None
    discriminator: Optional[str] = None
    optional_fields: Optional[List[str]] = Field(default=None, alias="optional-fields")

    @model_validator(mode="after")
    def check_groupby_aggregation_present(self) -> TableDef:
        if self.kind == "groupBy":
            if not self.groupBy:
                raise ValueError("groupBy key is required for 'groupBy' tables")
            if not self.aggregation:
                raise ValueError("aggregation is required for 'groupBy' tables")
        return self

    @model_validator(mode="after")
    def check_discriminator_present(self) -> TableDef:
        if self.kind == "oneToMany" and not self.discriminator:
            raise ValueError("'discriminator' is required for 'oneToMany' tables")
        return self


# ---------- ADTL root object ----------
class ADTL(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str
    tables: Dict[str, TableDef]
    defs: Optional[Dict[str, Any]] = None
    include_def: Optional[List[str]] = Field(default=None, alias="include-def")
    skipFieldPattern: Optional[str] = None
    defaultDateFormat: Optional[str] = None
    returnUnmatched: Optional[bool] = None
    emptyFields: Optional[str] = None


# ---------- Top-level model ----------
class ADTLDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    adtl: ADTL
    # Collect non-'adtl' top-level keys into this dict in a pre-validator
    wide_tables: Dict[str, dict[str, Mapping]]
    long_tables: Dict[str, List[LongEntry]]

    @model_validator(mode="before")
    def dispatch_tables(cls, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pull all keys except 'adtl' into 'tables' so we can validate them later.
        """

        out: Dict[str, Any] = {}
        # Start with the adtl entry if present
        if "adtl" in schema:
            out["adtl"] = schema["adtl"]
        # Put every other key into tables (keep original values)
        wide_tables = {}
        long_tables = {}
        for k, v in schema.items():
            if k == "adtl":
                continue
            elif isinstance(v, list):
                long_tables[k] = v
            elif isinstance(v, dict):
                wide_tables[k] = v
            else:
                out[k] = v  # unexpected - keep as-is, will error later
        out["wide_tables"] = wide_tables
        out["long_tables"] = long_tables
        return out
