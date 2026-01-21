from __future__ import annotations

from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag, model_validator


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


class ForField(BaseModel):
    range: Optional[list[int]] = None


class FieldMappingObject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str = Field(description="Corresponding field name in source file")
    values: Optional[Dict[str, Union[str, bool, int, List[Any]]]] = None
    description: Optional[str] = Field(
        default=None,
        description="Description of the source field, usually from an underlying data dictionary",
    )
    source_unit: Optional[FieldMapping] = Field(
        default=None, description="Specifies unit of the field in the source file"
    )
    unit: Optional[str] = Field(
        default=None,
        description=(
            "Specifies the unit that source_unit should be converted to."
            "Both source_unit and unit take unit names from the pint Python library"
        ),
    )
    source_date: Optional[str] = Field(
        default=None, description="Source date format, specified in strftime(3) format"
    )
    date: Optional[str] = Field(
        default=None,
        description="Format to convert source_date format to, in strftime(3) format",
    )
    apply: Optional[Apply] = None
    generate: Optional[Generate] = None
    fieldPattern: Optional[str] = Field(
        default=None,
        description="This is only used with combinedType, specifies a regular expression matching multiple fields",
    )
    if_: Optional[Dict[str, str | int | IfField]] = Field(default=None, alias="if")
    sensitive: Optional[Literal[True]] = Field(
        default=None,
        description=(
            "Indicates to the parser whether the field is sensitive."
            "Usually a sensitive field is hashed or encrypted before storing in the database."
        ),
    )
    ref: Optional[str] = None
    ignoreMissingKey: Optional[Literal[True]] = None
    can_skip: Optional[Literal[True]] = None
    caseInsensitive: Optional[Literal[True]] = None
    type_: Optional[Literal["enum_list"]] = Field(
        default=None,
        alias="type",
        description=(
            "Use when a list is the required output format."
            "Indicates the field contains a list of comma-separated values (with or without "
            "a square bracket surround) which should be converted to a list of strings."
        ),
    )


class CombinedMappingObject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: Optional[str] = None
    combinedType: Literal["list", "any", "all", "min", "max", "firstNonNull", "set"]
    fields: List[FieldMapping]
    excludeWhen: Optional[Union[Literal["none"], Literal["false-like"], List[str]]] = (
        None
    )


# ---------- Discriminated unions for mappings ----------


def field_discriminator(v) -> str:
    if isinstance(v, dict):
        return "field"
    if isinstance(v, str):
        return "string"
    if isinstance(v, bool):
        return "bool"


def mapping_discriminator(v) -> str:
    if isinstance(v, dict) and "combinedType" in v:
        return "combined"
    return "single"


FieldMapping = Annotated[
    Union[
        Annotated[str, Tag("string")],
        Annotated[bool, Tag("bool")],
        Annotated[FieldMappingObject, Tag("field")],
    ],
    Discriminator(field_discriminator),
]
Mapping = Annotated[
    Union[
        Annotated[FieldMapping, Tag("single")],
        Annotated[CombinedMappingObject, Tag("combined")],
    ],
    Discriminator(mapping_discriminator),
]


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
                raise ValueError(
                    f"Table '{k}' must be either a dict (constant, groupBy, oneToOne tables) or a list (oneToMany)"
                )
        out["wide_tables"] = wide_tables
        out["long_tables"] = long_tables
        return out

    @model_validator(mode="after")
    def check_all_tables_match(self) -> ADTLDocument:
        adtl_tables = set(self.adtl.tables.keys())
        wide_tables = set(self.wide_tables.keys())
        long_tables = set(self.long_tables.keys())

        table_maps = wide_tables | long_tables

        if adtl_tables != table_maps:
            missing_tables = adtl_tables - table_maps
            if missing_tables:
                raise ValueError(
                    f"Parser specification missing tables: {', '.join(missing_tables)}"
                )
            extra_tables = table_maps - adtl_tables
            if extra_tables:
                raise ValueError(
                    f"Parser specification has tables not defined in the header: {', '.join(extra_tables)}"
                )

        if any(self.adtl.tables[table].kind != "oneToMany" for table in long_tables):
            raise ValueError(
                "Long format tables must be given kind 'oneToMany' in the header"
            )

        return self
