"""
Common utility functions for autoparser
"""

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import tomli
from pydantic import BaseModel

DEFAULT_CONFIG = "config/autoparser.toml"


def read_config_schema(path: str | Path) -> Dict:
    if isinstance(path, str):
        path = Path(path)

    if path.suffix == ".json":
        return read_json(path)
    elif path.suffix == ".toml":
        with path.open("rb") as fp:
            return tomli.load(fp)
    else:
        raise ValueError(
            f"read_config_schema(): Unsupported file format: {path.suffix}"
        )


def read_json(file: str | Path) -> dict:
    if isinstance(file, str):
        file = Path(file)

    with file.open() as fp:
        return json.load(fp)


def read_data(file: str | Path | pd.DataFrame, file_type: str):
    """Reads in data/mapping files, which expect csv, excel or dataframe formats"""

    if isinstance(file, str):
        file = Path(file)
        if file.suffix == ".csv":
            return pd.read_csv(file)
        elif file.suffix == ".xlsx":
            return pd.read_excel(file)
        else:
            raise ValueError(f"Unsupported format (not CSV or XLSX): {file}")
    elif isinstance(file, pd.DataFrame):
        return file
    else:
        raise ValueError(
            f"{file_type} must be a path to a CSV or XLSX file, or a DataFrame"
        )


def parse_choices(config, s: str) -> Dict[str, Any]:
    delimiter = config["choice_delimiter"]
    delimiter_map = config["choice_delimiter_map"]

    lower_string = lambda s: s.strip().lower()  # NOQA
    if not isinstance(s, str):
        return None

    choices_list = [
        tuple(map(lower_string, x.split(delimiter_map)[:2])) for x in s.split(delimiter)
    ]
    if any(len(c) != 2 for c in choices_list):
        raise ValueError(f"Invalid choices list {choices_list!r}")
    choices = dict(
        tuple(map(lower_string, x.split(delimiter_map)[:2])) for x in s.split(delimiter)
    )

    for k, v in choices.copy().items():
        if v == "true":
            choices[k] = True
        if v == "false":
            choices[k] = False
        if v == "none":
            if k == "":
                choices.pop(k)
            else:
                choices[k] = ""
        if v == "" and k == "":
            choices.pop(k)
    return choices


def load_data_dict(
    config: dict[str:Any], data_dict: str | Path | pd.DataFrame
) -> pd.DataFrame:
    if isinstance(data_dict, str):
        data_dict = Path(data_dict)
        if data_dict.suffix == ".csv":
            data_dict = pd.read_csv(data_dict)
        elif data_dict.suffix == ".xlsx":  # pragma: no cover
            data_dict = pd.read_excel(data_dict)
        else:
            raise ValueError(f"Unsupported format (not CSV or XLSX): {data_dict}")

    column_mappings = {v: k for k, v in config["column_mappings"].items()}
    data_dict.rename(columns=column_mappings, inplace=True)
    return data_dict


# Data structures for llm calls --------------------------

# target classes for generating descriptions


class SingleField(BaseModel):
    field_name: str
    translation: str | None


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
