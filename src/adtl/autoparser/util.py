"""
Common utility functions for autoparser
"""

from __future__ import annotations

import difflib
import json
import re
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import tomli

from adtl.autoparser.language_models.gemini import GeminiLanguageModel
from adtl.autoparser.language_models.openai import OpenAILanguageModel

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


def parse_choices(s: str) -> Dict[str, Any] | None:
    """
    Takes the choices from llm as a string and turns into pairs.

    "oui=True, non=False, blah=None" -> {"oui": True, "non": False, "blah": ""}
    "vivant=alive, décédé=dead, " "=None" -> {"vivant": "alive", "décédé": "dead"}
    {2: True} -> None
    "" " = " ", poisson=fish" -> {"poisson": "fish"}
    ecouvillon+croûte=[swab, crust], ecouvillon=[swab]" ->
            {"ecouvillon+croûte": ["swab", "crust"], "ecouvillon": ["swab"]}

    """

    if not isinstance(s, str):
        return None

    split_str = re.split(r",(?!(?:[^\[]*\])|(?:[^\[]*\[[^\]]*$))", s)
    choices_list = [tuple(x.strip().split("=")) for x in split_str]
    if any(len(c) != 2 for c in choices_list):
        raise ValueError(
            f"autoparser: Invalid choices list for value mapping {choices_list!r}"
        )
    choices = dict(choices_list)

    for k, v in choices.copy().items():
        if v.lower() == "true":
            choices[k] = True
        if v.lower() == "false":
            choices[k] = False
        if v.lower() == "none":
            if k == "":
                choices.pop(k)
            else:
                choices[k] = ""
        if v == "" and k == "":
            choices.pop(k)
        if "[" and "]" in v:
            choices[k] = [i for i in v.strip("[]").replace(" ", "").split(",")]
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


def setup_llm(provider, api_key):
    """
    Setup the LLM to use to generate descriptions.

    Separate from the __init__ method to allow for extra barrier between raw data &
    LLM.

    Parameters
    ----------
    key
        API key
    name
        Name of the LLM to use (currently only OpenAI and Gemini are supported)
    """
    if api_key is None:
        raise ValueError("API key required to set up an LLM")

    if provider == "openai":  # pragma: no cover
        return OpenAILanguageModel(api_key=api_key)
    elif provider == "gemini":  # pragma: no cover
        return GeminiLanguageModel(api_key=api_key)
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")


def check_matches(llm: str, source: list[str], cutoff=0.8) -> str | None:
    """
    Use to check if a string returned by an llm is a close enough match to the original
    source.

    Useful for checking or finding the original word if the LLM misspells it when
    returning results.

    Parameters
    ----------
    llm
        String returned by the LLM
    source
        List of strings to compare against (usually the original fields/descriptions
        from the previous step)
    """
    if not isinstance(source, list):
        raise ValueError(
            f"check matches: source must be a list of strings, got '{source}'"
        )
    matches = difflib.get_close_matches(llm, source, n=1, cutoff=cutoff)
    return matches[0] if matches else None
