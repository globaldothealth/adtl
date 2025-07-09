import json
from pathlib import Path
from typing import Literal, Optional

import tomli
from pydantic import BaseModel


class ColumnMappingConfig(BaseModel):
    """
    Configuration for column mapping in ADTL autoparser.
    This class defines the structure and fields required for column mapping.
    """

    source_field: str = "Field Name"
    source_type: str = "Field Type"
    source_description: str = "Description"
    common_values: Optional[str] = "Common Values"
    choices: Optional[str] = "Choices"


class LongTableConfig(BaseModel):
    """
    Configuration for long table mapping in ADTL autoparser.
    This class defines the structure and fields required for long table mapping.
    """

    variable_col: str
    value_cols: list[str]
    common_cols: Optional[list[str]] = None
    common_fields: Optional[dict[str, str]] = None


class Config(BaseModel):
    name: str = "autoparser"
    description: str = "Configuration for ADTL autoparser"
    language: str = "en"
    llm_provider: Optional[Literal["openai", "gemini"]] = "openai"
    llm_model: Optional[str] = None
    api_key: Optional[str] = None
    choice_delimiter: str = ", "
    choice_delimiter_map: str = "="
    num_refs: int = 3
    max_common_count: int = 25
    min_common_frequency: Optional[float] = None
    schemas: dict[str, str]
    column_mappings: ColumnMappingConfig = ColumnMappingConfig()
    long_tables: Optional[dict[str, LongTableConfig]] = None


def _config():
    _config_instance: Optional[Config] = None

    def get_config() -> Config:
        """Returns the current config instance, if set."""
        if _config_instance is None:
            raise RuntimeError("Config not initialized. Call setup_config() first.")
        return _config_instance

    def setup_config(path: Path | dict) -> Config:
        """Initializes the config singleton from a file."""

        if isinstance(path, dict):
            data = path
        elif path.suffix == ".json":
            with path.open() as fp:
                data = json.load(fp)
        elif path.suffix == ".toml":
            with path.open("rb") as fp:
                data = tomli.load(fp)
        else:
            raise ValueError(f"Unsupported config file format: {path.suffix}")

        nonlocal _config_instance
        _config_instance = Config(**data)

    return get_config, setup_config


get_config, setup_config = _config()
