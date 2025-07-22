from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Literal, Optional

import tomli
from pydantic import BaseModel, SecretStr, field_validator, model_validator
from typing_extensions import Self

from adtl.autoparser.language_models.base_llm import LLMBase
from adtl.autoparser.language_models.gemini import GeminiLanguageModel
from adtl.autoparser.language_models.openai import OpenAILanguageModel


class ColumnMappingConfig(BaseModel):
    """
    Configuration for column mapping in ADTL autoparser.
    This class defines the structure and fields required for column mapping.
    """

    source_field: str
    source_type: str
    source_description: str
    common_values: Optional[str] = None
    choices: Optional[str] = None

    @model_validator(mode="after")
    def check_common_values_and_choices(self) -> Self:
        if self.common_values is not None and self.choices is not None:
            raise ValueError(
                "Only one from 'common values' and 'choices' can be set at once"
            )

        elif self.common_values is None and self.choices is None:
            raise ValueError(
                "Either 'common values' or 'choices' must be set in column mappings"
            )
        return self


class DefaultColumnMappingConfig(ColumnMappingConfig):
    """
    Default configuration for column mapping in ADTL autoparser.
    This class provides default values for the column mapping fields.
    """

    source_field: str = "Field Name"
    source_type: str = "Field Type"
    source_description: str = "Description"
    common_values: Optional[str] = "Common Values"


class LongTableConfig(BaseModel):
    """
    Configuration for long table mapping in ADTL autoparser.
    This class defines the structure and fields required for long table mapping.
    """

    variable_col: str
    value_cols: list[str]
    common_cols: Optional[list[str]] = []
    common_fields: Optional[dict[str, str]] = {}

    @model_validator(mode="after")
    def check_common_cols_fields(self) -> Self:
        if self.common_cols and self.common_fields:
            raise ValueError(
                "Only one from 'common_cols' and 'common_fields' can be set at once"
            )
        return self


class Config(BaseModel):
    name: str = "autoparser"
    description: str = "Configuration for ADTL autoparser"
    language: str
    schemas: dict[str, str]
    column_mappings: ColumnMappingConfig = DefaultColumnMappingConfig()
    llm_provider: Optional[Literal["openai", "gemini"]] = None
    llm_model: Optional[str] = None
    api_key: Optional[SecretStr] = None
    choice_delimiter: str = ","
    choice_delimiter_map: str = "="
    num_refs: int = 3
    max_common_count: int = 25
    min_common_frequency: Optional[float] = None
    long_tables: Optional[dict[str, LongTableConfig]] = None

    _llm: Optional[LLMBase]

    def model_post_init(self, context: Any) -> None:
        """
        Set up the LLM.
        """
        if self.api_key and (self.llm_provider or self.llm_model):
            kwargs = {"api_key": self.api_key.get_secret_value()}
            if self.llm_model is not None:
                kwargs["model"] = self.llm_model

            if (
                self.llm_provider == "openai"
                or self.llm_model in OpenAILanguageModel.valid_models()
            ):
                self._llm = OpenAILanguageModel(**kwargs)
            elif (
                self.llm_provider == "gemini"
                or self.llm_model in GeminiLanguageModel.valid_models()
            ):
                self._llm = GeminiLanguageModel(**kwargs)
            else:
                raise ValueError(
                    f"Could not set up LLM with provider '{self.llm_provider}' and model '{self.llm_model}'."
                )
        else:
            self._llm = None

    @field_validator("api_key", mode="after")
    @classmethod
    def retrieve_api_key(cls, k) -> Optional[SecretStr]:
        try:
            return SecretStr(os.environ[k.get_secret_value()])
        except KeyError:
            return k

    @model_validator(mode="after")
    def check_common_cols_fields(self) -> Self:
        if self.long_tables:
            for table in self.long_tables.keys():
                if table not in self.schemas:
                    raise ValueError(
                        f"Table '{table}' in 'long_tables' not found in 'schemas'"
                    )
        return self

    def check_llm_setup(self) -> None:
        """
        Check if the LLM is set up correctly.
        Raises an error if the LLM is not configured, and points to the potential error.
        """
        if self._llm is None:
            if self.api_key is None:
                raise ValueError("Config: API key required to set up an LLM")
            if self.llm_provider is None and self.llm_model is None:
                raise ValueError(
                    "Config: LLM provider or model must be specified to set up an LLM"
                )


def _config():
    _config_instance: Optional[Config] = None

    def get_config() -> Config:
        """Returns the current config instance, if set."""
        if _config_instance is None:
            raise RuntimeError("Config not initialized. Call setup_config() first.")
        return _config_instance

    def setup_config(path: Path | str | dict) -> Config:
        """Initializes the config singleton from a file."""

        if isinstance(path, dict):
            data = path
        else:
            path = Path(path)
            if path.suffix == ".json":
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
