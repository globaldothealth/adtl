import re

import pytest

from adtl.autoparser.config.config import get_config, setup_config
from adtl.autoparser.language_models.gemini import GeminiLanguageModel


def test_get_config_not_set_up():
    """
    Test that an error is raised if the config is not set up before accessing it.
    """
    with pytest.raises(
        RuntimeError,
        match=re.escape("Config not initialized. Call setup_config() first."),
    ):
        get_config()


@pytest.mark.parametrize(
    "input, error",
    [
        (
            {
                "language": "en",
                "schemas": {
                    "animals": "tests/test_autoparser/schemas/animals.schema.json"
                },
                "column_mappings": {
                    "source_field": "Field Name",
                    "source_description": "Description",
                    "source_type": "Field Type",
                    "common_values": "Common Values",
                    "choices": "Choices",
                },
            },
            "Only one from 'common values' and 'choices' can be set at once",
        ),
        (
            {
                "language": "en",
                "schemas": {
                    "animals": "tests/test_autoparser/schemas/animals.schema.json"
                },
                "column_mappings": {
                    "source_field": "Field Name",
                    "source_description": "Description",
                    "source_type": "Field Type",
                    "common_values": None,
                    "choices": None,
                },
            },
            "Either 'common values' or 'choices' must be set",
        ),
    ],
)
def test_valid_column_config(input, error):
    with pytest.raises(ValueError, match=error):
        setup_config(input)


@pytest.mark.parametrize(
    "input, error",
    [
        (
            {
                "language": "en",
                "schemas": {
                    "animals": "tests/test_autoparser/schemas/animals.schema.json"
                },
                "long_tables": {
                    "animals": {
                        "variable_col": "var",
                        "value_cols": ["value_bool", "value_str", "value"],
                        "common_cols": ["id", "start_date"],
                        "common_fields": {"id": "subjid", "start_date": "date"},
                    },
                },
            },
            "Only one from 'common_cols' and 'common_fields' can be set at once",
        )
    ],
)
def test_valid_long_table_config(input, error):
    with pytest.raises(ValueError, match=error):
        setup_config(input)


@pytest.mark.parametrize(
    "input, error",
    [
        (
            {
                "language": "en",
                "schemas": {
                    "animals": "tests/test_autoparser/schemas/animals.schema.json"
                },
                "long_tables": {
                    "vet_info": {
                        "variable_col": "var",
                        "value_cols": ["value_bool", "value_str", "value"],
                        "common_cols": ["id", "start_date"],
                    },
                },
            },
            "Table 'vet_info' in 'long_tables' not found in 'schemas'",
        )
    ],
)
def test_long_table_no_schema(input, error):
    with pytest.raises(ValueError, match=error):
        setup_config(input)


def test_check_llm_setup_no_key():
    with pytest.raises(ValueError, match="API key required to set up an LLM"):
        setup_config(
            {
                "language": "en",
                "llm_provider": "openai",
                "schemas": {
                    "animals": "tests/test_autoparser/schemas/animals.schema.json"
                },
            }
        )
        get_config().check_llm_setup()


def test_check_llm_setup_no_model_no_provider():
    with pytest.raises(ValueError, match="LLM provider or model must be specified"):
        setup_config(
            {
                "language": "en",
                "api_key": "abcd",
                "schemas": {
                    "animals": "tests/test_autoparser/schemas/animals.schema.json"
                },
            }
        )
        get_config().check_llm_setup()


def test_check_llm_setup_bad_provider():
    with pytest.raises(ValueError, match="Input should be 'openai' or 'gemini'"):
        setup_config(
            {
                "language": "en",
                "llm_provider": "fish",
                "api_key": "abcd",  # dummy API key
                "schemas": {
                    "animals": "tests/test_autoparser/schemas/animals.schema.json"
                },
            }
        )


def test_check_llm_setup_invalid_model_no_provider():
    with pytest.raises(ValueError, match="Could not set up LLM with provider"):
        setup_config(
            {
                "language": "en",
                "llm_model": "fish",
                "api_key": "abcd",  # dummy API key
                "schemas": {
                    "animals": "tests/test_autoparser/schemas/animals.schema.json"
                },
            }
        )


def test_check_llm_setup_valid_model_no_provider():
    setup_config(
        {
            "language": "en",
            "llm_model": "gemini-2.0-flash",
            "api_key": "abcd",  # dummy API key
            "schemas": {"animals": "tests/test_autoparser/schemas/animals.schema.json"},
        }
    )

    config = get_config()

    assert isinstance(config._llm, GeminiLanguageModel)
    assert config._llm.model == "gemini-2.0-flash"
