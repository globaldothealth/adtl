import pytest

from adtl.autoparser import setup_config


@pytest.fixture(scope="function")
def config():
    setup_config(
        {
            "name": "Test Config",
            "max_common_count": 8,
            "language": "fr",
            "llm_provider": "openai",
            "api_key": "1234",  # dummy API key
            "schemas": {
                "animals": "tests/test_autoparser/schemas/animals.schema.json",
                "vet_observations": "tests/test_autoparser/schemas/vet-obs.schema.json",
            },
            "long_tables": {
                "vet_observations": {
                    "common_cols": ["animal_id", "visit_date"],
                    "variable_col": "observation",
                    "value_cols": ["string_value", "boolean_value", "numeric_value"],
                }
            },
        }
    )
