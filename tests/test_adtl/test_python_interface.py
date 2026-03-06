from __future__ import annotations

from pathlib import Path

import pytest

import adtl


def test_parse(snapshot):
    adtl.parse(
        "tests/test_adtl/parsers/epoch.json",
        "tests/test_adtl/sources/epoch.csv",
        output="output",
        encoding="utf-8",
    )
    assert Path("output-table.csv").read_text() == snapshot
    Path("output-table.csv").unlink()


@pytest.mark.parametrize(
    "spec",
    [
        "tests/test_adtl/parsers/epoch.json",
        "tests/test_adtl/parsers/apply.toml",
        {
            "adtl": {
                "name": "constant",
                "description": "Fixed table",
                "tables": {"metadata": {"kind": "constant"}},
            },
            "metadata": {
                "dataset": "constant",
                "version": "20220505.1",
                "format": "csv",
            },
        },
    ],
    ids=["json", "toml", "dict"],
)
def test_validate_specification(spec):
    adtl.validate_specification(
        spec,
    )


@pytest.mark.parametrize(
    "if_field",
    [
        {"field_name": "value"},
        {"field_name": {"!=": "UNK"}},
        {"any": [{"field_name": "val1"}, {"field_name": "val2"}]},
        {"all": [{"field_name": {"!=": "UNK"}}, {"field_name": {"!=": "NA"}}]},
        {"all": [{"field_name": {"!=": "UNK"}}, {"other_field": 1}]},
        {"not": {"field_name": "excluded_value"}},
        {"not": {"field_name": {"==": "excluded"}}},
    ],
    ids=[
        "field_simple_equality",
        "field_comparison_operator",
        "top_level_any_simple",
        "top_level_all_operators",
        "top_level_all_mixed",
        "top_level_not_simple",
        "top_level_not_operator",
    ],
)
def test_validate_if_field_structures(if_field):
    """Test that various 'if' field structures are valid in both wide and long tables."""
    # Test in wide table (FieldMappingObject)
    wide_spec = {
        "adtl": {
            "name": "test_if_wide",
            "description": "Test if field in wide table",
            "tables": {"test_table": {"kind": "oneToOne"}},
        },
        "test_table": {
            "field_a": {"field": "source_field", "if": if_field},
        },
    }
    adtl.validate_specification(wide_spec)

    # Test in long table (LongEntry)
    long_spec = {
        "adtl": {
            "name": "test_if_long",
            "description": "Test if field in long table",
            "tables": {"long_table": {"kind": "oneToMany", "discriminator": "attr"}},
        },
        "long_table": [
            {
                "attribute": "test_attr",
                "value": {"field": "source_field"},
                "if": if_field,
            },
        ],
    }
    adtl.validate_specification(long_spec)


def test_validate_if_field_in_nested_mapping():
    """Test 'if' field with top-level conditions in nested field mappings (e.g., value_num)."""
    spec = {
        "adtl": {
            "name": "test_nested_if",
            "description": "Test nested if field",
            "tables": {"long_table": {"kind": "oneToMany", "discriminator": "attr"}},
        },
        "long_table": [
            {
                "attribute": "dose",
                "value_num": {
                    "field": "dose_field",
                    "if": {
                        "all": [
                            {"dose_field": {"!=": "UNK"}},
                            {"dose_field": {"!=": "NA"}},
                        ]
                    },
                },
                "attribute_status": {
                    "field": "dose_field",
                    "if": {
                        "any": [
                            {"dose_field": "UNK"},
                            {"dose_field": "NI"},
                            {"dose_field": "NA"},
                        ]
                    },
                },
            },
        ],
    }
    adtl.validate_specification(spec)
