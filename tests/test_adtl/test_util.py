import pytest

import adtl.util as util


@pytest.mark.parametrize(
    "value, target, expected_value",
    [
        ("123", "string", "123"),
        (123, "string", "123"),
        (123.12, "integer", 123),
        ("true", "boolean", "true"),
        ("15", "number", 15.0),
    ],
    ids=[
        "string to string",
        "int to string",
        "float to int",
        "string to bool",
        "string to number",
    ],
)
def test_convert_type_to_schema(value, target, expected_value):
    """
    Test that the get_value function coerces the value to the type specified in the schema.
    """
    new_value = util.convert_to_schema_type(value, target)
    assert new_value == expected_value


@pytest.mark.parametrize(
    "value, target, expected_log",
    [
        ("fish", "integer", "Could not convert value fish to integer"),
        ("fish", "number", "Could not convert value fish to type number"),
    ],
    ids=[
        "string to int",
        "string to number",
    ],
)
def test_convert_type_to_schema_failure(value, target, expected_log, caplog):
    """
    Test that logger writes a debug message when coercion fails.
    """
    with caplog.at_level("DEBUG", logger="adtl.util"):
        new_value = util.convert_to_schema_type(value, target)
    assert new_value == value
    # Check if expected log message is in caplog
    assert any(expected_log in msg for msg in caplog.text.splitlines())
