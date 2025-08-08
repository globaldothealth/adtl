import fastjsonschema
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


def test_expand_schema_no_oneof():
    schema = {
        "type": "object",
        "properties": {"type": {"type": "string"}, "value": {"type": "number"}},
        "required": ["type", "value"],
    }
    validator, expanded = util.expand_schema(schema, "type")
    assert callable(validator)
    assert expanded is False
    # Validator should accept valid dict
    validator({"type": "foo", "value": 1.23})
    # Validator should raise on missing property
    with pytest.raises(fastjsonschema.JsonSchemaException):
        validator({"type": "foo"})


def test_expand_schema_oneof_const():
    schema = {
        "oneOf": [
            {
                "properties": {"kind": {"const": "A"}, "value": {"type": "number"}},
                "required": ["kind", "value"],
            },
            {
                "properties": {"kind": {"const": "B"}, "amount": {"type": "integer"}},
                "required": ["kind", "amount"],
            },
        ],
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
    }
    validators, expanded = util.expand_schema(schema, "kind")
    assert isinstance(validators, dict)
    assert expanded is True
    assert set(validators.keys()) == {"A", "B"}
    # Each validator should require id + discriminator fields
    validators["A"]({"id": "x", "kind": "A", "value": 2.2})
    with pytest.raises(fastjsonschema.JsonSchemaException):
        validators["A"]({"id": "x", "kind": "A"})  # missing value


def test_expand_schema_oneof_enum():
    schema = {
        "oneOf": [
            {
                "properties": {
                    "mode": {"enum": ["foo", "bar"]},
                    "val": {"type": "string"},
                },
                "required": ["mode", "val"],
            }
        ],
        "properties": {"base": {"type": "boolean"}},
    }
    validators, expanded = util.expand_schema(schema, "mode")
    assert expanded is True
    assert set(validators.keys()) == {"foo", "bar"}
    for key in ["foo", "bar"]:
        validators[key]({"base": True, "mode": key, "val": "abc"})
    with pytest.raises(fastjsonschema.JsonSchemaException):
        validators["foo"]({"base": True, "mode": "foo"})  # missing val


def test_expand_schema_unhandled_discriminator():
    schema = {
        "oneOf": [
            {
                "properties": {
                    "type": {"type": "string"},
                },
                "required": ["type"],
            }
        ]
    }
    # Should fallback to not expanding since discriminator is not const/enum
    validator, expanded = util.expand_schema(schema, "type")
    assert callable(validator)
    assert expanded is False


def test_expand_schema_additional_properties():
    schema = {
        "oneOf": [
            {
                "properties": {
                    "k": {"const": "x"},
                },
                "required": ["k"],
            }
        ],
        "additionalProperties": True,
    }
    validators, expanded = util.expand_schema(schema, "k")
    assert expanded is True
    validators["x"]({"k": "x", "foo": "bar"})  # should not raise
