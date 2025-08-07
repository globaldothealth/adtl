from __future__ import annotations

import logging

import fastjsonschema

logger = logging.getLogger(__name__)


def convert_to_schema_type(value, target_type: str | list[str]):
    """
    Convert value to the target type specified in a JSON schema.
    """
    type_casters = {
        "string": str,
        "integer": int,
        "number": float,
    }

    if isinstance(target_type, str):
        target_type = [target_type]

    for tt in target_type:
        if tt in type_casters:
            try:
                return type_casters[tt](value)
            except (ValueError, TypeError):
                if tt == "integer":
                    # Special case for converting float to integer with rounding
                    try:
                        return int(round(float(value)))
                    except (ValueError, TypeError):
                        logger.debug(f"Could not convert value {value} to integer")
                        return value

                logger.debug(f"Could not convert value {value} to type {tt}")

    return value


def expand_schema(schema, discriminator):
    per_attribute_schemas = {}

    general_properties = schema.get("properties", {})
    general_required = schema.get("required", [])

    for subschema in schema.get("oneOf", []):
        props = subschema.get("properties", {})
        required = subschema.get("required", [])
        # Figure out which attributes this subschema applies to
        attr = props.get(discriminator, {})
        attr_keys = []
        if "const" in attr:
            attr_keys = [attr["const"]]
        elif "enum" in attr:
            attr_keys = attr["enum"]
        else:
            continue  # Unhandled case
        # Build the minimal schema for this attribute
        per_attr_schema = {
            "type": "object",
            "properties": general_properties | props,
            "required": general_required + required,
            "additionalProperties": schema.get("additionalProperties", False),
        }
        for key in attr_keys:
            per_attribute_schemas[key] = per_attr_schema

    return {k: fastjsonschema.compile(v) for k, v in per_attribute_schemas.items()}
