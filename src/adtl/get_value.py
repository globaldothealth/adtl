from __future__ import annotations

import hashlib
import logging
import re
import uuid
import warnings
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Union

import pint

import adtl.transformations as tf
import adtl.util as util
from adtl.transformations import AdtlTransformationWarning

StrDict = dict[str, Any]
Rule = Union[str, StrDict]
Context = Union[dict[str, Union[bool, int, str, list[str]]], None]

logger = logging.getLogger(__name__)

# helper functions


def matching_fields(fields: list[str], pattern: str) -> list[str]:
    "Returns fields matching pattern"
    compiled_pattern = re.compile(pattern)
    return [f for f in fields if compiled_pattern.match(f)]


def flatten(xs):
    """
    Flatten a list of lists +-/ non-list items
    e.g.
    [None, ['Dexamethasone']] -> [None, 'Dexamethasome']
    """
    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x


def hash_sensitive(value: str) -> str:
    """Hashes sensitive values. This is not generally sufficient for
    anonymisation, as the value still serves as a unique identifier,
    but is better than storing the value unprocessed."""
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def skip_field(row: StrDict, rule: StrDict, ctx: Context = None):
    "Returns True if the field is missing and allowed to be skipped"
    if rule.get("can_skip"):
        return rule["field"] not in row
    if ctx and ctx.get("skip_pattern") and ctx.get("skip_pattern").match(rule["field"]):
        return rule["field"] not in row
    return False


def apply_function(value, row: StrDict, rule: StrDict, ctx: Context):
    # apply data transformations.
    transformation = rule["apply"]["function"]
    params = None
    if "params" in rule["apply"]:
        params = []
        for i in range(len(rule["apply"]["params"])):
            if isinstance(rule["apply"]["params"][i], str) and rule["apply"]["params"][
                i
            ].startswith("$"):
                params.append(row[rule["apply"]["params"][i][1:]])
            elif isinstance(rule["apply"]["params"][i], list):
                param = [
                    (
                        row[rule["apply"]["params"][i][j][1:]]
                        if (
                            isinstance(rule["apply"]["params"][i][j], str)
                            and rule["apply"]["params"][i][j].startswith("$")
                        )
                        else rule["apply"]["params"][i][j]
                    )
                    for j in range(len(rule["apply"]["params"][i]))
                ]
                params.append(param)
            else:
                params.append(rule["apply"]["params"][i])

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", category=AdtlTransformationWarning)
            if params:
                value = getattr(tf, transformation)(value, *params)
            else:
                value = getattr(tf, transformation)(value)
    except AttributeError:
        raise AttributeError(
            f"Error using a data transformation: Function {transformation} "
            "has not been defined."
        )
    except AdtlTransformationWarning as e:
        if ctx and ctx.get("returnUnmatched"):
            warnings.warn(str(e), AdtlTransformationWarning)
            return value
        else:
            logger.error(str(e))
            return None
    return value


def convert_values(value, rule: StrDict, ctx: Context) -> str | list[str | None] | None:
    if rule.get("type") == "enum_list":
        try:
            value = [v.lstrip(" ").rstrip(" ") for v in value.strip("[]").split(",")]
            new_rule = {k: v for k, v in rule.items() if k != "type"}
            value = [convert_values(v, new_rule, ctx) for v in value]
            return value
        except Exception as e:
            logger.debug(f"Error converting {value} to a list: {e}")
            return value

    if rule.get("caseInsensitive") and isinstance(value, str):
        value = value.lower().lstrip(" ").rstrip(" ")
        rule["values"] = {k.lower(): v for k, v in rule["values"].items()}

    if rule.get("ignoreMissingKey") or (ctx and ctx.get("returnUnmatched")):
        value = rule["values"].get(value, value)
    else:
        value = rule["values"].get(value)

    # recheck if value is empty after mapping (use to map values to None)
    return None if value == "" else value


# main functions


def get_value(
    row: StrDict,
    rule: Rule,
    ctx: Context = None,
    coerce_type: str | list[str] | None = None,
) -> Any:
    """Gets value from row using rule

    Same as get_value_unhashed(), except it hashes if sensitive = True in rule.
    This function should be used instead of get_value_unhashed() for
    application code.

    row: dict with row data
    rule: rule to apply to row taken from parser file, can be a string or a dict
    ctx: top level rules from parser file
    coerce_type: field type taken from schema to coerce the value to
        If no type given, or no schema provided, no specific coercion is applied.
    """
    value = get_value_unhashed(row, rule, ctx)
    if isinstance(rule, dict) and rule.get("sensitive") and value is not None:
        return hash_sensitive(value)

    if coerce_type is not None and value is not None:
        return util.convert_to_schema_type(value, coerce_type)

    if not isinstance(value, str):
        return value
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def get_value_unhashed(row: StrDict, rule: Rule, ctx: Context = None) -> Any:
    """Gets value from row using rule (unhashed)

    Unlike get_value() this function does NOT hash sensitive data
    and should not be called directly, except for debugging. Use
    get_value() instead.
    """
    if not isinstance(rule, dict) or isinstance(rule, list):
        # not a container, is constant
        return rule
    # Check whether field is present if it's allowed to be passed over
    if "field" in rule:
        # do not check for condition if field is missing
        if skip_field(row, rule, ctx):
            return None
        # do not parse field if condition is not met
        if "if" in rule and not parse_if(row, rule["if"]):
            return None
        value = row[rule["field"]]
        if "apply" in rule:
            value = apply_function(value, row, rule, ctx)
        if value == "":
            return None
        if "values" in rule:
            value = convert_values(value, rule, ctx)
        # Either source_unit / unit OR source_date / date triggers conversion
        # do not parse units if value is empty
        if "source_unit" in rule and "unit" in rule:
            assert "source_date" not in rule and "date" not in rule
            source_unit = get_value(row, rule["source_unit"])
            unit = rule["unit"]
            if not isinstance(source_unit, str):
                logger.debug(
                    f"Error converting source_unit {source_unit} to {unit!r} with "
                    "rule: {rule}, defaulting to assume source_unit is {unit}"
                )
                return float(value)
            try:
                value = pint.Quantity(float(value), source_unit).to(unit).m
            except ValueError:
                if ctx and ctx.get("returnUnmatched"):
                    logger.debug(f"Could not convert {value} to a floating point")
                    return value
                raise ValueError(
                    f"Could not convert '{value}' from '{rule['field']}' to a floating point"
                )
        if "source_date" in rule or (ctx and ctx.get("is_date")):
            assert "source_unit" not in rule and "unit" not in rule
            target_date = rule.get("date", "%Y-%m-%d")
            source_date = (
                get_value(row, rule["source_date"])
                if "source_date" in rule
                else ctx["defaultDateFormat"]
            )
            if source_date != target_date:
                try:
                    value = datetime.strptime(value, source_date).strftime(target_date)
                except (TypeError, ValueError):
                    logger.info(f"Could not parse date: {value}")
                    if ctx and ctx.get("returnUnmatched"):
                        return value
                    return None
        return value
    elif "combinedType" in rule:
        return get_combined_type(row, rule, ctx)
    elif "generate" in rule:
        return generate_field(row, rule, ctx)
    else:
        raise ValueError(f"Could not return value for {rule}")


def parse_if(
    row: StrDict, rule: StrDict, ctx: Callable[[str], dict] = None, can_skip=False
) -> bool:
    "Parse conditional statements and return a boolean"

    n_keys = len(rule.keys())
    assert 1 <= n_keys < 3
    if n_keys > 1:
        if "can_skip" in rule:
            can_skip = True
    key = next(iter(rule.keys()))
    if key == "not" and isinstance(rule[key], dict):
        return not parse_if(row, rule[key], ctx, can_skip)
    elif key == "any" and isinstance(rule[key], list):
        return any(parse_if(row, r, ctx, can_skip) for r in rule[key])
    elif key == "all" and isinstance(rule[key], list):
        return all(parse_if(row, r, ctx, can_skip) for r in rule[key])
    try:
        attr_value = row[key].lower() if "caseInsensitive" in rule else row[key]
    except KeyError as e:
        if can_skip is True:
            return False
        elif ctx:
            if skip_field(row, {"field": key}, ctx(key)):
                return False
        raise ValueError(f"Column '{key}' not found.") from e

    if isinstance(rule[key], dict):
        cmp = next(iter(rule[key]))
        value = rule[key][cmp]
        try:
            cast_value = type(value)(attr_value)
        except ValueError:
            logger.debug(
                f"Error when casting value {attr_value!r} with rule: {rule}, defaulting"
                " to False"
            )
            return False
        if cmp == ">":
            return cast_value > value
        elif cmp == ">=":
            return cast_value >= value
        elif cmp == "<":
            return cast_value < value
        elif cmp == "<=":
            return cast_value <= value
        elif cmp == "!=":
            return cast_value != value
        elif cmp in ["=", "=="]:
            return cast_value == value
        elif cmp == "=~":
            return bool(re.match(value, cast_value, re.IGNORECASE))
        else:
            raise ValueError(f"Unrecognized operand: {cmp}")
    elif isinstance(rule[key], set):  # common error, missed colon to make it a dict
        raise ValueError(
            f"if-subexpressions should be a dictionary, is a set: {rule[key]}"
        )
    else:
        value = rule[key]
        try:
            cast_value = type(value)(attr_value)
        except ValueError:
            logger.debug(
                f"Error when casting value {attr_value!r} with rule: {rule}, defaulting"
                " to False"
            )
            return False
        return cast_value == value


def get_combined_type(row: StrDict, rule: StrDict, ctx: Context = None):
    """Gets value from row for a combinedType rule

    A rule with the combinedType key combines multiple fields in the row
    to get the value. Thus this rule assumes that the combinedType fields
    do NOT have repeated (possibly different) values across the dataset.

    Example of dataset that will be handled correctly, with modliv and
    mildliver being the categorical indicators for moderate and mild
    liver disease respectively:

        subjid,modliv,mildliver,otherfield
        1,0,1,NA
        1,,,

    Example of dataset that will not be handled correctly:

        subjid,modliv,mildliver,otherfield
        1,0,,
        1,,1,

    For a combinedType rule to successfully run, all the field values should
    be present in the same row.
    """
    assert "combinedType" in rule
    combined_type = rule["combinedType"]
    rules = []
    # expand fieldPattern rules
    for r in rule["fields"]:
        if "fieldPattern" in r:
            for match in matching_fields(list(row.keys()), r.get("fieldPattern")):
                rules.append({"field": match, **r})
        else:
            rules.append(r)
    if combined_type in ["all", "any", "min", "max"]:
        values = [get_value(row, r, ctx) for r in rules]
        values = [v for v in values if v not in [None, ""]]
        # normally calling eval() is a bad idea, but here values are restricted, so okay
        return eval(combined_type)(values) if values else None
    elif combined_type == "firstNonNull":
        try:
            return next(
                filter(
                    lambda item: item is not None,
                    flatten([get_value(row, r, ctx) for r in rules]),
                )
            )
        except StopIteration:
            return None
    elif combined_type == "list" or combined_type == "set":
        excludeWhen = rule.get("excludeWhen")
        if excludeWhen not in [None, "false-like", "none"] and not isinstance(
            excludeWhen, list
        ):
            raise ValueError(
                "excludeWhen rule should be 'none', 'false-like', or a list of values"
            )

        values = flatten([get_value(row, r, ctx) for r in rules])
        if combined_type == "set":
            values = [*set(values)]
        if excludeWhen is None:
            return list(values)
        if excludeWhen == "none":
            return [v for v in values if v is not None]
        elif excludeWhen == "false-like":
            return [v for v in values if v]
        else:
            return [v for v in values if v not in excludeWhen]
    else:
        raise ValueError(f"Unknown {combined_type} in {rule}")


def generate_field(row: StrDict, rule: StrDict, ctx: Context = None):
    """Generates a field value based on a specified method.

    Currently supports 'uuid5' and 'datetime' generation methods.

    Example rule for UUID generation:
        {
            "generate": {"type": "uuid5", "values": ["field1", "field2", "field3"]}
        }

    Example rule for datetime generation (will generate a datetime in current ISO format):
        {
            "generate": {"type": "datetime"},
        }
    """

    method = rule["generate"]["type"]

    if method == "datetime":
        return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
    elif method == "uuid5":
        seed = [
            str(get_value_unhashed(row, {"field": f}, ctx)).lower()
            for f in rule["generate"]["values"]
        ]
        return str(uuid.uuid5(ctx["namespace"], "|".join(seed)))
    raise ValueError(f"Unknown generation method: {method}")
