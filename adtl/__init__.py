import argparse
import csv
import hashlib
import io
import json
import logging
import itertools
import copy
import re
import importlib.metadata
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Union, Callable
from more_itertools import unique_everseen

import pint
import tomli
import requests
import fastjsonschema
from tqdm import tqdm

import adtl.transformations as tf

SUPPORTED_FORMATS = {"json": json.load, "toml": tomli.load}
DEFAULT_DATE_FORMAT = "%Y-%m-%d"

StrDict = Dict[str, Any]
Rule = Union[str, StrDict]
Context = Optional[Dict[str, Union[bool, int, str, List[str]]]]

__version__ = importlib.metadata.version("adtl")


def get_value(row: StrDict, rule: Rule, ctx: Context = None) -> Any:
    """Gets value from row using rule

    Same as get_value_unhashed(), except it hashes if sensitive = True in rule.
    This function should be used instead of get_value_unhashed() for
    application code.
    """
    value = get_value_unhashed(row, rule, ctx)
    if isinstance(rule, dict) and rule.get("sensitive") and value is not None:
        return hash_sensitive(value)
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
    if not isinstance(rule, dict) or isinstance(
        rule, list
    ):  # not a container, is constant
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
            # apply data transformations.
            transformation = rule["apply"]["function"]
            if "params" in rule["apply"]:
                params = []
                for i in range(len(rule["apply"]["params"])):
                    if isinstance(rule["apply"]["params"][i], str) and rule["apply"][
                        "params"
                    ][i].startswith("$"):
                        params.append(row[rule["apply"]["params"][i][1:]])
                    elif isinstance(rule["apply"]["params"][i], list):
                        param = [
                            row[rule["apply"]["params"][i][j][1:]]
                            if (
                                isinstance(rule["apply"]["params"][i][j], str)
                                and rule["apply"]["params"][i][j].startswith("$")
                            )
                            else rule["apply"]["params"][i][j]
                            for j in range(len(rule["apply"]["params"][i]))
                        ]
                        params.append(param)
                    else:
                        params.append(rule["apply"]["params"][i])

                try:
                    value = getattr(tf, transformation)(value, *params)
                except AttributeError:
                    raise AttributeError(
                        f"Error using a data transformation: Function {transformation} has not been defined."
                    )
            else:
                try:
                    value = getattr(tf, transformation)(value)
                except AttributeError:
                    raise AttributeError(
                        f"Error using a data transformation: Function {transformation} has not been defined."
                    )
            return value
        if value == "":
            return None
        if "values" in rule:
            if rule.get("ignoreMissingKey"):
                value = rule["values"].get(value, value)
            else:
                value = rule["values"].get(value)
        # Either source_unit / unit OR source_date / date triggers conversion
        # do not parse units if value is empty
        if "source_unit" in rule and "unit" in rule:
            assert "source_date" not in rule and "date" not in rule
            source_unit = get_value(row, rule["source_unit"])
            unit = rule["unit"]
            if type(source_unit) != str:
                logging.debug(
                    f"Error converting source_unit {source_unit} to {unit!r} with rule: {rule}, "
                    "defaulting to assume source_unit is {unit}"
                )
                return float(value)
            try:
                value = pint.Quantity(float(value), source_unit).to(unit).m
            except ValueError:
                raise ValueError(f"Could not convert {value} to a floating point")
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
                    logging.info(f"Could not parse date: {value}")
                    return None
        return value
    elif "combinedType" in rule:
        return get_combined_type(row, rule, ctx)
    else:
        raise ValueError(f"Could not return value for {rule}")


def matching_fields(fields: List[str], pattern: str) -> List[str]:
    "Returns fields matching pattern"
    compiled_pattern = re.compile(pattern)
    return [f for f in fields if compiled_pattern.match(f)]


def parse_if(
    row: StrDict, rule: StrDict, ctx: Callable[[str], dict] = None, can_skip=False
) -> bool:
    "Parse conditional statements and return a boolean"

    n_keys = len(rule.keys())
    assert n_keys == 1 or n_keys == 2
    if n_keys == 2:
        assert "can_skip" in rule
        can_skip = True
    key = next(iter(rule.keys()))
    if key == "not" and isinstance(rule[key], dict):
        return not parse_if(row, rule[key], ctx, can_skip)
    elif key == "any" and isinstance(rule[key], list):
        return any(parse_if(row, r, ctx, can_skip) for r in rule[key])
    elif key == "all" and isinstance(rule[key], list):
        return all(parse_if(row, r, ctx, can_skip) for r in rule[key])
    try:
        attr_value = row[key]
    except KeyError:
        if can_skip is True:
            return False
        elif ctx:
            if skip_field(row, {"field": key}, ctx(key)):
                return False
        raise

    if isinstance(rule[key], dict):
        cmp = next(iter(rule[key]))
        value = rule[key][cmp]
        try:
            cast_value = type(value)(attr_value)
        except ValueError:
            logging.debug(
                f"Error when casting value {attr_value!r} with rule: {rule}, defaulting to False"
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
            logging.debug(
                f"Error when casting value {attr_value!r} with rule: {rule}, defaulting to False"
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


def expand_refs(spec_fragment: StrDict, defs: StrDict) -> Union[StrDict, List[StrDict]]:
    "Expand all references (ref) with definitions (defs)"

    if spec_fragment == {}:
        return {}
    if isinstance(spec_fragment, dict):
        if "ref" in spec_fragment:
            reference_expanded = defs[spec_fragment["ref"]]
            del spec_fragment["ref"]
            spec_fragment = {**reference_expanded, **spec_fragment}
        return {k: expand_refs(spec_fragment[k], defs) for k in spec_fragment}
    elif isinstance(spec_fragment, list):
        return [expand_refs(m, defs) for m in spec_fragment]
    else:
        return spec_fragment


def expand_for(spec: List[StrDict]) -> List[StrDict]:
    "Expands for expressions in oneToMany table blocks"

    out = []

    def replace_val(
        item: Union[str, float, Dict[str, Any]], replace: Dict[str, Any]
    ) -> Dict[str, Any]:
        block = {}
        if isinstance(item, str):
            return item.format(**replace)
        elif isinstance(item, (float, int)):
            return item
        for k, v in item.items():
            if not isinstance(k, str):
                block[k] = v
                continue
            rk = k.format(**replace)
            if isinstance(v, dict):
                block[rk] = replace_val(v, replace)
            elif isinstance(v, str):
                block[rk] = v.format(**replace)
            elif isinstance(v, list):
                block[rk] = [replace_val(it, replace) for it in v]
            else:
                block[rk] = v
        return block

    for match in spec:
        if "for" not in match:
            out.append(match)
            continue
        for_expr = match.pop("for")
        if not isinstance(for_expr, dict):
            raise ValueError(
                f"for expression {for_expr!r} is not a dictionary of variables to list of values or a range"
            )

        # Expand ranges when available
        for var in for_expr:
            if (
                "range" in for_expr[var]
                and isinstance(for_expr[var]["range"], list)
                and len(for_expr[var]["range"]) == 2
                and isinstance(for_expr[var]["range"][0], int)
                and isinstance(for_expr[var]["range"][1], int)
                and for_expr[var]["range"][1] > for_expr[var]["range"][0]
            ):
                start, end = for_expr[var]["range"]
                for_expr[var] = range(start, end + 1)  # add one to include end in list
            elif isinstance(for_expr[var], list):
                pass
            else:
                raise ValueError(
                    f"for expression {for_expr!r} can only have lists or ranges for variables"
                )
        loop_vars = sorted(for_expr.keys())
        loop_assignments = [
            dict(zip(loop_vars, vals))
            for vals in itertools.product(*(for_expr[var] for var in loop_vars))
        ]
        for replacement in loop_assignments:
            out.append(replace_val(match, replacement))
    return out


def hash_sensitive(value: str) -> str:
    """Hashes sensitive values. This is not generally sufficient for
    anonymisation, as the value still serves as a unique identifier,
    but is better than storing the value unprocessed."""
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def remove_null_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    "Removes keys which map to null - but not empty strings or 'unknown' etc types"
    return {k: v for k, v in d.items() if v is not None}


def get_date_fields(schema: Dict[str, Any]) -> List[str]:
    "Returns list of date fields from schema"
    fields = [
        field
        for field in schema["properties"]
        if field == "date" or "date_" in field or "_date" in field
    ]
    format_date_fields = [
        field
        for field in schema["properties"]
        if schema["properties"][field].get("format") == "date"
    ]
    return sorted(set(fields + format_date_fields))


def make_fields_optional(
    schema: Dict[str, Any], optional_fields: List[str]
) -> Dict[str, Any]:
    "Returns JSON schema with required fields modified to drop optional fields"
    if optional_fields is None:
        return schema
    _schema = copy.deepcopy(schema)
    _schema["required"] = sorted(set(schema["required"]) - set(optional_fields))
    for opt in ["oneOf", "anyOf"]:
        if opt in _schema:
            if any("required" in _schema[opt][x] for x in range(len(_schema[opt]))):
                for x in range(len(_schema[opt])):
                    _schema[opt][x]["required"] = list(
                        set(_schema[opt][x]["required"]) - set(optional_fields or [])
                    )
                if all(
                    all(bool(v) is False for v in _schema[opt][x].values())
                    for x in range(len(_schema[opt]))
                ):
                    _schema.pop(opt)
                else:
                    _schema[opt] = list(unique_everseen(_schema[opt]))
    return _schema


def relative_path(source_file, target_file):
    return Path(source_file).parent / target_file


def read_definition(file: Path) -> Dict[str, Any]:
    "Reads definition from file into a dictionary"
    if isinstance(file, str):
        file = Path(file)
    if file.suffix == ".json":
        with file.open() as fp:
            return json.load(fp)
    elif file.suffix == ".toml":
        with file.open("rb") as fp:
            return tomli.load(fp)
    else:
        raise ValueError(f"Unsupported file format: {file}")


def skip_field(row: StrDict, rule: StrDict, ctx: Context = None):
    "Returns True if the field is missing and allowed to be skipped"
    if rule.get("can_skip"):
        return rule["field"] not in row
    if ctx and ctx.get("skip_pattern") and ctx.get("skip_pattern").match(rule["field"]):
        return rule["field"] not in row
    return False


class Parser:
    """Main parser class that loads a specification

    Typical use of this within Python code::

        import adtl

        parser = adtl.Parser(specification)
        print(parser.tables) # list of tables created

        for row in parser.parse().read_table(table):
            print(row)
    """

    def __init__(
        self,
        spec: Union[str, Path, StrDict],
        include_defs: List[str] = [],
        quiet: bool = False,
    ):
        """Loads specification from spec in format (default json)

        Args:
            spec: Either the specification file to read (as Path or str), or
                the specification loaded into a dictionary
            include_defs: Definition files to include. These are spliced
                directly into the adtl.defs section of the :ref:`specification`.
            quiet: Boolean that switches on the verbosity of the parser, default False
        """

        self.data: StrDict = {}
        self.defs: StrDict = {}
        self.fieldnames: Dict[str, List[str]] = {}
        self.specfile = None
        self.include_defs = include_defs
        self.validators: StrDict = {}
        self.schemas: StrDict = {}
        self.quiet = quiet
        self.date_fields = []
        self.report = {
            "validation_errors": defaultdict(Counter),
            "total_valid": defaultdict(int),
            "total": defaultdict(int),
        }
        self.report_available = False
        if isinstance(spec, str):
            spec = Path(spec)
        if isinstance(spec, Path):
            self.specfile = spec
            fmt = spec.suffix[1:]
            if fmt not in SUPPORTED_FORMATS:
                raise ValueError(f"adtl specification format not supported: {fmt}")
            with spec.open("rb") as fp:
                self.spec = SUPPORTED_FORMATS[fmt](fp)
        else:
            self.spec = spec
        self.header = self.spec.get("adtl", {})
        if self.specfile:
            self.include_defs = [
                relative_path(self.specfile, definition_file)
                for definition_file in self.header.get("include-def", [])
            ] + self.include_defs
        self.defs = self.header.get("defs", {})
        if self.include_defs:
            for definition_file in self.include_defs:
                self.defs.update(read_definition(definition_file))
        self.spec = expand_refs(self.spec, self.defs)

        self.validate_spec()
        for table in (t for t in self.tables if self.tables[t]["kind"] == "oneToMany"):
            self.spec[table] = expand_for(self.spec[table])
        for table in self.tables:
            if self.tables[table].get("groupBy"):
                self.data[table] = defaultdict(dict)
            else:
                self.data[table] = []
            if schema := self.tables[table].get("schema"):
                optional_fields = self.tables[table].get("optional-fields")
                if schema.startswith("http"):
                    try:
                        res = requests.get(schema)
                        if res.status_code != 200:
                            logging.warning(
                                f"Could not fetch schema for table {table!r}, will not validate"
                            )
                            continue
                    except ConnectionError:  # pragma: no cover
                        logging.warning(
                            f"Could not fetch schema for table {table!r}, will not validate"
                        )
                        continue
                    self.schemas[table] = make_fields_optional(
                        res.json(), optional_fields
                    )
                else:  # local file
                    with (self.specfile.parent / schema).open() as fp:
                        self.schemas[table] = make_fields_optional(
                            json.load(fp), optional_fields
                        )
                self.date_fields.extend(get_date_fields(self.schemas[table]))
                self.validators[table] = fastjsonschema.compile(self.schemas[table])

        self._set_field_names()

    @lru_cache
    def ctx(self, attribute: str):
        return {
            "is_date": attribute in self.date_fields,
            "defaultDateFormat": self.header.get(
                "defaultDateFormat", DEFAULT_DATE_FORMAT
            ),
            "skip_pattern": re.compile(self.header.get("skipFieldPattern"))
            if self.header.get("skipFieldPattern")
            else False,
        }

    def validate_spec(self):
        "Raises exceptions if specification is invalid"
        for required in ["tables", "name", "description"]:
            if required not in self.header:
                raise ValueError(f"Specification header requires key: {required}")
        self.tables = self.header["tables"]
        self.name = self.header["name"]
        self.description = self.header["description"]

        for table in self.tables:
            aggregation = self.tables[table].get("aggregation")
            group_field = self.tables[table].get("groupBy")
            kind = self.tables[table].get("kind")
            if kind is None:
                raise ValueError(
                    f"Required 'kind' attribute within 'tables' not present for {table}"
                )
            if group_field is not None and aggregation != "lastNotNull":
                raise ValueError(
                    f"groupBy needs aggregation=lastNotNull to be set for table: {table}"
                )

    def _set_field_names(self):
        for table in self.tables:
            if table not in self.spec:
                raise ValueError(
                    f"Parser specification missing required '{table}' element"
                )
            if self.tables[table].get("kind") != "oneToMany":
                self.fieldnames[table] = sorted(list(self.spec[table].keys()))
            else:
                if table not in self.schemas:
                    print(
                        f"Warning: no schema found for {table!r}, field names may be incomplete!"
                    )
                    self.fieldnames[table] = list(
                        self.tables[table].get("common", {}).keys()
                    ) + sorted(
                        list(set(sum([list(m.keys()) for m in self.spec[table]], [])))
                    )
                else:
                    self.fieldnames[table] = sorted(self.schemas[table]["properties"])
                if commonMappings := self.tables[table].get("common", {}):
                    for match in self.spec[table]:
                        match.update(commonMappings)

    def _default_if(self, table: str, rule: StrDict):
        """
        Default behaviour for oneToMany table, row not displayed if there's an empty
        string or values not mapped in the rule.
        """

        data_options = [
            option["required"][0] for option in self.schemas[table]["oneOf"]
        ]

        option = set(data_options).intersection(rule.keys()).pop()

        if "combinedType" not in rule[option]:
            field = rule[option]["field"]
            if "values" in rule[option]:
                values = rule[option]["values"]
                if "can_skip" in rule[option]:
                    if_rule = {"any": [{field: v, "can_skip": True} for v in values]}
                else:
                    if_rule = {"any": [{field: v} for v in values]}
            elif "can_skip" in rule[option]:
                if_rule = {field: {"!=": ""}, "can_skip": True}
            else:
                if_rule = {field: {"!=": ""}}
        else:
            assert rule[option]["combinedType"] in [
                "any",
                "all",
                "firstNonNull",
                "set",
                "list",
                "min",
                "max",
            ], f"Invalid combinedType: {rule[option]['combinedType']}"
            rules = rule[option]["fields"]

            def create_if_rule(rule):
                field = rule["field"]
                values = rule.get("values", [])
                can_skip = rule.get("can_skip", False)

                if_condition = {}

                if values and can_skip:
                    if_condition = [{field: v, "can_skip": True} for v in values]
                elif values:
                    if_condition = [{field: v} for v in values]
                elif can_skip:
                    if_condition[field] = {"!=": ""}
                    if_condition["can_skip"] = True
                    if_condition = [if_condition]
                else:
                    if_condition[field] = {"!=": ""}
                    if_condition = [if_condition]

                return if_condition

            if_rule = {"any": sum(map(create_if_rule, rules), [])}

        rule["if"] = if_rule
        return rule

    def update_table(self, table: str, row: StrDict):
        """Updates table with a new row

        Args:
            table: Table to update
            row: Dictionary with keys as field names and values as field values
        """

        group_field = self.tables[table].get("groupBy")
        kind = self.tables[table].get("kind")
        if group_field:
            group_key = get_value(row, self.spec[table][group_field])
            for attr in self.spec[table]:
                value = get_value(row, self.spec[table][attr], self.ctx(attr))
                # Check against all null elements, for combinedType=set/list, null is []
                if value is not None and value != []:
                    if attr not in self.data[table][group_key].keys():
                        # if data for this field hasn't already been captured
                        self.data[table][group_key][attr] = value

                    else:
                        if "combinedType" in self.spec[table][attr]:
                            combined_type = self.spec[table][attr]["combinedType"]
                            existing_value = self.data[table][group_key][attr]

                            if combined_type in ["all", "any", "min", "max"]:
                                values = [existing_value, value]
                                # normally calling eval() is a bad idea, but here values are restricted, so okay
                                self.data[table][group_key][attr] = eval(combined_type)(
                                    values
                                )
                            elif combined_type in ["list", "set"]:
                                if combined_type == "set":
                                    self.data[table][group_key][attr] = list(
                                        set(existing_value + value)
                                    )
                                else:
                                    self.data[table][group_key][attr] = (
                                        existing_value + value
                                    )
                            elif combined_type == "firstNonNull":
                                # only use the first value found
                                pass
                        else:
                            # otherwise overwrite?
                            logging.debug(
                                f"Multiple rows of data found for {attr} without a"
                                " combinedType listed. Data being overwritten."
                            )
                            self.data[table][group_key][attr] = value

        elif kind == "oneToMany":
            for match in self.spec[table]:
                if "if" not in match:
                    match = self._default_if(table, match)
                if parse_if(row, match["if"], self.ctx):
                    self.data[table].append(
                        remove_null_keys(
                            {
                                attr: get_value(row, match[attr], self.ctx(attr))
                                for attr in set(match.keys()) - {"if"}
                            }
                        )
                    )
        elif kind == "constant":  # only one row
            self.data[table] = [self.spec[table]]
        else:
            self.data[table].append(
                remove_null_keys(
                    {
                        attr: get_value(row, self.spec[table][attr], self.ctx(attr))
                        for attr in self.spec[table]
                    }
                )
            )

    def parse(self, file: str, encoding: str = "utf-8", skip_validation=False):
        """Transform file according to specification

        Args:
            file: Source file to transform
            encoding: Source file encoding
            skip_validation: Whether to skip validation, default off

        Returns:
            adtl.Parser: Returns an instance of itself, updated with the parsed tables
        """
        with open(file, encoding=encoding) as fp:
            reader = csv.DictReader(fp)
            return self.parse_rows(
                tqdm(
                    reader,
                    desc=f"[{self.name}] parsing {Path(file).name}",
                )
                if not self.quiet
                else reader,
                skip_validation=skip_validation,
            )

    def parse_rows(self, rows: Iterable[StrDict], skip_validation=False):
        """Transform rows from an iterable according to specification

        Args:
            rows: Iterable of rows, specified as a dictionary of (field name, field value) pairs
            skip_validation: Whether to skip validation, default off

        Returns:
            adtl.Parser: Returns an instance of itself, updated with the parsed tables
        """
        for row in rows:
            for table in self.tables:
                try:
                    self.update_table(table, row)
                except ValueError:  # pragma: no cover
                    print(
                        "\n".join(
                            [
                                f"{key} = {value}"
                                for key, value in row.items()
                                if value not in ["", None]
                            ]
                        )
                    )
                    raise
        self.report_available = not skip_validation
        if not skip_validation:
            for table in self.validators:
                for row in self.read_table(table):
                    self.report["total"][table] += 1
                    try:
                        self.validators[table](row)
                        row["adtl_valid"] = True
                        self.report["total_valid"][table] += 1
                    except fastjsonschema.exceptions.JsonSchemaValueException as e:
                        row["adtl_valid"] = False
                        row["adtl_error"] = e.message
                        self.report["validation_errors"][table].update([e.message])
        return self

    def clear(self):
        "Clears parser state"
        self.data = {}

    def read_table(self, table: str) -> Iterable[StrDict]:
        """Returns parsed table

        Args:
            table: Table to read

        Returns:
            Iterable of transformed rows in table
        """
        if table not in self.tables:
            raise ValueError(f"Invalid table: {table}")
        if "groupBy" in self.tables[table]:
            for i in self.data[table]:
                yield self.data[table][i]
        else:
            for row in self.data[table]:
                yield row

    def write_csv(
        self,
        table: str,
        output: Optional[str] = None,
    ) -> Optional[str]:
        """Writes to output as CSV a particular table

        Args:
            table: Table that should be written to CSV
            output: (optional) Output file name. If not specified, defaults to parser name + table name
                with a csv suffix.
        """

        def writerows(fp, table):
            writer = csv.DictWriter(
                fp,
                fieldnames=(
                    ["adtl_valid", "adtl_error"] if table in self.validators else []
                )
                + self.fieldnames[table],
            )
            writer.writeheader()
            for row in self.read_table(table):
                writer.writerow(row)
            return fp

        if output:
            with open(output, "w") as fp:
                writerows(fp, table)
            return None
        else:
            buf = io.StringIO()
            return writerows(buf, table).getvalue()

    def show_report(self):
        "Shows report with validation errors"
        if self.report_available:
            print("\n|table       \t|valid\t|total\t|percentage_valid|")
            print("|---------------|-------|-------|----------------|")
            for table in self.report["total"]:
                print(
                    f"|{table:14s}\t|{self.report['total_valid'][table]}\t|{self.report['total'][table]}\t"
                    f"|{self.report['total_valid'][table]/self.report['total'][table]:%} |"
                )
            print()
            for table in self.report["validation_errors"]:
                print(f"## {table}\n")
                for message, count in self.report["validation_errors"][
                    table
                ].most_common():
                    print(f"* {count}: {message}")
                print()

    def save(self, output: Optional[str] = None):
        """Saves all tables to CSV

        Args:
            output: (optional) Filename prefix that is used for all tables
        """

        for table in self.tables:
            self.write_csv(table, f"{output}-{table}.csv")


def main(argv=None):
    cmd = argparse.ArgumentParser(
        prog="adtl",
        description="Transforms and validates data into CSV given a specification",
    )
    cmd.add_argument(
        "spec",
        help="specification file to use",
    )
    cmd.add_argument("file", help="file to read in")
    cmd.add_argument(
        "-o", "--output", help="output file, if blank, writes to standard output"
    )
    cmd.add_argument(
        "--encoding", help="encoding input file is in", default="utf-8-sig"
    )
    cmd.add_argument(
        "-q",
        "--quiet",
        help="quiet mode - decrease verbosity, disable progress bar",
        action="store_true",
    )
    cmd.add_argument("--save-report", help="save report in JSON format")
    cmd.add_argument(
        "--include-def",
        action="append",
        help="include external definition (TOML or JSON)",
    )
    cmd.add_argument("--version", action="version", version="%(prog)s " + __version__)
    args = cmd.parse_args(argv)
    include_defs = args.include_def or []
    spec = Parser(args.spec, include_defs=include_defs, quiet=args.quiet)

    # run adtl
    adtl_output = spec.parse(args.file, encoding=args.encoding)
    adtl_output.save(args.output or spec.name)
    if args.save_report:
        adtl_output.report.update(
            dict(
                encoding=args.encoding,
                include_defs=include_defs,
                file=args.file,
                parser=args.spec,
            )
        )
        with open(args.save_report, "w") as fp:
            json.dump(adtl_output.report, fp, sort_keys=True, indent=2)
    else:
        adtl_output.show_report()


if __name__ == "__main__":
    main()
