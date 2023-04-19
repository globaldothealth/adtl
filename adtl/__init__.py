import argparse
import csv
import hashlib
import io
import json
import logging
import itertools
import copy
import re
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Union

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
    except TypeError:
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
    if "field" in rule:
        # do not parse field if condition is not met
        if "if" in rule and not parse_if(row, rule["if"]):
            return None
        value = row[rule["field"]]
        if "apply" in rule:
            # apply data transformations.
            transformation = rule["apply"]["function"]
            if "params" in rule["apply"]:
                params = [
                    row[rule["apply"]["params"][i]]
                    if isinstance(rule["apply"]["params"][i], str)
                    else rule["apply"]["params"][i]
                    for i in range(len(rule["apply"]["params"]))
                ]
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
        if value == "":
            return None
        if "values" in rule:
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
                except ValueError:
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


def parse_if(row: StrDict, rule: StrDict) -> bool:
    "Parse conditional statements and return a boolean"

    n_keys = len(rule.keys())
    assert n_keys == 1
    key = next(iter(rule.keys()))
    if key == "not" and isinstance(rule[key], dict):
        return not parse_if(row, rule[key])
    elif key == "any" and isinstance(rule[key], list):
        return any(parse_if(row, r) for r in rule[key])
    elif key == "all" and isinstance(rule[key], list):
        return all(parse_if(row, r) for r in rule[key])
    attr_value = row[key]
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
    if combined_type == "all":
        return all(get_value(row, r, ctx) for r in rules)
    elif combined_type == "any":
        return any(get_value(row, r, ctx) for r in rules)
    elif combined_type == "firstNonNull":
        try:
            return next(filter(None, [get_value(row, r, ctx) for r in rules]))
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

        values = [get_value(row, r, ctx) for r in rules]
        if combined_type == "set":
            values = [*set(values)]
        if excludeWhen is None:
            return values
        if excludeWhen == "none":
            return [v for v in values if v is not None]
        elif excludeWhen == "false-like":
            return [v for v in values if v]
        else:
            return [v for v in values if v not in excludeWhen]
    else:
        raise ValueError(f"Unknown {combined_type} in {rule}")


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

    def replace_val(item: Dict[str, Any], replace: Dict[str, Any]) -> Dict[str, Any]:
        block = {}
        for k, v in item.items():
            if not isinstance(k, str):
                block[k] = v
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
    "Removes keys which map to null"
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
    return _schema
    
    
class Parser:
    def __init__(self, spec: Union[str, Path, StrDict]):
        "Loads specification from spec in format (default json)"

        self.data: StrDict = {}
        self.defs: StrDict = {}
        self.fieldnames: Dict[str, List[str]] = {}
        self.specfile = None
        self.validators: StrDict = {}
        self.schemas: StrDict = {}
        self.date_fields = []
        self.report = {
            "errors": defaultdict(Counter),
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
        self.defs = self.header.get("defs", {})
        self.spec = expand_refs(self.spec, self.defs)

        self.validate_spec()
        for table in (t for t in self.tables if self.tables[t]["kind"] == "oneToMany"):
            self.spec[table] = expand_for(self.spec[table])
        for table in self.tables:
            if schema := self.tables[table].get("schema"):
                optional_fields = self.tables[table].get("optional-fields")
                if schema.startswith("http"):
                    try:
                        if (res := requests.get(schema)).status_code != 200:
                            logging.warning(
                                f"Could not fetch schema for table {table!r}, will not validate"
                            )
                            continue
                    except ConnectionError:
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

            if self.tables[table].get("groupBy"):
                self.data[table] = defaultdict(dict)
            else:
                self.data[table] = []

        print(self.date_fields)
        self._set_field_names()

    @lru_cache
    def ctx(self, attribute: str):
        return {
            "is_date": attribute in self.date_fields,
            "defaultDateFormat": self.header.get(
                "defaultDateFormat", DEFAULT_DATE_FORMAT
            ),
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

    def update_table(self, table: str, row: StrDict):
        # Currently only aggregations are supported

        group_field = self.tables[table].get("groupBy")
        kind = self.tables[table].get("kind")
        if group_field:
            if table not in self.data:
                self.data[table] = defaultdict(dict)
            group_key = get_value(row, self.spec[table][group_field])
            for attr in self.spec[table]:
                if (
                    value := get_value(row, self.spec[table][attr], self.ctx(attr))
                ) is not None:
                    self.data[table][group_key][attr] = value
        elif kind == "oneToMany":
            for match in self.spec[table]:
                if "if" not in match:
                    raise ValueError(
                        f"oneToMany tables must have a 'if' key setting condition for row to be emitted: {match!r}"
                    )
                if parse_if(row, match["if"]):
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
            # no grouping, one-to-one mapping
            if table not in self.data:
                self.data[table] = []
            self.data[table].append(
                remove_null_keys(
                    {
                        attr: get_value(row, self.spec[table][attr], self.ctx(attr))
                        for attr in self.spec[table]
                    }
                )
            )

    def parse(self, file: str, encoding: str = "utf-8", skip_validation=False):
        "Transform file according to specification"
        with open(file, encoding=encoding) as fp:
            reader = csv.DictReader(fp)
            return self.parse_rows(
                tqdm(
                    reader,
                    desc=f"[{self.name}] parsing {Path(file).name}",
                ),
                skip_validation=skip_validation,
            )

    def parse_rows(self, rows: Iterable[StrDict], skip_validation=False):
        "Transform rows from an iterable according to specification"
        for row in rows:
            for table in self.tables:
                try:
                    self.update_table(table, row)
                except ValueError:
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
                        self.report["errors"][table].update([e.message])
        return self

    def clear(self):
        "Clears parser state"
        self.data = {}

    def read_table(self, table: str) -> Iterable[StrDict]:
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
        "Writes to output as CSV a particular table"

        def writerows(fp, table):
            print(f"Writing {table}")
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
        if self.report_available:
            print("\n|table       \t|valid\t|total\t|percentage_valid|")
            print("|---------------|-------|-------|----------------|")
            for table in self.report["total"]:
                print(
                    f"|{table:14s}\t|{self.report['total_valid'][table]}\t|{self.report['total'][table]}\t"
                    f"|{self.report['total_valid'][table]/self.report['total'][table]:%} |"
                )
            print()
            for table in self.report["errors"]:
                print(f"## {table}\n")
                for message, count in self.report["errors"][table].most_common():
                    print(f"* {count}: {message}")
                print()

    def save(self, output: Optional[str] = None):
        "Saves all tables to CSV"

        for table in self.tables:
            self.write_csv(table, f"{output}-{table}.csv")
        self.show_report()


def main():
    cmd = argparse.ArgumentParser(
        prog="adtl",
        description="Transforms data into CSV given a specification (+validation)",
    )
    cmd.add_argument(
        "spec",
        help="Specification file to use",
    )
    cmd.add_argument("file", help="File to read in")
    cmd.add_argument(
        "-o", "--output", help="Output file, if blank, writes to standard output"
    )
    cmd.add_argument(
        "--encoding", help="Encoding input file is in", default="utf-8-sig"
    )
    args = cmd.parse_args()
    spec = Parser(args.spec)
    if output := spec.parse(args.file, encoding=args.encoding).save(
        args.output or spec.name
    ):
        print(output)


if __name__ == "__main__":
    main()
