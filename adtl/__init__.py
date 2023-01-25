import io
import re
import csv
import json
import logging
import hashlib
import argparse
from typing import Optional, Any, Union, Iterable, Dict, List
from collections import defaultdict
from pathlib import Path
from enum import Enum

import pint
from tqdm import tqdm

StrDict = Dict[str, Any]
Rule = Union[str, StrDict]


def get_value(row: StrDict, rule: Rule) -> Any:
    """Gets value from row using rule

    Same as get_value_unhashed(), except it hashes if sensitive = True in rule.
    This function should be used instead of get_value_unhashed() for
    application code.
    """
    value = get_value_unhashed(row, rule)
    if isinstance(rule, dict) and rule.get("sensitive") and value is not None:
        return hash_sensitive(value)
    else:
        return value


def get_value_unhashed(row: StrDict, rule: Rule) -> Any:
    """Gets value from row using rule (unhashed)

    Unlike get_value() this function does NOT hash sensitive data
    and should not be called directly, except for debugging. Use
    get_value() instead.
    """
    if isinstance(rule, str):  # constant
        return rule
    if "field" in rule:
        # do not parse field if condition is not met
        if "if" in rule and not parse_if(row, rule["if"]):
            return None
        value = row[rule["field"]]
        if "values" in rule:
            value = rule["values"].get(value)
        if "source_unit" in rule and "unit" in rule:  # perform unit conversion
            source_unit = get_value(row, rule["source_unit"])
            unit = rule["unit"]
            print(f"Will convert from {source_unit} to {unit}")
            try:
                value = pint.Quantity(float(value), source_unit).to(unit).m
            except ValueError:
                logging.error(f"Could not convert {value} to a floating point")
                raise
        return value
    elif "combinedType" in rule:
        return get_combined_type(row, rule)
    elif "otherField" in rule:
        logging.info("otherField not supported, returning None")
        return None
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
    if key == "any" and isinstance(rule[key], list):
        return any(parse_if(row, r) for r in rule[key])
    elif key == "all" and isinstance(rule[key], list):
        return all(parse_if(row, r) for r in rule[key])
    attr_value = row[key]
    if isinstance(rule[key], dict):
        cmp = next(iter(rule[key]))
        value = rule[key][cmp]
        if cmp == ">":
            return attr_value > value
        elif cmp == ">=":
            return attr_value >= value
        elif cmp == "<":
            return attr_value < value
        elif cmp == "<=":
            return attr_value <= value
        elif cmp == "!=":
            return attr_value != value
        elif cmp in ["=", "=="]:
            return attr_value == value
        else:
            raise ValueError(f"Unrecognized operand: {cmp}")
    else:
        value = rule[key]
        return attr_value == value


def get_list(row: StrDict, rule: StrDict) -> List[Any]:
    """Gets values from row for a combinedType: list rule"""

    assert "fields" in rule
    assert len(rule["fields"]) >= 1
    rules = []
    exclude = rule.get("exclude")
    if (
        exclude is not None
        and exclude not in ["null", "falsy"]
        and not isinstance(exclude, list)
    ):
        raise ValueError("exclude rule should be 'null', 'falsy' or a list of values")

    # expand fieldPattern rules
    for r in rule["fields"]:
        if "fieldPattern" in r:
            for match in matching_fields(list(row.keys()), r.get("fieldPattern")):
                rules.append({"field": match, **r})
        else:
            rules.append(r)
    values = [get_value(row, r) for r in rules]
    if exclude is None:
        return values
    if exclude == "null":
        return [v for v in values if v is not None]
    elif exclude == "falsy":
        return [v for v in values if v]
    else:
        return [v for v in values if v not in exclude]


def get_combined_type(row: StrDict, rule: StrDict):
    """Gets value from row for a combinedType rule.

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
    rules = rule["fields"]
    if combined_type == "all":
        return all(get_value(row, r) for r in rules)
    elif combined_type == "any":
        return any(get_value(row, r) for r in rules)
    elif combined_type == "firstNonNull":
        try:
            return next(filter(None, [get_value(row, r) for r in rules]))
        except StopIteration:
            return None
    elif combined_type == "list":
        return get_list(row, rule)
    else:
        raise ValueError(f"Unknown {combined_type} in {rule}")


def hash_sensitive(value: str) -> str:
    """Hashes sensitive values. This is not generally sufficient for
    anonymisation, as the value still serves as a unique identifier,
    but is better than storing the value unprocessed."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class Parser:

    data: StrDict = {}
    fieldnames: Dict[str, List[str]] = {}

    def __init__(self, spec: str):
        with open(spec) as fp:
            self.spec = json.load(fp)
        self.validate_spec()
        for table in self.tables:
            if self.tables[table].get("groupBy"):
                self.data[table] = defaultdict(dict)
            else:
                self.data[table] = []
            self.fieldnames[table] = list(self.spec[table].keys())

    def validate_spec(self):
        "Raises exceptions if specification is invalid"
        errors = []
        for required in ["tables", "name", "description"]:
            if required not in self.spec:
                raise ValueError(f"Specification requires key: {required}")
        self.tables = self.spec["tables"]
        for table in self.tables:
            if table not in self.spec:
                raise ValueError(
                    f"Parser specification missing required '{table}' element"
                )
            self.fieldnames[table] = list(self.spec[table].keys())
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

    def update_table(self, table: str, row: StrDict):
        # Currently only aggregations are supported

        group_field = self.tables[table].get("groupBy")
        kind = self.tables[table].get("kind")
        if group_field:
            if table not in self.data:
                self.data[table] = defaultdict(dict)
            group_key = row[self.spec[table][group_field]["field"]]
            for attr in self.spec[table]:
                if (value := get_value(row, self.spec[table][attr])) is not None:
                    self.data[table][group_key][attr] = value
        elif kind == "constant":  # only one row
            self.data[table] = [self.spec[table]]
        else:
            # no grouping, one-to-one mapping
            if table not in self.data:
                self.data[table] = []
            self.data[table].append(
                {
                    attr: get_value(row, self.spec[table][attr])
                    for attr in self.spec[table]
                }
            )

    def parse(self, file: str):
        self.clear()
        with open(file) as fp:
            reader = csv.DictReader(fp)
            for row in tqdm(
                reader,
                desc=f"[{self.spec['name']}] parsing {Path(file).name}",
            ):
                for table in self.tables:
                    self.update_table(table, row)
        self.validate()
        return self

    def validate(self):
        "Use schemas to validate data"
        pass

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
            print(f"groupBy not in table {table}")
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
            writer = csv.DictWriter(fp, fieldnames=self.fieldnames[table])
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

    def save(self, output: Optional[str] = None):
        "Saves all tables to CSV"

        for table in self.tables:
            self.write_csv(table, f"{output}-{table}.csv")


def main():
    cmd = argparse.ArgumentParser(
        prog="adtl",
        description="Transforms data into CSV given a specification",
    )
    cmd.add_argument(
        "spec",
        help="Specification file to use",
    )
    cmd.add_argument("file", help="File to read in")
    cmd.add_argument(
        "-o", "--output", help="Output file, if blank, writes to standard output"
    )
    args = cmd.parse_args()
    if output := Parser(args.spec).parse(args.file).save(args.output):
        print(output)


if __name__ == "__main__":
    main()
