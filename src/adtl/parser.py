from __future__ import annotations

import copy
import csv
import io
import itertools
import json
import logging
import re
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Literal, Union

import fastjsonschema
import requests
import tomli
from more_itertools import unique_everseen
from tqdm.autonotebook import tqdm

from adtl.get_value import get_value, parse_if

SUPPORTED_FORMATS = {"json": json.load, "toml": tomli.load}
DEFAULT_DATE_FORMAT = "%Y-%m-%d"

StrDict = dict[str, Any]


def expand_refs(spec_fragment: StrDict, defs: StrDict) -> Union[StrDict, list[StrDict]]:
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


def expand_for(spec: list[StrDict]) -> list[StrDict]:
    "Expands for expressions in oneToMany table blocks"

    out = []

    def replace_val(
        item: Union[str, float, dict[str, Any]], replace: dict[str, Any]
    ) -> dict[str, Any]:
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
                f"for expression {for_expr!r} is not a dictionary of variables to list "
                "of values or a range"
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
                    f"for expression {for_expr!r} can only have lists or ranges for "
                    "variables"
                )
        loop_vars = sorted(for_expr.keys())
        loop_assignments = [
            dict(zip(loop_vars, vals))
            for vals in itertools.product(*(for_expr[var] for var in loop_vars))
        ]
        for replacement in loop_assignments:
            out.append(replace_val(match, replacement))
    return out


def remove_null_keys(d: dict[str, Any]) -> dict[str, Any]:
    "Removes keys which map to null - but not empty strings or 'unknown' etc types"
    return {k: v for k, v in d.items() if v is not None}


def get_date_fields(schema: dict[str, Any]) -> list[str]:
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
    schema: dict[str, Any], optional_fields: list[str]
) -> dict[str, Any]:
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


def read_definition(file: Path) -> dict[str, Any]:
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
        include_defs: list[str] = [],
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
        self.fieldnames: dict[str, list[str]] = {}
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
                                f"Could not fetch schema for table {table!r}, will not "
                                "validate"
                            )
                            continue
                    except ConnectionError:  # pragma: no cover
                        logging.warning(
                            f"Could not fetch schema for table {table!r}, will not "
                            "validate"
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
            "skip_pattern": (
                re.compile(self.header.get("skipFieldPattern"))
                if self.header.get("skipFieldPattern")
                else False
            ),
            "returnUnmatched": self.header.get("returnUnmatched", False),
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
                    "groupBy needs aggregation=lastNotNull to be set for table: "
                    f"{table}"
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
                        f"Warning: no schema found for {table!r}, field names may be "
                        "incomplete!"
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
                                # normally calling eval() is a bad idea, but here
                                # values are restricted, so okay
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
                (
                    tqdm(
                        reader,
                        desc=f"[{self.name}] parsing {Path(file).name}",
                    )
                    if not self.quiet
                    else reader
                ),
                skip_validation=skip_validation,
            )

    def parse_rows(self, rows: Iterable[StrDict], skip_validation=False):
        """Transform rows from an iterable according to specification

        Args:
            rows: Iterable of rows, specified as a dictionary of
                    (field name, field value) pairs
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

    def write_csv(self, table: str, output: str | None = None) -> str | None:
        """Writes to output as CSV a particular table

        Args:
            table: Table that should be written to CSV
            output: (optional) Output file name. If not specified, defaults to parser
                    name + table name with a csv suffix.
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

    def write_parquet(self, table: str, output: str | None = None) -> str | None:
        """Writes to output as parquet a particular table

        Args:
            table: Table that should be written to parquet
            output: (optional) Output file name. If not specified, defaults to parser
                    name + table name with a parquet suffix.
        """

        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "Parquet output requires the polars library. "
                "Install with 'pip install polars'"
            )

        # Read the table data
        data = list(self.read_table(table))

        # Convert data to Polars DataFrame
        df = pl.DataFrame(data, infer_schema_length=len(data))

        if table in self.validators:
            valid_cols = [c for c in ["adtl_valid", "adtl_error"] if c in df.columns]
            df_validated = df.select(
                valid_cols
                + [
                    *[
                        col
                        for col in df.columns
                        if (col != "adtl_valid" and col != "adtl_error")
                    ],  # All other columns, in their original order
                ]
            )
        else:
            df_validated = df

        if output:
            df_validated.write_parquet(output)
        else:
            buf = io.BytesIO()
            df_validated.write_parquet(buf)
            return buf.getvalue()

    def show_report(self):
        "Shows report with validation errors"
        if self.report_available:
            print("\n|table       \t|valid\t|total\t|percentage_valid|")
            print("|---------------|-------|-------|----------------|")
            for table in self.report["total"]:
                print(
                    f"|{table:14s}\t|{self.report['total_valid'][table]}\t"
                    f"|{self.report['total'][table]}\t"
                    f"|{self.report['total_valid'][table] / self.report['total'][table]:%} |"  # noqa:E501
                )
            print()
            for table in self.report["validation_errors"]:
                print(f"## {table}\n")
                for message, count in self.report["validation_errors"][
                    table
                ].most_common():
                    print(f"* {count}: {message}")
                print()

    def save(
        self, output: str | None = None, format: Literal["csv", "parquet"] = "csv"
    ):
        """Saves all tables to CSV

        Args:
            output: (optional) Filename prefix that is used for all tables
        """

        if format == "parquet":
            for table in self.tables:
                self.write_parquet(table, f"{output}-{table}.parquet")

        elif format == "csv":
            for table in self.tables:
                self.write_csv(table, f"{output}-{table}.csv")

        else:
            raise ValueError(f"'Parser.save()': Invalid format: {format}")
