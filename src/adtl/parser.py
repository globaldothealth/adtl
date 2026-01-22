from __future__ import annotations

import copy
import csv
import hashlib
import importlib
import inspect
import io
import itertools
import json
import logging
import re
import uuid
import warnings
from collections import Counter, defaultdict
from functools import lru_cache
from itertools import chain
from pathlib import Path
from typing import Any, Iterable, Literal, Union

import fastjsonschema
import pandas as pd
import requests
import tomli
from joblib import Parallel, delayed
from more_itertools import unique_everseen
from tqdm.auto import tqdm

import adtl.transformations as tf
import adtl.util as util
from adtl.get_value import get_value, parse_if

from .adtl_pydantic import ADTLDocument

DEFAULT_DATE_FORMAT = "%Y-%m-%d"

StrDict = dict[str, Any]

logger = logging.getLogger(__name__)


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


def read_file(file: Path) -> dict[str, Any]:
    "Reads from a file into a dictionary"
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


def load_custom_transformations(filepath: str):
    """
    Load custom transformation functions from a Python file.

    Args:
        filepath: Path to a Python file containing transformation functions
    """
    if not Path(filepath).exists():
        raise FileNotFoundError(f"No such file: {filepath!r}")

    # Load the module from file
    spec = importlib.util.spec_from_file_location("custom_transformations", filepath)
    if spec is None or spec.loader is None:  # pragma: no cover
        raise ValueError(f"Cannot load transformations from {filepath}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Merge functions into the tf namespace
    for name, obj in inspect.getmembers(module):
        if callable(obj) and not name.startswith("_"):
            if getattr(tf, name, None):
                warnings.warn(
                    f"Overwriting existing transformation function: {name}", UserWarning
                )
            setattr(tf, name, obj)
            logger.info(f"Loaded custom transformation: {name}")


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
        include_transform: str | None = None,
        quiet: bool = False,
        verbose: bool = False,
        parallel: bool = False,
    ):
        """Loads specification from spec in format (default json)

        Args:
            spec: Either the specification file to read (as Path or str), or
                the specification loaded into a dictionary
            include_defs: Definition files to include. These are spliced
                directly into the adtl.defs section of the :ref:`specification`.
            include_transform: File with external transform functions to include.
            quiet: Boolean that switches on the verbosity of the parser, default False
            verbose: Boolean that switches on extra verbosity to show e.g. overwrite warnings, default False
            parallel: Boolean that switches on parallel processing for parsing, default False
        """

        self.data: StrDict = {}
        self.defs: StrDict = {}
        self.fieldnames: dict[str, list[str]] = {}
        self.specfile = None
        self.include_defs = include_defs
        self.include_transform = include_transform
        self.validators: StrDict = {}
        self.schemas: StrDict = {}
        self.quiet = quiet
        self.verbose = verbose
        self.parallel = parallel
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
            self.spec = read_file(spec)
        else:
            self.spec = spec

        self.validate_spec()
        self.header = self.spec["adtl"]
        if self.specfile:
            self.include_defs = [
                relative_path(self.specfile, definition_file)
                for definition_file in self.header.get("include-def", [])
            ] + self.include_defs
        self.defs = self.header.get("defs", {})
        if self.include_defs:
            for definition_file in self.include_defs:
                self.defs.update(read_file(definition_file))
        self.spec = expand_refs(self.spec, self.defs)

        if self.include_transform:
            load_custom_transformations(self.include_transform)

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
                            logger.warning(
                                f"Could not fetch schema for table {table!r}, will not "
                                "validate"
                            )
                            continue
                    except ConnectionError:  # pragma: no cover
                        logger.warning(
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

        self._set_field_names()
        self.empty_fields = self.header.get("emptyFields", None)

    @lru_cache
    def get_namespace_uuid(self):
        namespace_str = json.dumps(self.header, sort_keys=True)
        toml_hash = hashlib.sha1(namespace_str.encode("utf-8")).hexdigest()
        return uuid.uuid5(uuid.NAMESPACE_DNS, toml_hash)

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
            "namespace": self.get_namespace_uuid(),
        }

    def validate_spec(self):
        "Raises exceptions if specification is invalid"
        # Validate the specification against the pydantic model
        ADTLDocument.model_validate(self.spec)

        self.tables = self.spec["adtl"]["tables"]
        self.name = self.spec["adtl"]["name"]
        self.description = self.spec["adtl"]["description"]

    def validate_row(self, table, row, expanded):
        if (self.tables[table]["kind"] == "oneToMany") and expanded:
            # Need to validate each row against an individual subschema
            attr = row.get(self.tables[table]["discriminator"])
            validator = self.validators[table].get(attr)
            if not validator:
                raise fastjsonschema.JsonSchemaValueException(
                    f"No validator found for attribute '{attr}' in table '{table}'"
                )
            validator(row)
        else:
            self.validators[table](row)

    def _set_field_names(self):
        for table in self.tables:
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

            flags = {
                flag: True
                for flag in ("can_skip", "caseInsensitive")
                if flag in rule[option]
            }

            if "values" in rule[option] and not rule[option].get(
                "ignoreMissingKey", False
            ):
                values = rule[option]["values"]
                if_rule = {"any": [{field: v, **flags} for v in values]}
            else:
                if_rule = {field: {"!=": ""}, **flags}
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
                flags = {
                    flag: True
                    for flag in ("can_skip", "caseInsensitive")
                    if flag in rule
                }

                if values and not rule.get("ignoreMissingKey", False):
                    if_condition = [{field: v, **flags} for v in values]
                else:
                    if_condition = [{field: {"!=": ""}, **flags}]

                return if_condition

            if_rule = {"any": sum(map(create_if_rule, rules), [])}

        rule["if"] = if_rule
        return rule

    def _parse_row_for_table(self, table: str, row: StrDict):
        """
        Parses a row of data for a given table into the new format

        Args:
            table: Table to update
            row: Dictionary with keys as field names and values as field values
        """

        kind = self.tables[table].get("kind")
        if self.schemas.get(table):
            schema_p = self.schemas[table]["properties"]
        else:
            schema_p = None

        if kind == "oneToMany":
            data = []
            for match in self.spec[table]:
                if "if" not in match:
                    match = self._default_if(table, match)
                if parse_if(row, match["if"], self.ctx):
                    data.append(
                        remove_null_keys(
                            {
                                attr: get_value(
                                    row,
                                    match[attr],
                                    self.ctx(attr),
                                    schema_p[attr].get("type") if schema_p else None,
                                )
                                for attr in set(match.keys()) - {"if"}
                            }
                        )
                    )
            return data
        elif kind == "constant":  # only one row
            return self.spec[table]
        else:  # groupBy
            parsed_row = {}
            for attr in self.spec[table]:
                value = get_value(
                    row,
                    self.spec[table][attr],
                    self.ctx(attr),
                    schema_p[attr].get("type") if schema_p else None,
                )
                if value is not None and value != []:
                    parsed_row[attr] = value
            return remove_null_keys(parsed_row)

    def group_rows(
        self, table: str, group_field: str, aggregation: str, rows: Iterable[StrDict]
    ):
        """
        Applies the 'groupBy' rule and any 'combinedType' rules to the rows of data
        grouped by the group_field (e.g. an ID number).
        """

        def group_attrs(rows, attrs):
            """
            For a grouped set of rows, apply the combinedType rules to the attributes
            """
            if len(rows) == 1:
                return rows[0]  # Return a single dictionary

            combined_row = {}

            for attr in attrs:
                if ("combinedType" in self.spec[table][attr]) and (
                    aggregation == "applyCombinedType"
                ):
                    combined_type = self.spec[table][attr]["combinedType"]
                    values = [
                        row.get(attr) for row in rows if row.get(attr) not in (None, "")
                    ]

                    if not values:
                        continue
                    elif combined_type in ["all", "any", "min", "max"]:
                        combined_row[attr] = eval(combined_type)(values)
                    elif combined_type == "set":
                        combined_row[attr] = list(
                            set(item for sublist in values for item in sublist)
                        )
                    elif combined_type == "list":
                        combined_row[attr] = [
                            item for sublist in values for item in sublist
                        ]
                    elif combined_type == "firstNonNull":
                        combined_row[attr] = values[0]  # First non-null value
                    else:
                        warnings.warn(
                            f"Invalid combinedType: {combined_type} for {attr}",
                            UserWarning,
                        )
                else:
                    data = [
                        row.get(attr)
                        for row in rows
                        if row.get(attr) not in (None, "", [], {})
                    ]
                    if data:
                        if len(data) > 1 and not all(x == data[0] for x in data):
                            if self.verbose:
                                warnings.warn(
                                    f"Multiple rows of data found for {attr} without a"
                                    f" combinedType listed. Data being overwritten: {data}",
                                    UserWarning,
                                )
                            else:
                                logger.debug(
                                    f"Multiple rows of data found for {attr} without a"
                                    " combinedType listed. Data being overwritten."
                                )
                        combined_row[attr] = data[-1]

            return combined_row

        grouped_rows = defaultdict(list)

        # Group rows by the specified field
        for row in rows:
            grouped_rows[row[group_field]].append(row)

        fields = list(self.spec[table].keys())
        fields.remove(group_field)

        # Apply grouping function
        grouped_results = {
            key: remove_null_keys(group_attrs(group, fields))
            for key, group in grouped_rows.items()
        }

        # Convert back to list of dictionaries
        self.data[table] = [
            {group_field: key, **values} for key, values in grouped_results.items()
        ]

    def parse(self, file: str, encoding: str = "utf-8-sig", skip_validation=False):
        """Transform file according to specification

        Args:
            file: Source file to transform
            encoding: Source file encoding
            skip_validation: Whether to skip validation, default off

        Returns:
            adtl.Parser: Returns an instance of itself, updated with the parsed tables
        """
        with open(file, newline="") as f:
            row_count = sum(1 for _ in f) - 1  # Exclude header

        with open(file, encoding=encoding) as fp:
            reader = csv.DictReader(fp)

            def clean_empty_vals(reader, na_values=self.empty_fields):
                for row in reader:
                    yield {k: ("" if v == na_values else v) for k, v in row.items()}

            return self.parse_rows(
                clean_empty_vals(reader) if self.empty_fields else reader,
                Path(file).name,
                row_count,
                skip_validation=skip_validation,
            )

    def parse_rows(
        self,
        rows: Iterable[StrDict],
        file_name: str,
        row_count: float | None = None,
        skip_validation=False,
    ):
        """Transform rows from an iterable according to specification

        Args:
            rows: Iterable of rows, specified as a dictionary of
                    (field name, field value) pairs
            skip_validation: Whether to skip validation, default off

        Returns:
            adtl.Parser: Returns an instance of itself, updated with the parsed tables
        """

        def process_row(row):
            """Process a single row in the data file"""

            row_store = dict.fromkeys(self.tables, None)

            for table in self.tables:
                try:
                    row_store[table] = self._parse_row_for_table(table, row)
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

            return row_store

        data = Parallel(n_jobs=-1 if self.parallel else 1)(
            delayed(process_row)(row)
            for row in (
                tqdm(
                    rows,
                    desc=f"[{self.name}] parsing {file_name}",
                    total=row_count,
                    disable=self.quiet,
                )
            )
        )

        # merge each row for each table into one data dict per table
        self.data = {
            key: list(values)
            for key, values in zip(data[0], zip(*[d.values() for d in data]))
        }
        for table in self.tables:
            group_field = self.tables[table].get("groupBy")
            aggregation = self.tables[table].get("aggregation")
            if group_field:
                self.group_rows(table, group_field, aggregation, self.data[table])
            if self.tables[table].get("kind") == "oneToMany":
                self.data[table] = list(chain(*self.data[table]))

        self.report_available = not skip_validation
        if not skip_validation:
            for table in self.schemas:
                if self.tables[table]["kind"] == "oneToMany":
                    self.validators[table], attr_schemas = util.expand_schema(
                        self.schemas[table], self.tables[table].get("discriminator")
                    )
                else:
                    self.validators[table] = fastjsonschema.compile(self.schemas[table])
                    attr_schemas = False
                for row in tqdm(
                    self.read_table(table),
                    desc=f"[{self.name}] validating {table} table",
                    disable=self.quiet,
                ):
                    self.report["total"][table] += 1
                    try:
                        self.validate_row(table, row, attr_schemas)
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
            raise ValueError(f"Invalid table name: {table}")
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

        # Read the table data
        data = list(self.read_table(table))

        # Convert data to Pandas DataFrame
        df = pd.DataFrame(data)

        if table in self.validators:
            valid_cols = [c for c in ["adtl_valid", "adtl_error"] if c in df.columns]
            df_validated = df[
                valid_cols
                + [
                    *[
                        col
                        for col in df.columns
                        if (col != "adtl_valid" and col != "adtl_error")
                    ],  # All other columns, in their original order
                ]
            ]
        else:
            df_validated = df

        if output:
            df_validated.to_parquet(output)
        else:
            buf = io.BytesIO()
            df_validated.to_parquet(buf)
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

    def get_spec_fields(self) -> set:
        """
        Returns all fields mapped in the specification (parser) file.

        Returns:
            schema_fields: A set of fields present in the specification
        """

        def find_all_values(data, target_key):
            """
            Recursively yield all values in a nested structure for a given key.
            Works with nested dicts and lists.
            """
            if isinstance(data, dict):
                for key, value in data.items():
                    if key == target_key:
                        yield value
                    # Recurse into the value if it's a dict or list
                    yield from find_all_values(value, target_key)
            elif isinstance(data, list):
                for item in data:
                    yield from find_all_values(item, target_key)

        spec = self.spec
        schema_fields = set(find_all_values(spec, "field"))

        return schema_fields

    def check_spec_fields(self, file) -> tuple[set, set]:
        """
        Compares fields in a data file to a given specification, to check for unmapped
        (present in data but not in spec) and absent (present in spec but not in data) fields

        Args:
            file: File to compare

        Returns:
            A tuple (missing, absent), where 'missing' is a set of fields missing from schema,
            and 'absent' is a set of fields present in schema but not in file.
        """

        df = pd.read_csv(file)
        file_fields = set(df.columns)
        schema_fields = self.get_spec_fields()

        return file_fields - schema_fields, schema_fields - file_fields
