"""
Generate TOML parser from intermediate CSV file
"""

from __future__ import annotations

import abc
import argparse
import json
import logging
from functools import cached_property
from pathlib import Path
from typing import Any, Union

import pandas as pd

from .config.config import get_config, setup_config
from .mixin import LongTableMixin
from .toml_writer import dump
from .util import DEFAULT_CONFIG, parse_llm_mapped_values, read_data, read_json

logger = logging.getLogger(__name__)
INPUT_FORMAT = Union[pd.DataFrame, str, Path]


class TableParser(abc.ABC):
    INDEX_FIELD = "target_field"

    def __init__(self, mapping: pd.DataFrame, schema: dict, table_name: str):
        self.mapping = mapping
        self.schema = schema
        self.name = table_name
        self.config = get_config()

    def single_field_mapping(self): ...

    def make_toml_table(self) -> dict[str, Any]: ...

    @cached_property
    def constant_field(self) -> dict[str, bool]: ...

    @property
    def schema_fields(self):
        """Returns all the fields for `table` and their properties"""
        return self.schema["properties"]

    @cached_property
    def field_types(self) -> dict[str, list[str]]:
        """Returns the field types of the target schema"""
        s = self.schema_fields
        return {f: s[f].get("type", ["string", "null"]) for f in s}

    @cached_property
    def parsed_choices(self) -> pd.Series:
        """Returns the mapped values for each target field"""
        values = self.mapping.value_mapping.map(parse_llm_mapped_values)
        values.index = self.mapping[self.INDEX_FIELD]
        return values

    def update_constant_fields(self, fields: dict[str, bool]) -> None:
        """Update the constant fields"""
        for field, value in fields.items():
            if field not in self.constant_field:
                raise ValueError(f"Field '{field}' is not a valid schema field.")
            if value not in [True, False]:
                raise ValueError(f"Value for field '{field}' must be True or False.")
            self.constant_field[field] = value


class WideTableParser(TableParser):
    """
    Class for generating a wide table from the mappings.
    """

    @cached_property
    def constant_field(self) -> dict[str, bool]:
        """
        If a column in the mapping file should be pulled from a schema field, True,
        otherwise False
        """
        return {col: False for col in self.schema_fields}

    @cached_property
    def references_definitions(self) -> tuple[dict[str, str], dict[str, dict]]:
        """Finds and returns the references and definitions for the mappings"""
        # use value_counts() on parsed_choices normalise various flavours of Y/N/NK
        value_counts = self.parsed_choices.value_counts()
        return self.refs_defs(value_counts, self.config.num_refs)

    def refs_defs(self, choices, num_refs):
        references = {}
        definitions = {}

        top_mappings = choices[choices > 1][:num_refs].index

        # only add one boolean map for simplicity
        boolean_map_found = False
        for mapping in top_mappings:
            if boolean_map_found and True in mapping.values():
                continue
            if True in mapping.values():
                references[json.dumps(mapping, sort_keys=True)] = "Y/N/NK"
                definitions["Y/N/NK"] = {
                    "caseInsensitive": True,
                    "values": mapping,
                }
                boolean_map_found = True
                continue
            c = mapping
            name = "/".join(map(str, c.values()))
            references[json.dumps(mapping, sort_keys=True)] = name
            definitions[name] = {"values": c, "caseInsensitive": True}

        return references, definitions

    def single_field_mapping(self, match: pd.DataFrame) -> dict[str, Any]:
        """Make a single field mapping from a single row of the mappings dataframe"""

        choices = self.parsed_choices[match.target_field]

        out = {"field": match.source_field, "description": match.source_description}
        references = self.references_definitions[0]
        if choices:
            if (choice_key := json.dumps(choices, sort_keys=True)) in references:
                out["ref"] = references[choice_key]
            else:
                out["values"] = choices
                out["caseInsensitive"] = True

            if "array" in self.field_types[match.target_field]:
                out["type"] = "enum_list"

        return out

    def make_toml_table(self) -> dict[str, Any]:
        """Make single TOML table from mappings"""

        outmap = {}

        for field, field_matches in self.mapping.groupby("target_field"):
            if len(field_matches) == 1:  # single field
                if not any(field_matches["source_field"].isna()):
                    outmap[field] = self.single_field_mapping(field_matches.iloc[0])

            else:  # pragma: no cover
                raise NotImplementedError("CombinedType not supported")

        # check for missing required fields
        if "required" in self.schema:
            for field in self.schema["required"]:
                if field not in outmap:
                    logger.warning(
                        f"Missing required field {field} in {self.name} schema."
                        " Adding empty field..."
                    )
                    outmap[field] = ""

        return {self.name: outmap}, self.references_definitions[1]


class LongTableParser(TableParser, LongTableMixin):
    """
    Class for generating a long table from the mappings.
    """

    INDEX_FIELD = "source_field"

    @cached_property
    def constant_field(self) -> dict[str, bool]:
        """If a column in the mapping file should be pulled from a schema field, True, otherwise False"""
        config = {col: False for col in self.schema_fields}
        config[self.variable_col] = True

        for col in self.other_fields:
            config[col] = True

        return config

    def _validate_mapping(self):
        """Validate the mapping dataframe for the long table"""
        if any(self.mapping[self.variable_col].isna()):
            raise ValueError(
                f"Mapping dataframe must not contain NaN values in '{self.variable_col}' column."
            )
        if any(self.mapping["value_col"].isna()):
            raise ValueError(
                "Mapping dataframe must not contain NaN values in the 'value_col' column."
            )

    def single_entry_mapping(self, data: pd.DataFrame) -> dict[str, Any]:
        """Make a single entry mapping from a single row of the mappings dataframe"""

        def add_field(field, text: str) -> Any:
            """Check if a field should be added to the output"""
            if self.constant_field.get(field, False):
                return text
            return {"field": text}

        out = {
            self.variable_col: data[self.variable_col],
            data.value_col: add_field(data.value_col, data.source_field),
            **{field: add_field(field, data[field]) for field in self.common_cols},
        }

        choices = self.parsed_choices[data.source_field]
        if choices:
            out[data.value_col].update(
                {
                    "values": choices,
                    "caseInsensitive": True,
                }
            )

        for field in self.other_fields:
            if not pd.isna(data[field]):
                out[field] = add_field(field, data[field])

        return out

    def make_toml_table(self) -> dict[str, Any]:
        """Make single TOML table from mappings"""

        self._validate_mapping()

        outmap = []

        for _, row in self.mapping.iterrows():
            outmap.append(self.single_entry_mapping(row))

        return {self.name: outmap}, None


class ParserGenerator:
    """
    Class for creating a TOML parser from an intermediate CSV file.

    Use `create_parser()` to write out the TOML parser file, as the function equivalent
    of the command line `create-parser` script.

    Parameters
    ----------
    mappings : pd.DataFrame | str | Path
        The intermediate CSV file created by `create_mapping.py`
    schema_path : Path | str
        The path to the folder containing all the schema files
    parser_name : str
        The name of the parser
    description : str, optional
        The description of the parser
    constant_fields : dict[str, dict[str, bool]], optional
        Constant fields are those which are single values, rather than taken from a field from the source data.
        For example, if an entire dataset is from the DRC, but a country field is in the target schema, there may not be a
        field in the dataset stating the country.
        A dictionary of constant fields for each table, where the keys are the table names
        and the values are boolean True/False values indicating whether the field should be pulled from the source data
        or not. All fields in wide tables default to False, while long tables default to True for all columns except
        the value column(s).
    """

    def __init__(
        self,
        mappings: Union[INPUT_FORMAT, dict[str, INPUT_FORMAT]],
        schema_path: Union[Path, str],
        parser_name: str,
        description: Union[str, None] = None,
        constant_fields: Union[dict[str, dict[str, bool]], None] = None,
    ):
        if not isinstance(mappings, dict):
            # if not a dict, assume a single mapping file
            mappings = read_data(mappings, "A mapping file")

        self.schema_path = Path(schema_path)
        self.parser_name = parser_name
        self.parser_description = description or parser_name

        self.config = get_config()
        self.tables = list(self.config.schemas.keys())

        if len(self.tables) == 1:
            # if only one table, use the singular form
            self.mappings = {self.tables[0]: mappings}
        else:
            for table in self.tables:
                if table not in mappings:
                    raise ValueError(
                        f"Mapping for table '{table}' not found in provided mappings."
                    )
            self.mappings = {
                table: read_data(mappings[table], "A mapping file")
                for table in self.tables
            }

        self.schemas = {t: read_json(self.config.schemas[t]) for t in self.tables}

        self.table_types = {
            t: "wide" if "target_field" in m.columns else "long"
            for t, m in self.mappings.items()
        }

        self.constant_fields = constant_fields or {}

    def header(self) -> dict[str, Any]:
        "The ADTL-specific header for the TOML file"
        schemas = {}
        for table in self.schemas:
            schemas[table] = {
                "kind": (
                    "oneToOne" if self.table_types[table] == "wide" else "oneToMany"
                ),
                "schema": f"{self.schema_path / Path(self.config.schemas[table])}",
            }

        return {
            "adtl": {
                "name": self.parser_name,
                "description": self.parser_description,
                "returnUnmatched": True,
                "tables": schemas,
                "defs": {},
            }
        }

    def make_single_parser(self) -> dict[str, Any]:
        """
        Takes the csv mapping file from `create_mapping` and writes out a TOML parser

        Generates a TOML parser for use with ADTL using the intermediate CSV file from
        by `create_mapping`. This will generate a TOML file that can be used to parse
        raw data into the format expected by the schema.

        Returns
        -------
        dict
            Dictionary containing the TOML parser data, ready to be written out.
        """

        data = self.header()
        for table in self.tables:
            if self.table_types[table] == "wide":
                parser_class = WideTableParser
            else:
                parser_class = LongTableParser
            parser_class = parser_class(
                self.mappings[table],
                self.schemas[table],
                table,
            )
            if self.constant_fields:
                parser_class.update_constant_fields(self.constant_fields.get(table, {}))

            table_parser, references = parser_class.make_toml_table()
            data.update(table_parser)
            if references:
                data["adtl"]["defs"].update(references)
        return data

    def write_toml(self, data: dict[str, Any], output: str = None):
        """
        Write a dictionary structure to a TOML file, using `output` as the filename if
        provided.

        Parameters
        ----------
        data : dict
            Dictionary containing the TOML parser data
        output : str, optional
            Filename to write the TOML data to. Defaults to the name of the parser.
        """
        if not output:
            output = f"{self.parser_name}.toml"
        with open(output, "wb") as fp:
            dump(data, fp)

    def create_parser(self, file_name: str = None):
        """
        Main function to create the TOML parser from the intermediate CSV file.
        """
        toml_data = self.make_single_parser()
        self.write_toml(toml_data, output=file_name)


def create_parser(
    mappings: Union[pd.DataFrame, str],
    schema_path: Path,
    parser_name: str,
    description: Union[str, None] = None,
    constant_fields: Union[dict[str, dict[str, bool]], None] = None,
):
    """
    Takes the csv mapping file created by `create_mapping` and writes out a TOML parser

    Generates a TOML parser for use with ADTL from the intermediate CSV file generated
    by `create_mapping`. This will generate a TOML file that can be used to parse raw
    data into the format expected by the schema.

    Parameters
    ----------
    mappings
        Path to the CSV file containing the mappings
    schema_path
        Path to the schema file
    parser_name
        Name of the parser to create
    description
        Description of the parser. Defaults to the parser name.
    constant_fields
        Constant fields are those which are single values, rather than taken from a
        field in the source data.

    Returns
    -------
    None
    """
    ParserGenerator(
        mappings,
        schema_path,
        parser_name,
        description,
        constant_fields=constant_fields,
    ).create_parser(parser_name)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Make TOML from intermediate CSV file created by create_mapping.py",
        prog="autoparser create-parser",
    )
    parser.add_argument("mappings", help="Mapping file to create TOML from", type=str)
    parser.add_argument("schema_path", help="Path where schemas are located")
    parser.add_argument(
        "-o",
        "--output",
        help="Name of the parser to output (default=globalhealth)",
        default="globalhealth",
    )
    parser.add_argument("--description", help="Description of the parser")
    parser.add_argument(
        "-c",
        "--config",
        help=f"Configuration file to use (default={DEFAULT_CONFIG})",
        type=Path,
    )
    args = parser.parse_args(argv)

    schema_path = Path(args.schema_path)

    setup_config(args.config or DEFAULT_CONFIG)

    ParserGenerator(
        args.mappings,
        schema_path,
        args.output,
        args.description or None,
    ).create_parser()


if __name__ == "__main__":
    main()  # pragma: no cover
