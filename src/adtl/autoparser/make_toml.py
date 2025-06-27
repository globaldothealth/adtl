"""
Generate TOML parser from intermediate CSV file
"""

from __future__ import annotations

import abc
import argparse
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .toml_writer import dump
from .util import DEFAULT_CONFIG, parse_llm_mapped_values, read_config_schema, read_data

logger = logging.getLogger(__name__)
INPUT_FORMAT = pd.DataFrame | str | Path


class TableParser(abc.ABC):
    def __init__(self, mapping: pd.DataFrame, schema: dict, table_name: str, config):
        self.mapping = mapping
        self.schema = schema
        self.name = table_name
        self.config = config

    def single_field_mapping(self): ...

    def make_toml_table(self) -> dict[str, Any]: ...

    @property
    def schema_fields(self):
        """Returns all the fields for `table` and their properties"""
        return self.schema["properties"]

    @property
    def field_types(self) -> dict[str, list[str]]:
        """Returns the field types of the target schema"""
        if not hasattr(self, "_field_types"):
            s = self.schema_fields
            self._field_types = {f: s[f].get("type", ["string", "null"]) for f in s}
        return self._field_types


class WideTableParser(TableParser):
    """
    Class for generating a wide table from the mappings.
    """

    @property
    def parsed_choices(self) -> pd.Series:
        """Returns the mapped values for each target field"""
        if not hasattr(self, "_parsed_choices"):
            self._parsed_choices = self.mapping.value_mapping.map(
                parse_llm_mapped_values
            )
            self._parsed_choices.index = self.mapping.target_field
        return self._parsed_choices

    @property
    def references_definitions(self) -> tuple[dict[str, str], dict[str, dict]]:
        """Finds and returns the references and definitions for the mappings"""
        if not hasattr(self, "_references_definitions"):
            # use value_counts() on parsed_choices normalise various flavours of Y/N/NK
            value_counts = self.parsed_choices.value_counts()

            self._references_definitions = self.refs_defs(
                value_counts, self.config["num_refs"]
            )
        return self._references_definitions

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

            else:  # combinedType
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


class LongTableParser(TableParser):
    """
    Class for generating a long table from the mappings.
    """

    @property
    def other_fields(self) -> list[str]:
        """Returns the other fields in the schema that are not target fields"""
        return [
            f
            for f in self.schema_fields
            # shouldn't hard code these, but for now this is fine
            if f not in ["attribute", "value", "value_num", "value_bool"]
        ]

    def single_field_mapping(self, match: pd.DataFrame) -> dict[str, Any]:
        """Make a single field mapping from a single row of the mappings dataframe"""

        out = {
            "attribute": match.source_field,
            match.value_type: {"field": match.source_field},
        }

        for field in self.other_fields:
            if not pd.isna(match[field]):
                if self.schema["properties"][field].get("enum", None):
                    out[field] = match[field]
                else:
                    out[field] = {"field": match[field]}

        return out

    def make_toml_table(self) -> dict[str, Any]:
        """Make single TOML table from mappings"""

        outmap = []

        for _, row in self.mapping.iterrows():
            outmap.append(self.single_field_mapping(row))

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
    config : Path, optional
        The path to the configuration file to use if not using the default configuration
    """

    def __init__(
        self,
        mappings: INPUT_FORMAT | dict[str, INPUT_FORMAT],
        schema_path: Path | str,
        parser_name: str,
        description: str | None = None,
        config: Path | None = None,
    ):
        if not isinstance(mappings, dict):
            # if not a dict, assume a single mapping file
            mappings = read_data(mappings, "A mapping file")

        self.schema_path = Path(schema_path)
        self.parser_name = parser_name
        self.parser_description = description or parser_name

        self.config = read_config_schema(
            config or Path(Path(__file__).parent, DEFAULT_CONFIG)
        )
        self.tables = list(self.config["schemas"].keys())

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

        self.schemas = {
            t: read_config_schema(Path(schema_path, self.config["schemas"][t]))
            for t in self.tables
        }

        self.table_types = {
            t: "wide" if "target_field" in m.columns else "long"
            for t, m in self.mappings.items()
        }

    def header(self) -> dict[str, Any]:
        "The ADTL-specific header for the TOML file"
        schemas = {}
        for table in self.schemas:
            schemas[table] = {
                "kind": (
                    "oneToOne" if self.table_types[table] == "wide" else "oneToMany"
                ),
                "schema": f"{self.schema_path / Path(self.config['schemas'][table])}",
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
                table_parser, references = WideTableParser(
                    self.mappings[table],
                    self.schemas[table],
                    table,
                    self.config,
                ).make_toml_table()
            else:
                table_parser, references = LongTableParser(
                    self.mappings[table],
                    self.schemas[table],
                    table,
                    self.config,
                ).make_toml_table()
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
    mappings: pd.DataFrame | str,
    schema_path: Path,
    parser_name: str,
    description: str | None = None,
    config=DEFAULT_CONFIG,
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
    config
        Path to the configuration file to use. Default is `config/autoparser.toml`.

    Returns
    -------
    None
    """
    ParserGenerator(
        mappings,
        schema_path,
        parser_name,
        description,
        Path(config),
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

    ParserGenerator(
        args.mappings,
        schema_path,
        args.output,
        args.description or None,
        args.config or None,
    ).create_parser()


if __name__ == "__main__":
    main()
