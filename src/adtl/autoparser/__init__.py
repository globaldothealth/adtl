import sys

from .create_mapping import Mapper, create_mapping
from .dict_writer import DictWriter, create_dict, generate_descriptions
from .make_toml import ParserGenerator, create_parser

__all__ = [
    "DictWriter",
    "create_dict",
    "generate_descriptions",
    "Mapper",
    "create_mapping",
    "ParserGenerator",
    "create_parser",
]

from .create_mapping import main as csv_mapping_main
from .dict_writer import api_descriptions_only as add_descriptions_main
from .dict_writer import main as make_dd_main
from .make_toml import main as make_toml_main


def main():
    if len(sys.argv) < 2:
        print(
            """
            autoparser: specify subcommand to run

            Available subcommands:
            create-dict - Create a data dictionary from a dataset
            add-descriptions - Add descriptions to a data dictionary (LLM key required)
            create-mapping - Create initial CSV mapping from data dictionary (LLM key required) # noqa
            create-parser - Generate TOML parser from CSV mapping file
            """
        )
        sys.exit(1)
    subcommand = sys.argv[1]
    if subcommand not in [
        "create-parser",
        "create-mapping",
        "add-descriptions",
        "create-dict",
    ]:
        print("autoparser: unrecognised subcommand", subcommand)
        sys.exit(1)
    sys.argv = sys.argv[1:]
    if subcommand == "create-parser":
        make_toml_main()
    if subcommand == "create-mapping":
        csv_mapping_main()
    elif subcommand == "create-dict":
        make_dd_main()
    elif subcommand == "add-descriptions":
        add_descriptions_main()
    else:
        pass
