import sys

try:
    from .create_mapping import Mapper, create_mapping
    from .dict_writer import DictWriter, create_dict, generate_descriptions
    from .make_toml import ParserGenerator, create_parser
except ImportError:
    raise ImportError(
        "autoparser is not available. Import as 'adtl[autoparser]' to use."
    )

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
            adtl-autoparser: specify subcommand to run

            Available subcommands:
            create-dict - Create a data dictionary from a dataset
            add-descriptions - Add descriptions to a data dictionary (LLM key required)
            create-mapping - Create initial CSV mapping from data dictionary (LLM key required) # noqa
            create-parser - Generate TOML parser from CSV mapping file
            """
        )
        sys.exit(1)
    subcommand = sys.argv[1]

    subcommands = {
        "create-parser": make_toml_main,
        "create-mapping": csv_mapping_main,
        "add-descriptions": add_descriptions_main,
        "create-dict": make_dd_main,
    }

    if subcommand not in subcommands:
        print("adtl-autoparser: unrecognised subcommand", subcommand)
        sys.exit(1)
    sys.argv = sys.argv[1:]
    subcommands[subcommand]()
