import argparse
import sys

try:
    from .dict_writer import DictWriter, create_dict, generate_descriptions
    from .make_toml import ParserGenerator, create_parser
    from .mapping import Mapper, create_mapping
except ImportError:  # pragma: no cover
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

from .dict_writer import main as make_dd_main
from .make_toml import main as make_toml_main
from .mapping import main as csv_mapping_main


def main():
    parser = argparse.ArgumentParser(
        description="adtl-autoparser: A tool for creating data dictionaries and parsers"
    )

    subparsers = parser.add_subparsers(title="subcommands", dest="subcommand")

    # Subcommand: create-dict
    parser_create_dict = subparsers.add_parser(
        "create-dict",
        help="Create a data dictionary from a dataset",
    )
    parser_create_dict.set_defaults(func=make_dd_main)

    # Subcommand: create-mapping
    parser_create_mapping = subparsers.add_parser(
        "create-mapping",
        help="Create initial CSV mapping from data dictionary (LLM key required)",
    )
    parser_create_mapping.set_defaults(func=csv_mapping_main)

    # Subcommand: create-parser
    parser_create_parser = subparsers.add_parser(
        "create-parser",
        help="Generate TOML parser from CSV mapping file",
    )
    parser_create_parser.set_defaults(func=make_toml_main)

    args, unknown_args = parser.parse_known_args()

    if args.subcommand is None:
        parser.print_help()
        sys.exit(1)

    # Call the appropriate function with remaining arguments
    args.func(unknown_args)
