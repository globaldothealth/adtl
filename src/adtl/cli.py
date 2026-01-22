# adtl/cli.py
import argparse
import json

from .parser import Parser
from .python_interface import check_mapping


def add_parse_subparser(subparsers):
    cmd = subparsers.add_parser(
        "parse",
        help="Transforms and validates data into CSV given a specification",
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
        "--parquet", help="output file is in parquet format", action="store_true"
    )
    cmd.add_argument(
        "-q",
        "--quiet",
        help="quiet mode - decrease verbosity, disable progress bar",
        action="store_true",
    )
    cmd.add_argument(
        "-v",
        "--verbose",
        help="verbose mode - increase verbosity, show overwriting warnings",
        action="store_true",
    )
    cmd.add_argument(
        "-p",
        "--parallel",
        help="process data in parallel",
        action="store_true",
    )
    cmd.add_argument("--save-report", help="save report in JSON format")
    cmd.add_argument(
        "--include-def",
        action="append",
        help="include external definition (TOML or JSON)",
    )
    cmd.add_argument(
        "--include-transform",
        help="include external transforms (single .py file with functions)",
    )
    cmd.set_defaults(func=handle_parse)


def handle_parse(args):
    include_defs = args.include_def or []
    include_transform = args.include_transform or None
    spec = Parser(
        args.spec,
        include_defs=include_defs,
        include_transform=include_transform,
        quiet=args.quiet,
        verbose=args.verbose,
        parallel=args.parallel,
    )

    # check for incompatible options
    if spec.header.get("returnUnmatched") and args.parquet:
        raise ValueError("returnUnmatched and parquet options are incompatible")

    # run adtl
    adtl_output = spec.parse(args.file, encoding=args.encoding)
    adtl_output.save(args.output or spec.name, "parquet" if args.parquet else "csv")
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


def add_check_subparser(subparsers):
    cmd = subparsers.add_parser(
        "check", help="Run validation checks on an ADTL specification"
    )
    cmd.add_argument("spec", help="Specification file to check")
    cmd.add_argument("file", help="optional, data file", default=None, nargs="?")
    cmd.set_defaults(func=handle_check)


def handle_check(args):
    check_mapping(args.spec, args.file)


def build_parser():
    parser = argparse.ArgumentParser(
        prog="adtl", description="ADTL command-line interface"
    )
    subparsers = parser.add_subparsers(dest="cmd", required=True)
    add_parse_subparser(subparsers)
    add_check_subparser(subparsers)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    main()
