import argparse
import importlib.metadata
import json

from adtl.parser import Parser
from adtl.python_interface import parse

__all__ = ["Parser", "parse"]

__version__ = importlib.metadata.version("adtl")


def main(argv=None):
    cmd = argparse.ArgumentParser(
        prog="adtl",
        description="Transforms and validates data into CSV given a specification",
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
    cmd.add_argument("--save-report", help="save report in JSON format")
    cmd.add_argument(
        "--include-def",
        action="append",
        help="include external definition (TOML or JSON)",
    )
    cmd.add_argument("--version", action="version", version="%(prog)s " + __version__)
    args = cmd.parse_args(argv)
    include_defs = args.include_def or []
    spec = Parser(args.spec, include_defs=include_defs, quiet=args.quiet)

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


if __name__ == "__main__":
    main()
