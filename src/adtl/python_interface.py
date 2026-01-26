from __future__ import annotations

import warnings
from pathlib import Path
from typing import Literal

import pandas as pd

from adtl import Parser
from adtl.adtl_pydantic import ADTLDocument
from adtl.parser import read_file


def parse(
    spec: str | Path | dict[str, str],
    file: str | Path | pd.DataFrame,
    output=None,
    encoding: str = "utf-8-sig",
    include_defs=[],
    include_transform=None,
    save_as: Literal["csv", "parquet", None] = "csv",
    quiet=False,
    verbose=False,
    parallel=False,
):
    """Parse a file according to a specification

    Args:
        spec: Specification file to use
        file: File to parse
        encoding: Encoding of the file
        include_def: Additional definitions to include
        include_transform: File containing custom transformation functions
        save_as: Save the output as a CSV or parquet file, or don't save (None)
        quiet: Suppress all terminal output
        verbose: Increase verbosity, show overwriting warnings
        parallel: Use parallel processing for parsing. Not recommended for small datasets

    Returns:
        dict[str, pd.DataFrame]: Dictionary of tables parsed into new format
    """
    spec = Parser(
        spec,
        include_defs=include_defs,
        include_transform=include_transform,
        quiet=quiet,
        verbose=verbose,
        parallel=parallel,
    )

    # check for incompatible options
    if spec.header.get("returnUnmatched") and save_as == "parquet":
        raise ValueError("returnUnmatched and parquet options are incompatible")

    # run adtl
    adtl_output = spec.parse(file, encoding=encoding)
    if save_as:
        adtl_output.save(output or spec.name, save_as)
    return {k: pd.DataFrame(v) for k, v in adtl_output.data.items()}


def validate_specification(spec: str | Path | dict[str, str]):
    """Validate a specification (parser) file without running it

    Args:
        spec: Specification file to validate
    """
    if isinstance(spec, (str, Path)):
        spec = read_file(spec)

    ADTLDocument.model_validate(spec)


def check_mapping(spec: str | Path | dict, data: str | None = None):
    """
    Checks the specification file against the data provided to ensure all fields are mapped,
    there are no fields specified in the mapping which are not present in the data,
    and raises warnings or errors as appropriate.

    Note: This function checks for `field` keys in the spec only. If certain fields are
    only used in e.g. conditional tags (e.g. `if` statements like `if = {field_name = 2}`)
    where the field name is used as the key, they will not be checked here and will be
    returned as 'missing' fields.
    """

    validate_specification(spec)

    if data:
        parser = Parser(spec)
        missing, absent = parser.check_spec_fields(data)

        if len(absent) > 0:
            msg = f"There are {len(absent)} fields present in your spec file, but not in the dataset:"
            for field in sorted(absent):
                msg += f"\n - {field}"
            raise ValueError(msg)

        if len(missing) > 0:
            msg = f"There are {len(missing)} fields missing from your spec file:"
            for field in sorted(missing):
                msg += f"\n - {field}"
            warnings.warn(msg, UserWarning)
