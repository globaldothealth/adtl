"""Python interface to adtl.

These functions are the recommended entry points when using adtl as a library
rather than from the command line. They are re-exported at the top level, so
``adtl.parse`` and ``adtl.python_interface.parse`` are the same function::

    import adtl

    adtl.validate_specification("study.toml")          # check the spec is valid
    adtl.check_mapping("study.toml", "study-data.csv")  # check spec against data
    data = adtl.parse("study.toml", "study-data.csv")   # transform the data

For finer-grained control, such as iterating over parsed rows without building
DataFrames, use :class:`adtl.Parser` directly.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from adtl import Parser
from adtl.adtl_pydantic import ADTLDocument
from adtl.parser import read_file


def parse(
    spec: str | Path | dict[str, Any],
    file: str | Path,
    output: str | None = None,
    encoding: str = "utf-8-sig",
    include_defs: list[str | Path] = [],
    include_transform: str | Path | None = None,
    save_as: Literal["csv", "parquet", None] = "csv",
    quiet: bool = False,
    verbose: bool = False,
    parallel: bool = False,
) -> dict[str, pd.DataFrame]:
    """Parse a file according to a specification

    Args:
        spec: The :ref:`specification` file to use
        file: Path to the CSV data file to transform.
        output: Filename prefix for saved output files; each table is written to
            ``{output}-{table}.{ext}``. Defaults to the parser name given in the
            ``adtl.name`` key of the specification. Ignored when
            ``save_as=None``.
        encoding: Encoding of the data file, default ``utf-8-sig``
        include_defs: Additional definition files (TOML or JSON) to splice into
            the ``adtl.defs`` section of the specification at runtime, in
            addition to any listed under ``adtl.include-def``.
        include_transform: Path to a single Python file containing custom
            transformation functions, which are made available to the ``apply``
            key of the specification alongside the built-in
            :mod:`adtl.transformations`.
        save_as: File format to write the parsed tables to. Pass ``None`` to skip writing files.
        quiet: Suppress all terminal output
        verbose: Increase verbosity, show overwriting warnings
        parallel: Use parallel processing for parsing. Not recommended for small datasets

    Returns:
        dict[str, pd.DataFrame]: Dictionary of tables parsed into new format,
        keyed by the table names defined in the specification.

    Examples:
        Transform a data file, writing ``study-subject.csv`` and
        ``study-observation.csv`` to the current directory for a parser named
        ``study`` with tables ``subject`` and ``observation``:

        >>> import adtl
        >>> data = adtl.parse("study.toml", "study-data.csv")
        >>> data["subject"].head()

        Get the tables back without writing any files, using
        custom transformations and parallel processing:

        >>> data = adtl.parse(
        ...     "study.toml",
        ...     "study-data.csv",
        ...     include_transform="my_transformations.py",
        ...     save_as=None,
        ...     parallel=True,
        ... )
        >>> data["subject"].head()
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


def validate_specification(spec: str | Path | dict[str, Any]) -> None:
    """Validate a specification (parser) file without running it

    Checks the structure of the specification against the adtl schema. No data
    file is involved, so this does not check that the fields referenced by the
    specification exist in a dataset -- use :func:`check_mapping` for that.

    Args:
        spec: The :ref:`specification` to validate, either as a path to a TOML or
            JSON file, or as an already-loaded dictionary.

    Raises:
        pydantic.ValidationError: If the specification does not conform to the
            adtl schema.
        ValueError: If ``spec`` is a path with an extension other than ``.toml``
            or ``.json``.
    """
    if isinstance(spec, (str, Path)):
        spec = read_file(spec)

    ADTLDocument.model_validate(spec)


def check_mapping(
    spec: str | Path | dict[str, Any], data: str | Path | None = None
) -> None:
    """
    Checks the specification file against the data provided to ensure all fields are mapped,
    there are no fields specified in the mapping which are not present in the data,
    and raises warnings or errors as appropriate.

    The specification is always validated first, as by
    :func:`validate_specification`. If ``data`` is given, the field names in the
    specification are then cross-checked against the columns of the data file.
    This backs the ``adtl check`` command-line subcommand.

    Note: This function checks for `field` keys in the spec only. If certain fields are
    only used in e.g. conditional tags (e.g. `if` statements like `if = {field_name = 2}`)
    where the field name is used as the key, they will not be checked here and will be
    returned as 'missing' fields.

    Args:
        spec: The :ref:`specification` to check, either as a path to a TOML or
            JSON file, or as an already-loaded dictionary.
        data: (optional) Path to a CSV data file to cross-check the
            specification's field names against. If omitted, only the structure
            of the specification is validated.

    Raises:
        ValueError: If the specification references fields which are absent from
            the data file. The message lists the offending fields.
        pydantic.ValidationError: If the specification does not conform to the
            adtl schema.

    Warns:
        UserWarning: If the data file contains columns which the specification
            does not map. The message lists the unmapped fields. Unmapped fields
            are often intentional, so this is a warning rather than an error.
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
