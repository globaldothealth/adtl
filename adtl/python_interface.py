from adtl import Parser
import pandas as pd
from typing import Literal
from pathlib import Path


def parse(
    spec: str | Path | dict[str, str],
    file: str | Path | pd.DataFrame,
    output=None,
    encoding: str = "utf-8",
    include_defs=[],
    save_as: Literal["csv", "parquet", None] = "csv",
):
    """Parse a file according to a specification

    Args:
        spec: Specification file to use
        file: File to parse
        encoding: Encoding of the file
        include_def: Additional definitions to include
        save_as: Save the output as a CSV or parquet file, or don't save (None)

    Returns:
        dict[str, pd.DataFrame]: Dictionary of tables parsed into new format
    """
    spec = Parser(spec, include_defs=include_defs)

    # check for incompatible options
    if spec.header.get("returnUnmatched") and save_as == "parquet":
        raise ValueError("returnUnmatched and parquet options are incompatible")

    # run adtl
    adtl_output = spec.parse(file, encoding=encoding)
    if save_as:
        adtl_output.save(output or spec.name, save_as)
    return {k: pd.DataFrame(v) for k, v in adtl_output.data.items()}
