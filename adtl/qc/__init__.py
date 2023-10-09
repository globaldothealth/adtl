"""
Quality Control module for ADTL
"""
import functools
from typing import List, Union
from pathlib import Path
from typing import TypedDict, Dict, List, Any

import pandas as pd
import numpy as np


class Rule(TypedDict):
    module: str
    name: str
    description: str
    pattern: str


class WorkUnit(TypedDict):
    rule: Rule
    file: str
    dataset: str


class Dataset(TypedDict):
    folder: str
    files: List[str]


class WorkUnitResult(TypedDict):
    rule: str
    dataset: str
    file: str
    rows_success: int
    rows_fail: int
    ratio_success: float
    rows_fail_idx: List[int]
    success: bool
    mostly: float
    fail_data: pd.DataFrame


def rules_for(pattern: str, *rules):
    for r in rules:
        r.pattern = pattern


def rule(columns: List[str], mostly: float = 0, set_missing_columns: bool = True):
    """Decorator that indicates a QC rule

    Args:
        columns: List of required columns in the dataframe. If columns are not
        present, they are assigned null.

        mostly: Indicates the check in the rule should be considered a success
        if the ratio of successful rows is greater than this value. If not
        specified all rows have to succeed for the rule to be a considered a
        success.

        set_missing_columns: On by default, this setting assigns NA/null to
        required columns that are missing in the data
    """

    def decorator_rule(func):
        @functools.wraps(func)
        def wrapper(df, **kwargs):
            if set_missing_columns:
                for c in set(columns) - set(df.columns):
                    df[c] = None
            series = func(df, **kwargs)
            assert len(series) == len(
                df
            ), "Returned series must have same cardinality as source dataframe"
            rows_fail_idx = [i for i, val in enumerate(series) if val is False]
            if isinstance(series, (pd.Series, np.ndarray)):
                rows_success: int = series.sum()
                rows_fail = len(series) - rows_success
                ratio_success = rows_success / len(series)
                return dict(
                    rows_success=int(rows_success),
                    rows_fail=int(rows_fail),
                    ratio_success=ratio_success,
                    success=bool(ratio_success >= mostly),
                    mostly=mostly,
                    rows_fail_idx=rows_fail_idx,
                    fail_data=df.loc[rows_fail_idx][columns],
                )
            elif isinstance(series, bool):
                return dict(
                    rows_success=None,
                    rows_fail=None,
                    ratio_success=None,
                    success=series,
                )

        return wrapper

    return decorator_rule


def schema(schema_path: Union[str, Path], pattern: str = "*.csv"):
    pass


def main(args=None):
    from .runner import _main

    _main(args)
