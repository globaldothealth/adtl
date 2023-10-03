"""
Quality Control module for ADTL
"""
import functools
import importlib
from typing import List, Union
from pathlib import Path

import numpy as np
import pandas as pd


def rule(columns: List[str], mostly: float = 0):
    """Decorator that indicates a QC rule

    Args:
        columns: List of required columns in the dataframe. If columns are not
        present, they are assigned null.

        mostly: Indicates the check in the rule should be considered a success
        if the ratio of successful rows is greater than this value. If not
        specified all rows have to succeed for the rule to be a considered a
        success.
    """

    def decorator_rule(func):
        @functools.wraps(func)
        def wrapper(df, **kwargs):
            series = func(df, **kwargs)
            if isinstance(series, (pd.Series, np.ndarray)):
                rows_success: int = series.sum()
                rows_fail = len(series) - rows_success
                ratio_success = rows_success / len(series)
                return dict(
                    rows_success=rows_success,
                    rows_fail=rows_fail,
                    ratio_success=ratio_success,
                    success=ratio_success >= mostly,
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


def collect_rules(root: Path = Path("qc")):
    rule_files = list(root.glob("*.py"))
    print(rule_files)


def main():
    print("ADTL Quality Control Module")
    print(collect_rules())


if __name__ == "__main__":
    main()
