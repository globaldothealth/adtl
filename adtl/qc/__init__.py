"""
Quality Control module for ADTL
"""
import sys
import argparse
import functools
import importlib
from typing import List, Union
from pathlib import Path
from typing import TypedDict

import numpy as np
import pandas as pd

DEFAULT_PATTERN = "*.csv"


class Rule(TypedDict):
    rule: str
    description: str
    pattern: str


def rules_for(pattern: str, *rules):
    for r in rules:
        r.pattern = pattern


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
                    mostly=mostly,
                    series=series,
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


def collect_rules(root: Path = Path("qc")) -> list[Rule]:
    rule_files = list(root.glob("*.py"))

    sys.path.append(".")
    module_name = lambda path: str(path).replace(".py", "").replace("/", ".")
    for rule_file in rule_files:
        module = importlib.import_module(module_name(rule_file))
        rules = [x for x in dir(module) if x.startswith("rule_")]
        print(module_name(rule_file), ":", [_rule_properties(module, r) for r in rules])


def _rule_properties(module, rule_name: str) -> Rule:
    r = getattr(module, rule_name)
    return dict(
        rule=r.__name__,
        description=r.__doc__,
        pattern=getattr(r, "pattern", DEFAULT_PATTERN),
    )


def collect_datasets(root: Path = Path("."), file_format="csv") -> list[Path]:
    data = root.rglob(f"*.{file_format}")
    print(list(data))


def main(args=None):
    parser = argparse.ArgumentParser(prog="adtl-qc", description="ADTL Quality Control")
    parser.add_argument("data", help="path to datasets")
    parser.add_argument("-r", "--rule-root", help="path to rules", default=".")
    parser.add_argument(
        "--format", help="file format to include in datasets", default="csv"
    )
    args = parser.parse_args(args)
    collect_rules(Path(args.rule_root))
    collect_datasets(Path(args.data), args.format)


if __name__ == "__main__":
    main()
