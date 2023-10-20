"""
Quality Control module for ADTL
"""
import math
import json
import functools
import copy
from pathlib import Path
from typing import List, TypedDict, Optional, Callable, Tuple, Dict, Any

import pandas as pd
import numpy as np
import fastjsonschema


class Rule(TypedDict):
    module: str
    name: str
    description: str
    long_description: Optional[str]
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
    rows: int
    ratio_success: float
    rows_fail_idx: List[int]
    success: bool
    mostly: float
    reason: str
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
                    rows=int(rows_success) + int(rows_fail),
                    ratio_success=ratio_success,
                    success=bool(ratio_success >= mostly),
                    mostly=mostly,
                    rows_fail_idx=",".join(map(str, rows_fail_idx)),
                    fail_data=json.dumps(
                        df.loc[rows_fail_idx][columns].to_dict(),
                        sort_keys=True,
                    ),
                )
            elif isinstance(series, bool):
                return dict(
                    rows_success=None,
                    rows_fail=None,
                    rows=None,
                    ratio_success=None,
                    success=series,
                    mostly=mostly,
                    rows_fail_idx=None,
                    fail_data=None,
                )

        return wrapper

    return decorator_rule


def _get_columns_from_valid_data(df: pd.DataFrame) -> List[str]:
    cols = set()
    for row in df.itertuples():
        cols |= set(row.column.split(";"))
    return sorted(cols - {""})


def _to_json(row: Dict[str, Any]) -> Dict[str, Any]:
    """"""
    nrow = copy.deepcopy(row)
    for key, val in row.items():
        if isinstance(val, int):
            continue
        elif isinstance(val, float):
            if math.isnan(val):
                del nrow[key]
            continue
        else:
            val = str(val).strip()
        if val.upper() == "TRUE":
            nrow[key] = True
            continue
        elif val.upper() == "FALSE":
            nrow[key] = False
            continue
        elif val.upper() in ["NA", "NK", "N/A", "NAN", "N/K", "NONE", "-", ""]:
            del nrow[key]
            continue
        else:
            pass
        try:
            nrow[key] = int(val)
        except ValueError:
            try:
                nrow[key] = float(val)
            except ValueError:
                nrow[key] = str(val)
    return nrow


def schema(
    schema_path: str, pattern: str = "*.csv", mostly: float = 0.95
) -> Callable[[pd.DataFrame], List[WorkUnitResult]]:
    schema_path = Path(schema_path)  # type: ignore
    with schema_path.open() as fp:
        schema = json.load(fp)
        validator = fastjsonschema.compile(schema)

    def rule_schema(df: pd.DataFrame):
        valids: List[Tuple[bool, str, str]] = []
        for row in map(_to_json, df.to_dict(orient="records")):
            try:
                validator(row)
                valids.append((True, "", ""))
            except fastjsonschema.exceptions.JsonSchemaValueException as e:
                valids.append((False, e.message, ";".join(e.path[1:])))
        valid_data = pd.DataFrame(valids, columns=["is_valid", "reason", "column"])
        rows_success = valid_data.is_valid.sum()
        rows_fail = len(valid_data) - rows_success
        ratio_success = rows_success / (rows_success + rows_fail)
        reason_counts = valid_data[~valid_data.is_valid].reason.value_counts()
        res = [
            dict(
                rows_success=rows_success,
                rows_fail=rows_fail,
                rows=rows_success + rows_fail,
                ratio_success=ratio_success,
                success=bool(ratio_success >= mostly),
                mostly=mostly,
                rows_fail_idx=[i for i, v in enumerate(valids) if v[0] is False],
                fail_data=None,
            )
        ]
        for reason, count in zip(reason_counts.index, reason_counts):
            res.append(
                dict(
                    rows=count,
                    rows_success=0,
                    rows_fail=count,
                    ratio_success=0,
                    success=False,
                    mostly=0,
                    rows_fail_idx=list(
                        valid_data.loc[valid_data.reason == reason].index
                    ),
                    reason=reason,
                    fail_data=json.dumps(
                        df.loc[valid_data.reason == reason][
                            _get_columns_from_valid_data(
                                valid_data[valid_data.reason == reason]
                            )
                        ]
                        .head(10)
                        .to_dict()
                    ),
                )
            )
        return res

    rule_schema.__doc__ = f"{schema.get('title', schema_path)} schema"
    rule_schema.__name__ = "schema_" + schema_path.stem.split(".")[0]
    rule_schema.pattern = pattern
    return rule_schema


def main(args=None):
    from .runner import _main

    _main(args)
