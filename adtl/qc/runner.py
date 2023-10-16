"""
Quality Control module for ADTL, runner submodule
"""
import sys
import copy
import json
import argparse
import functools
import importlib
import sqlite3
import multiprocessing
from typing import List, Optional, Dict, Any
from pathlib import Path
from collections import defaultdict
from fnmatch import fnmatch

import pandas as pd

from . import Dataset, Rule, WorkUnit, WorkUnitResult

DEFAULT_PATTERN = "*.csv"

DDL_RESULTS = """CREATE TABLE IF NOT EXISTS results (
    rule TEXT,
    dataset TEXT,
    file TEXT,
    rows_success INTEGER,
    rows_fail INTEGER,
    ratio_success REAL,
    rows_fail_idx TEXT,
    success INTEGER,
    mostly REAL,
    fail_data TEXT
)"""

DDL_RULES = """CREATE TABLE IF NOT EXISTS rules (
    rule TEXT,
    description TEXT,
    long_description TEXT
)"""

INSERT_RESULTS = """INSERT INTO results VALUES (
    :rule, :dataset, :file, :rows_success,
    :rows_fail, :ratio_success, :rows_fail_idx,
    :success, :mostly, :fail_data
)"""

INSERT_RULES = """INSERT INTO rules VALUES (
    :name, :description, :long_description
)"""


def collect_datasets(
    root: Path = Path("."), file_formats: List[str] = ["csv"]
) -> List[Dataset]:
    files = []
    for fmt in file_formats:
        files.extend(list(root.rglob(f"*.{fmt}")))
    folders = defaultdict(list)
    for f in files:
        folders[f.parent.stem].append(f)
    return [Dataset(folder=folder, files=folders[folder]) for folder in folders]


def collect_rules(root: Path = Path("qc")) -> List[Rule]:
    rule_files = list(root.glob("*.py"))

    sys.path.append(".")
    all_rules = []
    for rule_file in rule_files:
        module = importlib.import_module(
            str(rule_file).replace(".py", "").replace("/", ".")
        )
        rules = [x for x in dir(module) if x.startswith("rule_")]
        all_rules.extend([make_rule(module, r) for r in rules])
    return all_rules


def make_rule(module, rule_name: str) -> Rule:
    r = getattr(module, rule_name)
    description, _, long_description = r.__doc__.partition("\n")
    return Rule(
        module=module.__name__,
        name=r.__name__,
        description=description,
        long_description=long_description,
        pattern=getattr(r, "pattern", DEFAULT_PATTERN),
    )


def collect_work_units(datasets: List[Dataset], rules: List[Rule]) -> List[WorkUnit]:
    """Returns a list of work units that can be run in parallel

    A work unit is a (file, rule) pair that can be processed to return
    metadata relating to the success or failure of the unit, this method
    is used by the work runners to run work units in parallel and save to a DB.
    """
    out = []
    for dataset, rule in zip(datasets, rules):
        files = dataset["files"]
        out.extend(
            [
                dict(dataset=dataset, file=f, rule=rule)
                for f in files
                if fnmatch(f, rule["pattern"])
            ]
        )
    return out


def prepare_result_for_insertion(work_unit_result: WorkUnitResult) -> Dict[str, Any]:
    result: Dict[str, Any] = copy.deepcopy(work_unit_result)  # type: ignore
    result["fail_data"] = (
        None
        if result["fail_data"].empty
        else json.dumps(result["fail_data"].to_dict(orient="records"))
    )
    result["rows_fail_idx"] = (
        None
        if not result["rows_fail_idx"]
        else ",".join(map(str, result["rows_fail_idx"]))
    )
    result["dataset"] = work_unit_result["dataset"]["folder"]
    result["file"] = str(result["file"])
    return result


def get_result_from_insertion(data: Dict[str, Any]) -> WorkUnitResult:
    result: Dict[str, Any] = copy.deepcopy(data)  # type: ignore
    if result["fail_data"]:
        result["fail_data"] = pd.DataFrame(json.loads(result["fail_data"]))
    if result["rows_fail_idx"]:
        result["rows_fail_idx"] = [int(x) for x in result["rows_fail_idx"].split(",")]
    return result


def process_work_unit(unit: WorkUnit, save_db: Optional[str] = None) -> WorkUnitResult:
    rule = unit["rule"]
    module = importlib.import_module(rule["module"])
    rule_function = getattr(module, rule["name"])

    # TODO: assumes file is CSV, should be a generic reader
    result = rule_function(pd.read_csv(unit["file"]))
    result.update(
        dict(rule=unit["rule"]["name"], dataset=unit["dataset"], file=unit["file"])
    )
    if save_db:
        con = sqlite3.connect(save_db)
        cur = con.cursor()
        cur.execute(DDL_RESULTS)
        cur.execute(INSERT_RESULTS, prepare_result_for_insertion(result))
        con.commit()
    return result


def start(
    data_path: Path,
    rules_path: Path = Path("qc"),
    data_file_formats: List[str] = ["csv"],
    store_database: Optional[str] = None,
) -> List[WorkUnitResult]:
    rules = collect_rules(rules_path)
    datasets = collect_datasets(data_path, data_file_formats)
    work_units = collect_work_units(datasets, rules)

    # Re-create rules list
    if store_database:
        conn = sqlite3.connect(store_database)
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS rules")
        cur.execute(DDL_RULES)
        cur.executemany(INSERT_RULES, rules)
        conn.commit()

    pool = multiprocessing.Pool()
    process_work_unit_db = functools.partial(process_work_unit, save_db=store_database)
    return pool.map(process_work_unit_db, work_units)


def _main(args=None):
    parser = argparse.ArgumentParser(prog="adtl-qc", description="ADTL Quality Control")
    parser.add_argument("data", help="path to datasets")
    parser.add_argument("-r", "--rule-root", help="path to rules", default="qc")
    parser.add_argument(
        "-d", "--database", help="Database to store QC results", default="adtl-qc.db"
    )
    parser.add_argument(
        "--format",
        help="file formats (comma separated) to include in datasets",
        default="csv",
    )
    parser.add_argument(
        "-n", "--no-report", help="do not generate HTML report", action="store_true"
    )
    args = parser.parse_args(args)
    start(
        Path(args.data),
        Path(args.rule_root),
        data_file_formats=args.format.split(","),
        store_database=args.database,
    )
