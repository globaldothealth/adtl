"""
Quality Control module for ADTL, runner submodule
"""
import sys
import argparse
import importlib
import multiprocessing
from typing import List, Optional
from pathlib import Path
from collections import defaultdict
from fnmatch import fnmatch

import pandas as pd

from . import Dataset, Rule, WorkUnit, WorkUnitResult
from .report import make_report

DEFAULT_PATTERN = "*.csv"


def collect_datasets(
    root: Path = Path("."), file_formats: List[str] = ["csv"]
) -> List[Dataset]:
    files = []
    for fmt in file_formats:
        files.extend(list(root.rglob(f"*.{fmt}")))
    folders = defaultdict(list)
    for f in files:
        folders[f.parent.stem].append(f)
    return [
        Dataset(folder=folder if folder else "_unlabelled", files=folders[folder])
        for folder in folders
    ]


def collect_rules(root: Path = Path("qc")) -> List[Rule]:
    rule_files = list(root.glob("*.py"))

    sys.path.append(".")
    all_rules = []
    for rule_file in rule_files:
        module = importlib.import_module(
            str(rule_file).replace(".py", "").replace("/", ".")
        )
        rules = [
            x for x in dir(module) if x.startswith("rule_") or x.startswith("schema_")
        ]
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


def process_work_unit(unit: WorkUnit) -> List[WorkUnitResult]:
    rule = unit["rule"]
    module = importlib.import_module(rule["module"])
    rule_function = getattr(module, rule["name"])

    # TODO: assumes file is CSV, should be a generic reader
    result = rule_function(pd.read_csv(unit["file"]))
    if not isinstance(result, list):
        result = [result]
    for res in result:
        res.update(
            dict(
                rule=unit["rule"]["name"],
                dataset=(
                    unit["dataset"]["folder"]
                    if unit["dataset"]["folder"]
                    else "_unlabelled"
                ),
                file=str(unit["file"]),
                reason=res.get("reason", ""),
                rows_fail_idx=",".join(map(str, res.get("rows_fail_idx", []))),
            )
        )
    return result


def invoke_runner(
    data_path: Path,
    rules_path: Path = Path("qc"),
    data_file_formats: List[str] = ["csv"],
    store_database: Optional[str] = None,
    disable_report: bool = False,
) -> pd.DataFrame:
    rules = collect_rules(rules_path)
    datasets = collect_datasets(data_path, data_file_formats)
    work_units = collect_work_units(datasets, rules)

    pool = multiprocessing.Pool()
    res = pool.map(process_work_unit, work_units)
    results = pd.DataFrame(sum(res, []))
    results.to_csv(store_database, index=False)
    if not disable_report:
        make_report(results, rules)
    return results


def _main(args=None):
    parser = argparse.ArgumentParser(prog="adtl-qc", description="ADTL Quality Control")
    parser.add_argument("data", help="path to datasets")
    parser.add_argument("-r", "--rule-root", help="path to rules", default="qc")
    parser.add_argument(
        "-d",
        "--database",
        help="Data file to store QC results",
        default="adtl-qc.csv",
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
    invoke_runner(
        Path(args.data),
        Path(args.rule_root),
        data_file_formats=args.format.split(","),
        store_database=args.database,
        disable_report=args.no_report,
    )
