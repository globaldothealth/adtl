"""
Tests for QC runner
"""

import os
import json
from pathlib import Path
from typing import List, Dict, Any

import pytest
from adtl.qc import WorkUnit
from adtl.qc.runner import (
    collect_datasets,
    collect_rules,
    collect_work_units,
    process_work_unit,
)


def test_collect_datasets():
    dataset = collect_datasets(Path(__file__).parent / "data")[0]
    assert dataset["folder"] == "dataset"
    assert str(dataset["files"][0]).endswith("data/dataset/test-subject.csv")


def test_collect_rules():
    cwd = os.getcwd()
    os.chdir(Path(__file__).parent)
    rules = collect_rules(Path("qc"))
    assert rules == [
        {
            "description": "Male patients are not pregnant",
            "long_description": "",
            "module": "qc.pregnancy",
            "name": "rule_male_patients_not_pregnant",
            "pattern": "*-subject.csv",
        },
        {
            "description": "Study subject schema",
            "long_description": "",
            "module": "qc.pregnancy",
            "name": "schema_subject",
            "pattern": "*-subject.csv",
        },
    ]
    os.chdir(cwd)


@pytest.fixture
def work_units():
    datasets = collect_datasets(Path(__file__).parent / "data")
    cwd = os.getcwd()
    os.chdir(Path(__file__).parent)
    rules = collect_rules(Path("qc"))
    _work_units = collect_work_units(datasets, rules)
    os.chdir(cwd)
    return _work_units


def test_collect_work_units(work_units: List[WorkUnit]):
    for unit in work_units:
        assert unit["dataset"]["folder"] == "dataset"
        assert str(unit["dataset"]["files"][0]).endswith(
            "data/dataset/test-subject.csv"
        )
        assert str(unit["file"]).endswith("data/dataset/test-subject.csv")
    assert work_units[0]["rule"] == {
        "description": "Male patients are not pregnant",
        "long_description": "",
        "module": "qc.pregnancy",
        "name": "rule_male_patients_not_pregnant",
        "pattern": "*-subject.csv",
    }
    assert work_units[1]["rule"] == {
        "description": "Study subject schema",
        "long_description": "",
        "module": "qc.pregnancy",
        "name": "schema_subject",
        "pattern": "*-subject.csv",
    }


def test_process_work_unit(work_units: List[WorkUnit]):
    result = process_work_unit(work_units[0])[0]
    assert result["dataset"] == "dataset"
    assert str(work_units[0]["dataset"]["files"][0]).endswith(
        "data/dataset/test-subject.csv"
    )
    assert str(work_units[0]["file"]).endswith("data/dataset/test-subject.csv")
    print(result)
    assert (
        dict(
            mostly=0,
            ratio_success=0.9,
            rows_fail=1,
            rows_success=9,
            rule="rule_male_patients_not_pregnant",
            rows_fail_idx="7",
        ).items()
        <= result.items()
    )
    assert json.loads(result["fail_data"]) == {
        "pregnancy": {"7": True},
        "sex": {"7": None},
        "sex_at_birth": {"7": "male"},
    }


def test_process_work_unit_schema(work_units: List[WorkUnit]):
    results: List[Dict[str, Any]] = process_work_unit(work_units[1])
    for res in results:
        del res["file"]
    assert results == [
        {
            "dataset": "dataset",
            "fail_data": None,
            "mostly": 0.95,
            "ratio_success": 0.8,
            "reason": "",
            "rows": 10,
            "rows_fail": 2,
            "rows_fail_idx": "2,5",
            "rows_success": 8,
            "rule": "schema_subject",
            "success": False,
        },
        {
            "dataset": "dataset",
            "fail_data": '{"age": {"2": "old"}}',
            "mostly": 0,
            "ratio_success": 0,
            "reason": "data.age must be integer",
            "rows": 1,
            "rows_fail": 1,
            "rows_fail_idx": "2",
            "rows_success": 0,
            "rule": "schema_subject",
            "success": False,
        },
        {
            "dataset": "dataset",
            "fail_data": '{"age": {"5": "200"}}',
            "mostly": 0,
            "ratio_success": 0,
            "reason": "data.age must be smaller than or equal to 120",
            "rows": 1,
            "rows_fail": 1,
            "rows_fail_idx": "5",
            "rows_success": 0,
            "rule": "schema_subject",
            "success": False,
        },
    ]