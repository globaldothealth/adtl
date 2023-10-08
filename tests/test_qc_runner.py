"""
Tests for QC runner
"""

import os
import pandas as pd
from pathlib import Path

import pytest
from adtl.qc.runner import (
    collect_datasets,
    collect_rules,
    collect_work_units,
    process_work_unit,
)


def test_collect_datasets():
    dataset = collect_datasets(Path(__file__).parent / "data")[0]
    assert dataset["dataset"] == "dataset"
    assert str(dataset["files"][0]).endswith("data/dataset/test-subject.csv")


def test_collect_rules():
    cwd = os.getcwd()
    os.chdir(Path(__file__).parent)
    rule = collect_rules(Path("qc"))[0]
    assert rule == {
        "description": "Male patients are not pregnant",
        "module": "qc.pregnancy",
        "name": "rule_male_patients_not_pregnant",
        "pattern": "*-subject.csv",
    }
    os.chdir(cwd)


@pytest.fixture
def work_unit():
    datasets = collect_datasets(Path(__file__).parent / "data")
    cwd = os.getcwd()
    os.chdir(Path(__file__).parent)
    rules = collect_rules(Path("qc"))
    _work_unit = collect_work_units(datasets, rules)[0]
    os.chdir(cwd)
    return _work_unit


def test_collect_work_units(work_unit):
    assert work_unit["dataset"]["dataset"] == "dataset"
    assert str(work_unit["dataset"]["files"][0]).endswith(
        "data/dataset/test-subject.csv"
    )
    assert str(work_unit["file"]).endswith("data/dataset/test-subject.csv")
    assert work_unit["rule"] == {
        "description": "Male patients are not pregnant",
        "module": "qc.pregnancy",
        "name": "rule_male_patients_not_pregnant",
        "pattern": "*-subject.csv",
    }


def test_process_work_unit(work_unit):
    result = process_work_unit(work_unit)
    assert result["dataset"]["dataset"] == "dataset"
    assert str(work_unit["dataset"]["files"][0]).endswith(
        "data/dataset/test-subject.csv"
    )
    assert str(work_unit["file"]).endswith("data/dataset/test-subject.csv")
    assert (
        dict(
            mostly=0,
            ratio_success=0.9,
            rows_fail=1,
            rows_success=9,
            rule="rule_male_patients_not_pregnant",
            rows_fail_idx=[7],
        ).items()
        <= result.items()
    )
    df = pd.DataFrame({"sex_at_birth": ["male"], "sex": [None], "pregnancy": [True]})
    assert df.to_dict(orient="records") == result["fail_data"].to_dict(orient="records")
