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
    prepare_result_for_insertion,
)


def test_collect_datasets():
    dataset = collect_datasets(Path(__file__).parent / "data")[0]
    assert dataset["folder"] == "dataset"
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
    assert work_unit["dataset"]["folder"] == "dataset"
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
    assert result["dataset"]["folder"] == "dataset"
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


def test_prepare_result_for_insertion():
    work_unit_result = dict(
        rule="sample-rule",
        dataset={"folder": "example", "files": ["example/file1.csv", "example/file2.csv"]},
        file=Path("example/file1.csv"),
        rows_success=90,
        rows_fail=10,
        ratio_success=0.9,
        rows_fail_idx=[0, 12, 18, 22, 45, 56, 88, 90, 92, 99],
        success=True,
        mostly=0.8,
        fail_data=pd.DataFrame({"values": range(10)}),
    )
    assert prepare_result_for_insertion(work_unit_result) == dict(  # type: ignore
        rule="sample-rule",
        dataset="example",
        file="example/file1.csv",
        rows_success=90,
        rows_fail=10,
        ratio_success=0.9,
        rows_fail_idx="0,12,18,22,45,56,88,90,92,99",
        success=True,
        mostly=0.8,
        fail_data="""[{"values": 0}, {"values": 1}, {"values": 2}, {"values": 3}, {"values": 4}, {"values": 5}, {"values": 6}, {"values": 7}, {"values": 8}, {"values": 9}]""",
    )
