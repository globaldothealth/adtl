import sys
import json
from pathlib import Path
import pytest

import adtl as parser
import adtl.transformations as transform


@pytest.mark.parametrize(
    "test_input,expected", [("1", True), (None, False), ("", False)]
)
def test_isNotNull(test_input, expected):
    assert transform.isNotNull(test_input) == expected


@pytest.mark.parametrize(
    "test_date_birth, test_date_current, expected",
    [
        ("1996-02-22", "2023-02-22", 27.0),
        ("", "2023-02-22", None),
        (None, "2023-02-22", None),
    ],
)
def test_yearsElasped(test_date_birth, test_date_current, expected):
    assert transform.yearsElapsed(test_date_birth, test_date_current) == pytest.approx(
        expected, 0.001
    )


@pytest.mark.parametrize(
    "test_duration_start, test_duration_current, expected",
    [
        ("2023-02-01", "2023-03-05", 32),
        ("", "2023-02-22", None),
        (None, "2023-02-22", None),
    ],
)
def test_durationDays(test_duration_start, test_duration_current, expected):
    assert (
        transform.durationDays(test_duration_start, test_duration_current) == expected
    )
