import pytest

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


@pytest.mark.parametrize(
    "test_startdate_start, test_startdate_duration, expected",
    [
        ("2023-02-01", 10, "2023-01-22"),
        ("", "2023-02-22", None),
        (None, "2023-02-22", None),
    ],
)
def test_startDate(test_startdate_start, test_startdate_duration, expected):
    assert (
        transform.startDate(test_startdate_start, test_startdate_duration) == expected
    )


@pytest.mark.parametrize(
    "test_enddate_start, test_enddate_duration, expected",
    [
        ("2023-01-24", 10, "2023-02-03"),
        ("", "2023-02-22", None),
        (None, "2023-02-22", None),
    ],
)
def test_endDate(test_enddate_start, test_enddate_duration, expected):
    assert transform.endDate(test_enddate_start, test_enddate_duration) == expected


@pytest.mark.parametrize(
    "badfloat, expected",
    [
        ('" - 11 ', -11.0),
        ('"3"', 3.0),
        ("3,4", 3.4),
        ("1,234.5", 1234.5),
        ("1.234,5", 1234.5),
        ("1.567.923,66", 1567923.66),
    ],
)
def test_getFloat(badfloat, expected):
    assert transform.getFloat(badfloat) == expected


@pytest.mark.parametrize(
    "year,month,day,expected",
    [
        ("", "", "", None),
        ("2020", "", "", None),
        ("", "13", "", None),
        ("2020", "05", "04", "2020-05-04"),
        ("1999", "12", "44", None),
    ],
)
def test_makeDate(year, month, day, expected):
    assert transform.makeDate(year, month, day) == expected


@pytest.mark.parametrize(
    "date,time_seconds,date_format,tzname,expected",
    [
        ("04/05/2020", "41400", "%d/%m/%Y", "UTC", "2020-05-04T11:30:00+00:00"),
        ("04/05/2020", "", "%d/%m/%Y", "UTC", "2020-05-04"),
        ("04/05/2020", "", "%m/%d/%Y", "UTC", "2020-04-05"),
        ("05/06/2020", "86399", "%d/%m/%Y", "UTC", "2020-06-05T23:59:00+00:00"),
        ("05/06/2020", "86399", "%d/%m/%Y", "Asia/Tokyo", "2020-06-05T23:59:00+09:00"),
    ],
)
def test_makeDateTimeFromSeconds(date, time_seconds, date_format, tzname, expected):
    assert (
        transform.makeDateTimeFromSeconds(date, time_seconds, date_format, tzname)
        == expected
    )


@pytest.mark.parametrize(
    "field, return_text, expected",
    [
        ("2023-01-24", "Ribavarin", "Ribavarin"),
        (True, "Dexamethasone", "Dexamethasone"),
        ("", "Prednisolone", None),
        (None, "Chloroquine", None),
    ],
)
def test_textIfNotNull(field, return_text, expected):
    assert transform.textIfNotNull(field, return_text) == expected
