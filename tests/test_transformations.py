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
    "test_date_birth, test_date_current, bd_format, cd_format, expected",
    [
        ("1950", "2023-01-01 00:00", "%Y", "%Y-%m-%d %H:%M", 73),
    ],
)
def test_yearsElasped_format(
    test_date_birth, test_date_current, bd_format, cd_format, expected
):
    assert transform.yearsElapsed(
        test_date_birth, test_date_current, bd_format, cd_format
    ) == pytest.approx(expected, 0.001)


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
        ((None, None, None), None),
        ((False, None, None), None),
        (('" - 11 ', None, None), -11.0),
        (('"3"', None, None), 3.0),
        (("-3.", None, None), -3),
        (('" 3.4 "', None, None), 3.4),
        (("3,4", ",", None), 3.4),
        (("1,234.5", None, ","), 1234.5),
        (("1.234,5", ",", "."), 1234.5),
        (("1.567.923,66", ",", "."), 1567923.66),
        (('" -1+1"', None, None), "-1+1"),
        ((" -3 - Moderate Sedation", None, None), -3),
    ],
)
def test_getFloat(badfloat, expected):
    badfloat, dec, sep = badfloat
    assert transform.getFloat(badfloat, set_decimal=dec, separator=sep) == expected


@pytest.mark.parametrize(
    "year,month,day,expected",
    [
        ("", "", "", None),
        ("2020", "", "", None),
        ("", "13", "", None),
        ("2020", "05", "04", "2020-05-04"),
        ("1999", "12", "44", None),
        ("2020", "May", "04", None),
    ],
)
def test_makeDate(year, month, day, expected):
    assert transform.makeDate(year, month, day) == expected


@pytest.mark.parametrize(
    "date,time_seconds,date_format,tzname,expected",
    [
        ("", "41400", "%d/%m/%Y", "UTC", None),
        ("04/05/2020", "41400", "%d/%m/%Y", "UTC", "2020-05-04T11:30:00+00:00"),
        ("04/05/2020", "", "%d/%m/%Y", "UTC", "2020-05-04"),
        ("04/05/2020", "", "%m/%d/%Y", "UTC", "2020-04-05"),
        ("04/05/2020", "", "%Y-%m-%d", "UTC", None),
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


@pytest.mark.parametrize(
    "date,time,date_format,tzname,expected",
    [
        ("", "00:00", "%d/%m/%Y", "UTC", None),
        ("04/05/2020", "10:00", "%d/%m/%Y", "UTC", "2020-05-04T10:00:00+00:00"),
        ("04/05/2020", "", "%d/%m/%Y", "UTC", "2020-05-04"),
        ("04/05/2020", "", "%m/%d/%Y", "UTC", "2020-04-05"),
        ("04/05/2020", "", "%Y-%m-%d", "UTC", None),
        ("05/06/2020", "16:00", "%d/%m/%Y", "UTC", "2020-06-05T16:00:00+00:00"),
        ("05/06/2020", "16:00", "%d/%m/%Y", "Asia/Tokyo", "2020-06-05T16:00:00+09:00"),
    ],
)
def test_makeDateTime(date, time, date_format, tzname, expected):
    assert transform.makeDateTime(date, time, date_format, tzname) == expected


@pytest.mark.parametrize(
    "date,option,date_format,expected",
    [
        ("", "year", None, None),
        (None, "year", None, None),
        ("2023-07-28", "blah", "%Y-%m-%d", None),
        ("2023-07-28", "year", "%Y-%m-%d", 2023),
        ("2023-07-28", "month", "%Y-%m-%d", 7),
        ("2023-07-28", "day", "%Y-%m-%d", 28),
        ("28/07/2023", "year", "%Y-%m-%d", None),
    ],
)
def test_splitDate(date, option, date_format, expected):
    assert transform.splitDate(date, option, date_format) == expected


@pytest.mark.parametrize(
    "date,duration,format,type,expected",
    [
        ("", 30, None, "years", None),
        (None, 30, None, "years", None),
        ("2023-07-28", "", "%Y-%m-%d", "years", None),
        ("2023-07-28", None, "%Y-%m-%d", "years", None),
        ("2023-07-28", 30, "%Y-%m-%d", "blah", None),
        ("2023-07-28", 30, "%Y-%m-%d", "years", 1993),
        ("2023-07-28", 8, "%Y-%m-%d", "months", 2022),
        ("2023-07-28", 20, "%Y-%m-%d", "days", 2023),
        ("28/07/2023", 30, "%Y-%m-%d", "years", None),
    ],
)
def test_startYear(date, duration, format, type, expected):
    assert transform.startYear(date, duration, format, type) == expected
