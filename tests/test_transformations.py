import pytest

from datetime import datetime

import adtl.transformations as transform


@pytest.mark.parametrize(
    "test_input,expected", [("1", True), (None, False), ("", False)]
)
def test_isNotNull(test_input, expected):
    assert transform.isNotNull(test_input) == expected


@pytest.mark.parametrize(
    "test_date_birth, test_date_current, epoch, expected",
    [
        ("1996-02-22", "2023-02-22", 2022, 27.0),
        ("", "2023-02-22", 2022, None),
        (None, "2023-02-22", 2022, None),
    ],
)
def test_yearsElasped(test_date_birth, test_date_current, epoch, expected):
    assert transform.yearsElapsed(
        test_date_birth, test_date_current, epoch
    ) == pytest.approx(expected, 0.001)


@pytest.mark.parametrize(
    "test_date_birth, test_date_current, epoch, bd_format, cd_format, expected",
    [
        ("1950", "2023-01-01 00:00", 2022, "%Y", "%Y-%m-%d %H:%M", 73),
    ],
)
def test_yearsElasped_format(
    test_date_birth, test_date_current, epoch, bd_format, cd_format, expected
):
    assert transform.yearsElapsed(
        test_date_birth, test_date_current, epoch, bd_format, cd_format
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
    "date,option,epoch,date_format,expected",
    [
        ("", "year", 2022, None, None),
        (None, "year", 2022, None, None),
        ("2023-07-28", "blah", 2022, "%Y-%m-%d", None),
        ("2020-07-28", "year", 2022, "%Y-%m-%d", 2020),
        ("2023-07-28", "month", 2022, "%Y-%m-%d", 7),
        ("2023-07-28", "day", 2022, "%Y-%m-%d", 28),
        ("28/07/2023", "year", 2022, "%Y-%m-%d", None),
    ],
)
def test_splitDate(date, option, epoch, date_format, expected):
    assert transform.splitDate(date, option, epoch, date_format) == expected


@pytest.mark.parametrize(
    "date,duration,epoch,format,type,expected",
    [
        ("", 30, 2022, None, "years", None),
        (None, 30, 2022, None, "years", None),
        ("2023-07-28", "", 2022, "%Y-%m-%d", "years", None),
        ("2023-07-28", None, 2022, "%Y-%m-%d", "years", None),
        ("2023-07-28", 30, 2022, "%Y-%m-%d", "blah", None),
        ("2021-05-28", 30, 2022, "%Y-%m-%d", "years", 1991),
        ("2021-06-28", 8, 2022, "%Y-%m-%d", "months", 2020),
        ("2021-06-28", 8.5, 2022, "%Y-%m-%d", "months", 2020),
        ("2021-07-28", 20, 2022, "%Y-%m-%d", "days", 2021),
        ("28/08/2023", 30, 2022, "%Y-%m-%d", "years", None),
    ],
)
def test_startYear(date, duration, epoch, format, type, expected):
    assert transform.startYear(date, duration, epoch, format, type) == expected


@pytest.mark.parametrize(
    "date,duration,epoch,format,type,expected",
    [
        ("", 30, 2022, None, "months", None),
        (None, 30, 2022, None, "months", None),
        ("2023-07-28", "", 2022, "%Y-%m-%d", "months", None),
        ("2023-07-28", None, 2022, "%Y-%m-%d", "months", None),
        ("2023-07-28", 30, 2022, "%Y-%m-%d", "blah", None),
        ("2021-05-28", 3, 2022, "%Y-%m-%d", "months", 2),
        ("2021-06-28", 8.5, 2022, "%Y-%m-%d", "months", 10),
        ("2021-07-28", 20, 2022, "%Y-%m-%d", "days", 7),
        ("28/08/2023", 30, 2022, "%Y-%m-%d", "months", None),
    ],
)
def test_startMonth(date, duration, epoch, format, type, expected):
    assert transform.startMonth(date, duration, epoch, format, type) == expected


@pytest.mark.parametrize(
    "date,epoch,format,returntype,expected",
    [
        ("", 2022, None, False, None),
        (None, 2022, "%Y-%m-%d", True, None),
        ("01/01/24", 2022, "%Y-%m-%d", True, None),
        ("01/01/24", 2022, "%d/%m/%y", True, datetime(1924, 1, 1, 0, 0)),
        ("01/01/20", 2022, "%d/%m/%y", True, datetime(2020, 1, 1, 0, 0)),
        ("01/01/20", 2022, "%d/%m/%y", False, "2020-01-01"),
        ("01/01/2030", 2022, "%d/%m/%Y", False, "2030-01-01"),
    ],
)
def test_correctOldDate(date, epoch, format, returntype, expected):
    assert transform.correctOldDate(date, epoch, format, returntype) == expected
