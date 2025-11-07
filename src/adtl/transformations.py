"""Functions which can be applied to source fields, allowing extensibility"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from math import floor
from typing import Any, Literal

from dateutil.relativedelta import relativedelta

try:
    import zoneinfo
except ImportError:  # pragma: no cover
    from backports import zoneinfo  # type: ignore

import re
import warnings

import pint


class AdtlTransformationWarning(UserWarning):
    pass


def isNotNull(value: str | None) -> bool:
    "Returns whether value is not null or an empty string"
    return value not in [None, ""]


def textIfNotNull(field: str, return_val: Any) -> Any:
    "Returns a default value if field is not null"
    return return_val if field not in [None, ""] else None


def wordSubstituteSet(value: str, *params) -> list[str]:
    """
    For a value that can have multiple words, use substitutions from params.

    Args:
        value: String containing a list of words that should be substituted
        params: List of 2-tuples, in the form
            [(w1, s1), (w2, s2), ... (w_n, s_n)]
            where w1 is replaced by s1, w2 is replaced by s2.

            Word matches are regular expressions, delimited by the `\b` word
            boundary delimiter so can have arbitrary regular expressions to
            match. Any match of regex w_n will use substitute s_n. Case is
            ignored when matching.

    Returns:
        List of words after finding matches and substituting.
        Duplicate words are only represented once.
    """
    out = []
    for i in params:
        if not isinstance(i, (tuple, list)) or not all(isinstance(s, str) for s in i):
            raise ValueError("wordSubstituteSet: params item not a tuple or list")
        sub_map = dict(params)
        for match, subst in sub_map.items():
            if re.search(r"\b" + match + r"\b", value, re.IGNORECASE):
                out.append(subst)
    if not out and (value not in [None, ""]):
        warnings.warn(
            f"No matches found for: '{value}'",
            AdtlTransformationWarning,
            stacklevel=2,
        )
    return sorted(set(out)) if out else None


def getFloat(value: str, set_decimal: str | None = None, separator: str | None = None):
    """Returns value transformed into a float.

    Args:
        value: Value to be transformed to float
        set_decimal: optional, set if decimal separator is not a
            full stop or period (.)
        separator: optional, set to the character used for separating
            thousands (such as `,`).
    """

    if not value:
        return None

    if '"' in value or " " in value:
        value = value.strip('"').replace(" ", "")

    if set_decimal:
        # handle comma decimal separator
        # partition always splits on last instance so copes if decimal == separator
        value_int, _, fraction = value.partition(set_decimal)
        value = value_int + "." + fraction

    if separator:
        if separator in value and separator != ".":
            value = value.replace(separator, "")
        elif separator in value_int:
            value = value_int.replace(separator, "") + "." + fraction

    values = [float(d) for d in re.findall(r"[-+]?\d*\.?\d+", value)]
    value = values[0] if len(values) == 1 else value

    try:
        return float(value)
    except ValueError:
        return value if value != "" else None


def Percentage(value: float):
    "transform a decimal into a percentage"
    try:
        value = float(value)
    except (ValueError, TypeError):
        return value

    if value > 1:
        return value

    return value * 100


def yearsElapsed(
    birthdate: str,
    currentdate: str,
    epoch: float,
    bd_format: str = "%Y-%m-%d",
    cd_format: str = "%Y-%m-%d",
):
    """
    Returns the number of years elapsed between two dates, useful for calculating ages

    Args:
        birthdate: Start date of duration
        currentdate: End date of duration
        epoch: Epoch year after which dates will be converted to the last century.
            As an example, if epoch is 2022, then the date 1/1/23 will be converted
            to the January 1, 1923.
        bd_format: Date format for *birthdate* specified using :manpage:`strftime(3)`
            conventions. Defaults to ISO format ("%Y-%m-%d")
        cd_format: Date format for *currentdate* specified using :manpage:`strftime(3)`
            conventions. Defaults to ISO format ("%Y-%m-%d")

    Returns:
        int | None: Number of years elapsed or None if invalid dates were encountered
    """

    if birthdate in [None, ""] or currentdate in [None, ""]:
        return None

    bd = correctOldDate(birthdate, epoch, bd_format, return_datetime=True)

    if bd is None:
        return None

    cd = datetime.strptime(currentdate, cd_format)

    try:
        days = cd - bd
        return pint.Quantity(days.days, "days").to("years").m
    except ValueError:
        warnings.warn(
            f"Failed calculation yearsElapsed: {birthdate}, {currentdate}",
            AdtlTransformationWarning,
            stacklevel=2,
        )


def durationDays(startdate: str, currentdate: str, format: str = "%Y-%m-%d") -> int:
    """
    Returns the number of days between two dates.
    Preferable to Y-M-D elapsed, as month length is ambiguous -
    can be anywhere between 28-31 days.
    """
    if startdate in [None, ""] or currentdate in [None, ""]:
        return None

    bd = datetime.strptime(startdate, format)
    cd = datetime.strptime(currentdate, format)

    days = cd - bd
    return days.days


def startDate(enddate: str, duration: str) -> str:
    """
    Returns the start date in ISO format, given the end date and the duration.
    """
    if enddate in [None, ""] or duration in [None, ""]:
        return None

    ed = datetime.strptime(enddate, "%Y-%m-%d")

    sd = ed - timedelta(days=float(duration))

    return sd.strftime("%Y-%m-%d")


def endDate(startdate: str, duration: str, format="%Y-%m-%d"):
    """
    Returns the end date in ISO format, given the start date and the duration.

    Args:
        startdate: Start date
        duration: Duration in days
        format: :manpage:`strftime(3)` format that dates are in

    Returns:
        End date in the specified format
    """
    if startdate in [None, ""] or duration in [None, ""]:
        return None

    sd = datetime.strptime(startdate, format)

    duration = float(duration)

    ed = sd + timedelta(days=duration)

    return ed.strftime("%Y-%m-%d")


def makeDate(year: str, month: str, day: str) -> str:
    "Returns a date from components specified as year, month and day"
    if year in [None, ""] or month in [None, ""] or day in [None, ""]:
        return None
    try:
        year, month, day = int(year), int(month), int(day)
    except ValueError:
        warnings.warn(
            f"Could not construct date from: year={year}, month={month}, day={day}",
            AdtlTransformationWarning,
            stacklevel=2,
        )
        return None
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        warnings.warn(
            f"Could not construct date from: year={year}, month={month}, day={day}",
            AdtlTransformationWarning,
            stacklevel=2,
        )
        return None


def makeDateTimeFromSeconds(
    date: str, time_seconds: int, date_format: str, timezone: str
) -> datetime:
    """Returns a datetime from date and time specified in
    elapsed seconds since the beginning of the day

    Args:
        date: Date to be converted
        time_seconds: Elapsed time in seconds within that day (0 - 86399)
        date_format: Date format in :manpage:`strftime(3)` format
        timezone: Timezone to use, specified in tzdata format

    Returns:
        datetime.datetime: A timezone aware datetime object
    """
    if date == "":
        return None
    try:
        t = datetime.strptime(date, date_format).replace(
            tzinfo=zoneinfo.ZoneInfo(timezone)
        )
    except ValueError:
        warnings.warn(
            f"Could not convert date {date!r} from date format {date_format!r}",
            AdtlTransformationWarning,
            stacklevel=2,
        )
        return None
    if time_seconds == "":
        return t.date().isoformat()  # return date only
    time_seconds = int(time_seconds)
    hour = time_seconds // 3600
    minute = (time_seconds % 3600) // 60
    return t.replace(hour=hour, minute=minute).isoformat()


def makeDateTime(
    date: str, time_24hr: str, date_format: str, timezone: str
) -> datetime:
    """Returns a combined date and time

    Args:
        date: Date to be converted
        time_24hr: Time specified in HH:MM format
        date_format: Date format in :manpage:`strftime(3)` format
        timezone: Timezone to use, specified in tzdata format

    Returns:
        datetime.datetime: A timezone aware datetime object
    """
    if date == "":
        return None

    try:
        dt = datetime.strptime(date, date_format).replace(
            tzinfo=zoneinfo.ZoneInfo(timezone)
        )
    except ValueError:
        warnings.warn(
            f"Could not convert date {date!r} from date format {date_format!r}",
            AdtlTransformationWarning,
            stacklevel=2,
        )
        return None

    if time_24hr == "":
        return dt.date().isoformat()  # return date only

    tm = datetime.strptime(time_24hr, "%H:%M").time()

    return datetime.combine(dt, tm, tzinfo=zoneinfo.ZoneInfo(timezone)).isoformat()


def splitDate(
    date: str,
    option: Literal["year", "month", "day"],
    epoch: float,
    format: str = "%Y-%m-%d",
):
    "Splits a date into year, month, day"

    if date in [None, ""]:
        return None

    sd = correctOldDate(date, epoch, format, return_datetime=True)

    if sd is None:
        return None

    if option == "year":
        return sd.year
    elif option == "month":
        return sd.month
    elif option == "day":
        return sd.day
    else:
        warnings.warn(
            f"Invalid option {option!r} for splitDate",
            AdtlTransformationWarning,
            stacklevel=2,
        )
        return None


def startYear(
    duration: str | float,
    currentdate: list | str,
    epoch: float,
    dateformat: str = "%Y-%m-%d",
    duration_type: Literal["years", "months", "days"] = "years",
    provide_month_day: bool | list = False,
) -> int | float:
    """
    Use to calculate year e.g. of birth from date (e.g. current date) and
    duration (e.g. age)

    The date can be provided as a list of possible dates (if a hierarchy needs
    searching through)

    Args:
        duration: Duration value
        currentdate: Date to offset duration from
        epoch: Epoch year to use for conversion of two digit years. Any dates
            after the epoch are converted to the last century
        dateformat: Date format that currentdate is in
        duration_type: One of 'years', 'months' or 'days'
        provide_month_day: If currentdate is only year, and this is specified
            as a tuple of (month, day), uses these to construct the date

    Returns:
        Starting year, offset by duration
    """
    if isinstance(currentdate, list):
        # find the first non nan instance, else return None
        currentdate = next((s for s in currentdate if s), None)

    if currentdate in [None, ""] or duration in [None, ""]:
        return None

    if provide_month_day:
        cd = makeDate(currentdate, provide_month_day[0], provide_month_day[1])
        cd = datetime.strptime(cd, "%Y-%m-%d")
    else:
        cd = correctOldDate(currentdate, epoch, dateformat, return_datetime=True)

    if cd is None:
        return None

    if duration_type == "years":
        return cd.year - floor(float(duration))

    elif duration_type == "months":
        new_date = cd - relativedelta(months=floor(float(duration)))  # rounds down
        return new_date.year

    elif duration_type == "days":
        new_date = cd - timedelta(days=float(duration))
        return new_date.year


def startMonth(
    duration: str | float,
    currentdate: list | str,
    epoch: float,
    dateformat: str = "%Y-%m-%d",
    duration_type: Literal["years", "months", "days"] = "years",
    provide_month_day: bool | list = False,
):
    """
    Use to calculate month e.g. of birth from date (e.g. current date) and
    duration (e.g. age), parameter descriptions are same as
    :meth:`adtl.transformations.startYear`, except this function
    returns the month component
    """
    if isinstance(currentdate, list):
        # find the first non nan instance, else return None
        currentdate = next((s for s in currentdate if s), None)

    if currentdate in [None, ""] or duration in [None, ""]:
        return None

    if provide_month_day:
        cd = makeDate(currentdate, provide_month_day[0], provide_month_day[1])
        cd = datetime.strptime(cd, "%Y-%m-%d")
    else:
        cd = correctOldDate(currentdate, epoch, dateformat, return_datetime=True)

    if cd is None:
        return None

    if duration_type == "months":
        new_date = cd - relativedelta(months=floor(float(duration)))
        return new_date.month

    elif duration_type == "days":
        new_date = cd - timedelta(days=float(duration))
        return new_date.month


def correctOldDate(date: str, epoch: float, format: str, return_datetime: bool = False):
    """
    Fixes dates so that they are the correct century for when the year
    is not fully specified. The time module converts 2 digit dates by
    mapping values 69-99 to 1969-1999, and values 0-68 are mapped to
    2000-2068. This doesn't work for e.g. birthdates here where they are
    frequently below the cutoff.

    Switches the pivot point to that set by epoch so that epoch+ converts to 19xx.

    Only use for birth dates to avoid unintentional conversion for recent
    dates.

    Args:
        date: Date to convert
        epoch: Epoch as year
        format: :manpage:`strftime(3)` format that date is in
        return_datetime: Whether to return date in a datetime.datetime format
            (when True), or a string (when False, default)

    Returns:
        str | datetime.datetime: Fixed date, return type depends on return_datetime
    """

    if date in [None, ""]:
        return None

    try:
        cd = datetime.strptime(date, format)
    except ValueError:
        warnings.warn(
            f"Could not convert date {date!r} from date format {format!r}",
            AdtlTransformationWarning,
            stacklevel=2,
        )
        return None

    if cd.year >= epoch and "y" in format:
        cd = cd.replace(year=cd.year - 100)

    if return_datetime:
        return cd
    else:
        return cd.strftime("%Y-%m-%d")
