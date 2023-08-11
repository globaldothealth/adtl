""" Functions which can be applied to source fields, allowing extensibility """

import logging
from typing import Any, Optional, List
from datetime import datetime, timedelta, date

try:
    import zoneinfo
except ImportError:  # pragma: no cover
    from backports import zoneinfo  # noqa

import pint
import re


def isNotNull(value: Optional[str]) -> bool:
    "Returns whether value is not null or an empty string"
    return value not in [None, ""]


def textIfNotNull(field: str, return_val: Any) -> Any:
    "Returns a default value if field is not null"
    return return_val if field not in [None, ""] else None


def wordSubstituteSet(value: str, *params) -> List[str]:
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
    return sorted(set(out)) if out else None


def getFloat(
    value: str, set_decimal: Optional[str] = None, separator: Optional[str] = None
):
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
        return value


def yearsElapsed(
    birthdate: str,
    currentdate: str,
    bd_format: str = "%Y-%m-%d",
    cd_format: str = "%Y-%m-%d",
):
    """Returns the number of years elapsed between two dates, useful for calculating ages

    Args:
        birthdate: Start date of duration
        currentdate: End date of duration
        bd_format: Date format for *birthdate* specified using :manpage:`strftime(3)` conventions.
            Defaults to ISO format ("%Y-%m-%d")
        cd_format: Date format for *currentdate* specified using :manpage:`strftime(3)` conventions.
            Defaults to ISO format ("%Y-%m-%d")

    Returns:
        int | None: Number of years elapsed or None if invalid dates were encountered
    """
    if birthdate in [None, ""] or currentdate in [None, ""]:
        return None

    bd = datetime.strptime(birthdate, bd_format)
    cd = datetime.strptime(currentdate, cd_format)

    days = cd - bd
    return pint.Quantity(days.days, "days").to("years").m


def durationDays(startdate: str, currentdate: str) -> int:
    """
    Returns the number of days between two dates.
    Preferable to Y-M-D elapsed, as month length is ambiguous -
    can be anywhere between 28-31 days.
    """
    if startdate in [None, ""] or currentdate in [None, ""]:
        return None

    bd = datetime.strptime(startdate, "%Y-%m-%d")
    cd = datetime.strptime(currentdate, "%Y-%m-%d")

    days = cd - bd
    return days.days


def startDate(enddate: str, duration: str) -> str:
    """
    Returns the start date in ISO format, given the end date and the duration.
    """
    if enddate in [None, ""] or duration in [None, ""]:
        return None

    ed = datetime.strptime(enddate, "%Y-%m-%d")

    sd = ed - timedelta(days=duration)

    return sd.strftime("%Y-%m-%d")


def endDate(startdate: str, duration: str) -> str:
    """
    Retuns the end date in ISO format, given the start date and the duration.
    """
    if startdate in [None, ""] or duration in [None, ""]:
        return None

    sd = datetime.strptime(startdate, "%Y-%m-%d")

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
        logging.error(
            f"Error in casting to integer: year={year}, month={month}, day={day}"
        )
        return None
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        logging.error(
            f"Could not construct date from: year={year}, month={month}, day={day}"
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
        logging.error(
            f"Could not convert date {date!r} from date format {date_format!r}"
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
        logging.error(
            f"Could not convert date {date!r} from date format {date_format!r}"
        )
        return None

    if time_24hr == "":
        return dt.date().isoformat()  # return date only

    tm = datetime.strptime(time_24hr, "%H:%M").time()

    return datetime.combine(dt, tm, tzinfo=zoneinfo.ZoneInfo(timezone)).isoformat()
