""" Functions which can be applied to source fields, allowing extensibility """

import logging
from datetime import datetime, timedelta, date

from dateutil.relativedelta import relativedelta
from math import floor

try:
    import zoneinfo
except ImportError:  # pragma: no cover
    from backports import zoneinfo  # noqa

import pint
import re

from typing import Literal


def isNotNull(value):
    return value not in [None, ""]


def textIfNotNull(field, return_val):
    return return_val if field not in [None, ""] else None


def getFloat(value, set_decimal=None, separator=None):
    """
    In cases where the decimal seperators is not a . you can
    use set_decimal. Similarly, if thousand seperators are
    used they can be specified.
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


def yearsElapsed(birthdate, currentdate, bd_format="%Y-%m-%d", cd_format="%Y-%m-%d"):
    if birthdate in [None, ""] or currentdate in [None, ""]:
        return None

    bd = correctOldDate(birthdate, bd_format, return_datetime=True)
    # bd = datetime.strptime(birthdate, bd_format)
    cd = datetime.strptime(currentdate, cd_format)

    try:
        days = cd - bd
        return pint.Quantity(days.days, "days").to("years").m
    except TypeError:
        return None


def durationDays(symptomstartdate, currentdate):
    """
    Returns the number of days between two dates.
    Preferable to Y-M-D elapsed, as month length is ambiguous -
    can be anywhere between 28-31 days.
    """
    if symptomstartdate in [None, ""] or currentdate in [None, ""]:
        return None

    bd = datetime.strptime(symptomstartdate, "%Y-%m-%d")
    cd = datetime.strptime(currentdate, "%Y-%m-%d")

    days = cd - bd
    return days.days


def startDate(enddate, duration):
    """
    Retuns the start date in ISO format, given the end date and the duration.
    """
    if enddate in [None, ""] or duration in [None, ""]:
        return None

    ed = datetime.strptime(enddate, "%Y-%m-%d")

    sd = ed - timedelta(days=float(duration))

    return sd.strftime("%Y-%m-%d")


def endDate(startdate, duration):
    """
    Retuns the end date in ISO format, given the start date and the duration.
    """
    if startdate in [None, ""] or duration in [None, ""]:
        return None

    sd = datetime.strptime(startdate, "%Y-%m-%d")

    duration = float(duration)

    ed = sd + timedelta(days=duration)

    return ed.strftime("%Y-%m-%d")


def makeDate(year, month, day):
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


def makeDateTimeFromSeconds(date, time_seconds, date_format, tzname):
    if date == "":
        return None
    try:
        t = datetime.strptime(date, date_format).replace(
            tzinfo=zoneinfo.ZoneInfo(tzname)
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


def makeDateTime(date, time_24hr, date_format, timezone):
    """Combine date and time fields into one"""
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


def splitDate(date, option: Literal["year", "month", "day"], format="%Y-%m-%d"):
    "Splits a date into year, month, day"

    if date in [None, ""]:
        return None

    sd = correctOldDate(date, format, return_datetime=True)

    if sd is None:
        return None

    if option == "year":
        return sd.year
    elif option == "month":
        return sd.month
    elif option == "day":
        return sd.day
    else:
        return None


def startYear(currentdate, duration, dateformat="%Y-%m-%d", duration_type="years"):
    """
    Use to calculate year e.g. of birth from date (e.g. current date) and
    duration (e.g. age)
    """
    if currentdate in [None, ""] or duration in [None, ""]:
        return None

    cd = correctOldDate(currentdate, dateformat, return_datetime=True)

    if cd is None:
        return None

    if duration_type == "years":
        return cd.year - float(duration)

    elif duration_type == "months":
        new_date = cd - relativedelta(months=floor(float(duration)))  # rounds down
        return new_date.year

    elif duration_type == "days":
        new_date = cd - timedelta(days=float(duration))
        return new_date.year


def startMonth(currentdate, duration, dateformat="%Y-%m-%d", duration_type="months"):
    """
    Use to calculate month e.g. of birth from date (e.g. current date) and
    duration (e.g. age)
    """
    if currentdate in [None, ""] or duration in [None, ""]:
        return None

    cd = correctOldDate(currentdate, dateformat, return_datetime=True)

    if cd is None:
        return None

    if duration_type == "months":
        new_date = cd - relativedelta(months=floor(float(duration)))
        return new_date.month

    elif duration_type == "days":
        new_date = cd - timedelta(days=float(duration))
        return new_date.month


def correctOldDate(date, format, return_datetime=False):
    """
    the time module converts 2 digit dates as:
    values 69-99 are mapped to 1969-1999, and values 0-68 are mapped to
    2000-2068. This doesn't work for e.g. birthdates here where they are
    frequently below the cutoff.

    Switches the pivot point so that 22+ converts to 19xx.

    Only use for birth dates to avoid unintentional conversion for recent
    dates.
    """

    if date in [None, ""]:
        return None

    try:
        cd = datetime.strptime(date, format)
    except ValueError:
        logging.error(f"Could not convert date {date!r} from date format {format!r}")
        return None

    if cd.year >= 2022 and "y" in format:
        cd = cd.replace(year=cd.year - 100)

    if return_datetime:
        return cd
    else:
        return cd.strftime("%Y-%m-%d")
