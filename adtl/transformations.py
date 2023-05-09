""" Functions which can be applied to source fields, allowing extensibility """

import logging
from datetime import datetime, timedelta, date
import pint


def isNotNull(value):
    return value not in [None, ""]


def getFloat(value):
    if not value:
        return None

    if '"' in value or " " in value:
        value = value.strip('"').replace(" ", "")

    try:
        return float(value)
    except ValueError:
        return value


def yearsElapsed(birthdate, currentdate):
    if birthdate in [None, ""] or currentdate in [None, ""]:
        return None

    bd = datetime.strptime(birthdate, "%Y-%m-%d")
    cd = datetime.strptime(currentdate, "%Y-%m-%d")

    days = cd - bd
    return pint.Quantity(days.days, "days").to("years").m


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

    sd = ed - timedelta(days=duration)

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
