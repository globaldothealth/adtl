""" Functions which can be applied to source fields, allowing extensibility """

from datetime import datetime
import pint


def isNotNull(value):
    return value not in [None, ""]


def yearsElapsed(birthdate, currentdate):
    if birthdate in [None, ""] or currentdate in [None, ""]:
        return None

    bd = datetime.strptime(birthdate, "%Y-%m-%d")
    cd = datetime.strptime(currentdate, "%Y-%m-%d")

    days = cd - bd
    return pint.Quantity(days.days, "days").to("years").m


def durationPeriod(symptomstartdate, currentdate):
    """Returns an ISO time interval (start-Date/end-Date)."""
    if symptomstartdate in [None, ""] or currentdate in [None, ""]:
        return None

    sd = datetime.strptime(symptomstartdate, "%Y-%m-%d")
    sd = sd.strftime("%Y-%m-%d")
    cd = datetime.strptime(currentdate, "%Y-%m-%d")
    cd = cd.strftime("%Y-%m-%d")

    return sd + "/" + cd


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
