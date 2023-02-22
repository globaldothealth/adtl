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
