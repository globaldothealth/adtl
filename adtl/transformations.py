""" Functions which can be applied to source fields, allowing extensibility """

from datetime import datetime
import pint


def isNotNull(value):
    True if value != None and value != "" else False


def yearsElapsed(birthdate, currentdate):
    if birthdate == (None or "") or currentdate == (None or ""):
        return None

    bd = datetime.strptime(birthdate, "%Y-%m-%d")
    cd = datetime.strptime(currentdate, "%Y-%m-%d")

    days = cd - bd
    return pint.Quantity(days.days, "days").to("years").m
