"""Inclusive day-range helpers.

Dates are turned into timestamp strings explicitly, so DATE vs TIMESTAMP implicit
casts behave the same on every Spark version.
"""

from datetime import date

from pyspark.sql import Column
from pyspark.sql.functions import lit, to_timestamp


def day_start(d: date) -> Column:
    """d at 00:00:00."""
    return to_timestamp(lit(d.isoformat()))


def day_end(d: date) -> Column:
    """d at 23:59:59 (inclusive end of day)."""
    return to_timestamp(lit(f"{d.isoformat()} 23:59:59"))


def between_days(column: Column, start: date, end: date) -> Column:
    """start 00:00:00 <= column <= end 23:59:59."""
    return (column >= day_start(start)) & (column <= day_end(end))
