import datetime


def month_bounds(year: int, month: int) -> tuple[datetime.datetime, datetime.datetime]:
    """Return (start, end) of a calendar month as naive datetimes; end is exclusive.

    Naive deliberately: TLC's own timestamp columns are tz-naive and the
    timezone is undocumented, so asserting one here would invent a fact.
    """
    start = datetime.datetime(year, month, 1)  # noqa: DTZ001
    end = (
        datetime.datetime(year + 1, 1, 1)  # noqa: DTZ001
        if month == 12
        else datetime.datetime(year, month + 1, 1)  # noqa: DTZ001
    )
    return start, end
