from datetime import datetime, timezone
from time import gmtime
from calendar import timegm
from typing import Optional, Union

"""
Note: this module assumes use of new (Python 3.9+) zoneinfo based timezones,
rather than pytz.
"""

Numeric = Union[int, float]


def utc_from_posix(n: Numeric) -> datetime:
    return datetime(*gmtime(n)[:6])


def utc_to_posix(dt: datetime) -> float:
    return timegm(dt.utctimetuple())


def from_posix(n: Numeric, tz: Optional[timezone] = None) -> datetime:
    dt = utc_from_posix(n)
    if tz is not None:
        return dt.replace(tzinfo=tz)
    return dt


def to_posix(dt: datetime, tz: Optional[timezone] = None) -> float:
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        if tz is not None:
            return to_posix(dt.replace(tzinfo=tz))
        else:
            raise ValueError(
                "You must specify a timezone, or the datetime you provide must be localized"
            )
    else:
        return utc_to_posix(dt)
