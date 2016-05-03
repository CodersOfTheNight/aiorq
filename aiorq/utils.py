"""
    aiorq.utils
    ~~~~~~~~~~~

    Utility functions.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

from calendar import timegm
from datetime import datetime


def current_timestamp():
    """Current UTC timestamp."""

    return timegm(datetime.utcnow().utctimetuple())


def utcparse(byte):
    return datetime.strptime(byte.decode(), '%Y-%m-%dT%H:%M:%SZ')


def utcformat(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ').encode()


def utcnow():
    return datetime.utcnow()
