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
