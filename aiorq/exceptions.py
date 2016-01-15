"""
    aiorq.exceptions
    ~~~~~~~~~~~~~~~~

    This module imports all rq exceptions so you can use it through our api.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.exceptions module written by Vincent
# Driessen and released under 2-clause BSD license.

from rq.exceptions import *     # noqa


class JobTimeoutException(Exception):
    """Error signify that coroutine is not finished in time."""

    pass
