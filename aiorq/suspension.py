"""
    aiorq.suspension
    ~~~~~~~~~~~~~~~~

    This module contain helper function for workers suspension.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.suspension module written by Vincent
# Driessen and released under 2-clause BSD license.

import asyncio

from rq.suspension import WORKERS_SUSPENDED


@asyncio.coroutine
def is_suspended(connection):
    """Check if rq workers are suspended."""

    return (yield from connection.exists(WORKERS_SUSPENDED))
