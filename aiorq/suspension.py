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


@asyncio.coroutine
def suspend(connection, ttl=None):
    """Suspend RQ workers execution.

    :param connection: aioredis.Redis connection instance
    :param ttl: time to live in seconds, default is no expiration

    If you pass in 0 for ``ttl`` value it will invalidate right away.
    """

    yield from connection.set(WORKERS_SUSPENDED, 1)
    if ttl is not None:
        yield from connection.expire(WORKERS_SUSPENDED, ttl)


@asyncio.coroutine
def resume(connection):
    """Resume RQ workers execution."""

    return (yield from connection.delete(WORKERS_SUSPENDED))
