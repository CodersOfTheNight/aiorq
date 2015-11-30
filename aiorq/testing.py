"""
    aiorq.testing
    ~~~~~~~~~~~~~

    This module implements testing facility for aiorq applications.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio
import gc

from aioredis import create_redis
from rq.local import release_local

from . import pop_connection, push_connection
from .connections import _connection_stack


@asyncio.coroutine
def find_connection(loop):
    """Get test redis connection."""

    return (yield from create_redis(('localhost', 6379), loop=loop))


def async_test(f):
    """Run asynchronous tests inside event loop as coroutines."""

    def wrapper():

        @asyncio.coroutine
        def coroutine(loop):
            redis = yield from find_connection(loop)
            push_connection(redis)
            try:
                yield from asyncio.coroutine(f)(redis=redis, loop=loop)
            finally:
                yield from redis.flushdb()
                connection = pop_connection()
                release_local(_connection_stack)
                assert connection == redis, (
                    'Wow, something really nasty happened to the '
                    'Redis connection stack. Check your setup.')

        assert not len(_connection_stack), \
            'Test require empty connection stack'
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        loop.run_until_complete(coroutine(loop))
        loop.stop()
        loop.run_forever()
        loop.close()
        gc.collect()
        asyncio.set_event_loop(None)

    return wrapper
