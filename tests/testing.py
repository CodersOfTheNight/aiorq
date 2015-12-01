import asyncio
import gc

from aioredis import create_redis
from rq.local import release_local

from aiorq import pop_connection, push_connection
from aiorq.connections import _connection_stack


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
            except Exception:
                raise
            else:
                connection = pop_connection()
                assert connection == redis, (
                    'Wow, something really nasty happened to the '
                    'Redis connection stack. Check your setup.')
            finally:
                yield from redis.flushdb()
                release_local(_connection_stack)

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
