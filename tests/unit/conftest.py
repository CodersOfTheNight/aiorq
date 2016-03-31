import asyncio
import functools
import gc

import aioredis
import pytest
import rq

from aiorq import pop_connection, push_connection
from aiorq.connections import _connection_stack
from aiorq.registry import StartedJobRegistry


# Fixtures.


@pytest.fixture
def redis():
    """Suppress pytest errors about missing fixtures.

    Actual redis connection will be inserted by asynchronous test
    decorator instead of this fixture.

    """

    pass


@pytest.fixture
def loop():
    """Suppress pytest errors about missing fixtures.

    Actual asyncio event loop will be inserted by asynchronous test
    decorator instead of this fixture.

    """

    pass


@pytest.fixture
def set_loop():
    """Suppress pytest errors about missing fixtures.

    Current event loop for asyncio will be set by asynchronous test
    decorator instead of this fixture.  This fixture serves for
    indication to preserve global event loop and keep it accessible
    for asyncio.sleep for example.

    """

    pass


@pytest.fixture
def registry():
    """Suppress pytest errors about missing fixtures.

    Actual StartedJobRegistry instance will be inserted by
    asynchronous test decorator instead of this fixture.

    """

    pass


@pytest.fixture
def timestamp():
    """Current timestamp."""

    return rq.utils.current_timestamp()


@pytest.fixture
def connect():
    """aioredis.Redis connection factory."""

    return find_connection


# Collector.


def pytest_pycollect_makeitem(collector, name, obj):

    # Collect generators as regular tests.
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        return list(collector._genfunctions(name, obj))


def pytest_pyfunc_call(pyfuncitem):

    funcargs = pyfuncitem.funcargs
    argnames = pyfuncitem._fixtureinfo.argnames
    testargs = {arg: funcargs[arg] for arg in argnames}
    async_test(pyfuncitem.obj)(**testargs)
    return True


# Asynchronous testing suite.


@asyncio.coroutine
def find_connection(loop):
    """Get test redis connection."""

    return (yield from aioredis.create_redis(('localhost', 6379), loop=loop))


def async_test(f):
    """Run asynchronous tests inside event loop as coroutines."""

    @functools.wraps(f)
    def wrapper(**kwargs):

        @asyncio.coroutine
        def coroutine(loop, kwargs):
            redis = yield from find_connection(loop)
            push_connection(redis)
            if 'redis' in kwargs:
                kwargs['redis'] = redis
            if 'loop' in kwargs:
                kwargs['loop'] = loop
            if 'registry' in kwargs:
                kwargs['registry'] = StartedJobRegistry(connection=redis)
            try:
                yield from asyncio.coroutine(f)(**kwargs)
            except Exception:
                raise
            else:
                connection = pop_connection()
                assert connection == redis, (
                    'Wow, something really nasty happened to the '
                    'Redis connection stack. Check your setup.')
            finally:
                yield from redis.flushdb()
                redis.close()
                yield from redis.wait_closed()
                rq.local.release_local(_connection_stack)

        assert not len(_connection_stack), \
            'Test require empty connection stack'
        loop = asyncio.new_event_loop()
        if 'set_loop' in kwargs:
            asyncio.set_event_loop(loop)
        else:
            asyncio.set_event_loop(None)
        loop.run_until_complete(coroutine(loop, kwargs))
        loop.stop()
        loop.run_forever()
        loop.close()
        gc.collect()
        asyncio.set_event_loop(None)

    return wrapper


# Testing `async_test` works properly.


class CatchMe(Exception):
    """Exception for testing purposes."""

    pass


@async_test
def f(**kwargs):
    """Asynchronous test wraps regular function."""

    raise CatchMe


@async_test
def g(**kwargs):
    """Asynchronous test wraps generator."""

    if False:
        yield
    raise CatchMe


try:
    f()
except CatchMe:
    pass
else:
    raise Exception('async_test does not works with regular function')


try:
    g()
except CatchMe:
    pass
else:
    raise Exception('async_test does not works with wrapped generator')
