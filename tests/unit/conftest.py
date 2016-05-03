import asyncio
import functools
import gc

import aioredis
import pytest

import aiorq


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
def timestamp():
    """Current timestamp."""

    return aiorq.utils.current_timestamp()


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
            if 'redis' in kwargs:
                kwargs['redis'] = redis
            if 'loop' in kwargs:
                kwargs['loop'] = loop
            try:
                yield from asyncio.coroutine(f)(**kwargs)
            except Exception:
                raise
            finally:
                yield from redis.flushdb()
                redis.close()
                yield from redis.wait_closed()

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
