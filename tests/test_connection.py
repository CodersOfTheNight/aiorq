import pytest

from aiorq import (Connection, use_connection, push_connection,
                   get_current_connection, Queue)
from aiorq.connections import _connection_stack
from aiorq.testing import async_test, find_connection


@async_test
def test_connection_detection(redis, **kwargs):
    """Automatic detection of the connection."""

    q = Queue()
    assert q.connection == redis


@async_test
def test_connection_stacking(redis, loop):
    """Connection stacking."""

    conn1 = yield from find_connection(loop)
    conn2 = yield from find_connection(loop)

    with (yield from Connection(conn1)):
        q1 = Queue()
        with (yield from Connection(conn2)):
            q2 = Queue()

    assert q1.connection != q2.connection
    assert q1.connection == conn1
    assert q2.connection == conn2


@async_test
def test_implicit_connection_stacking(redis, loop):
    """Connection stacking with implicit connection creation."""

    kwargs = dict(address=('localhost', 6379), loop=loop)
    with (yield from Connection(**kwargs)):
        q1 = Queue()
        with (yield from Connection(**kwargs)):
            q2 = Queue()

    assert q1.connection != q2.connection
    assert isinstance(q1.connection, type(redis))
    assert isinstance(q2.connection, type(redis))


@async_test
def test_use_connection(redis, loop):
    """Replace connection stack."""

    kwargs = dict(address=('localhost', 6379), loop=loop)
    yield from use_connection(**kwargs)
    assert get_current_connection() != redis
    push_connection(redis)      # Make test finalizer happy.


@async_test
def test_use_connection_explicit_redis(redis, loop):
    """Pass redis connection explicitly."""

    connection = yield from find_connection(loop)
    yield from use_connection(redis=connection)
    assert get_current_connection() == connection
    push_connection(redis)      # Make test finalizer happy.


@async_test
def test_use_connection_cleanup_stack(redis, loop):
    """Ensure connection stack cleanup."""

    kwargs = dict(address=('localhost', 6379), loop=loop)
    yield from use_connection(**kwargs)
    assert len(_connection_stack) == 1
    push_connection(redis)      # Make test finalizer happy.


@async_test
def test_connection_stacking_with_use_connection(redis, loop):
    """Disallow use of use_connection() together with stacked contexts."""

    kwargs = dict(address=('localhost', 6379), loop=loop)
    with pytest.raises(AssertionError):
        with (yield from Connection(**kwargs)):
            with (yield from Connection(**kwargs)):
                yield from use_connection(**kwargs)
