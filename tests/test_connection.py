from aiorq import Connection, Queue
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
