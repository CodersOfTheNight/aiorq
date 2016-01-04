from aiorq import Connection, Queue
from testing import find_connection


def test_connection_detection(redis):
    """Automatic detection of the connection."""

    q = Queue()
    assert q.connection == redis


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
