from aiorq import Connection, Queue


def test_connection_detection(redis):
    """Automatic detection of the connection."""

    q = Queue()
    assert q.connection == redis


def test_connection_stacking(redis, loop, connect):
    """Connection stacking."""

    conn1 = yield from connect(loop)
    conn2 = yield from connect(loop)

    with Connection(conn1):
        q1 = Queue()
        with Connection(conn2):
            q2 = Queue()

    assert q1.connection != q2.connection
    assert q1.connection == conn1
    assert q2.connection == conn2