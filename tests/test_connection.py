from aiorq import Queue
from aiorq.testing import async_test


@async_test
def test_connection_detection(redis, **kwargs):
    """Automatic detection of the connection."""

    q = Queue()
    assert q.connection == redis
