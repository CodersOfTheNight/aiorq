from aiorq import Queue
from aiorq.testing import async_test


@async_test
def test_create_queue(*args):
    """We can create queue instance."""

    q = Queue()
    assert q.name == 'default'


@async_test
def test_create_named_queue(*args):
    """We can create named queue instance."""

    q = Queue('my-queue')
    assert q.name == 'my-queue'


@async_test
def test_equality(*args):
    """Mathematical equality of queues."""

    q1 = Queue('foo')
    q2 = Queue('foo')
    q3 = Queue('bar')
    assert q1 == q2
    assert q2 == q1
    assert q1 != q3
    assert q2 != q3


@async_test
def test_empty_queue(redis):
    """Emptying queues."""

    q = Queue('example', connection=redis)
    yield from redis.rpush('rq:queue:example', 'foo')
    yield from redis.rpush('rq:queue:example', 'bar')
    assert (yield from q.is_empty()) == False
    yield from q.empty()
    assert (yield from q.is_empty()) == True
    assert (yield from redis.lpop('rq:queue:example')) is None
