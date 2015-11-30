from aiorq import Queue
from aiorq.job import Job
from aiorq.testing import async_test


@async_test
def test_create_queue(**kwargs):
    """We can create queue instance."""

    q = Queue()
    assert q.name == 'default'


@async_test
def test_create_named_queue(**kwargs):
    """We can create named queue instance."""

    q = Queue('my-queue')
    assert q.name == 'my-queue'


@async_test
def test_equality(**kwargs):
    """Mathematical equality of queues."""

    q1 = Queue('foo')
    q2 = Queue('foo')
    q3 = Queue('bar')
    assert q1 == q2
    assert q2 == q1
    assert q1 != q3
    assert q2 != q3


@async_test
def test_empty_queue(redis, **kwargs):
    """Emptying queues."""

    q = Queue('example', connection=redis)
    yield from redis.rpush('rq:queue:example', 'foo')
    yield from redis.rpush('rq:queue:example', 'bar')
    assert not (yield from q.is_empty())
    yield from q.empty()
    assert (yield from q.is_empty())
    assert (yield from redis.lpop('rq:queue:example')) is None


@async_test
def test_empty_remove_jobs(redis, **kwargs):
    """Emptying a queue deletes the associated job objects."""

    q = Queue('example')
    job = yield from q.enqueue(lambda x: x)
    assert (yield from Job.exists(job.id))
    yield from q.empty()
    assert not (yield from Job.exists(job.id))
