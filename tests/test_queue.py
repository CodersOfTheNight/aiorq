import pytest

from aiorq import Queue
from aiorq.job import Job
from fixtures import say_hello
from testing import async_test


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


@async_test
def test_queue_is_empty(redis, **kwargs):
    """Detecting empty queues."""

    q = Queue('example')
    assert (yield from q.is_empty())
    yield from redis.rpush('rq:queue:example', 'sentinel message')
    assert not (yield from q.is_empty())


@async_test
def test_remove(**kwargs):
    """Ensure queue.remove properly removes Job from queue."""

    q = Queue('example')
    job = yield from q.enqueue(say_hello)
    assert job.id in (yield from q.job_ids)
    yield from q.remove(job)
    assert job.id not in (yield from q.job_ids)

    job = yield from q.enqueue(say_hello)
    assert job.id in (yield from q.job_ids)
    yield from q.remove(job.id)
    assert job.id not in (yield from q.job_ids)


@async_test
def test_jobs(**kwargs):
    """Getting jobs out of a queue."""

    q = Queue('example')
    assert not (yield from q.jobs)
    job = yield from q.enqueue(say_hello)
    assert (yield from q.jobs) == [job]

    # Deleting job removes it from queue
    yield from job.delete()
    assert not (yield from q.job_ids)


@async_test
def test_compact(redis, **kwargs):
    """Queue.compact() removes non-existing jobs."""

    q = Queue()

    yield from q.enqueue(say_hello, 'Alice')
    yield from q.enqueue(say_hello, 'Charlie')
    yield from redis.lpush(q.key, '1', '2')

    assert (yield from q.count) == 4
    yield from q.compact()
    assert (yield from q.count) == 2

    with pytest.raises(RuntimeError):
        len(q)
