import pytest

from aiorq import Queue
from aiorq.job import Job
from fixtures import say_hello, Number


def test_create_queue():
    """We can create queue instance."""

    q = Queue()
    assert q.name == 'default'


def test_create_named_queue():
    """We can create named queue instance."""

    q = Queue('my-queue')
    assert q.name == 'my-queue'


def test_equality():
    """Mathematical equality of queues."""

    q1 = Queue('foo')
    q2 = Queue('foo')
    q3 = Queue('bar')
    assert q1 == q2
    assert q2 == q1
    assert q1 != q3
    assert q2 != q3


def test_empty_queue(redis):
    """Emptying queues."""

    q = Queue('example', connection=redis)
    yield from redis.rpush('rq:queue:example', 'foo')
    yield from redis.rpush('rq:queue:example', 'bar')
    assert not (yield from q.is_empty())
    yield from q.empty()
    assert (yield from q.is_empty())
    assert (yield from redis.lpop('rq:queue:example')) is None


def test_empty_remove_jobs(redis):
    """Emptying a queue deletes the associated job objects."""

    q = Queue('example')
    job = yield from q.enqueue(lambda x: x)
    assert (yield from Job.exists(job.id))
    yield from q.empty()
    assert not (yield from Job.exists(job.id))


def test_queue_is_empty(redis):
    """Detecting empty queues."""

    q = Queue('example')
    assert (yield from q.is_empty())
    yield from redis.rpush('rq:queue:example', 'sentinel message')
    assert not (yield from q.is_empty())


def test_remove():
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


def test_jobs():
    """Getting jobs out of a queue."""

    q = Queue('example')
    assert not (yield from q.jobs)
    job = yield from q.enqueue(say_hello)
    assert (yield from q.jobs) == [job]

    # Deleting job removes it from queue
    yield from job.delete()
    assert not (yield from q.job_ids)


def test_compact(redis):
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


def test_enqueue(redis):
    """Enqueueing job onto queues."""

    q = Queue()
    assert (yield from q.is_empty())

    # say_hello spec holds which queue this is sent to
    job = yield from q.enqueue(say_hello, 'Nick', foo='bar')
    job_id = job.id
    assert job.origin == q.name

    # Inspect data inside Redis
    q_key = 'rq:queue:default'
    assert 1 == (yield from redis.llen(q_key))
    assert job_id == (yield from redis.lrange(q_key, 0, -1))[0].decode('ascii')


def test_enqueue_sets_metadata():
    """Enqueueing job onto queues modifies meta data."""

    q = Queue()
    job = Job.create(func=say_hello, args=('Nick',), kwargs=dict(foo='bar'))

    # Preconditions
    assert not job.enqueued_at

    # Action
    yield from q.enqueue_job(job)

    # Postconditions
    assert job.enqueued_at


def test_pop_job_id():
    """Popping job IDs from queues."""

    # Set up
    q = Queue()
    uuid = '112188ae-4e9d-4a5b-a5b3-f26f2cb054da'
    yield from q.push_job_id(uuid)

    # Pop it off the queue...
    assert (yield from q.count)
    assert (yield from q.pop_job_id()) == uuid

    # ...and assert the queue count when down
    assert not (yield from q.count)


def test_dequeue():
    """Dequeueing jobs from queues."""

    # Set up
    q = Queue()
    result = yield from q.enqueue(say_hello, 'Rick', foo='bar')

    # Dequeue a job (not a job ID) off the queue
    assert (yield from q.count)
    job = yield from q.dequeue()
    assert job.id == result.id
    assert job.func == say_hello
    assert job.origin == q.name
    assert job.args[0] == 'Rick'
    assert job.kwargs['foo'] == 'bar'

    # ...and assert the queue count when down
    assert not (yield from q.count)


def test_dequeue_deleted_jobs():
    """Dequeueing deleted jobs from queues don't blow the stack."""

    q = Queue()
    for _ in range(1, 1000):
        job = yield from q.enqueue(say_hello)
        yield from job.delete()
    yield from q.dequeue()


def test_dequeue_instance_method():
    """Dequeueing instance method jobs from queues."""

    q = Queue()
    n = Number(2)
    yield from q.enqueue(n.div, 4)

    job = yield from q.dequeue()

    # The instance has been pickled and unpickled, so it is now a
    # separate object. Test for equality using each object's __dict__
    # instead.
    assert job.instance.__dict__ == n.__dict__
    assert job.func.__name__ == 'div'
    assert job.args == (4,)
