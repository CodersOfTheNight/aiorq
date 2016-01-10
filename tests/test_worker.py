import pytest
from rq.utils import utcnow

from aiorq import Worker, Queue, get_failed_queue
from aiorq.job import Job
from fixtures import say_hello


def test_create_worker():
    """Worker creation using various inputs."""

    # With single string argument
    w = Worker('foo')
    assert w.queues[0].name == 'foo'

    # With list of strings
    w = Worker(['foo', 'bar'])
    assert w.queues[0].name == 'foo'
    assert w.queues[1].name == 'bar'

    # With iterable of strings
    w = Worker(iter(['foo', 'bar']))
    assert w.queues[0].name == 'foo'
    assert w.queues[1].name == 'bar'

    # With single Queue
    w = Worker(Queue('foo'))
    assert w.queues[0].name == 'foo'

    # With iterable of Queues
    w = Worker(iter([Queue('foo'), Queue('bar')]))
    assert w.queues[0].name == 'foo'
    assert w.queues[1].name == 'bar'

    # With list of Queues
    w = Worker([Queue('foo'), Queue('bar')])
    assert w.queues[0].name == 'foo'
    assert w.queues[1].name == 'bar'


def test_work_and_quit():
    """Worker processes work, then quits."""

    fooq, barq = Queue('foo'), Queue('bar')
    w = Worker([fooq, barq])
    assert not (yield from w.work(burst=True))

    yield from fooq.enqueue(say_hello, name='Frank')
    assert (yield from w.work(burst=True))


def test_worker_ttl(redis):
    """Worker ttl."""

    w = Worker([])
    yield from w.register_birth()
    [worker_key] = yield from redis.smembers(Worker.redis_workers_keys)
    assert (yield from redis.ttl(worker_key))
    yield from w.register_death()


def test_work_via_string_argument():
    """Worker processes work fed via string arguments."""

    q = Queue('foo')
    w = Worker([q])
    job = yield from q.enqueue('fixtures.say_hello', name='Frank')
    assert (yield from w.work(burst=True))
    assert (yield from job.result) == 'Hi there, Frank!'


def test_job_times():
    """Job times are set correctly."""

    q = Queue('foo')
    w = Worker([q])
    before = utcnow().replace(microsecond=0)
    job = yield from q.enqueue(say_hello)

    assert job.enqueued_at
    assert not job.started_at
    assert not job.ended_at
    assert (yield from w.work(burst=True))
    assert (yield from job.result) == 'Hi there, Stranger!'

    after = utcnow().replace(microsecond=0)
    yield from job.refresh()

    assert before <= job.enqueued_at <= after
    assert before <= job.started_at <= after
    assert before <= job.ended_at <= after


@pytest.mark.xfail
def test_work_is_unreadable(redis):
    """Unreadable jobs are put on the failed queue."""

    q = Queue()
    failed_q = get_failed_queue()

    assert (yield from failed_q.count) == 0
    assert (yield from q.count) == 0

    # NOTE: We have to fake this enqueueing for this test case.
    # What we're simulating here is a call to a function that is not
    # importable from the worker process.
    job = Job.create(func=say_hello, args=(3,))
    yield from job.save()
    data = yield from redis.hget(job.key, 'data')
    invalid_data = data.replace(b'say_hello', b'nonexisting')
    assert data != invalid_data
    yield from redis.hset(job.key, 'data', invalid_data)

    # We use the low-level internal function to enqueue any data
    # (bypassing validity checks)
    yield from q.push_job_id(job.id)

    assert (yield from q.count) == 1

    # All set, we're going to process it
    w = Worker([q])
    yield from w.work(burst=True)   # should silently pass
    assert (yield from q.count) == 0
    assert (yield from failed_q.count) == 1
