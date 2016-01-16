import asyncio

from rq.compat import as_text
from rq.job import JobStatus
from rq.utils import utcnow

from aiorq import Worker, Queue, get_failed_queue
from aiorq.job import Job
from fixtures import (say_hello, div_by_zero, mock, touch_a_mock,
                      touch_a_mock_after_timeout)
from helpers import strip_microseconds


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


def test_work_and_quit(loop):
    """Worker processes work, then quits."""

    fooq, barq = Queue('foo'), Queue('bar')
    w = Worker([fooq, barq])
    assert not (yield from w.work(burst=True, loop=loop))

    yield from fooq.enqueue(say_hello, name='Frank')
    assert (yield from w.work(burst=True, loop=loop))


def test_worker_ttl(redis):
    """Worker ttl."""

    w = Worker([])
    yield from w.register_birth()
    [worker_key] = yield from redis.smembers(Worker.redis_workers_keys)
    assert (yield from redis.ttl(worker_key))
    yield from w.register_death()


def test_work_via_string_argument(loop):
    """Worker processes work fed via string arguments."""

    q = Queue('foo')
    w = Worker([q])
    job = yield from q.enqueue('fixtures.say_hello', name='Frank')
    assert (yield from w.work(burst=True, loop=loop))
    assert (yield from job.result) == 'Hi there, Frank!'


def test_job_times(loop):
    """Job times are set correctly."""

    q = Queue('foo')
    w = Worker([q])
    before = utcnow().replace(microsecond=0)
    job = yield from q.enqueue(say_hello)

    assert job.enqueued_at
    assert not job.started_at
    assert not job.ended_at
    assert (yield from w.work(burst=True, loop=loop))
    assert (yield from job.result) == 'Hi there, Stranger!'

    after = utcnow().replace(microsecond=0)
    yield from job.refresh()

    assert before <= job.enqueued_at <= after
    assert before <= job.started_at <= after
    assert before <= job.ended_at <= after


def test_work_is_unreadable(redis, loop):
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

    # NOTE: replacement and original strings must have the same length
    data = yield from redis.hget(job.key, 'data')
    invalid_data = data.replace(b'say_hello', b'fake_attr')
    assert data != invalid_data
    yield from redis.hset(job.key, 'data', invalid_data)

    # We use the low-level internal function to enqueue any data
    # (bypassing validity checks)
    yield from q.push_job_id(job.id)

    assert (yield from q.count) == 1

    # All set, we're going to process it
    w = Worker([q])
    yield from w.work(burst=True, loop=loop)  # Should silently pass
    assert (yield from q.count) == 0
    assert (yield from failed_q.count) == 1


def test_work_fails(loop):
    """Failing jobs are put on the failed queue."""

    q = Queue()
    failed_q = get_failed_queue()

    # Preconditions
    assert not (yield from failed_q.count)
    assert not (yield from q.count)

    # Action
    job = yield from q.enqueue(div_by_zero)
    assert (yield from q.count) == 1

    # keep for later
    enqueued_at_date = strip_microseconds(job.enqueued_at)

    w = Worker([q])
    yield from w.work(burst=True, loop=loop)  # Should silently pass

    # Postconditions
    assert not (yield from q.count)
    assert (yield from failed_q.count) == 1
    assert not (yield from w.get_current_job_id())

    # Check the job
    job = yield from Job.fetch(job.id)
    assert job.origin == q.name

    # Should be the original enqueued_at date, not the date of enqueueing
    # to the failed queue
    assert job.enqueued_at == enqueued_at_date
    assert job.exc_info  # should contain exc_info


def test_custom_exc_handling(loop):
    """Custom exception handling."""

    @asyncio.coroutine
    def black_hole(job, *exc_info):
        # Don't fall through to default behaviour (moving to failed
        # queue)
        return False

    q = Queue()
    failed_q = get_failed_queue()

    # Preconditions
    assert not (yield from failed_q.count)
    assert not (yield from q.count)

    # Action
    job = yield from q.enqueue(div_by_zero)
    assert (yield from q.count) == 1

    w = Worker([q], exception_handlers=black_hole)
    yield from w.work(burst=True, loop=loop)  # Should silently pass

    # Postconditions
    assert not (yield from q.count)
    assert not (yield from failed_q.count)

    # Check the job
    job = yield from Job.fetch(job.id)
    assert job.is_failed


def test_cancelled_jobs_arent_executed(redis, loop):
    """Cancelling jobs."""

    q = Queue()
    job = yield from q.enqueue(touch_a_mock)

    # Here, we cancel the job, so the sentinel file may not be created
    yield from redis.delete(job.key)

    w = Worker([q])
    yield from w.work(burst=True, loop=loop)
    assert not (yield from q.count)

    # Should not have created evidence of execution
    assert not mock.call_count
    mock.reset_mock()


def test_timeouts(set_loop):
    """Worker kills jobs after timeout."""

    q = Queue()
    w = Worker([q])

    # Put it on the queue with a timeout value
    res = yield from q.enqueue(
        touch_a_mock_after_timeout, args=(4,), timeout=1)

    assert not mock.call_count
    yield from w.work(burst=True)
    assert not mock.call_count

    # TODO: Having to do the manual refresh() here is really ugly!
    yield from res.refresh()
    assert 'JobTimeoutException' in as_text(res.exc_info)
    mock.reset_mock()


def test_worker_sets_result_ttl(redis, loop):
    """Ensure that Worker properly sets result_ttl for individual jobs."""

    q = Queue()
    job = yield from q.enqueue(say_hello, args=('Frank',), result_ttl=10)
    w = Worker([q])
    yield from w.work(burst=True, loop=loop)
    assert (yield from redis.ttl(job.key))

    # Job with -1 result_ttl don't expire
    job = yield from q.enqueue(say_hello, args=('Frank',), result_ttl=-1)
    w = Worker([q])
    yield from w.work(burst=True, loop=loop)
    assert (yield from redis.ttl(job.key)) == -1

    # Job with result_ttl = 0 gets deleted immediately
    job = yield from q.enqueue(say_hello, args=('Frank',), result_ttl=0)
    w = Worker([q])
    yield from w.work(burst=True, loop=loop)
    assert not (yield from redis.get(job.key))


def test_worker_sets_job_status(loop):
    """Ensure that worker correctly sets job status."""

    q = Queue()
    w = Worker([q])

    job = yield from q.enqueue(say_hello)
    assert (yield from job.get_status()) == JobStatus.QUEUED
    assert (yield from job.is_queued)
    assert not (yield from job.is_finished)
    assert not (yield from job.is_failed)

    yield from w.work(burst=True, loop=loop)
    job = yield from Job.fetch(job.id)
    assert (yield from job.get_status()) == JobStatus.FINISHED
    assert not (yield from job.is_queued)
    assert (yield from job.is_finished)
    assert not (yield from job.is_failed)

    # Failed jobs should set status to "failed"
    job = yield from q.enqueue(div_by_zero, args=(1,))
    yield from w.work(burst=True, loop=loop)
    job = yield from Job.fetch(job.id)
    assert (yield from job.get_status()) == JobStatus.FAILED
    assert not (yield from job.is_queued)
    assert not (yield from job.is_finished)
    assert (yield from job.is_failed)
