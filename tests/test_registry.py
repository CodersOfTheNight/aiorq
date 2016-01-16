import pytest
from rq.compat import as_text
from rq.job import JobStatus

from aiorq import Worker, Queue
from aiorq.job import Job
from aiorq.queue import FailedQueue
from aiorq.registry import (clean_registries, FinishedJobRegistry,
                            StartedJobRegistry, DeferredJobRegistry)
from fixtures import say_hello, div_by_zero


# Started job registry.


def test_add_and_remove(redis, registry, timestamp):
    """Adding and removing job to StartedJobRegistry."""

    job = Job()

    # Test that job is added with the right score
    yield from registry.add(job, 1000)
    assert (yield from redis.zscore(registry.key, job.id)) < timestamp + 1002

    # Ensure that a timeout of -1 results in a score of -1
    yield from registry.add(job, -1)
    assert (yield from redis.zscore(registry.key, job.id)) == -1

    # Ensure that job is properly removed from sorted set
    yield from registry.remove(job)
    assert not (yield from redis.zscore(registry.key, job.id))


def test_get_job_ids(redis, registry, timestamp):
    """Getting job ids from StartedJobRegistry."""

    yield from redis.zadd(registry.key, timestamp + 10, 'foo')
    yield from redis.zadd(registry.key, timestamp + 20, 'bar')
    assert (yield from registry.get_job_ids()) == ['foo', 'bar']


def test_get_expired_job_ids(redis, registry, timestamp):
    """Getting expired job ids form StartedJobRegistry."""

    yield from redis.zadd(registry.key, 1, 'foo')
    yield from redis.zadd(registry.key, timestamp + 10, 'bar')
    yield from redis.zadd(registry.key, timestamp + 30, 'baz')

    expired = yield from registry.get_expired_job_ids()
    assert expired == ['foo']
    expired = yield from registry.get_expired_job_ids(timestamp + 20)
    assert expired == ['foo', 'bar']


def test_cleanup(redis, registry):
    """Moving expired jobs to FailedQueue."""

    failed_queue = FailedQueue(connection=redis)
    assert (yield from failed_queue.is_empty())

    queue = Queue(connection=redis)
    job = yield from queue.enqueue(say_hello)

    yield from redis.zadd(registry.key, 2, job.id)

    yield from registry.cleanup(1)
    assert job.id not in (yield from failed_queue.job_ids)
    assert (yield from redis.zscore(registry.key, job.id)) == 2

    yield from registry.cleanup()
    assert job.id in (yield from failed_queue.job_ids)
    assert not (yield from redis.zscore(registry.key, job.id))
    yield from job.refresh()
    assert (yield from job.get_status()) == JobStatus.FAILED


def test_job_execution(redis, registry):
    """Job is removed from StartedJobRegistry after execution."""

    # We need synchronous jobs for synchronous workers
    queue = Queue()
    worker = Worker(queue)

    job = yield from queue.enqueue(say_hello)

    yield from worker.prepare_job_execution(job)
    assert job.id in (yield from registry.get_job_ids())

    yield from worker.perform_job(job)
    assert job.id not in (yield from registry.get_job_ids())

    # Job that fails
    job = yield from queue.enqueue(div_by_zero)

    yield from worker.prepare_job_execution(job)
    assert job.id in (yield from registry.get_job_ids())

    yield from worker.perform_job(job)
    assert job.id not in (yield from registry.get_job_ids())


def test_get_job_count(redis, registry, timestamp):
    """StartedJobRegistry returns the right number of job count."""

    timestamp = timestamp + 10
    yield from redis.zadd(registry.key, timestamp, 'foo')
    yield from redis.zadd(registry.key, timestamp, 'bar')
    assert (yield from registry.count) == 2
    with pytest.raises(RuntimeError):
        len(registry)


# Finished job registry.


def test_finished_cleanup(redis, timestamp):
    """Finished job registry removes expired jobs."""

    registry = FinishedJobRegistry()

    yield from redis.zadd(registry.key, 1, 'foo')
    yield from redis.zadd(registry.key, timestamp + 10, 'bar')
    yield from redis.zadd(registry.key, timestamp + 30, 'baz')

    yield from registry.cleanup()
    assert (yield from registry.get_job_ids()) == ['bar', 'baz']

    yield from registry.cleanup(timestamp + 20)
    assert (yield from registry.get_job_ids()) == ['baz']


def test_jobs_are_put_in_registry(loop):
    """Completed jobs are added to FinishedJobRegistry."""

    registry = FinishedJobRegistry()

    assert not (yield from registry.get_job_ids())

    # We need synchronous jobs for synchronous workers
    queue = Queue()
    worker = Worker(queue)

    # Completed jobs are put in FinishedJobRegistry
    job = yield from queue.enqueue(say_hello)
    yield from worker.perform_job(job, loop=loop)
    assert (yield from registry.get_job_ids()) == [job.id]

    # Failed jobs are not put in FinishedJobRegistry
    failed_job = yield from queue.enqueue(div_by_zero)
    yield from worker.perform_job(failed_job)
    assert (yield from registry.get_job_ids()) == [job.id]


# Deferred job registries.


def test_add(redis):
    """Adding a job to DeferredJobsRegistry."""

    registry = DeferredJobRegistry()
    job = Job()

    yield from registry.add(job)
    job_ids = [as_text(job_id) for job_id in
               (yield from redis.zrange(registry.key, 0, -1))]
    assert job_ids == [job.id]


# Clean all registries.


def test_clean_registries(redis):
    """clean_registries() cleans Started and Finished job registries."""

    queue = Queue(connection=redis)

    finished_job_registry = FinishedJobRegistry(connection=redis)
    yield from redis.zadd(finished_job_registry.key, 1, 'foo')

    started_job_registry = StartedJobRegistry(connection=redis)
    yield from redis.zadd(started_job_registry.key, 1, 'foo')

    yield from clean_registries(queue)
    assert not (yield from redis.zcard(finished_job_registry.key))
    assert not (yield from redis.zcard(started_job_registry.key))
