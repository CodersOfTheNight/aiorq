import pytest

import stubs
from aiorq.exceptions import InvalidOperationError
from aiorq.keys import (queues_key, queue_key, failed_queue_key,
                        job_key, started_registry, finished_registry,
                        deferred_registry, workers_key, worker_key,
                        dependents)
from aiorq.protocol import (queues, jobs, job_status, started_jobs,
                            finished_jobs, deferred_jobs, empty_queue,
                            queue_length, enqueue_job, dequeue_job,
                            cancel_job, start_job, finish_job,
                            fail_job, requeue_job, workers,
                            worker_birth, worker_death,
                            worker_shutdown_requested)
from aiorq.specs import JobStatus, WorkerStatus
from aiorq.utils import current_timestamp, utcparse, utcformat, utcnow


# Queues.


def test_queues(redis):
    """All redis queues."""

    yield from redis.sadd(queues_key(), stubs.queue)
    assert (yield from queues(redis)) == [stubs.queue]


# Jobs.


def test_jobs(redis):
    """All queue jobs."""

    yield from redis.rpush(queue_key(stubs.queue), b'foo')
    yield from redis.rpush(queue_key(stubs.queue), b'bar')
    assert set((yield from jobs(redis, stubs.queue))) == {b'foo', b'bar'}


def test_jobs_args(redis):
    """Test jobs behavior with limit and offset arguments."""

    yield from redis.rpush(queue_key(stubs.queue), b'foo')
    yield from redis.rpush(queue_key(stubs.queue), b'bar')
    assert set((yield from jobs(redis, stubs.queue, 0, 0))) == {b'foo'}


# Job status.


def test_job_status(redis):
    """Get job status."""

    yield from redis.hset(job_key(stubs.job_id), b'status', JobStatus.QUEUED)
    assert (yield from job_status(redis, stubs.job_id)) == JobStatus.QUEUED


# Started jobs.


def test_started_jobs(redis):
    """All started jobs from this queue."""

    yield from redis.zadd(started_registry(stubs.queue), 1, b'foo')
    yield from redis.zadd(started_registry(stubs.queue), 2, b'bar')
    assert set((yield from started_jobs(redis, stubs.queue))) == {b'foo', b'bar'}


def test_started_jobs_args(redis):
    """All started jobs from this queue limited by arguments."""

    yield from redis.zadd(started_registry(stubs.queue), 1, b'foo')
    yield from redis.zadd(started_registry(stubs.queue), 2, b'bar')
    assert set((yield from started_jobs(redis, stubs.queue, 0, 0))) == {b'foo'}


# Finished jobs.


def test_finished_jobs(redis):
    """All jobs that have been completed."""

    yield from redis.zadd(finished_registry(stubs.queue), 1, b'foo')
    yield from redis.zadd(finished_registry(stubs.queue), 2, b'bar')
    assert set((yield from finished_jobs(redis, stubs.queue))) == {b'foo', b'bar'}


def test_finished_jobs_args(redis):
    """All finished jobs from this queue limited by arguments."""

    yield from redis.zadd(finished_registry(stubs.queue), 1, b'foo')
    yield from redis.zadd(finished_registry(stubs.queue), 2, b'bar')
    assert set((yield from finished_jobs(redis, stubs.queue, 0, 0))) == {b'foo'}


# Deferred jobs.


def test_deferred_jobs(redis):
    """All jobs that are waiting for another job to finish."""

    yield from redis.zadd(deferred_registry(stubs.queue), 1, b'foo')
    yield from redis.zadd(deferred_registry(stubs.queue), 2, b'bar')
    assert set((yield from deferred_jobs(redis, stubs.queue))) == {b'foo', b'bar'}


def test_deferred_jobs_args(redis):
    """All deferred jobs from this queue limited by arguments."""

    yield from redis.zadd(deferred_registry(stubs.queue), 1, b'foo')
    yield from redis.zadd(deferred_registry(stubs.queue), 2, b'bar')
    assert set((yield from deferred_jobs(redis, stubs.queue, 0, 0))) == {b'foo'}


# Queue length.


def test_queue_length(redis):
    """RQ queue size."""

    yield from redis.rpush(queue_key(stubs.queue), b'foo')
    yield from redis.rpush(queue_key(stubs.queue), b'bar')
    assert (yield from queue_length(redis, stubs.queue)) == 2


# Empty queue.


def test_empty_queue(redis):
    """Emptying queue."""

    yield from redis.rpush(queue_key(stubs.queue), b'foo')
    yield from redis.rpush(queue_key(stubs.queue), b'bar')
    assert (yield from queue_length(redis, stubs.queue))
    yield from empty_queue(redis, stubs.queue)
    assert not (yield from queue_length(redis, stubs.queue))


def test_empty_removes_jobs(redis):
    """Emptying a queue deletes the associated job objects."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from empty_queue(redis, stubs.queue)
    assert not (yield from redis.exists(job_key(stubs.job_id)))


def test_empty_queue_removes_dependents(redis):
    """Remove dependent jobs for jobs from cleaned queue."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    yield from empty_queue(redis, stubs.queue)
    assert not (yield from redis.smembers(dependents(stubs.job_id)))


# Compact queue.


# Enqueue job.


def test_enqueue_job_store_job_hash(redis):
    """Storing jobs."""

    yield from enqueue_job(redis=redis, **stubs.job)
    assert (yield from redis.type(job_key(stubs.job_id))) == b'hash'
    assert (yield from redis.hget(job_key(stubs.job_id), b'data')) == stubs.job_data


def test_enqueue_job_register_queue(redis):
    """Enqueue job will add its queue into queues storage."""

    yield from enqueue_job(redis=redis, **stubs.job)
    assert (yield from queues(redis)) == [stubs.queue]


def test_enqueue_job_add_job_key_to_the_queue(redis):
    """Enqueue job must add its id to the queue."""

    yield from enqueue_job(redis=redis, **stubs.job)
    queue_content = [stubs.job_id]
    assert (yield from redis.lrange(queue_key(stubs.queue), 0, -1)) == queue_content


def test_enqueue_job_at_front(redis):
    """Enqueue job at front must add its id to the front of the queue."""

    yield from redis.lpush(queue_key(stubs.queue), b'xxx')
    yield from enqueue_job(redis=redis, at_front=True, **stubs.job)
    queue_content = [stubs.job_id, b'xxx']
    assert (yield from redis.lrange(queue_key(stubs.queue), 0, -1)) == queue_content


def test_enqueue_job_set_job_status(redis):
    """Enqueue job must set job status to QUEUED."""

    yield from enqueue_job(redis=redis, **stubs.job)
    assert (yield from job_status(redis, stubs.job_id)) == JobStatus.QUEUED


def test_enqueue_job_set_job_origin(redis):
    """Enqueue job must set jobs origin queue."""

    yield from enqueue_job(redis=redis, **stubs.job)
    assert (yield from redis.hget(job_key(stubs.job_id), b'origin')) == stubs.queue


def test_enqueue_job_set_job_enqueued_at(redis):
    """Enqueue job must set enqueued_at time."""

    yield from enqueue_job(redis=redis, **stubs.job)
    utcparse((yield from redis.hget(job_key(stubs.job_id), b'enqueued_at')))


def test_enqueue_job_dependent_status(redis):
    """Set DEFERRED status to jobs with unfinished dependencies."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    assert (yield from job_status(redis, stubs.child_job_id)) == JobStatus.DEFERRED


def test_enqueue_job_defer_dependent(redis):
    """Defer dependent job.  It shouldn't present in the queue."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    assert (yield from redis.lrange(queue_key(stubs.queue), 0, -1)) == [stubs.job_id]


def test_enqueue_job_finished_dependency(redis):
    """Enqueue job immediately if its dependency already finished."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    assert (yield from redis.lrange(queue_key(stubs.queue), 0, -1)) == [stubs.child_job_id]


def test_enqueue_job_deferred_job_registry(redis):
    """Add job with unfinished dependency to deferred job registry."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    assert (yield from deferred_jobs(redis, stubs.queue)) == [stubs.child_job_id]


def test_enqueue_job_dependents_set(redis):
    """Add job to the dependents set if its dependency isn't finished yet."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    assert (yield from redis.smembers(dependents(stubs.job_id))) == [stubs.child_job_id]


def test_enqueue_job_defer_without_date(redis):
    """Enqueue job with dependency doesn't set enqueued_at date."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    assert not (yield from redis.hexists(job_key(stubs.child_job_id), b'enqueued_at'))


# TODO: enqueue_job checks dependency status, it isn't finished, then
# another worker set it status to finished, then we defer job with
# already finished dependency.  It will never be executed.


# Dequeue job.


def test_dequeue_job_from_empty_queue(redis):
    """Nothing happens if there is an empty queue."""

    assert (yield from dequeue_job(redis, stubs.queue)) == (None, {})


def test_dequeue_job(redis):
    """Dequeueing jobs from queues."""

    yield from enqueue_job(redis=redis, result_ttl=5000, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    assert not (yield from queue_length(redis, stubs.queue))
    assert stored_id == stubs.job_id
    assert stored_spec == {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': stubs.queue,
        b'enqueued_at': utcformat(utcnow()),
    }


def test_dequeue_job_no_such_job(redis):
    """Silently skip job ids from queue if there is no such job hash."""

    yield from redis.rpush(queue_key(stubs.queue), b'foo')  # Job id without hash.
    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    assert stored_id == stubs.job_id


# Cancel job.


def test_cancel_job(redis):
    """Remove jobs from queue."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from cancel_job(redis, stubs.queue, stubs.job_id)
    assert not (yield from queue_length(redis, stubs.queue))


# Start job.


def test_start_job_sets_job_status(redis):
    """Start job sets corresponding job status."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    assert (yield from job_status(redis, stubs.job_id)) == JobStatus.STARTED


def test_start_job_sets_started_time(redis):
    """Start job sets it started at time."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    started_at = yield from redis.hget(job_key(stubs.job_id), b'started_at')
    assert started_at == utcformat(utcnow())


def test_start_job_add_job_to_registry(redis):
    """Start job will add it to started job registry."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    score = current_timestamp() + stored_spec[b'timeout'] + 60
    started = yield from redis.zrange(started_registry(queue), withscores=True)
    assert started == [stubs.job_id, score]


def test_start_job_persist_job(redis):
    """Start job set persistence to the job hash during job execution."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from redis.expire(job_key(stubs.job_id), 3)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    assert (yield from redis.ttl(job_key(stubs.job_id))) == -1


# Finish job.


def test_finish_job_sets_ended_at(redis):
    """Finish job sets ended at field."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    ended_at = yield from redis.hget(job_key(stubs.job_id), b'ended_at')
    assert ended_at == utcformat(utcnow())


def test_finish_job_sets_corresponding_status(redis):
    """Finish job sets corresponding status."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    assert (yield from job_status(redis, stubs.job_id)) == JobStatus.FINISHED


def test_finish_job_sets_results_ttl(redis):
    """Finish job sets results TTL."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    assert (yield from redis.ttl(job_key(stubs.job_id))) == 500


def test_finish_job_use_custom_ttl(redis):
    """Finish job sets custom results TTL."""

    yield from enqueue_job(redis=redis, result_ttl=5000, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    result_ttl = stored_spec[b'result_ttl']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id, result_ttl=result_ttl)
    assert (yield from redis.ttl(job_key(stubs.job_id))) == 5000


def test_finish_job_remove_results_zero_ttl(redis):
    """Finish job removes jobs with zero TTL."""

    yield from enqueue_job(redis=redis, result_ttl=0, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    result_ttl = stored_spec[b'result_ttl']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id, result_ttl=result_ttl)
    assert not (yield from redis.exists(job_key(stubs.job_id)))


def test_finish_job_non_expired_job(redis):
    """Finish job persist non expired job."""

    yield from enqueue_job(redis=redis, result_ttl=None, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    result_ttl = stored_spec[b'result_ttl']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id, result_ttl=result_ttl)
    assert (yield from redis.ttl(job_key(stubs.job_id))) == -1


def test_finish_job_started_registry(redis):
    """Finish job removes job from started job registry."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    assert not (yield from started_jobs(redis, stubs.queue))


def test_finish_job_finished_registry(redis):
    """Finish job add job to the finished job registry."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    finish = yield from redis.zrange(finished_registry(queue), withscores=True)
    assert finish == [stubs.job_id, current_timestamp() + 500]


def test_finish_job_finished_registry_negative_ttl(redis):
    """Don't use current timestamp in job score with empty result TTL."""

    yield from enqueue_job(redis=redis, result_ttl=None, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    result_ttl = stored_spec[b'result_ttl']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id, result_ttl=result_ttl)
    finish = yield from redis.zrange(finished_registry(queue), withscores=True)
    assert finish == [stubs.job_id, -1]


def test_finish_job_enqueue_dependents(redis):
    """Finish job will enqueue its dependents."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    assert (yield from redis.lrange(queue_key(stubs.queue), 0, -1)) == [stubs.child_job_id]


def test_finish_job_enqueue_dependents_status(redis):
    """Finish job will set dependents status to QUEUED."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    assert (yield from redis.hget(job_key(stubs.child_job_id), b'status')) == JobStatus.QUEUED


def test_finish_job_dependents_enqueue_date(redis):
    """Finish job will enqueue its dependents with proper date field."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    enqueued_at = yield from redis.hget(job_key(stubs.job_id), b'enqueued_at')
    assert enqueued_at == utcformat(utcnow())


def test_finish_job_cleanup_dependents(redis):
    """Finish job will cleanup parent job dependents set."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    assert not (yield from redis.smembers(dependents(stubs.job_id)))


def test_finish_job_dependents_defered_registry(redis):
    """Finish job will remove its dependents from deferred job registries."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from enqueue_job(redis=redis, **stubs.child_job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from finish_job(redis, queue, stored_id)
    assert not (yield from deferred_jobs(redis, queue))


# Fail job.


def test_fail_job_registers_failed_queue(redis):
    """Register failed queue on quarantine job."""

    yield from fail_job(redis, stubs.queue, stubs.job_id, b"Exception('We are here')")
    assert (yield from queues(redis)) == [failed_queue_key()]


def test_fail_job_enqueue_into_faileld_queue(redis):
    """Failed job appears in the failed queue."""

    yield from fail_job(redis, stubs.queue, stubs.job_id, b"Exception('We are here')")
    assert stubs.job_id in (yield from jobs(redis, b'failed'))


def test_fail_job_set_status(redis):
    """Failed job should have corresponding status."""

    yield from enqueue_job(redis=redis, **stubs.job)
    yield from fail_job(redis, stubs.queue, stubs.job_id, b"Exception('We are here')")
    assert (yield from job_status(redis, stubs.job_id)) == JobStatus.FAILED


def test_fail_job_sets_ended_at(redis):
    """Failed job should have ended at time."""

    yield from fail_job(redis, stubs.queue, stubs.job_id, b"Exception('We are here')")
    ended_at = yield from redis.hget(job_key(stubs.job_id), b'ended_at')
    assert ended_at == utcformat(utcnow())


def test_fail_job_sets_exc_info(redis):
    """Failed job should have exception information."""

    yield from fail_job(redis, stubs.queue, stubs.job_id, b"Exception('We are here')")
    exc_info = yield from redis.hget(job_key(stubs.job_id), b'exc_info')
    assert exc_info == b"Exception('We are here')"


def test_fail_job_removes_from_started_registry(redis):
    """Fail job remove given job from started registry."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    queue = stored_spec[b'origin']
    timeout = stored_spec[b'timeout']
    yield from start_job(redis, queue, stored_id, timeout)
    yield from fail_job(redis, stubs.queue, stubs.job_id, b"Exception('We are here')")
    assert stubs.job_id not in (yield from started_jobs(redis, queue))


# Requeue job.


def test_requeue_job_set_status(redis):
    """Requeue existing job set corresponding job status."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    yield from fail_job(redis, stubs.queue, stubs.job_id, b"Exception('We are here')")
    yield from requeue_job(redis, stored_id)
    assert (yield from job_status(redis, stubs.job_id)) == JobStatus.QUEUED


def test_requeue_job_clean_exc_info(redis):
    """Requeue existing job cleanup exception information."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    yield from fail_job(redis, stubs.queue, stubs.job_id, b"Exception('We are here')")
    yield from requeue_job(redis, stored_id)
    assert not (yield from redis.hget(job_key(stubs.job_id), b'exc_info'))


def test_requeue_job_enqueue_into_origin(redis):
    """Requeue existing job puts it into jobs origin queue."""

    yield from enqueue_job(redis=redis, **stubs.job)
    stored_id, stored_spec = yield from dequeue_job(redis, stubs.queue)
    yield from fail_job(redis, stubs.queue, stubs.job_id, b"Exception('We are here')")
    yield from requeue_job(redis, stored_id)
    assert stubs.job_id in (yield from jobs(redis, stubs.queue))


def test_requeue_job_removes_non_existing_job(redis):
    """Requeue job removes job id from the failed queue if job doesn't
    exists anymore.
    """

    yield from redis.rpush(failed_queue_key(), stubs.job_id)
    yield from requeue_job(redis, stubs.job_id)
    assert not (yield from jobs(redis, b'failed'))


def test_requeue_job_error_on_non_failed_job(redis):
    """Throw error if anyone tries to requeue non failed job."""

    yield from enqueue_job(redis=redis, **stubs.job)
    with pytest.raises(InvalidOperationError):
        yield from requeue_job(redis, stubs.job_id)


# Workers.


def test_workers(redis):
    """Get all worker keys."""

    yield from redis.sadd(workers_key(), b'foo')
    yield from redis.sadd(workers_key(), b'bar')
    assert set((yield from workers(redis))) == {b'foo', b'bar'}


# Worker birth.


def test_worker_birth_workers_set(redis):
    """Add worker to the workers set."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    assert (yield from redis.smembers(workers_key())) == [worker_key(b'foo')]


def test_worker_birth_sets_birth_date(redis):
    """Set worker birth date."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    birth = utcformat(utcnow())
    assert (yield from redis.hget(worker_key(b'foo'), b'birth')) == birth


def test_worker_birth_sets_queue_names(redis):
    """Set worker queue names."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    assert (yield from redis.hget(worker_key(b'foo'), b'queues')) == b'bar,baz'


def test_worker_birth_removes_old_hash(redis):
    """Remove old hash stored from the previous run."""

    yield from redis.hmset(worker_key(b'foo'), b'bar', b'baz', b'death', 0)
    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    assert not (yield from redis.hget(worker_key(b'foo'), b'bar'))


def test_worker_birth_sets_worker_ttl(redis):
    """Set worker ttl."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    assert (yield from redis.ttl(worker_key(b'foo'))) == 420


def test_worker_birth_sets_custom_worker_ttl(redis):
    """Set custom worker ttl."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'], 1000)
    assert (yield from redis.ttl(worker_key(b'foo'))) == 1000


def test_worker_birth_fail_worker_exists(redis):
    """Register birth will fail with worker which already exists."""

    yield from redis.hset(worker_key(b'foo'), b'bar', b'baz')
    with pytest.raises(ValueError):
        yield from worker_birth(redis, b'foo', [b'bar', b'baz'])


def test_worker_birth_worker_status(redis):
    """Register birth sets worker status to STARTED."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    status = yield from redis.hget(worker_key(b'foo'), b'status')
    assert status == WorkerStatus.STARTED


def test_worker_birth_current_job(redis):
    """Signify that aiorq doesn't support current_job worker attribute."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    current_job = yield from redis.hget(worker_key(b'foo'), b'current_job')
    assert current_job == b'not supported'


# Worker death.


def test_worker_death_workers_set(redis):
    """Remove worker from workers set."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    yield from worker_death(redis, b'foo')
    assert not (yield from redis.smembers(workers_key()))


def test_worker_death_sets_death_date(redis):
    """Set worker death date."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    yield from worker_death(redis, b'foo')
    death = utcformat(utcnow())
    assert (yield from redis.hget(worker_key(b'foo'), b'death')) == death


def test_worker_death_sets_worker_ttl(redis):
    """Set worker hash ttl."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    yield from worker_death(redis, b'foo')
    assert (yield from redis.ttl(worker_key(b'foo'))) == 60


def test_worker_death_sets_status(redis):
    """Set worker status to IDLE."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    yield from worker_death(redis, b'foo')
    status = yield from redis.hget(worker_key(b'foo'), b'status')
    assert status == WorkerStatus.IDLE


# Worker shutdown requested.


def test_worker_shutdown_requested(redis):
    """Set worker shutdown requested date."""

    yield from worker_birth(redis, b'foo', [b'bar', b'baz'])
    yield from worker_shutdown_requested(redis, b'foo')
    shutdown = yield from redis.hget(
        worker_key(b'foo'), b'shutdown_requested_date')
    assert shutdown == utcformat(utcnow())
