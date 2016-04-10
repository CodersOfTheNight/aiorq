import pytest

from aiorq.exceptions import InvalidOperationError
from aiorq.job import utcparse, utcformat, utcnow
from aiorq.keys import queues_key, queue_key, failed_queue_key, job_key
from aiorq.protocol import (queues, jobs, started_jobs, empty_queue,
                            queue_length, enqueue_job, dequeue_job,
                            cancel_job, start_job, fail_job,
                            requeue_job)
from aiorq.specs import JobStatus


# Queues.


def test_queues(redis):
    """All redis queues."""

    yield from redis.sadd(queues_key(), b'default')
    assert (yield from queues(redis)) == [b'default']


# Jobs.


def test_jobs(redis):
    """All queue jobs."""

    yield from redis.rpush(queue_key(b'default'), b'foo')
    yield from redis.rpush(queue_key(b'default'), b'bar')
    assert set((yield from jobs(redis, b'default'))) == {b'foo', b'bar'}


# TODO: offset and length arguments.


# Queue length.


def test_queue_length(redis):
    """RQ queue size."""

    yield from redis.rpush(queue_key(b'example'), b'foo')
    yield from redis.rpush(queue_key(b'example'), b'bar')
    assert (yield from queue_length(redis, b'example')) == 2


# Empty queue.


def test_empty_queue(redis):
    """Emptying queue."""

    yield from redis.rpush(queue_key(b'example'), b'foo')
    yield from redis.rpush(queue_key(b'example'), b'bar')
    assert (yield from queue_length(redis, b'example'))
    yield from empty_queue(redis, b'example')
    assert not (yield from queue_length(redis, b'example'))


def test_empty_removes_jobs(redis):
    """Emptying a queue deletes the associated job objects."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    yield from empty_queue(redis, b'default')
    assert not (yield from redis.exists(job_key(id)))


# TODO: test `empty_queue` removes job dependents.


# Compact queue.


# Enqueue job.


def test_enqueue_job_store_job_hash(redis):
    """Storing jobs."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from redis.type(job_key(id))) == b'hash'
    assert spec[b'data'] == (yield from redis.hget(job_key(id), b'data'))


def test_enqueue_job_register_queue(redis):
    """Enqueue job will add its queue into queues storage."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from queues(redis)) == [b'default']


def test_enqueue_job_add_job_key_to_the_queue(redis):
    """Enqueue job must add its id to the queue."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    queue_content = [b'2a5079e7-387b-492f-a81c-68aa55c194c8']
    assert (yield from redis.lrange(queue_key(queue), 0, -1)) == queue_content


def test_enqueue_job_set_job_status(redis):
    """Enqueue job must set job status to QUEUED."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from redis.hget(job_key(id), b'status')) == b'queued'


def test_enqueue_job_set_job_origin(redis):
    """Enqueue job must set jobs origin queue."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from redis.hget(job_key(id), b'origin')) == b'default'


def test_enqueue_job_set_job_enqueued_at(redis):
    """Enqueue job must set enqueued_at time."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    utcparse((yield from redis.hget(job_key(id), b'enqueued_at')))


# Dequeue job.


def test_dequeue_job_from_empty_queue(redis):
    """Nothing happens if there is an empty queue."""

    queue = b'default'
    assert not (yield from dequeue_job(redis, queue))


def test_dequeue_job(redis):
    """Dequeueing jobs from queues."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    result = yield from dequeue_job(redis, queue)
    assert not (yield from queue_length(redis, queue))
    assert result == {
        b'id': id,
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': b'180',
        b'status': JobStatus.QUEUED,
        b'origin': queue,
        b'enqueued_at': utcformat(utcnow()),
    }


def test_dequeue_job_no_such_job(redis):
    """Silently skip job ids from queue if there is no such job hash."""

    queue = b'default'
    yield from redis.rpush(queue_key(queue), b'foo')  # Job id without hash.
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from dequeue_job(redis, queue))[b'id'] == id


# Cancel job.


def test_cancel_job(redis):
    """Remove jobs from queue."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    yield from cancel_job(redis, queue, id)
    assert not (yield from queue_length(redis, queue))


# Start job.


def test_start_job_sets_job_status(redis):
    """Start job sets corresponding job status."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    yield from start_job(redis, queue, id)
    assert (yield from redis.hget(job_key(id), b'status')) == JobStatus.STARTED


def test_start_job_sets_started_time(redis):
    """Start job sets it started at time."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    yield from start_job(redis, queue, id)
    started_at = yield from redis.hget(job_key(id), b'started_at')
    assert started_at == utcformat(utcnow())


def test_start_job_add_job_to_registry(redis):
    """Start job will add it to started job registry."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    yield from start_job(redis, queue, id)
    assert (yield from started_jobs(redis, queue)) == [id]


def test_start_job_persist_job(redis):
    """Start job set persistence to the job hash during job execution."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    yield from redis.expire(job_key(id), 3)
    yield from start_job(redis, queue, id)
    assert (yield from redis.ttl(job_key(id))) == -1


# Finish job.


# Fail job.


def test_fail_job_registers_failed_queue(redis):
    """Register failed queue on quarantine job."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    assert (yield from queues(redis)) == [failed_queue_key()]


def test_fail_job_enqueue_into_faileld_queue(redis):
    """Failed job appears in the failed queue."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    assert id in (yield from jobs(redis, b'failed'))


def test_fail_job_set_status(redis):
    """Failed job should have corresponding status."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    assert (yield from redis.hget(job_key(id), b'status')) == JobStatus.FAILED


def test_fail_job_sets_ended_at(redis):
    """Failed job should have ended at time."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    ended_at = yield from redis.hget(job_key(id), b'ended_at')
    assert ended_at == utcformat(utcnow())


def test_fail_job_sets_exc_info(redis):
    """Failed job should have exception information."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    exc_info = yield from redis.hget(job_key(id), b'exc_info')
    assert exc_info == b"Exception('We are here')"


def test_fail_job_removes_from_started_registry(redis):
    """Fail job remove given job from started registry."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    yield from dequeue_job(redis, queue)
    yield from start_job(redis, queue, id)
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    assert id not in (yield from started_jobs(redis, queue))


# Requeue job.


def test_requeue_job_set_status(redis):
    """Requeue existing job set corresponding job status."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    yield from dequeue_job(redis, queue)
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    yield from requeue_job(redis, id)
    assert (yield from redis.hget(job_key(id), b'status')) == JobStatus.QUEUED


def test_requeue_job_clean_exc_info(redis):
    """Requeue existing job cleanup exception information."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    yield from dequeue_job(redis, queue)
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    yield from requeue_job(redis, id)
    assert not (yield from redis.hget(job_key(id), b'exc_info'))


def test_requeue_job_enqueue_into_origin(redis):
    """Requeue existing job puts it into jobs origin queue."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    yield from dequeue_job(redis, queue)
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    yield from requeue_job(redis, id)
    assert id in (yield from jobs(redis, queue))


def test_requeue_job_removes_non_existing_job(redis):
    """Requeue job removes job id from the failed queue if job doesn't
    exists anymore.
    """

    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    yield from redis.rpush(failed_queue_key(), id)
    yield from requeue_job(redis, id)
    assert not (yield from jobs(redis, b'failed'))


def test_requeue_job_error_on_non_failed_job(redis):
    """Throw error if anyone tries to requeue non failed job."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    with pytest.raises(InvalidOperationError):
        yield from requeue_job(redis, id)
