"""
    aiorq.protocol
    ~~~~~~~~~~~~~~

    Redis related state manipulations.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio
import itertools

from .exceptions import InvalidOperationError
from .keys import (queues_key, queue_key, failed_queue_key, job_key,
                   started_registry, finished_registry, deferred_registry,
                   workers_key, worker_key, dependents)
from .job import utcformat, utcnow
from .specs import JobStatus, WorkerStatus
from .utils import current_timestamp


@asyncio.coroutine
def queues(redis):
    """All RQ queues.

    :type redis: `aioredis.Redis`

    """

    return (yield from redis.smembers(queues_key()))


@asyncio.coroutine
def jobs(redis, queue, start=0, end=-1):
    """All queue jobs.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type start: int
    :type end: int

    """

    return (yield from redis.lrange(queue_key(queue), start, end))


@asyncio.coroutine
def job_status(redis, id):
    """Get job status.

    :type redis: `aioredis.Redis`
    :type id: bytes

    """

    return (yield from redis.hget(job_key(id), b'status'))


@asyncio.coroutine
def started_jobs(redis, queue, start=0, end=-1):
    """All started jobs from this queue.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type start: int
    :type end: int

    """

    return (yield from redis.zrange(started_registry(queue), start, end))


@asyncio.coroutine
def finished_jobs(redis, queue, start=0, end=-1):
    """All finished jobs from this queue.  Jobs are added to this registry
    after they have successfully completed for monitoring purposes.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type start: int
    :type end: int

    """

    return (yield from redis.zrange(finished_registry(queue), start, end))


@asyncio.coroutine
def deferred_jobs(redis, queue, start=0, end=-1):
    """All deferred jobs from this queue.  Jobs are waiting for another
    job to finish in this registry.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type start: int
    :type end: int

    """

    return (yield from redis.zrange(deferred_registry(queue), start, end))


@asyncio.coroutine
def queue_length(redis, name):
    """Get length of given queue.

    :type redis: `aioredis.Redis`
    :type name: bytes

    """

    return (yield from redis.llen(queue_key(name)))


@asyncio.coroutine
def empty_queue(redis, name):
    """Removes all jobs on the queue.

    :type redis: `aioredis.Redis`
    :type name: bytes

    """

    script = b"""
        local prefix = "rq:job:"
        local q = KEYS[1]
        local count = 0
        while true do
            local job_id = redis.call("lpop", q)
            if job_id == false then
                break
            end

            -- Delete the relevant keys
            redis.call("del", prefix..job_id)
            redis.call("del", prefix..job_id..":dependents")
            count = count + 1
        end
        return count
    """
    return (yield from redis.eval(script, keys=[queue_key(name)]))


@asyncio.coroutine
def compact_queue(redis):
    pass


@asyncio.coroutine
def enqueue_job(redis, queue, id, spec, *, at_front=False):
    """Persists the job specification to it corresponding Redis id.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type id: bytes
    :type spec: dict
    :type at_front: bool

    """

    has_dependency = False
    if b'dependency_id' in spec:
        coroutine = job_status(redis, spec[b'dependency_id'])
        dependency_status = yield from coroutine
        if dependency_status != JobStatus.FINISHED:
            has_dependency = True
    if has_dependency:
        status = JobStatus.DEFERRED
    else:
        status = JobStatus.QUEUED
    if b'result_ttl' in spec and spec[b'result_ttl'] is None:
        spec[b'result_ttl'] = -1
    default_fields = (b'status', status,
                      b'origin', queue,
                      b'enqueued_at', utcformat(utcnow()))
    spec_fields = itertools.chain.from_iterable(spec.items())
    fields = itertools.chain(spec_fields, default_fields)
    multi = redis.multi_exec()
    multi.sadd(queues_key(), queue)
    multi.hmset(job_key(id), *fields)
    if has_dependency:
        score = current_timestamp()
        multi.zadd(deferred_registry(queue), score, id)
        multi.sadd(dependents(spec[b'dependency_id']), id)
    else:
        if at_front:
            multi.lpush(queue_key(queue), id)
        else:
            multi.rpush(queue_key(queue), id)
    yield from multi.execute()


@asyncio.coroutine
def dequeue_job(redis, queue):
    """Dequeue the front-most job from this queue.

    :type redis: `aioredis.Redis`
    :type queue: bytes

    """

    while True:
        job_id = yield from redis.lpop(queue_key(queue))
        if not job_id:
            return None, {}
        job = yield from redis.hgetall(job_key(job_id))
        if not job:
            continue
        job[b'timeout'] = int(job[b'timeout'])
        if b'result_ttl' in job:
            job[b'result_ttl'] = int(job[b'result_ttl'])
        return job_id, job


@asyncio.coroutine
def cancel_job(redis, queue, id):
    """Removes job from queue.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type id: bytes

    """

    yield from redis.lrem(queue_key(queue), 1, id)


@asyncio.coroutine
def start_job(redis, queue, id, spec):
    """Start given job.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type id: bytes
    :type spec: dict

    """

    fields = (b'status', JobStatus.STARTED,
              b'started_at', utcformat(utcnow()))
    score = current_timestamp() + spec[b'timeout'] + 60
    # TODO: worker state, current job and heartbeat.
    multi = redis.multi_exec()
    multi.hmset(job_key(id), *fields)
    multi.zadd(started_registry(queue), score, id)
    multi.persist(job_key(id))
    yield from multi.execute()


@asyncio.coroutine
def finish_job(redis, id, spec):
    """Finish given job.

    :type redis: `aioredis.Redis`
    :type id: bytes
    :type spec: dict

    """

    if b'result_ttl' not in spec:
        result_ttl = 500
    elif spec[b'result_ttl'] == 0:
        yield from redis.delete(job_key(id))
        return
    else:
        result_ttl = spec[b'result_ttl']
    fields = (b'status', JobStatus.FINISHED,
              b'ended_at', utcformat(utcnow()))
    multi = redis.multi_exec()
    multi.hmset(job_key(id), *fields)
    if result_ttl == -1:
        multi.persist(job_key(id))
    else:
        multi.expire(job_key(id), result_ttl)
    yield from multi.execute()


@asyncio.coroutine
def fail_job(redis, queue, id, exc_info):
    """Puts the given job in failed queue.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type id: bytes
    :type exc_info: bytes

    """

    multi = redis.multi_exec()
    multi.sadd(queues_key(), failed_queue_key())
    multi.rpush(failed_queue_key(), id)
    fields = (b'status', JobStatus.FAILED,
              b'ended_at', utcformat(utcnow()),
              b'exc_info', exc_info)
    multi.hmset(job_key(id), *fields)
    multi.zrem(started_registry(queue), id)
    yield from multi.execute()


@asyncio.coroutine
def requeue_job(redis, id):
    """Requeue job with the given job ID.

    :type redis: `aioredis.Redis`
    :type id: bytes

    """

    job = yield from redis.hgetall(job_key(id))
    is_failed_job = yield from redis.lrem(failed_queue_key(), 1, id)
    if not job:
        return
    if not is_failed_job:
        raise InvalidOperationError('Cannot requeue non-failed job')
    multi = redis.multi_exec()
    multi.hset(job_key(id), b'status', JobStatus.QUEUED)
    multi.hdel(job_key(id), b'exc_info')
    multi.rpush(queue_key(job[b'origin']), id)
    yield from multi.execute()


@asyncio.coroutine
def workers(redis):
    """Worker keys.

    :type redis: `aioredis.Redis`

    """

    return (yield from redis.smembers(workers_key()))


@asyncio.coroutine
def worker_birth(redis, id, queues, ttl=None):
    """Register worker birth.

    :type redis: `aioredis.Redis`
    :type id: bytes
    :type queues: list
    :type ttl: int or None

    """

    if (yield from redis.exists(worker_key(id))):
        if not (yield from redis.hexists(worker_key(id), b'death')):
            msg = 'There exists an active worker named {!r} already'.format(id)
            raise ValueError(msg)
    multi = redis.multi_exec()
    multi.delete(worker_key(id))
    multi.hmset(worker_key(id),
                b'birth', utcformat(utcnow()),
                b'queues', b','.join(queues),
                b'status', WorkerStatus.STARTED)
    multi.expire(worker_key(id), ttl or 420)
    multi.sadd(workers_key(), worker_key(id))
    yield from multi.execute()


@asyncio.coroutine
def worker_death(redis, id):
    """Register worker death.

    :type redis: `aioredis.Redis`
    :type id: bytes

    """

    multi = redis.multi_exec()
    multi.srem(workers_key(), worker_key(id))
    multi.hset(worker_key(id), b'death', utcformat(utcnow()))
    multi.expire(worker_key(id), 60)
    yield from multi.execute()


@asyncio.coroutine
def worker_shutdown_requested(redis, id):
    """Set worker shutdown requested date.

    :type redis: `aioredis.Redis`
    :type id: bytes

    """

    yield from redis.hset(
        worker_key(id), b'shutdown_requested_date', utcformat(utcnow()))
