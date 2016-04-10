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
                   started_registry)
from .job import utcformat, utcnow
from .specs import JobStatus
from .utils import current_timestamp


@asyncio.coroutine
def queues(redis):
    """All RQ queues.

    :type redis: `aioredis.Redis`

    """

    return (yield from redis.smembers(queues_key()))


@asyncio.coroutine
def jobs(redis, queue):
    """All queue jobs.

    :type redis: `aioredis.Redis`
    :type queue: bytes

    """

    return (yield from redis.lrange(queue_key(queue), 0, -1))


@asyncio.coroutine
def started_jobs(redis, queue):
    """All started jobs from this queue.

    :type redis: `aioredis.Redis`
    :type queue: bytes

    """

    # TODO: start and end arguments.
    return (yield from redis.zrange(started_registry(queue), 0, -1))


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
def enqueue_job(redis, queue, id, spec):
    """Persists the job specification to it corresponding Redis id.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type id: bytes
    :type spec: dict

    """

    multi = redis.multi_exec()
    multi.sadd(queues_key(), queue)
    default_fields = (b'status', JobStatus.QUEUED,
                      b'origin', queue,
                      b'enqueued_at', utcformat(utcnow()))
    spec_fields = itertools.chain.from_iterable(spec.items())
    fields = itertools.chain(spec_fields, default_fields)
    multi.hmset(job_key(id), *fields)
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
            return
        job = yield from redis.hgetall(job_key(job_id))
        if not job:
            continue
        job[b'id'] = job_id
        return job


@asyncio.coroutine
def cancel_job(redis, queue, id):
    """Removes job from queue.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type id: bytes

    """

    yield from redis.lrem(queue_key(queue), 1, id)


@asyncio.coroutine
def start_job(redis, queue, id):
    """Start given job.

    :type redis: `aioredis.Redis`
    :type queue: bytes
    :type id: bytes

    """

    fields = (b'status', JobStatus.STARTED,
              b'started_at', utcformat(utcnow()))
    # TODO: TTL argument for registry
    score = current_timestamp()
    # TODO: worker state, current job and heartbeat.
    multi = redis.multi_exec()
    multi.hmset(job_key(id), *fields)
    multi.zadd(started_registry(queue), score, id)
    multi.persist(job_key(id))
    yield from multi.execute()


@asyncio.coroutine
def finish_job(redis, id):
    """Finish given job.

    :type redis: `aioredis.Redis`
    :type id: bytes

    """

    yield from redis.hset(job_key(id), b'ended_at', utcformat(utcnow()))


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
