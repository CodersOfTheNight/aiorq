"""
    aiorq.protocol
    ~~~~~~~~~~~~~~

    Redis related state manipulations.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio
import itertools

from .keys import queues_key, queue_key, job_key
from .job import utcformat, utcnow
from .specs import JobStatus


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

    job_id = yield from redis.lpop(queue_key(queue))
    if not job_id:
        return
    job = yield from redis.hgetall(job_key(job_id))
    job[b'id'] = job_id
    # TODO: silently pass on NoSuchJobError
    return job


@asyncio.coroutine
def remove_job(redis):
    pass


@asyncio.coroutine
def quarantine_job(redis):
    pass


@asyncio.coroutine
def requeue_job(redis):
    pass


@asyncio.coroutine
def cancel_job(redis):
    pass


@asyncio.coroutine
def start_job(redis):
    pass


@asyncio.coroutine
def finish_job(redis):
    pass


@asyncio.coroutine
def fail_job(redis):
    pass
