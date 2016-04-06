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
def queue_length(connection, name):
    """Get length of given queue.

    :type connection: `aioredis.Redis`
    :type name: str

    """

    return (yield from connection.llen(queue_key(name)))


@asyncio.coroutine
def empty_queue(connection, name):
    """Removes all jobs on the queue.

    :type connection: `aioredis.Redis`
    :type name: str

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
    return (yield from connection.eval(script, keys=[queue_key(name)]))


@asyncio.coroutine
def compact_queue(connection):
    pass


@asyncio.coroutine
def enqueue_job(connection, queue, id, spec):
    """Persists the job specification to it corresponding Redis id.

    :type connection: `aioredis.Redis`
    :type queue: str
    :type id: str
    :type spec: dict

    """

    multi = connection.multi_exec()
    multi.sadd(queues_key(), queue)
    default_fields = ('status', JobStatus.QUEUED,
                      'origin', queue,
                      'enqueued_at', utcformat(utcnow()))
    spec_fields = itertools.chain.from_iterable(spec.items())
    fields = itertools.chain(spec_fields, default_fields)
    multi.hmset(job_key(id), *fields)
    multi.rpush(queue_key(queue), id)
    yield from multi.execute()


@asyncio.coroutine
def dequeue_job(connection):
    pass


@asyncio.coroutine
def remove_job(connection):
    pass


@asyncio.coroutine
def quarantine_job(connection):
    pass


@asyncio.coroutine
def requeue_job(connection):
    pass


@asyncio.coroutine
def cancel_job(connection):
    pass


@asyncio.coroutine
def start_job(connection):
    pass


@asyncio.coroutine
def finish_job(connection):
    pass


@asyncio.coroutine
def fail_job(connection):
    pass
