"""
    aiorq.protocol
    ~~~~~~~~~~~~~~

    Redis related state manipulations.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio

from .keys import queues_key, queue_key, job_key


@asyncio.coroutine
def queue_length(connection, name):
    """Get length of given queue."""

    return (yield from connection.llen(queue_key(name)))


@asyncio.coroutine
def empty_queue(connection, name):
    """Removes all jobs on the queue.

    :param connection: Asynchronous redis connection
    :type connection: `aioredis.Redis`
    :param name: Queue name
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

    :param connection: Asynchronous redis connection
    :type connection: `aioredis.Redis`
    :param queue: Queue name
    :type queue: str
    :param id: Job uuid
    :type id: str
    :param spec: Job specification
    :type spec: dict

    """

    # TODO: add id to the queue, motherfucker!
    multi = connection.multi_exec()
    multi.sadd(queues_key(), queue)
    fields = (field
              for item_fields in spec.items()
              for field in item_fields)
    multi.hmset(job_key(id), *fields)
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
