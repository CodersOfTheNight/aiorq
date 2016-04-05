"""
    aiorq.protocol
    ~~~~~~~~~~~~~~

    Redis related state manipulations.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio

from .keys import queue_key


@asyncio.coroutine
def queue_length(connection, name):
    """Get length of given queue."""

    return (yield from connection.llen(queue_key(name)))


@asyncio.coroutine
def empty_queue(connection, name):
    """Removes all jobs on the queue."""

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
def enqueue_job(connection):
    pass


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
