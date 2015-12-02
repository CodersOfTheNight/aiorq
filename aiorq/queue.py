"""
    aiorq.queue
    ~~~~~~~~~~~

    This module define asyncio compatible job queue.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio

from rq.job import JobStatus

from .connections import resolve_connection
from .job import Job


class Queue:
    """asyncio job queue."""

    job_class = Job
    redis_queue_namespace_prefix = 'rq:queue:'
    redis_queues_keys = 'rq:queues'

    def __init__(self, name='default', connection=None):

        self.connection = resolve_connection(connection)
        prefix = self.redis_queue_namespace_prefix
        self.name = name
        self._key = '{0}{1}'.format(prefix, name)

    @property
    def key(self):
        """Returns the Redis key for this Queue."""

        return self._key

    @asyncio.coroutine
    def empty(self):
        """Removes all messages on the queue."""

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
        return (yield from self.connection.eval(script, keys=[self.key]))

    @asyncio.coroutine
    def is_empty(self):
        """Returns whether the current queue is empty."""

        return (yield from self.count) == 0

    @property
    @asyncio.coroutine
    def count(self):
        """Returns a count of all messages in the queue."""

        return (yield from self.connection.llen(self.key))

    @asyncio.coroutine
    def remove(self, job_or_id, pipeline=None):
        """Removes Job from queue, accepts either a Job instance or ID."""

        job_id = (job_or_id.id
                  if isinstance(job_or_id, self.job_class)
                  else job_or_id)
        # TODO: yield from if pipeline is none and return lrem result
        connection = pipeline
        connection.lrem(self.key, 1, job_id)

    @asyncio.coroutine
    def push_job_id(self, job_id, pipeline=None, at_front=False):
        """Pushes a job ID on the corresponding Redis queue.

        'at_front' allows you to push the job onto the front instead
        of the back of the queue
        """

        # TODO: implement pipeline and at_front behavior
        yield from self.connection.rpush(self.key, job_id)

    @asyncio.coroutine
    def enqueue(self, f, *args, **kwargs):
        """Creates a job to represent the delayed function call and enqueues
        it.

        Expects the function to call, along with the arguments and keyword
        arguments.

        The function argument `f` may be any of the following:

        * A reference to a function
        * A reference to an object's instance method
        * A string, representing the location of a function (must be
          meaningful to the import context of the workers)
        """

        return (yield from self.enqueue_call(func=f, args=args, kwargs=kwargs))

    @asyncio.coroutine
    def enqueue_call(self, func, args=None, kwargs=None):
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """

        job = self.job_class.create(
            func, args=args, kwargs=kwargs, connection=self.connection)
        job = yield from self.enqueue_job(job)
        return job

    @asyncio.coroutine
    def enqueue_job(self, job):
        """Enqueues a job for delayed execution."""

        pipe = self.connection.pipeline()
        pipe.sadd(self.redis_queues_keys, self.key)
        yield from job.set_status(JobStatus.QUEUED, pipeline=pipe)
        job.origin = self.name
        yield from job.save(pipeline=pipe)
        yield from pipe.execute()
        yield from self.push_job_id(job.id)
        return job

    def __eq__(self, other):

        return self.name == other.name
