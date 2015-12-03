"""
    aiorq.queue
    ~~~~~~~~~~~

    This module define asyncio compatible job queue.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio

from rq.job import JobStatus
from rq.compat import as_text

from .connections import resolve_connection
from .exceptions import NoSuchJobError
from .job import Job


def get_failed_queue(connection=None):
    """Returns a handle to the special failed queue."""

    return FailedQueue(connection=connection)


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

    @asyncio.coroutine
    def fetch_job(self, job_id):
        try:
            return (yield from self.job_class.fetch(
                job_id, connection=self.connection))
        except NoSuchJobError:
            yield from self.remove(job_id)

    @asyncio.coroutine
    def get_job_ids(self, offset=0, length=-1):
        """Returns a slice of job IDs in the queue."""

        start = offset
        if length >= 0:
            end = offset + (length - 1)
        else:
            end = length
        return [as_text(job_id) for job_id in
                (yield from self.connection.lrange(self.key, start, end))]

    @asyncio.coroutine
    def get_jobs(self, offset=0, length=-1):
        """Returns a slice of jobs in the queue."""

        job_ids = yield from self.get_job_ids(offset, length)
        # NOTE: yielding from list comprehension instantiate not
        # started generator object.  Asyncio will fail since we don't
        # yield from future.
        jobs = []
        for job_id in job_ids:
            job = yield from self.fetch_job(job_id)
            if job is not None:
                jobs.append(job)
        return jobs

    @property
    @asyncio.coroutine
    def job_ids(self):
        """Returns a list of all job IDS in the queue."""

        return (yield from self.get_job_ids())

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
        connection = pipeline if pipeline else self.connection
        coroutine = connection.lrem(self.key, 1, job_id)
        if not pipeline:
            return (yield from coroutine)

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

        ttl = kwargs.pop('ttl', None)
        job_id = kwargs.pop('job_id', None)

        if 'args' in kwargs or 'kwargs' in kwargs:
            # TODO: assert args == (), 'Extra positional arguments cannot be used when using explicit args and kwargs'  # noqa
            args = kwargs.pop('args', None)
            kwargs = kwargs.pop('kwargs', None)

        return (yield from self.enqueue_call(
            func=f, args=args, kwargs=kwargs, ttl=ttl, job_id=job_id))

    @asyncio.coroutine
    def enqueue_call(self, func, args=None, kwargs=None, ttl=None,
                     job_id=None):
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """

        job = self.job_class.create(
            func, args=args, kwargs=kwargs, connection=self.connection,
            ttl=ttl, id=job_id)
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


class FailedQueue(Queue):
    """Special queue for failed asynchronous jobs."""

    def __init__(self, connection=None):

        super().__init__(JobStatus.FAILED, connection=connection)
