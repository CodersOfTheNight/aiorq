"""
    aiorq.queue
    ~~~~~~~~~~~

    This module define asyncio compatible job queue.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio
import uuid

from rq.job import JobStatus
from rq.compat import as_text
from rq.utils import utcnow

from .connections import resolve_connection
from .exceptions import NoSuchJobError, UnpickleError, DequeueTimeout
from .job import Job


def get_failed_queue(connection=None):
    """Returns a handle to the special failed queue."""

    return FailedQueue(connection=connection)


class Queue:
    """asyncio job queue."""

    job_class = Job
    redis_queue_namespace_prefix = 'rq:queue:'
    redis_queues_keys = 'rq:queues'

    @classmethod
    def from_queue_key(cls, queue_key, connection=None):
        """Returns a Queue instance, based on the naming conventions for
        naming the internal Redis keys.  Can be used to reverse-lookup
        Queues by their Redis keys.
        """

        prefix = cls.redis_queue_namespace_prefix
        if not queue_key.startswith(prefix):
            raise ValueError('Not a valid RQ queue key: {0}'.format(queue_key))
        name = queue_key[len(prefix):]
        return cls(name, connection=connection)

    def __init__(self, name='default', connection=None):

        self.connection = resolve_connection(connection)
        prefix = self.redis_queue_namespace_prefix
        self.name = name
        self._key = '{0}{1}'.format(prefix, name)

    def __len__(self):
        """Queue length."""

        raise RuntimeError('Do not use `len` on asynchronous queues'
                           ' (use queue.count instead).')

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
    def jobs(self):
        """Returns a list of all (valid) jobs in the queue."""

        return (yield from self.get_jobs())

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
    def compact(self):
        """Removes all "dead" jobs from the queue by cycling through it, while
        guaranteeing FIFO semantics.
        """

        COMPACT_QUEUE = 'rq:queue:_compact:{0}'.format(uuid.uuid4())

        yield from self.connection.rename(self.key, COMPACT_QUEUE)
        while True:
            job_id = as_text((yield from self.connection.lpop(COMPACT_QUEUE)))
            if job_id is None:
                break
            if (yield from self.job_class.exists(job_id, self.connection)):
                (yield from self.connection.rpush(self.key, job_id))

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

        timeout = kwargs.pop('timeout', None)
        result_ttl = kwargs.pop('result_ttl', None)
        ttl = kwargs.pop('ttl', None)
        job_id = kwargs.pop('job_id', None)

        if 'args' in kwargs or 'kwargs' in kwargs:
            assert args == (), ('Extra positional arguments cannot be used '
                                'when using explicit args and kwargs')
            args = kwargs.pop('args', None)
            kwargs = kwargs.pop('kwargs', None)

        return (yield from self.enqueue_call(
            func=f, args=args, kwargs=kwargs, timeout=timeout,
            result_ttl=result_ttl, ttl=ttl, job_id=job_id))

    @asyncio.coroutine
    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, job_id=None):
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """

        job = self.job_class.create(
            func, args=args, kwargs=kwargs, timeout=timeout,
            connection=self.connection, result_ttl=result_ttl, ttl=ttl,
            id=job_id)
        job = yield from self.enqueue_job(job)
        return job

    @asyncio.coroutine
    def enqueue_job(self, job):
        """Enqueues a job for delayed execution."""

        # TODO: process pipeline and at_front method arguments.
        pipe = self.connection.pipeline()
        pipe.sadd(self.redis_queues_keys, self.key)
        yield from job.set_status(JobStatus.QUEUED, pipeline=pipe)

        job.origin = self.name
        job.enqueued_at = utcnow()

        # TODO: process job.timeout field.

        yield from job.save(pipeline=pipe)
        yield from pipe.execute()
        yield from self.push_job_id(job.id)
        return job

    @asyncio.coroutine
    def pop_job_id(self):
        """Pops a given job ID from this Redis queue."""

        return as_text((yield from self.connection.lpop(self.key)))

    @classmethod
    @asyncio.coroutine
    def lpop(cls, queue_keys, timeout, connection=None):
        """Helper method.  Intermediate method to abstract away from some
        Redis API details, where LPOP accepts only a single key,
        whereas BLPOP accepts multiple.  So if we want the
        non-blocking LPOP, we need to iterate over all queues, do
        individual LPOPs, and return the result.

        Until Redis receives a specific method for this, we'll have to
        wrap it this way.

        The timeout parameter is interpreted as follows:
            None - non-blocking (return immediately)
             > 0 - maximum number of seconds to block
        """

        connection = resolve_connection(connection)
        if timeout is not None:  # TODO: test me
            if timeout == 0:
                raise ValueError(
                    'RQ does not support indefinite timeouts. '
                    'Please pick a timeout value > 0')
            result = yield from connection.blpop(queue_keys, timeout)
            if result is None:
                raise DequeueTimeout(timeout, queue_keys)
            queue_key, job_id = result
            return queue_key, job_id
        else:
            for queue_key in queue_keys:
                blob = yield from connection.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    @asyncio.coroutine
    def dequeue(self):
        """Dequeues the front-most job from this queue.

        Returns a job_class instance, which can be executed or
        inspected.
        """

        while True:
            job_id = yield from self.pop_job_id()
            if job_id is None:
                return None
            try:
                job = yield from self.job_class.fetch(
                    job_id, connection=self.connection)
            except NoSuchJobError as e:
                # Silently pass on jobs that don't exist (anymore),
                continue
            except UnpickleError as e:
                # Attach queue information on the exception for improved error
                # reporting
                e.job_id = job_id
                e.queue = self
                raise e
            return job

    @classmethod
    @asyncio.coroutine
    def dequeue_any(cls, queues, timeout, connection=None):
        """Class method returning the job_class instance at the front of the
        given set of Queues, where the order of the queues is
        important.

        When all of the Queues are empty, depending on the `timeout`
        argument, either blocks execution of this function for the
        duration of the timeout or until new messages arrive on any of
        the queues, or returns None.

        See the documentation of cls.lpop for the interpretation of
        timeout.
        """

        while True:
            queue_keys = [q.key for q in queues]
            result = yield from cls.lpop(
                queue_keys, timeout, connection=connection)
            if result is None:
                return None
            queue_key, job_id = map(as_text, result)
            queue = cls.from_queue_key(queue_key, connection=connection)
            try:
                job = yield from cls.job_class.fetch(
                    job_id, connection=connection)
            except NoSuchJobError:
                # Silently pass on jobs that don't exist (anymore),
                # and continue in the look
                continue
            except UnpickleError as e:
                # Attach queue information on the exception for
                # improved error reporting
                e.job_id = job_id
                e.queue = queue
                raise e
            return job, queue
        return None, None

    def __eq__(self, other):

        return self.name == other.name


class FailedQueue(Queue):
    """Special queue for failed asynchronous jobs."""

    def __init__(self, connection=None):

        super().__init__(JobStatus.FAILED, connection=connection)
