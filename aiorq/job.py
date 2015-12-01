"""
    aiorq.job
    ~~~~~~~~~~~~~~~~~

    This module implement serializable deferred jobs.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio

from rq.job import Job as SynchronousJob, UNEVALUATED, loads
from rq.utils import utcnow

from .connections import resolve_connection


class Job(SynchronousJob):
    """A Job is just convenient data structure to pass around (meta) data."""

    @asyncio.coroutine
    def set_status(self, status, pipeline=None):
        """Set job status asynchronously."""

        # TODO: yield from if pipeline is none
        self._status = status
        connection = pipeline
        connection.hset(self.key, 'status', self._status)

    @classmethod
    @asyncio.coroutine
    def exists(cls, job_id, connection=None):
        """Returns whether a job hash exists for the given job ID."""

        conn = resolve_connection(connection)
        return (yield from conn.exists(cls.key_for(job_id)))

    def __init__(self, id=None, connection=None):

        self.connection = resolve_connection(connection)
        self._id = id
        self.created_at = utcnow()
        self._data = UNEVALUATED
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        self._args = UNEVALUATED
        self._kwargs = UNEVALUATED
        self.description = None
        self.origin = None
        self.enqueued_at = None
        self.started_at = None
        self.ended_at = None
        self._result = None
        self.exc_info = None
        self.timeout = None
        self.result_ttl = None
        self.ttl = None
        self._status = None
        self._dependency_id = None
        self.meta = {}

    @property
    @asyncio.coroutine
    def result(self):
        """Returns the return value of the job.

        Initially, right after enqueueing a job, the return value will be
        None.  But when the job has been executed, and had a return value or
        exception, this will return that value or exception.

        Note that, when the job has no return value (i.e. returns None), the
        ReadOnlyJob object is useless, as the result won't be written back to
        Redis.

        Also note that you cannot draw the conclusion that a job has _not_
        been executed when its return value is None, since return values
        written back to Redis will expire after a given amount of time (500
        seconds by default).
        """

        if self._result is None:
            rv = yield from self.connection.hget(self.key, 'result')
            if rv is not None:
                self._result = loads(rv)
        return self._result

    @asyncio.coroutine
    def save(self, pipeline=None):
        """Persists the current job instance to its corresponding Redis key."""

        # TODO: yield from if pipeline is none
        key = self.key
        connection = pipeline
        fields = (field
                  for item_fields in self.to_dict().items()
                  for field in item_fields)
        connection.hmset(key, *fields)
        # TODO: don't pass connection if pipeline is none
        yield from self.cleanup(self.ttl, pipeline=connection)

    @asyncio.coroutine
    def cleanup(self, ttl=None, pipeline=None):
        """Prepare job for eventual deletion (if needed).

        This method is usually called after successful execution. How
        long we persist the job and its result depends on the value of
        ttl:

        - If ttl is 0, cleanup the job immediately.
        - If it's a positive number, set the job to expire in X seconds.
        - If ttl is negative, don't set an expiry to it (persist forever)
        """

        if ttl == 0:
            yield from self.delete()
        elif not ttl:
            return
        elif ttl > 0:
            # TODO: yield from if pipeline is none
            connection = pipeline
            connection.expire(self.key, ttl)

    @asyncio.coroutine
    def delete(self, pipeline=None):
        """Cancels the job and deletes the job hash from Redis."""

        yield from self.cancel()
        # TODO: yield from if pipeline is none
        connection = pipeline
        connection.delete(self.key)
        connection.delete(self.dependents_key)

    @asyncio.coroutine
    def cancel(self):
        """Cancels the given job, which will prevent the job from ever being
        ran (or inspected).

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.
        """

        if self.origin:
            from .queue import Queue
            pipeline = self.connection.pipeline()
            queue = Queue(name=self.origin, connection=self.connection)
            yield from queue.remove(self, pipeline=pipeline)
            yield from pipeline.execute()
