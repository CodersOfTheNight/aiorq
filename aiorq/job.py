"""
    aiorq.job
    ~~~~~~~~~

    This module implement serializable deferred jobs.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.job module written by Vincent Driessen
# and released under 2-clause BSD license.

import asyncio

from rq.compat import as_text, decode_redis_hash
from rq.job import (Job as SynchronousJob, UNEVALUATED, loads,
                    unpickle, JobStatus)
from rq.job import dumps        # noqa
from rq.local import LocalStack
from rq.utils import utcnow, utcparse

from .connections import resolve_connection
from .exceptions import NoSuchJobError


@asyncio.coroutine
def cancel_job(job_id, connection=None):
    """Cancels the job with the given job ID, preventing execution.

    Discards any job info (i.e. it can't be requeued later).
    """

    job = Job(job_id, connection=connection)
    yield from job.refresh()
    yield from job.cancel()


@asyncio.coroutine
def requeue_job(job_id, connection=None):
    """Requeues the job with the given job ID.

    The job ID should be refer to a failed job (i.e. it should be on
    the failed queue).  If no such (failed) job exists, a
    NoSuchJobError is raised.
    """

    from .queue import get_failed_queue
    fq = get_failed_queue(connection=connection)
    yield from fq.requeue(job_id)


@asyncio.coroutine
def get_current_job(connection=None):
    """Returns the Job instance that is currently being executed.

    If this function is invoked from outside a job context, None is
    returned.
    """

    job_id = _job_stack.top
    if job_id is None:
        return None
    return (yield from Job.fetch(job_id, connection=connection))


class Job(SynchronousJob):
    """A Job is just convenient data structure to pass around (meta) data."""

    @asyncio.coroutine
    def get_status(self):
        """Get job status asynchronously."""

        self._status = as_text(
            (yield from self.connection.hget(self.key, 'status')))
        return self._status

    @asyncio.coroutine
    def set_status(self, status, pipeline=None):
        """Set job status asynchronously."""

        self._status = status
        connection = pipeline if pipeline else self.connection
        coroutine = connection.hset(self.key, 'status', self._status)
        if not pipeline:
            yield from coroutine

    @property
    @asyncio.coroutine
    def is_finished(self):

        return (yield from self.get_status()) == JobStatus.FINISHED

    @property
    @asyncio.coroutine
    def is_queued(self):

        return (yield from self.get_status()) == JobStatus.QUEUED

    @property
    @asyncio.coroutine
    def is_failed(self):

        return (yield from self.get_status()) == JobStatus.FAILED

    @property
    @asyncio.coroutine
    def is_started(self):

        return (yield from self.get_status()) == JobStatus.STARTED

    @classmethod
    @asyncio.coroutine
    def exists(cls, job_id, connection=None):
        """Returns whether a job hash exists for the given job ID."""

        conn = resolve_connection(connection)
        return (yield from conn.exists(cls.key_for(job_id)))

    @property
    @asyncio.coroutine
    def dependency(self):
        """Returns a job's dependency.

        To avoid repeated Redis fetches, we cache job.dependency as
        job._dependency.
        """

        if self._dependency_id is None:
            return None
        if hasattr(self, '_dependency'):
            return self._dependency
        job = yield from Job.fetch(
            self._dependency_id, connection=self.connection)
        yield from job.refresh()
        self._dependency = job
        return job

    @classmethod
    @asyncio.coroutine
    def fetch(cls, id, connection=None):
        """Fetches a persisted job from its corresponding Redis key and
        instantiates it.
        """

        job = cls(id, connection=connection)
        yield from job.refresh()
        return job

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

    # Persistence
    @asyncio.coroutine
    def refresh(self):
        """Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key
        exists.
        """

        key = self.key
        obj = decode_redis_hash((yield from self.connection.hgetall(key)))
        if len(obj) == 0:
            raise NoSuchJobError('No such job: {0}'.format(key))

        to_date = lambda text: utcparse(as_text(text)) if text else None

        try:
            self.data = obj['data']
        except KeyError:
            raise NoSuchJobError('Unexpected job format: {0}'.format(obj))

        self.created_at = to_date(obj.get('created_at'))
        self.origin = as_text(obj.get('origin'))
        self.description = as_text(obj.get('description'))
        self.enqueued_at = to_date(obj.get('enqueued_at'))
        self.started_at = to_date(obj.get('started_at'))
        self.ended_at = to_date(obj.get('ended_at'))
        self._result = (unpickle(obj.get('result'))
                        if obj.get('result') else None)
        self.exc_info = obj.get('exc_info')
        self.timeout = int(obj.get('timeout')) if obj.get('timeout') else None
        self.result_ttl = (int(obj.get('result_ttl'))
                           if obj.get('result_ttl') else None)
        self._status = as_text(obj.get('status')
                               if obj.get('status') else None)
        self._dependency_id = as_text(obj.get('dependency_id', None))
        self.ttl = int(obj.get('ttl')) if obj.get('ttl') else None
        self.meta = unpickle(obj.get('meta')) if obj.get('meta') else {}

    @asyncio.coroutine
    def save(self, pipeline=None):
        """Persists the current job instance to its corresponding Redis key."""

        key = self.key
        connection = pipeline if pipeline else self.connection
        fields = (field
                  for item_fields in self.to_dict().items()
                  for field in item_fields)
        coroutine = connection.hmset(key, *fields)
        if not pipeline:
            yield from coroutine
        kwargs = {'pipeline': connection} if pipeline else {}
        yield from self.cleanup(self.ttl, **kwargs)

    @asyncio.coroutine
    def register_dependency(self, pipeline=None):
        """Jobs may have dependencies.  Jobs are enqueued only if the job they
        depend on is successfully performed.  We record this relation
        as a reverse dependency (a Redis set), with a key that looks
        something like:

            rq:job:job_id:dependents = {'job_id_1', 'job_id_2'}

        This method adds the job in its dependency's dependents set
        and adds the job to DeferredJobRegistry.
        """

        from .registry import DeferredJobRegistry
        registry = DeferredJobRegistry(self.origin, connection=self.connection)
        yield from registry.add(self, pipeline=pipeline)

        connection = pipeline if pipeline else self.connection
        key = Job.dependents_key_for(self._dependency_id)
        coroutine = connection.sadd(key, self.id)
        if not pipeline:
            yield from coroutine

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
            connection = pipeline if pipeline else self.connection
            coroutine = connection.expire(self.key, ttl)
            if not pipeline:
                yield from coroutine

    @asyncio.coroutine
    def delete(self, pipeline=None):
        """Cancels the job and deletes the job hash from Redis."""

        yield from self.cancel()
        connection = pipeline if pipeline else self.connection
        coroutine = connection.delete(self.key)
        if not pipeline:
            yield from coroutine
        coroutine = connection.delete(self.dependents_key)
        if not pipeline:
            yield from coroutine

    # Job execution
    @asyncio.coroutine
    def perform(self):
        """Invokes the job function with the job arguments."""

        yield from self.connection.persist(self.key)
        self.ttl = -1
        _job_stack.push(self.id)
        try:
            # TODO: yield from here since this will run inside event loop.
            self._result = self.func(*self.args, **self.kwargs)
        finally:
            assert self.id == _job_stack.pop()
        return self._result

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


_job_stack = LocalStack()
