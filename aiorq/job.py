"""
    aiorq.job
    ~~~~~~~~~

    This module implement serializable deferred jobs.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.job module written by Vincent Driessen
# and released under 2-clause BSD license.

import asyncio

from .protocol import job_status
from .specs import UNEVALUATED, JobStatus
from .utils import utcnow


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

    return None


def dumps(job):
    """Create protocol job spec from job instance."""

    return None, {}


def loads(id, spec):
    """Create job instance from job id and protocol job spec."""

    return None


class Job:
    """A Job is just convenient data structure to pass around (meta) data."""

    @asyncio.coroutine
    def get_status(self):
        """Get job status asynchronously."""

        self._status = yield from job_status(self.connection, self.id)
        return self._status.decode()

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

    def __init__(self, id=None, connection=None):

        self.connection = connection
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
