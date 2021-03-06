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
import inspect
import pickle

from .protocol import job_status
from .specs import JobStatus
from .utils import utcformat, utcparse, import_attribute


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

    id = job.id.encode()
    instance = None
    if inspect.ismethod(job.func):
        func_name = job.func.__name__
        instance = job.func.__self__
    elif isinstance(job.func, str):
        func_name = job.func
    elif isinstance(job.func, bytes):
        func_name = job.func.decode()
    elif inspect.isfunction(job.func) or inspect.isbuiltin(job.func):
        func_name = '{}.{}'.format(job.func.__module__, job.func.__name__)
    elif hasattr(job.func, '__call__'):
        func_name = '__call__'
        instance = job.func
    else:
        msg = 'Expected a callable or a string, but got: {}'.format(job.func)
        raise TypeError(msg)
    data = func_name, instance, job.args, job.kwargs
    spec = {}
    spec[b'created_at'] = utcformat(job.created_at)
    if job.enqueued_at:
        spec[b'enqueued_at'] = utcformat(job.enqueued_at)
    spec[b'data'] = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
    spec[b'description'] = job.description.encode()
    if job.status:
        spec[b'status'] = job.status.encode()
    spec[b'origin'] = job.origin.encode()
    spec[b'timeout'] = job.timeout
    spec[b'result_ttl'] = job.result_ttl
    if job.dependency_id:
        spec[b'dependency_id'] = job.dependency_id.encode()
    return id, spec


def loads(id, spec):
    """Create job instance from job id and protocol job spec."""

    job_id = id.decode()
    created_at = utcparse(spec[b'created_at'])
    enqueued_at = utcparse(spec[b'enqueued_at'])
    func_name, instance, args, kwargs = pickle.loads(spec[b'data'])
    if instance:
        func = getattr(instance, func_name)
    else:
        func = import_attribute(func_name)
    description = spec[b'description'].decode()
    status = spec[b'status'].decode()
    origin = spec[b'origin'].decode()
    timeout = spec[b'timeout']
    result_ttl = spec[b'result_ttl']
    job = Job(id=job_id, created_at=created_at,
              enqueued_at=enqueued_at, func=func, args=args,
              kwargs=kwargs, description=description, timeout=timeout,
              result_ttl=result_ttl, status=status, origin=origin)
    return job


def description(func, args, kwargs):
    """String representation of the call."""

    if isinstance(func, str):
        func_name = func
    elif isinstance(func, bytes):
        func_name = func.decode()
    elif inspect.isfunction(func) or inspect.isbuiltin(func):
        func_name = '{}.{}'.format(func.__module__, func.__name__)
    elif not inspect.ismethod(func) and hasattr(func, '__call__'):
        func_name = '__call__'
    else:
        func_name = func.__name__
    args_name = [repr(a) for a in args]
    args_name += ['{}={!r}'.format(k, v) for k, v in kwargs.items()]
    args_name = ', '.join(args_name)
    return '{}({})'.format(func_name, args_name)


class Job:
    """A Job is just convenient data structure to pass around (meta) data."""

    def __init__(self, connection, id, func, args, kwargs, description,
                 timeout, result_ttl, origin, created_at,
                 enqueued_at=None, status=None, dependency_id=None):

        self.connection = connection
        self.id = id
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.description = description
        self.timeout = timeout
        self.result_ttl = result_ttl
        self.origin = origin
        self.created_at = created_at
        self.enqueued_at = enqueued_at  # TODO: don't store in spec if None
        self.status = status  # TODO: don't store in spec if None
        self.dependency_id = dependency_id  # TODO: don't store in spec if None

    @asyncio.coroutine
    def get_status(self):
        """Get job status asynchronously."""

        status = yield from job_status(self.connection, self.id.encode())
        self.status = status.decode()
        return self.status

    @property
    @asyncio.coroutine
    def is_finished(self):

        status = yield from job_status(self.connection, self.id.encode())
        self.status = status.decode()
        return status == JobStatus.FINISHED

    @property
    @asyncio.coroutine
    def is_queued(self):

        status = yield from job_status(self.connection, self.id.encode())
        self.status = status.decode()
        return status == JobStatus.QUEUED

    @property
    @asyncio.coroutine
    def is_failed(self):

        status = yield from job_status(self.connection, self.id.encode())
        self.status = status.decode()
        return status == JobStatus.FAILED

    @property
    @asyncio.coroutine
    def is_started(self):

        status = yield from job_status(self.connection, self.id.encode())
        self.status = status.decode()
        return status == JobStatus.STARTED

    @property
    @asyncio.coroutine
    def is_deferred(self):

        status = yield from job_status(self.connection, self.id.encode())
        self.status = status.decode()
        return status == JobStatus.DEFERRED

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
