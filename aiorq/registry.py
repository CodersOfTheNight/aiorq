"""
    aiorq.registry
    ~~~~~~~~~~~~~~

    This module contain Redis based job registry.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.registry module written by Vincent
# Driessen and released under 2-clause BSD license.

import asyncio

from rq.compat import as_text
from rq.job import JobStatus
from rq.utils import current_timestamp

from .connections import resolve_connection
from .exceptions import NoSuchJobError
from .job import Job
from .queue import FailedQueue


class BaseRegistry:
    """Base implementation of a job registry, implemented in Redis sorted set.

    Each job is stored as a key in the registry, scored by expiration
    time (unix timestamp).
    """

    def __init__(self, name='default', connection=None):

        self.name = name
        self.connection = resolve_connection(connection)

    def __len__(self):

        raise RuntimeError('Do not use `len` on asynchronous registries'
                           ' (use registry.count instead).')

    @property
    @asyncio.coroutine
    def count(self):
        """Returns the number of jobs in this registry."""

        yield from self.cleanup()
        return (yield from self.connection.zcard(self.key))

    @asyncio.coroutine
    def add(self, job, ttl=0, pipeline=None):
        """Adds a job to a registry with expiry time of now + ttl."""

        score = ttl if ttl < 0 else current_timestamp() + ttl
        connection = pipeline if pipeline else self.connection
        coroutine = connection.zadd(self.key, score, job.id)
        if not pipeline:
            return (yield from coroutine)

    @asyncio.coroutine
    def remove(self, job, pipeline=None):
        """Removes a job from registry."""

        connection = pipeline if pipeline else self.connection
        coroutine = connection.zrem(self.key, job.id)
        if not pipeline:
            return (yield from coroutine)

    @asyncio.coroutine
    def get_expired_job_ids(self, timestamp=None):
        """Returns job ids whose score are less than current timestamp.

        Returns ids for jobs with an expiry time earlier than
        timestamp, specified as seconds since the Unix
        epoch. timestamp defaults to call time if unspecified.
        """

        score = timestamp if timestamp is not None else current_timestamp()
        return [as_text(job_id) for job_id in
                (yield from self.connection.zrangebyscore(self.key, 0, score))]

    @asyncio.coroutine
    def get_job_ids(self, start=0, end=-1):
        """Returns list of all job ids."""

        yield from self.cleanup()
        return [as_text(job_id) for job_id in
                (yield from self.connection.zrange(self.key, start, end))]


class StartedJobRegistry(BaseRegistry):
    """Registry of currently executing jobs.

    Each queue maintains a StartedJobRegistry. Jobs in this registry
    are ones that are currently being executed.

    Jobs are added to registry right before they are executed and
    removed right after completion (success or failure).

    """

    def __init__(self, name='default', connection=None):
        super().__init__(name, connection)
        self.key = 'rq:wip:{0}'.format(name)

    @asyncio.coroutine
    def cleanup(self, timestamp=None):
        """Remove expired jobs from registry and add them to FailedQueue.

        Removes jobs with an expiry time earlier than timestamp,
        specified as seconds since the Unix epoch. timestamp defaults
        to call time if unspecified. Removed jobs are added to the
        global failed job queue.
        """

        score = timestamp if timestamp else current_timestamp()
        job_ids = yield from self.get_expired_job_ids(score)

        if job_ids:
            failed_queue = FailedQueue(connection=self.connection)

            for job_id in job_ids:
                multi = self.connection.multi_exec()
                try:
                    job = yield from Job.fetch(
                        job_id, connection=self.connection)
                    yield from job.set_status(JobStatus.FAILED)
                    yield from job.save(pipeline=multi)
                    yield from failed_queue.push_job_id(job_id, pipeline=multi)
                except NoSuchJobError:
                    pass

                multi.zremrangebyscore(self.key, 0, score)
                yield from multi.execute()

        return job_ids


class FinishedJobRegistry(BaseRegistry):
    """Registry of jobs that have been completed.

    Jobs are added to this registry after they have successfully
    completed for monitoring purposes.
    """

    def __init__(self, name='default', connection=None):
        super().__init__(name, connection)
        self.key = 'rq:finished:{0}'.format(name)

    @asyncio.coroutine
    def cleanup(self, timestamp=None):
        """Remove expired jobs from registry.

        Removes jobs with an expiry time earlier than timestamp,
        specified as seconds since the Unix epoch. timestamp defaults
        to call time if unspecified.
        """

        score = timestamp if timestamp else current_timestamp()
        yield from self.connection.zremrangebyscore(self.key, 0, score)


class DeferredJobRegistry(BaseRegistry):
    """Registry of deferred jobs (waiting for another job to finish)."""

    def __init__(self, name='default', connection=None):
        super().__init__(name, connection)
        self.key = 'rq:deferred:{0}'.format(name)

    @asyncio.coroutine
    def cleanup(self):
        """This method is only here to prevent errors because this method is
        automatically called by `count()` and `get_job_ids()` methods
        implemented in BaseRegistry.
        """

        pass


@asyncio.coroutine
def clean_registries(queue):
    """Cleans StartedJobRegistry and FinishedJobRegistry of a queue."""

    name = queue.name
    connection = queue.connection

    registry = FinishedJobRegistry(name, connection)
    yield from registry.cleanup()
    registry = StartedJobRegistry(name, connection)
    yield from registry.cleanup()
