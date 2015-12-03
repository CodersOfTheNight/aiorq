"""
    aiorq.registry
    ~~~~~~~~~~~~~~

    This module contain Redis based job registry.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio

from rq.compat import as_text
from rq.utils import current_timestamp

from .connections import resolve_connection


class BaseRegistry(object):
    """Base implementation of a job registry, implemented in Redis sorted set.

    Each job is stored as a key in the registry, scored by expiration
    time (unix timestamp).
    """

    def __init__(self, name='default', connection=None):
        self.name = name
        self.connection = resolve_connection(connection)

    @asyncio.coroutine
    def add(self, job, ttl=0, pipeline=None):
        """Adds a job to a registry with expiry time of now + ttl."""

        score = ttl if ttl < 0 else current_timestamp() + ttl
        # TODO: support pipeline mode
        # if pipeline is not None:
        #     return pipeline.zadd(self.key, score, job.id)

        return (yield from self.connection.zadd(self.key, score, job.id))

    @asyncio.coroutine
    def get_job_ids(self, start=0, end=-1):
        """Returns list of all job ids."""

        yield from self.cleanup()
        return [as_text(job_id) for job_id in
                (yield from self.connection.zrange(self.key, start, end))]


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
