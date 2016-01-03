"""
    aiorq.worker
    ~~~~~~~~~~~~

    This module implement asyncio compatible worker

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.worker module written by Vincent
# Driessen and released under 2-clause BSD license.

import asyncio
import logging

from rq.compat import text_type, string_types
from rq.defaults import DEFAULT_RESULT_TTL, DEFAULT_WORKER_TTL
from rq.logutils import setup_loghandlers
from rq.job import JobStatus
from rq.worker import Worker as SynchronousWorker, WorkerStatus, StopRequested
from rq.utils import ensure_list, import_attribute, utcformat, utcnow

from .connections import get_current_connection
from .queue import Queue, get_failed_queue
from .job import Job
from .suspension import is_suspended


logger = logging.getLogger(__name__)


class Worker(SynchronousWorker):
    """asyncio compatible worker"""

    queue_class = Queue
    job_class = Job

    def __init__(self, queues, name=None, default_result_ttl=None,
                 connection=None, exception_handlers=None,
                 default_worker_ttl=None, job_class=None):
        if connection is None:
            connection = get_current_connection()
        self.connection = connection

        queues = [self.queue_class(name=q) if isinstance(q, text_type) else q
                  for q in ensure_list(queues)]
        self._name = name
        self.queues = queues
        self.validate_queues()
        self._exc_handlers = []

        if default_result_ttl is None:
            default_result_ttl = DEFAULT_RESULT_TTL
        self.default_result_ttl = default_result_ttl

        if default_worker_ttl is None:
            default_worker_ttl = DEFAULT_WORKER_TTL
        self.default_worker_ttl = default_worker_ttl

        self._state = 'starting'
        self._is_horse = False
        self._horse_pid = 0
        self._stop_requested = False
        self.log = logger
        self.failed_queue = get_failed_queue(connection=self.connection)
        self.last_cleaned_at = None

        # By default, push the "move-to-failed-queue" exception handler onto
        # the stack
        if exception_handlers is None:
            self.push_exc_handler(self.move_to_failed_queue)
        elif isinstance(exception_handlers, list):
            for h in exception_handlers:
                self.push_exc_handler(h)
        elif exception_handlers is not None:
            self.push_exc_handler(exception_handlers)

        if job_class is not None:
            if isinstance(job_class, string_types):
                job_class = import_attribute(job_class)
            self.job_class = job_class

    @asyncio.coroutine
    def register_birth(self):
        """Registers its own birth."""

        self.log.debug('Registering birth of worker {0}'.format(self.name))
        if (yield from self.connection.exists(self.key)) and \
                not (yield from self.connection.hexists(self.key, 'death')):
            msg = 'There exists an active worker named {0!r} already'
            raise ValueError(msg.format(self.name))
        key = self.key
        queues = ','.join(self.queue_names())
        with (yield from self.connection.pipeline()) as pipe:
            pipe.delete(key)
            pipe.hset(key, 'birth', utcformat(utcnow()))
            pipe.hset(key, 'queues', queues)
            pipe.sadd(self.redis_workers_keys, key)
            pipe.expire(key, self.default_worker_ttl)
            yield from pipe.execute()

    @asyncio.coroutine
    def register_death(self):
        """Registers its own death."""

        self.log.debug('Registering death')
        with (yield from self.connection.pipeline()) as pipe:
            # We cannot use self.state = 'dead' here, because that
            # would rollback the pipeline
            pipe.srem(self.redis_workers_keys, self.key)
            pipe.hset(self.key, 'death', utcformat(utcnow()))
            pipe.expire(self.key, 60)
            yield from pipe.execute()

    @asyncio.coroutine
    def set_state(self, state, pipeline=None):
        self._state = state
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, 'state', state)

    @asyncio.coroutine
    def check_for_suspension(self, burst):
        """Check to see if workers have been suspended by `rq suspend`"""

        before_state = None
        notified = False

        while not self._stop_requested and \
                (yield from is_suspended(self.connection)):

            if burst:
                self.log.info('Suspended in burst mode, exiting')
                self.log.info('Note: There could still be unfinished jobs on the queue')
                raise StopRequested

            if not notified:
                self.log.info('Worker suspended, run `rq resume` to resume')
                before_state = self.get_state()
                self.set_state(WorkerStatus.SUSPENDED)
                notified = True
            time.sleep(1)

        if before_state:
            self.set_state(before_state)

    @asyncio.coroutine
    def work(self, burst=False):
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.
        When all queues are empty, block and wait for new jobs to
        arrive on any of the queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.
        """

        setup_loghandlers()
        self._install_signal_handlers()

        did_perform_work = False
        yield from self.register_birth()
        self.log.info("RQ worker {0!r} started".format(self.key))
        yield from self.set_state(WorkerStatus.STARTED)

        try:
            while True:
                try:
                    yield from self.check_for_suspension(burst)

                    if self.should_run_maintenance_tasks:
                        self.clean_registries()

                    if self._stop_requested:
                        self.log.info('Stopping on request')
                        break

                    timeout = None if burst else max(1, self.default_worker_ttl - 60)

                    result = self.dequeue_job_and_maintain_ttl(timeout)
                    if result is None:
                        if burst:
                            self.log.info("RQ worker {0!r} done, quitting".format(self.key))
                        break
                except StopRequested:
                    break

                job, queue = result
                self.execute_job(job)
                self.heartbeat()

                if job.get_status() == JobStatus.FINISHED:
                    queue.enqueue_dependents(job)

                did_perform_work = True

        finally:
            if not self.is_horse:
                yield from self.register_death()
        return did_perform_work
