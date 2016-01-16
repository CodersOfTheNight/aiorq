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
import os
import signal
import socket
import sys
import traceback
from datetime import timedelta

from rq.compat import text_type, string_types
from rq.defaults import DEFAULT_RESULT_TTL, DEFAULT_WORKER_TTL
from rq.logutils import setup_loghandlers
from rq.job import JobStatus
from rq.worker import (WorkerStatus, StopRequested, signal_name,
                       green, blue, yellow)
from rq.utils import ensure_list, import_attribute, utcformat, utcnow, as_text

from .connections import resolve_connection
from .exceptions import DequeueTimeout, JobTimeoutException
from .job import Job
from .queue import Queue, get_failed_queue
from .registry import clean_registries, StartedJobRegistry, FinishedJobRegistry
from .suspension import is_suspended


logger = logging.getLogger(__name__)


class Worker:
    """asyncio compatible worker."""

    redis_worker_namespace_prefix = 'rq:worker:'
    redis_workers_keys = 'rq:workers'
    queue_class = Queue
    job_class = Job

    def __init__(self, queues, name=None, default_result_ttl=None,
                 connection=None, exception_handlers=None,
                 default_worker_ttl=None, job_class=None):
        self.connection = resolve_connection(connection)

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
        self._stop_requested = False
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

    def validate_queues(self):
        """Sanity check for the given queues."""

        for queue in self.queues:
            if not isinstance(queue, self.queue_class):
                raise TypeError('{} is not of type {} or text type'
                                .format(queue, self.queue_class))

    def push_exc_handler(self, handler):
        """Pushes an exception handler onto the exc handler stack."""

        self._exc_handlers.append(handler)

    @asyncio.coroutine
    def move_to_failed_queue(self, job, *exc_info):
        """Default exception handler.

        Move the job to the failed queue.
        """

        exc_string = ''.join(traceback.format_exception(*exc_info))
        logger.warning('Moving job to "%s" queue', self.failed_queue)
        yield from self.failed_queue.quarantine(job, exc_info=exc_string)

    @asyncio.coroutine
    def register_birth(self):
        """Registers its own birth."""

        logger.debug('Registering birth of worker {0}'.format(self.name))
        if (yield from self.connection.exists(self.key)) and \
                not (yield from self.connection.hexists(self.key, 'death')):
            msg = 'There exists an active worker named {0!r} already'
            raise ValueError(msg.format(self.name))
        key = self.key
        queues = ','.join(self.queue_names())
        pipe = self.connection.multi_exec()
        pipe.delete(key)
        pipe.hset(key, 'birth', utcformat(utcnow()))
        pipe.hset(key, 'queues', queues)
        pipe.sadd(self.redis_workers_keys, key)
        pipe.expire(key, self.default_worker_ttl)
        yield from pipe.execute()

    @asyncio.coroutine
    def register_death(self):
        """Registers its own death."""

        logger.debug('Registering death')
        # We cannot use self.state = 'dead' here, because that would
        # rollback the pipeline
        pipe = self.connection.multi_exec()
        pipe.srem(self.redis_workers_keys, self.key)
        pipe.hset(self.key, 'death', utcformat(utcnow()))
        pipe.expire(self.key, 60)
        yield from pipe.execute()

    @asyncio.coroutine
    def set_state(self, state, pipeline=None):

        self._state = state
        pipe = pipeline if pipeline else self.connection
        coroutine = pipe.hset(self.key, 'state', state)
        if not pipeline:
            yield from coroutine

    @asyncio.coroutine
    def check_for_suspension(self, burst, *, loop=None):
        """Check to see if workers have been suspended by `rq suspend`"""

        before_state = None
        notified = False

        while not self._stop_requested and \
                (yield from is_suspended(self.connection)):

            if burst:
                logger.info('Suspended in burst mode, exiting')
                logger.info(
                    'Note: There could still be unfinished jobs on the queue')
                raise StopRequested

            if not notified:
                logger.info('Worker suspended, run `rq resume` to resume')
                before_state = self.get_state()
                yield from self.set_state(WorkerStatus.SUSPENDED)
                notified = True
            yield from asyncio.sleep(1)

        if before_state:
            yield from self.set_state(before_state)

    @asyncio.coroutine
    def work(self, burst=False, *, loop=None):
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
        logger.info("RQ worker %s started", self.key)
        yield from self.set_state(WorkerStatus.STARTED)

        try:
            while True:
                try:
                    yield from self.check_for_suspension(burst, loop=loop)

                    if self.should_run_maintenance_tasks:
                        yield from self.clean_registries()

                    if self._stop_requested:
                        logger.info('Stopping on request')
                        break

                    if burst:
                        timeout = None
                    else:
                        timeout = max(1, self.default_worker_ttl - 60)

                    result = yield from self.dequeue_job_and_maintain_ttl(
                        timeout)

                    if result is None:
                        if burst:
                            logger.info(
                                'RQ worker %s done, quitting', self.key)
                        break
                except StopRequested:
                    break

                job, queue = result
                yield from self.execute_job(job, loop=loop)
                yield from self.heartbeat()

                if (yield from job.get_status()) == JobStatus.FINISHED:
                    yield from queue.enqueue_dependents(job)

                did_perform_work = True

        finally:
            yield from self.register_death()
        return did_perform_work

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.
        """

        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def request_stop(self, signum, frame):
        """Stops the current worker loop but waits for child processes to end
        gracefully (warm shutdown).
        """

        logger.debug('Got signal %s', signal_name(signum))

        signal.signal(signal.SIGINT, self.request_force_stop)
        signal.signal(signal.SIGTERM, self.request_force_stop)

        logger.warning('Warm shut down requested')

        # If shutdown is requested in the middle of a job, wait until
        # finish before shutting down
        if self.get_state() == 'busy':
            self._stop_requested = True
            logger.debug('Stopping after current horse is finished.  '
                         'Press Ctrl+C again for a cold shutdown.')
        else:
            raise StopRequested

    def queue_names(self):
        """Returns the queue names of this worker's queues."""

        return [q.name for q in self.queues]

    @property
    def name(self):
        """Returns the name of the worker, under which it is registered to the
        monitoring system.

        By default, the name of the worker is constructed from the
        current (short) host name and the current PID.
        """

        if self._name is None:
            hostname = socket.gethostname()
            shortname, _, _ = hostname.partition('.')
            self._name = '{0}.{1}'.format(shortname, self.pid)
        return self._name

    @property
    def pid(self):
        """The current process ID."""

        return os.getpid()

    @property
    def key(self):
        """Returns the worker's Redis hash key."""

        return self.redis_worker_namespace_prefix + self.name

    @property
    def should_run_maintenance_tasks(self):
        """Maintenance tasks should run on first startup or every hour."""

        if self.last_cleaned_at is None:
            return True
        if (utcnow() - self.last_cleaned_at) > timedelta(hours=1):
            return True
        return False

    @asyncio.coroutine
    def clean_registries(self):
        """Runs maintenance jobs on each Queue's registries."""

        for queue in self.queues:
            logger.info('Cleaning registries for queue: %s', queue.name)
            yield from clean_registries(queue)
        self.last_cleaned_at = utcnow()

    @asyncio.coroutine
    def dequeue_job_and_maintain_ttl(self, timeout):

        result = None
        qnames = self.queue_names()

        yield from self.set_state(WorkerStatus.IDLE)
        logger.info('')
        logger.info('*** Listening on %s...', green(', '.join(qnames)))

        while True:
            yield from self.heartbeat()

            try:
                result = yield from self.queue_class.dequeue_any(
                    self.queues, timeout, connection=self.connection)
                if result is not None:
                    job, queue = result
                    logger.info('%s: %s (%s)', green(queue.name),
                                blue(job.description), job.id)

                break
            except DequeueTimeout:
                pass

        yield from self.heartbeat()
        return result

    @asyncio.coroutine
    def heartbeat(self, timeout=0, pipeline=None):
        """Specifies a new worker timeout, typically by extending the
        expiration time of the worker, effectively making this a
        "heartbeat" to not expire the worker until the timeout passes.

        The next heartbeat should come before this time, or the worker
        will die (at least from the monitoring dashboards).

        The effective timeout can never be shorter than
        default_worker_ttl, only larger.
        """

        timeout = max(timeout, self.default_worker_ttl)
        pipe = pipeline if pipeline else self.connection
        logger.debug('Sent heartbeat to prevent worker timeout.  '
                     'Next one should arrive within %s seconds.', timeout)
        coroutine = pipe.expire(self.key, timeout)
        if not pipeline:
            yield from coroutine

    @asyncio.coroutine
    def execute_job(self, job, *, loop=None):
        """Send a job into asyncio event loop."""

        yield from self.set_state('busy')
        yield from self.perform_job(job, loop=loop)
        yield from self.set_state('idle')

    @asyncio.coroutine
    def perform_job(self, job, *, loop=None):
        """Performs the actual work of a job.

        Will/should only be called inside the work horse's process.
        """

        yield from self.prepare_job_execution(job)

        pipe = self.connection.multi_exec()
        started_job_registry = StartedJobRegistry(job.origin, self.connection)

        try:
            timeout = job.timeout or self.queue_class.DEFAULT_TIMEOUT
            try:
                rv = yield from asyncio.wait_for(
                    job.perform(), timeout, loop=loop)
            except asyncio.TimeoutError as error:
                raise JobTimeoutException from error

            # Pickle the result in the same try-except block since we
            # need to use the same exc handling when pickling fails
            yield from self.set_current_job_id(None, pipeline=pipe)

            result_ttl = job.get_result_ttl(self.default_result_ttl)
            if result_ttl != 0:
                job.ended_at = utcnow()
                job._status = JobStatus.FINISHED
                yield from job.save(pipeline=pipe)

                finished_job_registry = FinishedJobRegistry(
                    job.origin, self.connection)
                yield from finished_job_registry.add(job, result_ttl, pipe)

            yield from job.cleanup(result_ttl, pipeline=pipe)
            yield from started_job_registry.remove(job, pipeline=pipe)

            yield from pipe.execute()

        except Exception:
            yield from job.set_status(JobStatus.FAILED, pipeline=pipe)
            yield from started_job_registry.remove(job, pipeline=pipe)
            yield from self.set_current_job_id(None, pipeline=pipe)
            try:
                yield from pipe.execute()
            except Exception:
                # Ensure that custom exception handlers are called
                # even if Redis is down
                pass
            yield from self.handle_exception(job, *sys.exc_info())
            return False

        logger.info('%s: %s (%s)', green(job.origin), blue('Job OK'), job.id)
        if rv:
            log_result = "{!r}".format(as_text(text_type(rv)))
            logger.debug('Result: %s', yellow(log_result))

        if result_ttl == 0:
            logger.info('Result discarded immediately')
        elif result_ttl > 0:
            logger.info('Result is kept for %s seconds', result_ttl)
        else:
            logger.warning(
                'Result will never expire, clean up result key manually')

        return True

    @asyncio.coroutine
    def prepare_job_execution(self, job):
        """Performs misc bookkeeping like updating states prior to job
        execution.
        """

        timeout = (job.timeout or 180) + 60

        pipe = self.connection.multi_exec()
        yield from self.set_state(WorkerStatus.BUSY, pipeline=pipe)
        yield from self.set_current_job_id(job.id, pipeline=pipe)
        yield from self.heartbeat(timeout, pipeline=pipe)
        registry = StartedJobRegistry(job.origin, self.connection)
        yield from registry.add(job, timeout, pipeline=pipe)
        yield from job.set_status(JobStatus.STARTED, pipeline=pipe)
        pipe.hset(job.key, 'started_at', utcformat(utcnow()))
        yield from pipe.execute()

    @asyncio.coroutine
    def set_current_job_id(self, job_id, pipeline=None):

        pipe = pipeline if pipeline else self.connection

        if job_id is None:
            coroutine = pipe.hdel(self.key, 'current_job')
        else:
            coroutine = pipe.hset(self.key, 'current_job', job_id)

        if not pipeline:
            yield from coroutine

    @asyncio.coroutine
    def get_current_job_id(self, pipeline=None):

        connection = pipeline if pipeline else self.connection
        coroutine = connection.hget(self.key, 'current_job')
        if not pipeline:
            return as_text((yield from coroutine))

    @asyncio.coroutine
    def get_current_job(self):
        """Returns the job id of the currently executing job."""

        job_id = yield from self.get_current_job_id()

        if job_id is None:
            return None

        return (yield from self.job_class.fetch(job_id, self.connection))

    @asyncio.coroutine
    def handle_exception(self, job, *exc_info):
        """Walks the exception handler stack to delegate exception handling."""

        logger.exception('Coroutine error', extra={
            'func': job.func_name,  # FIXME: we can fuckup with UnpickleError
            'arguments': job.args,
            'kwargs': job.kwargs,
            'queue': job.origin,
        })

        for handler in reversed(self._exc_handlers):
            logger.debug('Invoking exception handler %s', handler)
            fallthrough = yield from handler(job, *exc_info)

            # Only handlers with explicit return values should disable
            # further exc handling, so interpret a None return value
            # as True.
            if fallthrough is None:
                fallthrough = True

            if not fallthrough:
                break
