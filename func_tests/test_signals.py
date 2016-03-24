from os import environ
from os.path import abspath, dirname, join
from signal import SIGTERM
from subprocess import Popen, PIPE
from threading import Thread
from time import sleep

import pytest
import rq


pytestmark = pytest.mark.usefixtures('flush_redis')


# Helpers.


def run_worker(*, stop_with=None):
    """Run aiorq worker instance listening the `foo` queue."""

    cmd = ['aiorq', 'worker', 'foo']
    fixtures = join(dirname(dirname(abspath(__file__))), 'fixtures')
    environment = environ.copy()
    environment['PYTHONPATH'] = fixtures
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, env=environment)

    for num, signum in enumerate(stop_with or [], 1):
        timeout = 0.5 * num
        Thread(target=kill_worker, args=(proc, signum, timeout))

    stdout, stderr = proc.communicate()
    print(stdout.decode())
    print(stderr.decode())

    return proc


def kill_worker(process, signum, timeout):
    """Wait for timeout and send signal to the process."""

    sleep(timeout)
    process.send_signal(signum)


# Tests.


def test_idle_worker_warm_shutdown():
    """Worker with no ongoing job receiving single SIGTERM signal and
    shutting down.

    """

    proc = run_worker(stop_with=[SIGTERM])
    assert proc.returncode == 0


def test_working_worker_warm_shutdown():
    """Worker with an ongoing job receiving single SIGTERM signal,
    allowing job to finish then shutting down.

    """

    with rq.Connection():
        queue = rq.Queue('foo')

    job = queue.enqueue('fixtures.long_running_job')

    run_worker(stop_with=[SIGTERM])

    assert job.is_finished
    assert 'Done sleeping...' == job.result


def test_working_worker_cold_shutdown():
    """Worker with an ongoing job receiving double SIGTERM signal and
    shutting down immediately.

    """

    with rq.Connection():
        queue = rq.Queue('foo')

    job = queue.enqueue('fixtures.long_running_job')

    run_worker(stop_with=[SIGTERM, SIGTERM])

    assert not job.is_finished
