from threading import Thread
from signal import SIGTERM
from subprocess import Popen, PIPE
from time import sleep

import pytest
import rq


pytestmark = pytest.mark.usefixtures('flush_redis')


def run_worker():
    """Run aiorq worker instance listening the `foo` queue."""

    proc = Popen(['aiorq', 'worker', 'foo'], stdout=PIPE, stderr=PIPE)
    Thread(target=kill_worker, args=(proc))
    stdout, stderr = proc.communicate()
    print(stdout.decode())
    print(stderr.decode())
    return proc


def kill_worker(process):
    """Wait for the worker to be started over on the main process and kill
    it.

    """

    sleep(0.5)
    process.send_signal(SIGTERM)


def test_idle_worker_warm_shutdown():
    """Worker with no ongoing job receiving single SIGTERM signal and
    shutting down.

    """

    proc = run_worker()
    assert proc.returncode == 0


def test_working_worker_warm_shutdown():
    """Worker with an ongoing job receiving single SIGTERM signal,
    allowing job to finish then shutting down.

    """

    with rq.Connection():
        queue = rq.Queue('foo')

    job = queue.enqueue('fixtures.long_running_job', 1)

    run_worker()

    assert job.is_finished
    assert 'Done sleeping...' == job.result
