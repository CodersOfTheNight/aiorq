from signal import SIGTERM

import pytest
import rq


pytestmark = pytest.mark.usefixtures('flush_redis')


def test_idle_worker_warm_shutdown(worker):
    """Worker with no ongoing job receiving single SIGTERM signal and
    shutting down.

    """

    worker.stop_with(SIGTERM)
    assert worker.returncode == 0


def test_working_worker_warm_shutdown(worker):
    """Worker with an ongoing job receiving single SIGTERM signal,
    allowing job to finish then shutting down.

    """

    with rq.Connection():
        queue = rq.Queue('foo')

    job = queue.enqueue('fixtures.long_running_job')

    worker.stop_with(SIGTERM)

    assert worker.returncode == 0
    assert job.is_finished
    assert 'Done sleeping...' == job.result


def test_working_worker_cold_shutdown(worker):
    """Worker with an ongoing job receiving double SIGTERM signal and
    shutting down immediately.

    """

    with rq.Connection():
        queue = rq.Queue('foo')

    job = queue.enqueue('fixtures.long_running_job')

    worker.stop_with(SIGTERM, SIGTERM)

    assert worker.returncode == 0
    assert not job.is_finished
