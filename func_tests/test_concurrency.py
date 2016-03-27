from signal import SIGTERM
from time import sleep

import pytest
import rq


pytestmark = pytest.mark.usefixtures('flush_redis')


def test_concurrency(worker):
    """Worker execute many tasks concurrently."""

    with rq.Connection():
        queue = rq.Queue('foo')

    job1 = queue.enqueue('fixtures.long_running_job', 50)
    job2 = queue.enqueue('fixtures.long_running_job', 50)

    sleep(30)

    assert job1.is_started
    assert job2.is_started
