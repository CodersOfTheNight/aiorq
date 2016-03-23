from threading import Thread
from signal import SIGTERM
from subprocess import Popen, PIPE
from time import sleep


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

    proc = Popen(['aiorq', 'worker', 'foo'], stdout=PIPE, stderr=PIPE)
    Thread(target=kill_worker, args=(proc))
    stdout, stderr = proc.communicate()
    print(stdout.decode())
    print(stderr.decode())
    assert proc.returncode == 0
