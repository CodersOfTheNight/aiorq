import signal
import subprocess
import threading
import time

import pytest
import redis


@pytest.yield_fixture
def flush_redis():

    yield
    connection = redis.StrictRedis()
    connection.flushdb()


@pytest.yield_fixture
def worker():

    worker = Worker()
    yield worker
    worker.kill_after = 0
    worker.kill()


class Worker:

    command = ['aiorq', 'worker', 'foo', '--verbose']
    stop_interval = 5
    kill_after = 300

    def __init__(self):

        self.process = None
        self.is_running = False
        self.start()
        self.schedule_kill()

    def start(self):

        self.process = subprocess.Popen(args=self.command,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
        self.is_running = True

    def stop_with(self, *signals):

        self.schedule_stop(signals)
        self.wait()
        self.is_running = False

    def schedule_stop(self, signals):

        for generation, signum in enumerate(signals, 1):
            timeout = self.stop_interval * generation
            thread = threading.Thread(target=self.send, args=(signum, timeout))
            thread.start()

    def schedule_kill(self):

        thread = threading.Thread(target=self.kill)
        thread.start()

    def wait(self):

        stdout, stderr = self.process.communicate()
        print(stdout.decode())
        print(stderr.decode())

    def send(self, signum, sleep_for):

        time.sleep(sleep_for)
        self.process.send_signal(signum)

    def kill(self):

        sleeping_for = 0
        while sleeping_for < self.kill_after:
            time.sleep(self.stop_interval)
            if self.is_running:
                sleeping_for += self.stop_interval
            else:
                break
        else:
            try:
                self.send(signal.SIGKILL, 0)
            except ProcessLookupError:
                pass

    @property
    def returncode(self):

        return self.process.returncode
