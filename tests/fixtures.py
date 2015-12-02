import time

from aiorq import get_current_job


def say_hello(name=None):

    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


def some_calculation(x, y, z=1):

    return x * y / z


class Number:

    def __init__(self, value):

        self.value = value

    def div(self, y):

        return self.value / y


class CallableObject(object):

    def __call__(self):

        return "I'm callable"


def access_self():

    assert get_current_job() is not None


def long_running_job(timeout=10):

    time.sleep(timeout)
    return 'Done sleeping...'
