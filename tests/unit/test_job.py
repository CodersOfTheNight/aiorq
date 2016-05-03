import time
from datetime import datetime

import pytest

from aiorq import (cancel_job, get_current_job, requeue_job, Queue,
                   get_failed_queue, Worker)
from aiorq.exceptions import NoSuchJobError, UnpickleError
from aiorq.job import Job, loads, dumps
from aiorq.specs import JobStatus
from aiorq.utils import utcformat, utcnow
from fixtures import (Number, some_calculation, say_hello,
                      CallableObject, access_self, long_running_job,
                      echo, UnicodeStringObject, div_by_zero, l)
from helpers import strip_microseconds


# Dumps.


def test_dumps():
    """Dumps job spec from the job."""

    job = Job(
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=some_calculation,
        args=(3, 4),
        kwargs={'z': 2},
        description='fixtures.some_calculation(3, 4, z=2)',
        timeout=180,
        result_ttl=5000,
        status='queued',
        origin='default',
        enqueued_at=datetime(2016, 5, 3, 12, 10, 11))
    id, spec = dumps(job)
    assert id == b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert spec == {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }


def test_dumps_instance_method():
    """Dumps job spec from the instance method job."""

    n = Number(2)
    job = Job(
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=n.div,
        args=(4,),
        kwargs={},
        description='div(4)',
        timeout=180,
        result_ttl=5000,
        status='queued',
        origin='default',
        enqueued_at=datetime(2016, 5, 3, 12, 10, 11))
    id, spec = dumps(job)
    assert id == b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert spec == {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x959\x00\x00\x00\x00\x00\x00\x00(\x8c\x03div\x94\x8c\x08fixtures\x94\x8c\x06Number\x94\x93\x94)}\x94\x92\x94}\x94\x8c\x05value\x94K\x02sbK\x04\x85\x94}\x94t\x94.',  # noqa
        b'description': b'div(4)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }


def test_dumps_string_function():
    """Dump job spec from the string function job."""

    job = Job(
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func='fixtures.say_hello',
        args=('World',),
        kwargs={},
        description="fixtures.say_hello('World')",
        timeout=180,
        result_ttl=5000,
        status='queued',
        origin='default',
        enqueued_at=datetime(2016, 5, 3, 12, 10, 11))
    id, spec = dumps(job)
    assert id == b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert spec == {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x95&\x00\x00\x00\x00\x00\x00\x00(\x8c\x12fixtures.say_hello\x94N\x8c\x05World\x94\x85\x94}\x94t\x94.',  # noqa
        b'description': b"fixtures.say_hello('World')",
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }


def test_dumps_bytes_function():
    """Dump job spec from the bytes function job."""

    job = Job(
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=b'fixtures.say_hello',
        args=('World',),
        kwargs={},
        description="fixtures.say_hello('World')",
        timeout=180,
        result_ttl=5000,
        status='queued',
        origin='default',
        enqueued_at=datetime(2016, 5, 3, 12, 10, 11))
    id, spec = dumps(job)
    assert id == b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert spec == {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x95&\x00\x00\x00\x00\x00\x00\x00(\x8c\x12fixtures.say_hello\x94N\x8c\x05World\x94\x85\x94}\x94t\x94.',  # noqa
        b'description': b"fixtures.say_hello('World')",
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }


def test_dumps_builtin():
    """Dump job spec from the builtin job."""

    job = Job(
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=len,
        args=([],),
        kwargs={},
        description='builtins.len([])',
        timeout=180,
        result_ttl=5000,
        status='queued',
        origin='default',
        enqueued_at=datetime(2016, 5, 3, 12, 10, 11))
    id, spec = dumps(job)
    assert id == b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert spec == {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x95\x1a\x00\x00\x00\x00\x00\x00\x00(\x8c\x0cbuiltins.len\x94N]\x94\x85\x94}\x94t\x94.',  # noqa
        b'description': b'builtins.len([])',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }


def test_dumps_callable_object():
    """Dump job spec from the callable object job."""

    kallable = CallableObject()
    job = Job(
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=kallable,
        args=(),
        kwargs={},
        description='__call__()',
        timeout=180,
        result_ttl=5000,
        status='queued',
        origin='default',
        enqueued_at=datetime(2016, 5, 3, 12, 10, 11))
    id, spec = dumps(job)
    assert id == b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert spec == {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x955\x00\x00\x00\x00\x00\x00\x00(\x8c\x08__call__\x94\x8c\x08fixtures\x94\x8c\x0eCallableObject\x94\x93\x94)}\x94\x92\x94)}\x94t\x94.',  # noqa
        b'description': b'__call__()',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }


def test_dumps_lambda_function():
    """Dump job spec from the lambda function job."""

    job = Job(
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=l,
        args=(),
        kwargs={},
        description='fixtures.<lambda>()',
        timeout=180,
        result_ttl=5000,
        status='queued',
        origin='default',
        enqueued_at=datetime(2016, 5, 3, 12, 10, 11))
    id, spec = dumps(job)
    assert id == b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert spec == {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x95\x1c\x00\x00\x00\x00\x00\x00\x00(\x8c\x11fixtures.<lambda>\x94N)}\x94t\x94.',  # noqa
        b'description': b'fixtures.<lambda>()',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }


def test_dumps_broken_func():
    """Raise type error if func argument is unusable."""

    job = Job(
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=1,
        args=(),
        kwargs={},
        description='builtins.int()',
        timeout=180,
        result_ttl=5000,
        status='queued',
        origin='default',
        enqueued_at=datetime(2016, 5, 3, 12, 10, 11))
    with pytest.raises(TypeError):
        dumps(job)


# Loads.


def test_loads():
    """Loads job form the job spec."""

    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }
    job = loads(id, spec)
    assert job.id == '2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert job.created_at == datetime(2016, 4, 5, 22, 40, 35)
    assert job.func == some_calculation
    assert job.args == (3, 4)
    assert job.kwargs == {'z': 2}
    assert job.description == 'fixtures.some_calculation(3, 4, z=2)'
    assert job.timeout == 180
    assert job.result_ttl == 5000
    assert job.status == 'queued'
    assert job.origin == 'default'
    assert job.enqueued_at == datetime(2016, 5, 3, 12, 10, 11)


def test_loads_instance_method():
    """Loads instance method job form the job spec."""

    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x959\x00\x00\x00\x00\x00\x00\x00(\x8c\x03div\x94\x8c\x08fixtures\x94\x8c\x06Number\x94\x93\x94)}\x94\x92\x94}\x94\x8c\x05value\x94K\x02sbK\x04\x85\x94}\x94t\x94.',  # noqa
        b'description': b'div(4)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }
    n = Number(2)
    job = loads(id, spec)
    assert job.id == '2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert job.created_at == datetime(2016, 4, 5, 22, 40, 35)
    assert job.func.__name__ == n.div.__name__
    assert job.func.__self__.__class__ == n.div.__self__.__class__
    assert job.args == (4,)
    assert job.kwargs == {}
    assert job.description == 'div(4)'
    assert job.timeout == 180
    assert job.result_ttl == 5000
    assert job.status == 'queued'
    assert job.origin == 'default'
    assert job.enqueued_at == datetime(2016, 5, 3, 12, 10, 11)


# Job.


def test_unicode():
    """Unicode in job description."""

    job = Job('myfunc', args=[12, "☃"], kwargs=dict(snowman="☃", null=None))
    expected_string = "myfunc(12, '☃', null=None, snowman='☃')"
    assert job.description, expected_string


def test_fetching_unreadable_data(redis):
    """Fetching succeeds on unreadable data, but lazy props fail."""

    # Set up
    job = Job(func=some_calculation, args=(3, 4), kwargs=dict(z=2))
    yield from job.save()

    # Just replace the data hkey with some random noise
    yield from redis.hset(job.key, 'data', 'this is no pickle string')
    yield from job.refresh()

    for attr in ('func_name', 'instance', 'args', 'kwargs'):
        with pytest.raises(UnpickleError):
            getattr(job, attr)


def test_job_is_unimportable(redis):
    """Jobs that cannot be imported throw exception on access."""

    job = Job(func=say_hello, args=('Lionel',))
    yield from job.save()

    # Now slightly modify the job to make it unimportable (this is
    # equivalent to a worker not having the most up-to-date source
    # code and unable to import the function)
    data = yield from redis.hget(job.key, 'data')
    unimportable_data = data.replace(b'say_hello', b'nay_hello')
    yield from redis.hset(job.key, 'data', unimportable_data)

    yield from job.refresh()
    with pytest.raises(AttributeError):
        job.func  # accessing the func property should fail


def test_custom_meta_is_persisted(redis):
    """Additional meta data on jobs are stored persisted correctly."""

    job = Job(func=say_hello, args=('Lionel',))
    job.meta['foo'] = 'bar'
    yield from job.save()

    raw_data = yield from redis.hget(job.key, 'meta')
    assert loads(raw_data)['foo'] == 'bar'

    job2 = yield from Job.fetch(job.id)
    assert job2.meta['foo'] == 'bar'


def test_result_ttl_is_persisted(redis):
    """Ensure that job's result_ttl is set properly"""

    job = Job(func=say_hello, args=('Lionel',), result_ttl=10)
    yield from job.save()
    yield from Job.fetch(job.id, connection=redis)
    assert job.result_ttl == 10

    job = Job(func=say_hello, args=('Lionel',))
    yield from job.save()
    yield from Job.fetch(job.id, connection=redis)
    assert not job.result_ttl


def test_description_is_persisted(redis):
    """Ensure that job's custom description is set properly."""

    job = Job(func=say_hello, args=('Lionel',), description='Say hello!')
    yield from job.save()
    yield from Job.fetch(job.id, connection=redis)
    assert job.description == 'Say hello!'

    # Ensure job description is constructed from function call string
    job = Job(func=say_hello, args=('Lionel',))
    yield from job.save()
    yield from Job.fetch(job.id, connection=redis)
    assert job.description == "fixtures.say_hello('Lionel')"


def test_job_access_outside_job_fails():
    """The current job is accessible only within a job context."""

    assert not (yield from get_current_job())


def test_job_access_within_job_function(loop):
    """The current job is accessible within the job function."""

    q = Queue()
    # access_self calls get_current_job() and asserts
    yield from q.enqueue(access_self)
    w = Worker(q)
    yield from w.work(burst=True, loop=loop)


def test_get_result_ttl():
    """Getting job result TTL."""

    job_result_ttl = 1
    default_ttl = 2

    job = Job(func=say_hello, result_ttl=job_result_ttl)
    yield from job.save()
    assert job.get_result_ttl(default_ttl=default_ttl) == job_result_ttl
    assert job.get_result_ttl() == job_result_ttl

    job = Job(func=say_hello)
    yield from job.save()
    assert job.get_result_ttl(default_ttl=default_ttl) == default_ttl
    assert not job.get_result_ttl()


def test_get_job_ttl():
    """Getting job TTL."""

    ttl = 1

    job = Job(func=say_hello, ttl=ttl)
    yield from job.save()
    assert job.get_ttl() == ttl

    job = Job(func=say_hello)
    yield from job.save()
    assert not job.get_ttl()


def test_ttl_via_enqueue(redis):
    """Enqueue set custom TTL on job."""

    ttl = 1
    queue = Queue(connection=redis)
    job = yield from queue.enqueue(say_hello, ttl=ttl)
    assert job.get_ttl() == ttl


def test_create_job_with_id(redis):
    """Create jobs with a custom ID."""

    queue = Queue(connection=redis)
    job = yield from queue.enqueue(say_hello, job_id="1234")

    assert job.id == "1234"
    yield from job.perform()

    with pytest.raises(TypeError):
        yield from queue.enqueue(say_hello, job_id=1234)


def test_get_call_string_unicode(redis):
    """Call string with unicode keyword arguments."""

    queue = Queue(connection=redis)
    job = yield from queue.enqueue(
        echo, arg_with_unicode=UnicodeStringObject())
    assert job.get_call_string()
    yield from job.perform()


def test_create_job_with_ttl_should_have_ttl_after_enqueued(redis):
    """Create jobs with ttl and checks if get_jobs returns it properly."""

    queue = Queue(connection=redis)
    yield from queue.enqueue(say_hello, job_id="1234", ttl=10)
    job = (yield from queue.get_jobs())[0]
    assert job.ttl == 10


def test_create_job_with_ttl_should_expire(redis):
    """A job created with ttl expires."""

    queue = Queue(connection=redis)
    queue.enqueue(say_hello, job_id="1234", ttl=1)
    time.sleep(1)
    assert not len((yield from queue.get_jobs()))


def test_job_status():
    """Access job status checkers like is_started."""

    job = Job()
    yield from job.set_status(JobStatus.FINISHED)
    assert (yield from job.is_finished)
    yield from job.set_status(JobStatus.QUEUED)
    assert (yield from job.is_queued)
    yield from job.set_status(JobStatus.FAILED)
    assert (yield from job.is_failed)
    yield from job.set_status(JobStatus.STARTED)
    assert (yield from job.is_started)
