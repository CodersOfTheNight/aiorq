import time
from datetime import datetime
from pickle import UnpicklingError

import pytest

from aiorq import (cancel_job, get_current_job, requeue_job, Queue,
                   get_failed_queue, Worker)
from aiorq.exceptions import NoSuchJobError
from aiorq.job import Job, loads, dumps, description
from aiorq.protocol import (enqueue_job, dequeue_job, start_job,
                            finish_job, fail_job)
from aiorq.specs import JobStatus
from aiorq.utils import utcformat, utcnow
from fixtures import (Number, some_calculation, say_hello,
                      CallableObject, access_self, long_running_job,
                      echo, UnicodeStringObject, div_by_zero, l)
from helpers import strip_microseconds


# Dumps.


def test_dumps(redis):
    """Dumps job spec from the job."""

    job = Job(
        connection=redis,
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


def test_dumps_instance_method(redis):
    """Dumps job spec from the instance method job."""

    n = Number(2)
    job = Job(
        connection=redis,
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


def test_dumps_string_function(redis):
    """Dump job spec from the string function job."""

    job = Job(
        connection=redis,
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


def test_dumps_bytes_function(redis):
    """Dump job spec from the bytes function job."""

    job = Job(
        connection=redis,
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


def test_dumps_builtin(redis):
    """Dump job spec from the builtin job."""

    job = Job(
        connection=redis,
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


def test_dumps_callable_object(redis):
    """Dump job spec from the callable object job."""

    kallable = CallableObject()
    job = Job(
        connection=redis,
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


def test_dumps_lambda_function(redis):
    """Dump job spec from the lambda function job."""

    job = Job(
        connection=redis,
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


def test_dumps_broken_func(redis):
    """Raise type error if func argument is unusable."""

    job = Job(
        connection=redis,
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


def test_loads_unreadable_data(redis):
    """Loads unreadable pickle string will raise UnpickleError."""

    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'this is no pickle string',
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }
    with pytest.raises(UnpicklingError):
        loads(id, spec)


def test_loads_unimportable_data(redis):
    """Loads unimportable data will raise attribute error."""

    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        # nay_hello instead of say_hello
        b'data': b"\x80\x04\x95'\x00\x00\x00\x00\x00\x00\x00(\x8c\x12fixtures.nay_hello\x94N\x8c\x06Lionel\x94\x85\x94}\x94t\x94.",  # noqa
        b'description': b"fixtures.say_hello('Lionel')",
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': b'default',
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }
    with pytest.raises(AttributeError):
        loads(id, spec)


# Description.


def test_description():
    """Make job description."""

    n = Number(2)
    kallable = CallableObject()
    desc = description(some_calculation, (3, 4), {'z': 2})
    assert desc == 'fixtures.some_calculation(3, 4, z=2)'
    desc = description(n.div, (4,), {})
    assert desc == 'div(4)'
    desc = description('fixtures.say_hello', ('World',), {})
    assert desc == "fixtures.say_hello('World')"
    desc = description(b'fixtures.say_hello', ('World',), {})
    assert desc == "fixtures.say_hello('World')"
    desc = description(len, ([],), {})
    assert desc == 'builtins.len([])'
    desc = description(kallable, (), {})
    assert desc == '__call__()'
    desc = description(l, (), {})
    assert desc == 'fixtures.<lambda>()'
    desc = description('myfunc', [12, '☃'], {'snowman': '☃'})
    assert desc == "myfunc(12, '☃', snowman='☃')"


# Job.


def test_job_status(redis):
    """Access job status checkers like is_started."""

    job = Job(
        connection=redis,
        id='56e6ba45-1aa3-4724-8c9f-51b7b0031cee',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=some_calculation,
        args=(3, 4),
        kwargs={'z': 2},
        description='fixtures.some_calculation(3, 4, z=2)',
        timeout=180,
        result_ttl=5000,
        origin='default')
    id, spec = dumps(job)
    queue = spec[b'origin']
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from job.is_queued)
    stored_id, stored_spec = yield from dequeue_job(redis, queue)
    yield from start_job(redis, queue, id, stored_spec)
    assert (yield from job.is_started)
    yield from finish_job(redis, id, stored_spec)
    assert (yield from job.is_finished)
    yield from fail_job(redis, queue, id, b"Exception('We are here')")
    assert (yield from job.is_failed)
    deferred_job = Job(
        connection=redis,
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=some_calculation,
        args=(3, 4),
        kwargs={'z': 2},
        description='fixtures.some_calculation(3, 4, z=2)',
        timeout=180,
        result_ttl=5000,
        origin='default',
        dependency_id=job.id)
    deferred_id, deferred_spec = dumps(deferred_job)
    deferred_queue = deferred_spec[b'origin']
    yield from enqueue_job(redis, deferred_queue, deferred_id, deferred_spec)
    assert (yield from job.is_deferred)
    status = yield from job.get_status()
    assert status == 'deferred'


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
