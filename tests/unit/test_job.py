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
                      echo, UnicodeStringObject, div_by_zero)
from helpers import strip_microseconds


def test_loads():
    """Loads job form the job spec."""

    queue = b'default'
    id = b'2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        b'created_at': b'2016-04-05T22:40:35Z',
        b'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        b'description': b'fixtures.some_calculation(3, 4, z=2)',
        b'timeout': 180,
        b'result_ttl': 5000,
        b'status': JobStatus.QUEUED,
        b'origin': queue,
        b'enqueued_at': b'2016-05-03T12:10:11Z',
    }
    job = loads(id, spec)
    assert job.id == '2a5079e7-387b-492f-a81c-68aa55c194c8'
    assert job.created_at == datetime(2016, 4, 5, 22, 40, 35)
    assert job.func == some_calculation
    assert job.instance is None
    assert job.args == (3, 4)
    assert job.kwargs == {'z': 2}
    assert job.description == 'fixtures.some_calculation(3, 4, z=2)'
    assert job.timeout == 180
    assert job.result_ttl == 5000
    assert job.status == 'queued'
    assert job.origin == 'default'
    assert job.enqueued_at == datetime(2016, 5, 3, 12, 10, 11)


def test_dumps():
    """Dumps job spec from the job."""

    job = Job(
        id='2a5079e7-387b-492f-a81c-68aa55c194c8',
        created_at=datetime(2016, 4, 5, 22, 40, 35),
        func=some_calculation,
        instance=None,
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


def test_unicode():
    """Unicode in job description."""

    job = Job('myfunc', args=[12, "☃"], kwargs=dict(snowman="☃", null=None))
    expected_string = "myfunc(12, '☃', null=None, snowman='☃')"
    assert job.description, expected_string


def test_create_empty_job():
    """Creation of new empty jobs."""

    job = Job()

    # Jobs have a random UUID and a creation date
    assert job.id
    assert job.created_at

    # ...and nothing else
    assert not job.origin
    assert not job.enqueued_at
    assert not job.started_at
    assert not job.ended_at
    assert not (yield from job.result)
    assert not job.exc_info

    with pytest.raises(ValueError):
        job.func
    with pytest.raises(ValueError):
        job.instance
    with pytest.raises(ValueError):
        job.args
    with pytest.raises(ValueError):
        job.kwargs


def test_create_typical_job():
    """Creation of jobs for function calls."""
    job = Job(func=some_calculation, args=(3, 4), kwargs=dict(z=2))

    # Jobs have a random UUID
    assert job.id
    assert job.created_at
    assert job.description
    assert not job.instance

    # Job data is set...
    assert job.func == some_calculation
    assert job.args == (3, 4)
    assert job.kwargs == {'z': 2}

    # ...but metadata is not
    assert not job.origin
    assert not job.enqueued_at
    assert not (yield from job.result)


def test_create_instance_method_job():
    """Creation of jobs for instance methods."""

    n = Number(2)
    job = Job(func=n.div, args=(4,))

    # Job data is set
    assert job.func == n.div
    assert job.instance == n
    assert job.args == (4,)


def test_create_job_from_string_function():
    """Creation of jobs using string specifier."""

    job = Job(func='fixtures.say_hello', args=('World',))

    # Job data is set
    assert job.func == say_hello
    assert not job.instance
    assert job.args == ('World',)


def test_create_job_from_callable_class():
    """Creation of jobs using a callable class specifier."""

    kallable = CallableObject()
    job = Job(func=kallable)

    assert job.func == kallable.__call__
    assert job.instance == kallable


def test_job_properties_set_data_property():
    """Data property gets derived from the job tuple."""

    job = Job()
    job.func_name = 'foo'
    fname, instance, args, kwargs = loads(job.data)

    assert fname == job.func_name
    assert not instance
    assert args == ()
    assert kwargs == {}


def test_data_property_sets_job_properties():
    """Job tuple gets derived lazily from data property."""

    job = Job()
    job.data = dumps(('foo', None, (1, 2, 3), {'bar': 'qux'}))

    assert job.func_name == 'foo'
    assert not job.instance
    assert job.args == (1, 2, 3)
    assert job.kwargs == {'bar': 'qux'}


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
