from datetime import datetime

import pytest
from rq import (Worker as SynchronousWorker,
                Connection as SynchronousConnection,
                Queue as SynchronousQueue)
from rq.compat import as_text
from rq.utils import utcformat

from aiorq import get_current_job, Queue
from aiorq.job import Job, loads, dumps
from aiorq.exceptions import NoSuchJobError, UnpickleError
from aiorq.registry import DeferredJobRegistry
from testing import async_test
from fixtures import (Number, some_calculation, say_hello,
                      CallableObject, access_self, long_running_job)
from helpers import strip_microseconds


@async_test
def test_unicode(**kwargs):
    """Unicode in job description."""

    job = Job.create('myfunc', args=[12, "☃"],
                     kwargs=dict(snowman="☃", null=None))
    expected_string = "myfunc(12, '☃', null=None, snowman='☃')"
    assert job.description, expected_string


@async_test
def test_create_empty_job(**kwargs):
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


@async_test
def test_create_typical_job(**kwargs):
    """Creation of jobs for function calls."""
    job = Job.create(func=some_calculation,
                     args=(3, 4), kwargs=dict(z=2))

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


@async_test
def test_create_instance_method_job(**kwargs):
    """Creation of jobs for instance methods."""

    n = Number(2)
    job = Job.create(func=n.div, args=(4,))

    # Job data is set
    assert job.func == n.div
    assert job.instance == n
    assert job.args == (4,)


@async_test
def test_create_job_from_string_function(**kwargs):
    """Creation of jobs using string specifier."""

    job = Job.create(func='fixtures.say_hello', args=('World',))

    # Job data is set
    assert job.func == say_hello
    assert not job.instance
    assert job.args == ('World',)


@async_test
def test_create_job_from_callable_class(**kwargs):
    """Creation of jobs using a callable class specifier."""

    kallable = CallableObject()
    job = Job.create(func=kallable)

    assert job.func == kallable.__call__
    assert job.instance == kallable


@async_test
def test_job_properties_set_data_property(**kwargs):
    """Data property gets derived from the job tuple."""

    job = Job()
    job.func_name = 'foo'
    fname, instance, args, kwargs = loads(job.data)

    assert fname == job.func_name
    assert not instance
    assert args == ()
    assert kwargs == {}


@async_test
def test_data_property_sets_job_properties(**kwargs):
    """Job tuple gets derived lazily from data property."""

    job = Job()
    job.data = dumps(('foo', None, (1, 2, 3), {'bar': 'qux'}))

    assert job.func_name == 'foo'
    assert not job.instance
    assert job.args == (1, 2, 3)
    assert job.kwargs == {'bar': 'qux'}


@async_test
def test_save(redis, **kwargs):
    """Storing jobs."""

    job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))

    # Saving creates a Redis hash
    assert not (yield from redis.exists(job.key))
    yield from job.save()
    assert (yield from redis.type(job.key)) == b'hash'

    # Saving writes pickled job data
    unpickled_data = loads((yield from redis.hget(job.key, 'data')))
    assert unpickled_data[0] == 'fixtures.some_calculation'


@async_test
def test_fetch(redis, **kwargs):
    """Fetching jobs."""

    yield from redis.hset('rq:job:some_id', 'data',
                          "(S'fixtures.some_calculation'\n"
                          "N(I3\nI4\nt(dp1\nS'z'\nI2\nstp2\n.")
    yield from redis.hset('rq:job:some_id', 'created_at',
                          '2012-02-07T22:13:24Z')

    # Fetch returns a job
    job = yield from Job.fetch('some_id')

    assert job.id == 'some_id'
    assert job.func_name == 'fixtures.some_calculation'
    assert not job.instance
    assert job.args == (3, 4)
    assert job.kwargs == dict(z=2)
    assert job.created_at == datetime(2012, 2, 7, 22, 13, 24)


@async_test
def test_persistence_of_empty_jobs(**kwargs):
    """Storing empty jobs."""

    job = Job()
    with pytest.raises(ValueError):
        yield from job.save()


@async_test
def test_persistence_of_typical_jobs(redis, **kwargs):
    """Storing typical jobs."""

    job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))
    yield from job.save()

    expected_date = strip_microseconds(job.created_at)
    stored_date = (yield from redis.hget(job.key, 'created_at')) \
        .decode('utf-8')
    assert stored_date == utcformat(expected_date)

    # ... and no other keys are stored
    assert sorted((yield from redis.hkeys(job.key))) \
        == [b'created_at', b'data', b'description']


@async_test
def test_persistence_of_parent_job(**kwargs):
    """Storing jobs with parent job, either instance or key."""

    parent_job = Job.create(func=some_calculation)
    yield from parent_job.save()

    job = Job.create(func=some_calculation, depends_on=parent_job)
    yield from job.save()

    stored_job = yield from Job.fetch(job.id)
    assert stored_job._dependency_id == parent_job.id
    assert (yield from stored_job.dependency) == parent_job

    job = Job.create(func=some_calculation, depends_on=parent_job.id)
    yield from job.save()
    stored_job = yield from Job.fetch(job.id)

    assert stored_job._dependency_id == parent_job.id
    assert (yield from stored_job.dependency) == parent_job


@async_test
def test_store_then_fetch(**kwargs):
    """Store, then fetch."""

    job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))
    yield from job.save()

    job2 = yield from Job.fetch(job.id)
    assert job.func == job2.func
    assert job.args == job2.args
    assert job.kwargs == job2.kwargs

    # Mathematical equation
    assert job == job2


@async_test
def test_fetching_can_fail(**kwargs):
    """Fetching fails for non-existing jobs."""

    with pytest.raises(NoSuchJobError):
        yield from Job.fetch('b4a44d44-da16-4620-90a6-798e8cd72ca0')


@async_test
def test_fetching_unreadable_data(redis, **kwargs):
    """Fetching succeeds on unreadable data, but lazy props fail."""

    # Set up
    job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))
    yield from job.save()

    # Just replace the data hkey with some random noise
    yield from redis.hset(job.key, 'data', 'this is no pickle string')
    yield from job.refresh()

    for attr in ('func_name', 'instance', 'args', 'kwargs'):
        with pytest.raises(UnpickleError):
            getattr(job, attr)


@async_test
def test_job_is_unimportable(redis, **kwargs):
    """Jobs that cannot be imported throw exception on access."""

    job = Job.create(func=say_hello, args=('Lionel',))
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


@async_test
def test_custom_meta_is_persisted(redis, **kwargs):
    """Additional meta data on jobs are stored persisted correctly."""

    job = Job.create(func=say_hello, args=('Lionel',))
    job.meta['foo'] = 'bar'
    yield from job.save()

    raw_data = yield from redis.hget(job.key, 'meta')
    assert loads(raw_data)['foo'] == 'bar'

    job2 = yield from Job.fetch(job.id)
    assert job2.meta['foo'] == 'bar'


@async_test
def test_result_ttl_is_persisted(redis, **kwargs):
    """Ensure that job's result_ttl is set properly"""

    job = Job.create(func=say_hello, args=('Lionel',), result_ttl=10)
    yield from job.save()
    yield from Job.fetch(job.id, connection=redis)
    assert job.result_ttl == 10

    job = Job.create(func=say_hello, args=('Lionel',))
    yield from job.save()
    yield from Job.fetch(job.id, connection=redis)
    assert not job.result_ttl


@async_test
def test_description_is_persisted(redis, **kwargs):
    """Ensure that job's custom description is set properly."""

    job = Job.create(func=say_hello, args=('Lionel',),
                     description='Say hello!')
    yield from job.save()
    yield from Job.fetch(job.id, connection=redis)
    assert job.description == 'Say hello!'

    # Ensure job description is constructed from function call string
    job = Job.create(func=say_hello, args=('Lionel',))
    yield from job.save()
    yield from Job.fetch(job.id, connection=redis)
    assert job.description == "fixtures.say_hello('Lionel')"


@async_test
def test_job_access_outside_job_fails(**kwargs):
    """The current job is accessible only within a job context."""

    assert not (yield from get_current_job())


@async_test
def test_job_access_within_job_function(**kwargs):
    """The current job is accessible within the job function."""

    q = Queue()
    # access_self calls get_current_job() and asserts
    yield from q.enqueue(access_self)
    with SynchronousConnection():
        w = SynchronousWorker([SynchronousQueue()])
    w.work(burst=True)


@async_test
def test_get_result_ttl(**kwargs):
    """Getting job result TTL."""

    job_result_ttl = 1
    default_ttl = 2

    job = Job.create(func=say_hello, result_ttl=job_result_ttl)
    yield from job.save()
    assert job.get_result_ttl(default_ttl=default_ttl) == job_result_ttl
    assert job.get_result_ttl() == job_result_ttl

    job = Job.create(func=say_hello)
    yield from job.save()
    assert job.get_result_ttl(default_ttl=default_ttl) == default_ttl
    assert not job.get_result_ttl()


@async_test
def test_get_job_ttl(**kwargs):
    """Getting job TTL."""

    ttl = 1

    job = Job.create(func=say_hello, ttl=ttl)
    yield from job.save()
    assert job.get_ttl() == ttl

    job = Job.create(func=say_hello)
    yield from job.save()
    assert not job.get_ttl()


@async_test
def test_ttl_via_enqueue(redis, **kwargs):
    """Enqueue set custom TTL on job."""

    ttl = 1
    queue = Queue(connection=redis)
    job = yield from queue.enqueue(say_hello, ttl=ttl)
    assert job.get_ttl() == ttl


@async_test
def test_never_expire_during_execution(redis, **kwargs):
    """Test what happens when job expires during execution."""

    ttl = 1

    queue = Queue(connection=redis)
    job = yield from queue.enqueue(long_running_job, args=(2,), ttl=ttl)
    assert job.get_ttl() == ttl

    yield from job.save()
    yield from job.perform()
    assert job.get_ttl() == -1
    assert (yield from job.exists(job.id))
    assert (yield from job.result) == 'Done sleeping...'


@async_test
def test_cleanup(redis, **kwargs):
    """Test that jobs and results are expired properly."""

    job = Job.create(func=say_hello)
    yield from job.save()

    # Jobs with negative TTLs don't expire
    yield from job.cleanup(ttl=-1)
    assert (yield from redis.ttl(job.key)) == -1

    # Jobs with positive TTLs are eventually deleted
    yield from job.cleanup(ttl=100)
    assert (yield from redis.ttl(job.key)) == 100

    # Jobs with 0 TTL are immediately deleted
    yield from job.cleanup(ttl=0)
    with pytest.raises(NoSuchJobError):
        yield from Job.fetch(job.id, redis)


@async_test
def test_register_dependency(redis, **kwargs):
    """Ensure dependency registration works properly."""

    origin = 'some_queue'
    registry = DeferredJobRegistry(origin, redis)

    job = Job.create(func=say_hello, origin=origin)
    job._dependency_id = 'id'
    yield from job.save()

    assert not (yield from registry.get_job_ids())
    yield from job.register_dependency()
    assert as_text((yield from redis.spop('rq:job:id:dependents'))) == job.id
    assert (yield from registry.get_job_ids()) == [job.id]


@async_test
def test_delete(redis, **kwargs):
    """job.delete() deletes itself & dependents mapping from Redis."""

    queue = Queue(connection=redis)
    job = yield from queue.enqueue(say_hello)
    job2 = Job.create(func=say_hello, depends_on=job)
    yield from job2.register_dependency()
    yield from job.delete()

    assert not (yield from redis.exists(job.key))
    assert not (yield from redis.exists(job.dependents_key))
    assert job.id not in (yield from queue.get_job_ids())
