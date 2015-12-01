import pytest

from aiorq.job import Job
from testing import async_test
from fixtures import Number, some_calculation, say_hello


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
