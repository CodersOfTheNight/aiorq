import pytest

from aiorq.job import Job
from aiorq.testing import async_test


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