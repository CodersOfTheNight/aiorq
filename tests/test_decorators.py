from aiorq.job import Job
from fixtures import decorated_job


def test_decorator_preserves_functionality():
    """Ensure that a decorated function's functionality is still preserved."""

    assert decorated_job(1, 2) == 3


def test_decorator_adds_delay_attr():
    """Ensure that decorator adds a delay attribute to function that
    returns a Job instance when called.
    """

    assert hasattr(decorated_job, 'delay')
    result = yield from decorated_job.delay(1, 2)
    assert isinstance(result, Job)
    # Ensure that job returns the right result when performed
    assert (yield from result.perform()) == 3
