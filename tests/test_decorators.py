from aiorq.job import Job
from aiorq.decorators import job
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


def test_decorator_accepts_queue_name_as_argument():
    """Ensure that passing in queue name to the decorator puts the job in
    the right queue.
    """

    @job(queue='queue_name')
    def hello():
        return 'Hi'

    result = yield from hello.delay()
    assert result.origin == 'queue_name'


def test_decorator_accepts_result_depends_on_as_argument():
    """Ensure that passing in depends_on to the decorator sets the correct
    dependency on the job.
    """

    @job(queue='queue_name')
    def foo():
        return 'Firstly'

    @job(queue='queue_name')
    def bar():
        return 'Secondly'

    foo_job = yield from foo.delay()
    bar_job = yield from bar.delay(depends_on=foo_job)

    assert not foo_job._dependency_id

    assert (yield from bar_job.dependency) == foo_job

    assert bar_job._dependency_id == foo_job.id
