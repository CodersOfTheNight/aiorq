import pytest
from rq.job import JobStatus

from aiorq import Queue, get_failed_queue, Worker
from aiorq.exceptions import InvalidJobOperationError, DequeueTimeout
from aiorq.job import Job
from aiorq.registry import DeferredJobRegistry
from fixtures import say_hello, Number, echo, div_by_zero


def test_create_queue(set_loop):
    """We can create queue instance."""

    q = Queue()
    assert q.name == 'default'


def test_create_named_queue(set_loop):
    """We can create named queue instance."""

    q = Queue('my-queue')
    assert q.name == 'my-queue'


def test_queue_magic_methods(set_loop):
    """Test simple magic method behavior of the Queue class."""

    q = Queue()
    assert hash(q) == hash('default')
    assert str(q) == "<Queue 'default'>"
    assert repr(q) == "Queue('default')"


def test_custom_job_class(set_loop):
    """Ensure custom job class assignment works as expected."""

    class CustomJob(Job):
        pass

    q = Queue(job_class=CustomJob)
    assert q.job_class == CustomJob


def test_equality(set_loop):
    """Mathematical equality of queues."""

    q1 = Queue('foo')
    q2 = Queue('foo')
    q3 = Queue('bar')
    assert q1 == q2
    assert q2 == q1
    assert q1 != q3
    assert q2 != q3


def test_queue_order(set_loop):
    """Mathematical order of queues."""

    q1 = Queue('a')
    q2 = Queue('b')
    q3 = Queue('c')
    assert q1 < q2
    assert q3 > q2


def test_empty_queue(loop, redis):
    """Emptying queues."""

    q = Queue('example', connection=redis, loop=loop)
    yield from redis.rpush('rq:queue:example', 'foo')
    yield from redis.rpush('rq:queue:example', 'bar')
    assert not (yield from q.is_empty())
    yield from q.empty()
    assert (yield from q.is_empty())
    assert (yield from redis.lpop('rq:queue:example')) is None


def test_empty_removes_jobs(loop, redis):
    """Emptying a queue deletes the associated job objects."""

    q = Queue('example', loop=loop)
    job = yield from q.enqueue(lambda x: x)
    assert (yield from Job.exists(job.id))
    yield from q.empty()
    assert not (yield from Job.exists(job.id))


def test_queue_is_empty(loop, redis):
    """Detecting empty queues."""

    q = Queue('example', loop=loop)
    assert (yield from q.is_empty())
    yield from redis.rpush('rq:queue:example', 'sentinel message')
    assert not (yield from q.is_empty())


def test_remove(set_loop):
    """Ensure queue.remove properly removes Job from queue."""

    q = Queue('example')
    job = yield from q.enqueue(say_hello)
    assert job.id in (yield from q.job_ids)
    yield from q.remove(job)
    assert job.id not in (yield from q.job_ids)

    job = yield from q.enqueue(say_hello)
    assert job.id in (yield from q.job_ids)
    yield from q.remove(job.id)
    assert job.id not in (yield from q.job_ids)


def test_jobs(set_loop):
    """Getting jobs out of a queue."""

    q = Queue('example')
    assert not (yield from q.jobs)
    job = yield from q.enqueue(say_hello)
    assert (yield from q.jobs) == [job]

    # Deleting job removes it from queue
    yield from job.delete()
    assert not (yield from q.job_ids)


def test_compact(loop, redis):
    """Queue.compact() removes non-existing jobs."""

    q = Queue(loop=loop)

    yield from q.enqueue(say_hello, 'Alice')
    yield from q.enqueue(say_hello, 'Charlie')
    yield from redis.lpush(q.key, '1', '2')

    assert (yield from q.count) == 4
    yield from q.compact()
    assert (yield from q.count) == 2

    with pytest.raises(RuntimeError):
        len(q)


def test_enqueue(loop, redis):
    """Enqueueing job onto queues."""

    q = Queue(loop=loop)
    assert (yield from q.is_empty())

    # say_hello spec holds which queue this is sent to
    job = yield from q.enqueue(say_hello, 'Nick', foo='bar')
    job_id = job.id
    assert job.origin == q.name

    # Inspect data inside Redis
    q_key = 'rq:queue:default'
    assert 1 == (yield from redis.llen(q_key))
    assert job_id == (yield from redis.lrange(q_key, 0, -1))[0].decode('ascii')


def test_enqueue_sets_metadata(set_loop):
    """Enqueueing job onto queues modifies meta data."""

    q = Queue()
    job = Job.create(func=say_hello, args=('Nick',), kwargs=dict(foo='bar'))

    # Preconditions
    assert not job.enqueued_at

    # Action
    yield from q.enqueue_job(job)

    # Postconditions
    assert job.enqueued_at


def test_pop_job_id(set_loop):
    """Popping job IDs from queues."""

    # Set up
    q = Queue()
    uuid = '112188ae-4e9d-4a5b-a5b3-f26f2cb054da'
    yield from q.push_job_id(uuid)

    # Pop it off the queue...
    assert (yield from q.count)
    assert (yield from q.pop_job_id()) == uuid

    # ...and assert the queue count when down
    assert not (yield from q.count)


def test_dequeue(set_loop):
    """Dequeueing jobs from queues."""

    # Set up
    q = Queue()
    result = yield from q.enqueue(say_hello, 'Rick', foo='bar')

    # Dequeue a job (not a job ID) off the queue
    assert (yield from q.count)
    job = yield from q.dequeue()
    assert job.id == result.id
    assert job.func == say_hello
    assert job.origin == q.name
    assert job.args[0] == 'Rick'
    assert job.kwargs['foo'] == 'bar'

    # ...and assert the queue count when down
    assert not (yield from q.count)


def test_dequeue_deleted_jobs(set_loop):
    """Dequeueing deleted jobs from queues don't blow the stack."""

    q = Queue()
    for _ in range(1, 1000):
        job = yield from q.enqueue(say_hello)
        yield from job.delete()
    yield from q.dequeue()


def test_dequeue_instance_method(set_loop):
    """Dequeueing instance method jobs from queues."""

    q = Queue()
    n = Number(2)
    yield from q.enqueue(n.div, 4)

    job = yield from q.dequeue()

    # The instance has been pickled and unpickled, so it is now a
    # separate object. Test for equality using each object's __dict__
    # instead.
    assert job.instance.__dict__ == n.__dict__
    assert job.func.__name__ == 'div'
    assert job.args == (4,)


def test_dequeue_class_method(set_loop):
    """Dequeueing class method jobs from queues."""

    q = Queue()
    yield from q.enqueue(Number.divide, 3, 4)

    job = yield from q.dequeue()

    assert job.instance.__dict__ == Number.__dict__
    assert job.func.__name__ == 'divide'
    assert job.args == (3, 4)


def test_dequeue_ignores_nonexisting_jobs(set_loop):
    """Dequeuing silently ignores non-existing jobs."""

    q = Queue()
    uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
    yield from q.push_job_id(uuid)
    yield from q.push_job_id(uuid)
    result = yield from q.enqueue(say_hello, 'Nick', foo='bar')
    yield from q.push_job_id(uuid)

    # Dequeue simply ignores the missing job and returns None
    assert (yield from q.count) == 4
    assert (yield from q.dequeue()).id == result.id
    assert not (yield from q.dequeue())
    assert not (yield from q.count)


def test_dequeue_any(set_loop):
    """Fetching work from any given queue."""

    fooq = Queue('foo')
    barq = Queue('bar')

    assert not (yield from Queue.dequeue_any([fooq, barq], None))

    # Enqueue a single item
    yield from barq.enqueue(say_hello)
    job, queue = yield from Queue.dequeue_any([fooq, barq], None)
    assert job.func == say_hello
    assert queue == barq

    # Enqueue items on both queues
    yield from barq.enqueue(say_hello, 'for Bar')
    yield from fooq.enqueue(say_hello, 'for Foo')

    job, queue = yield from Queue.dequeue_any([fooq, barq], None)
    assert queue == fooq
    assert job.func == say_hello
    assert job.origin == fooq.name
    assert job.args[0] == 'for Foo', 'Foo should be dequeued first.'

    job, queue = yield from Queue.dequeue_any([fooq, barq], None)
    assert queue == barq
    assert job.func == say_hello
    assert job.origin == barq.name
    assert job.args[0] == 'for Bar', 'Bar should be dequeued second.'


def test_dequeue_any_ignores_nonexisting_jobs(set_loop):
    """Dequeuing (from any queue) silently ignores non-existing jobs."""

    q = Queue('low')
    uuid = '49f205ab-8ea3-47dd-a1b5-bfa186870fc8'
    yield from q.push_job_id(uuid)

    # Dequeue simply ignores the missing job and returns None
    assert (yield from q.count) == 1
    assert not (yield from Queue.dequeue_any([Queue(), Queue('low')], None))
    assert not (yield from q.count)


def test_dequeue_any_with_timeout(set_loop):
    """Dequeue any behavior with timeout."""

    queue = Queue()
    with pytest.raises(ValueError):
        yield from Queue.dequeue_any([queue], 0)
    with pytest.raises(DequeueTimeout):
        yield from Queue.dequeue_any([queue], 1)
    job = yield from queue.enqueue(say_hello)
    assert (yield from Queue.dequeue_any([queue], 1)) == (job, queue)


def test_enqueue_sets_status(set_loop):
    """Enqueueing a job sets its status to "queued"."""

    q = Queue()
    job = yield from q.enqueue(say_hello)
    assert (yield from job.get_status()) == JobStatus.QUEUED


def test_enqueue_call_sets_status(set_loop):
    """Enqueueing call sets jobs state to 'queued'."""

    q = Queue()
    job = yield from q.enqueue_call(say_hello)
    assert (yield from job.is_queued)


def test_enqueue_explicit_args(set_loop):
    """enqueue() works for both implicit/explicit args."""

    q = Queue()

    # Implicit args/kwargs mode
    job = yield from q.enqueue(echo, 1, timeout=1, result_ttl=1, bar='baz')
    assert job.timeout == 1
    assert job.result_ttl == 1
    assert (yield from job.perform()) == ((1,), {'bar': 'baz'})

    # Explicit kwargs mode
    job = yield from q.enqueue(
        echo, timeout=2, result_ttl=2,
        args=[1], kwargs={'timeout': 1, 'result_ttl': 1})
    assert job.timeout == 2
    assert job.result_ttl == 2
    assert (yield from job.perform()) == \
        ((1,), {'timeout': 1, 'result_ttl': 1})


def test_all_queues(loop):
    """All queues"""

    q1 = Queue('first-queue', loop=loop)
    q2 = Queue('second-queue', loop=loop)
    q3 = Queue('third-queue', loop=loop)

    # Ensure a queue is added only once a job is enqueued
    assert not len((yield from Queue.all()))
    yield from q1.enqueue(say_hello)
    assert len((yield from Queue.all())) == 1

    # Ensure this holds true for multiple queues
    yield from q2.enqueue(say_hello)
    yield from q3.enqueue(say_hello)
    names = [q.name for q in (yield from Queue.all())]
    assert len((yield from Queue.all())) == 3

    # Verify names
    assert 'first-queue' in names
    assert 'second-queue' in names
    assert 'third-queue' in names

    # Now empty two queues
    w = Worker([q2, q3], loop=loop)
    yield from w.work(burst=True)

    # Queue.all() should still report the empty queues
    assert len((yield from Queue.all())) == 3


def test_enqueue_dependents(loop, redis):
    """Enqueueing dependent jobs pushes all jobs in the depends set to the
    queue and removes them from DeferredJobQueue.
    """

    q = Queue(loop=loop)
    parent_job = Job.create(func=say_hello)
    yield from parent_job.save()
    job_1 = yield from q.enqueue(say_hello, depends_on=parent_job)
    job_2 = yield from q.enqueue(say_hello, depends_on=parent_job)

    registry = DeferredJobRegistry(q.name, connection=redis)
    assert set((yield from registry.get_job_ids())) == {job_1.id, job_2.id}

    # After dependents is enqueued, job_1 and job_2 should be in queue
    assert not (yield from q.job_ids)
    yield from q.enqueue_dependents(parent_job)
    assert set((yield from q.job_ids)) == {job_2.id, job_1.id}
    assert not (yield from redis.exists(parent_job.dependents_key))

    # DeferredJobRegistry should also be empty
    assert not (yield from registry.get_job_ids())


def test_enqueue_dependents_on_multiple_queues(loop, redis):
    """Enqueueing dependent jobs on multiple queues pushes jobs in the
    queues and removes them from DeferredJobRegistry for each
    different queue.
    """

    q1 = Queue('queue_1', loop=loop)
    q2 = Queue('queue_2', loop=loop)
    parent_job = Job.create(func=say_hello)
    yield from parent_job.save()
    job_1 = yield from q1.enqueue(say_hello, depends_on=parent_job)
    job_2 = yield from q2.enqueue(say_hello, depends_on=parent_job)

    # Each queue has its own DeferredJobRegistry
    registry_1 = DeferredJobRegistry(q1.name, connection=redis)
    assert set((yield from registry_1.get_job_ids())) == {job_1.id}
    registry_2 = DeferredJobRegistry(q2.name, connection=redis)
    assert set((yield from registry_2.get_job_ids())) == {job_2.id}

    # After dependents is enqueued, job_1 on q1 and job_2 should be in q2
    assert not (yield from q1.job_ids)
    assert not (yield from q2.job_ids)
    yield from q1.enqueue_dependents(parent_job)
    yield from q2.enqueue_dependents(parent_job)
    assert set((yield from q1.job_ids)) == {job_1.id}
    assert set((yield from q2.job_ids)) == {job_2.id}
    assert not (yield from redis.exists(parent_job.dependents_key))

    # DeferredJobRegistry should also be empty
    assert not (yield from registry_1.get_job_ids())
    assert not (yield from registry_2.get_job_ids())


def test_enqueue_job_with_dependency(set_loop):
    """Jobs are enqueued only when their dependencies are finished."""

    # Job with unfinished dependency is not immediately enqueued
    parent_job = Job.create(func=say_hello)
    q = Queue()
    job = yield from q.enqueue_call(say_hello, depends_on=parent_job)
    assert not (yield from q.job_ids)
    assert (yield from job.get_status()) == JobStatus.DEFERRED

    # Jobs dependent on finished jobs are immediately enqueued
    yield from parent_job.set_status(JobStatus.FINISHED)
    yield from parent_job.save()
    job = yield from q.enqueue_call(say_hello, depends_on=parent_job)
    assert (yield from q.job_ids) == [job.id]
    assert job.timeout == Queue.DEFAULT_TIMEOUT
    assert (yield from job.get_status()) == JobStatus.QUEUED


def test_enqueue_job_with_dependency_by_id(set_loop):
    """"Can specify job dependency with job object or job id."""

    parent_job = Job.create(func=say_hello)
    q = Queue()
    yield from q.enqueue_call(say_hello, depends_on=parent_job.id)
    assert not (yield from q.job_ids)

    # Jobs dependent on finished jobs are immediately enqueued
    yield from parent_job.set_status(JobStatus.FINISHED)
    yield from parent_job.save()
    job = yield from q.enqueue_call(say_hello, depends_on=parent_job.id)
    assert (yield from q.job_ids) == [job.id]
    assert job.timeout == Queue.DEFAULT_TIMEOUT


def test_enqueue_job_with_dependency_and_timeout(set_loop):
    """Jobs remember their timeout when enqueued as a dependency."""

    # Job with unfinished dependency is not immediately enqueued
    parent_job = Job.create(func=say_hello)
    q = Queue()
    job = yield from q.enqueue_call(say_hello, depends_on=parent_job,
                                    timeout=123)
    assert not (yield from q.job_ids)
    assert job.timeout == 123

    # Jobs dependent on finished jobs are immediately enqueued
    yield from parent_job.set_status(JobStatus.FINISHED)
    yield from parent_job.save()
    job = yield from q.enqueue_call(say_hello, depends_on=parent_job,
                                    timeout=123)
    assert (yield from q.job_ids) == [job.id]
    assert job.timeout == 123


def test_enqueue_job_with_queue_default_timeout(set_loop):
    """Default timeout specified in queue constructor will be applied to job.
    """

    q = Queue('foo', default_timeout=9999)
    job = yield from q.enqueue(say_hello)
    assert job.timeout == 9999


def test_enqueue_call_custom_description(set_loop):
    """Custom description passed into enqueue_call will be saved in
    returned job.
    """

    q = Queue()
    job = yield from q.enqueue_call(say_hello, description='...')
    assert job.description == '...'


def test_enqueue_call_custom_meta(set_loop):
    """Custom meta passed into enqueue_call will be stored as job attribute."""

    q = Queue()
    job = yield from q.enqueue_call(say_hello, meta={'foo': 'bar'})
    assert job.meta == {}


# Failed queue tests.


def test_requeue_job(set_loop):
    """Requeueing existing jobs."""

    job = Job.create(func=div_by_zero, args=(1, 2, 3))
    job.origin = 'fake'
    yield from job.save()
    yield from get_failed_queue().quarantine(job, Exception('Some fake error'))

    assert (yield from Queue.all()) == [get_failed_queue()]
    assert (yield from get_failed_queue().count) == 1

    yield from get_failed_queue().requeue(job.id)

    assert not (yield from get_failed_queue().count)
    assert (yield from Queue('fake').count) == 1


def test_requeue_nonfailed_job_fails(set_loop):
    """Requeueing non-failed jobs raises error."""

    q = Queue()
    job = yield from q.enqueue(say_hello, 'Nick', foo='bar')

    # Assert that we cannot requeue a job that's not on the failed queue
    with pytest.raises(InvalidJobOperationError):
        yield from get_failed_queue().requeue(job.id)


def test_quarantine_preserves_timeout(set_loop):
    """Quarantine preserves job timeout."""

    job = Job.create(func=div_by_zero, args=(1, 2, 3))
    job.origin = 'fake'
    job.timeout = 200
    yield from job.save()
    yield from get_failed_queue().quarantine(job, Exception('Some fake error'))

    assert job.timeout == 200


def test_requeueing_preserves_timeout(set_loop):
    """Requeueing preserves job timeout."""

    job = Job.create(func=div_by_zero, args=(1, 2, 3))
    job.origin = 'fake'
    job.timeout = 200
    yield from job.save()
    yield from get_failed_queue().quarantine(job, Exception('Some fake error'))
    yield from get_failed_queue().requeue(job.id)

    job = yield from Job.fetch(job.id)
    assert job.timeout == 200


def test_requeue_sets_status_to_queued(set_loop):
    """Requeueing a job should set its status back to QUEUED."""

    job = Job.create(func=div_by_zero, args=(1, 2, 3))
    yield from job.save()
    yield from get_failed_queue().quarantine(job, Exception('Some fake error'))
    yield from get_failed_queue().requeue(job.id)

    job = yield from Job.fetch(job.id)
    assert (yield from job.get_status()) == JobStatus.QUEUED


def test_skip_queue(set_loop):
    """Ensure the skip_queue option functions."""

    q = Queue('foo')
    job1 = yield from q.enqueue(say_hello)
    job2 = yield from q.enqueue(say_hello)
    assert (yield from q.dequeue()) == job1
    skip_job = yield from q.enqueue(say_hello, at_front=True)
    assert (yield from q.dequeue()) == skip_job
    assert (yield from q.dequeue()) == job2
