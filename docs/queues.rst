Queues
======

I make assumption that you are already familiar with RQ library.
Queues, jobs and workers preserve its original meanings.  This library
tries to repeat all public API of original package and guarantee
binary compatibility with its latest release.

Enqueueing jobs
---------------

First of all you need a queue.  To do that create new Redis connection
and pass it into Queue constructor.  We need started event loop for
this task so Queue's creation is possible only inside running
coroutines.  Then simply put job into queue.

.. block:: python

    @asyncio.coroutine
    def go():
        redis = yield from create_redis(('localhost', 6379))
        queue = Queue('my_queue', connection=redis)
        job = yield from queue.enqueue(mylib.add, 1, 2)
        yield from asyncio.sleep(0.2)
        result = yield from job.result
        assert result == 3, '{!r} is not equal 3'.format(result)
        print('Well done, Turner!')
        redis.close()

Start old good rq worker with familiar command.

.. code:: bash

    rq worker my_queue

Remember ``mylib`` must be importable to worker, i.e. add ``examples``
directory into python path.

The ``@job`` decorator
----------------------

You can also use Celery-style decorated tasks.

.. code:: python

    @job('my_queue')
    def summator(*args):

        return add(*args)

.. code:: python

    @asyncio.coroutine
    def go():
        redis = yield from create_redis(('localhost', 6379))
        push_connection(redis)
        job = yield from mylib.summator.delay(1, 2)
        yield from asyncio.sleep(0.2)
        result = yield from job.result
        assert result == 3, '{!r} is not equal 3'.format(result)
        print('Well done, Turner!')
        redis.close()

Job dependencies
----------------

To execute a job that depends on another job, use the ``depends_on``
argument.

.. code:: python

    from rq import Connection, Queue

    def job_summator(id1, id2):

        with Connection():
            job1 = Queue().fetch_job(id1)
            job2 = Queue().fetch_job(id2)

        return add(job1.result, job2.result)

.. code:: python

    queue = Queue('my_queue', connection=redis)
    job1 = yield from queue.enqueue(mylib.add, 1, 2)
    job2 = yield from queue.enqueue(mylib.add, 1, 2, depends_on=job1)
    job3 = yield from queue.enqueue(mylib.job_summator, job1.id, job2.id,
                                    depends_on=job2)

In the example above we use ``rq.Job`` to fetch result.  The reason we
do that is synchronous worker provided by RQ package.  Call to
``aiorq.Job`` methods require running event loop and can't be done
easily without asynchronous workers and enqueued coroutines.  We will
discus this topic later.

Working with Queues
-------------------

Queues have a few useful methods you can use to get some interesting
information.

.. code:: python

    redis = yield from create_redis(('localhost', 6379))
    q = Queue(connection=redis)

    # Getting the number of jobs in the queue
    yield from q.count

    # Get a list of job IDs from the queue
    yield from q.job_ids

    # Get a list of enqueued job instances
    yield from q.jobs

    # Returns job having ID "my_id"
    yield from q.fetch_job('my_id')

Bypassing workers
-----------------

Synchronous queues are not supported.  You can't pass ``async=False``
to ``Queue`` constructor as you do with original package.  If you need
this for testing purposes, that's simple: separate business logic from
any task queue as much as possible and test it independently.  That's
it!  Or use ``unittest.mock.patch`` if you don't share my point of
view in system design.
