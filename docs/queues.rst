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

.. literalinclude:: ../examples/enqueueing.py
    :language: python
    :lines: 9-18

Start old good rq worker with familiar command.

.. code:: bash

    rq worker my_queue

Remember ``mylib`` must be importable to worker, i.e. add ``examples``
directory into python path.

The ``@job`` decorator
----------------------

You can also use Celery-style decorated tasks.

.. literalinclude:: ../examples/mylib.py
    :language: python
    :lines: 9-12

.. literalinclude:: ../examples/job_decorator.py
    :language: python
    :lines: 9-18

Job dependencies
----------------

To execute a job that depends on another job, use the ``depends_on``
argument.

.. literalinclude:: ../examples/mylib.py
    :language: python
    :lines: 2-4,17-23

.. literalinclude:: ../examples/dependencies.py
    :language: python
    :lines: 12-16
    :dedent: 4

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
