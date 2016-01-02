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
