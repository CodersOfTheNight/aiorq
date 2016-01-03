Jobs
====

Following code examples will demonstrate some useful techniques you
can apply in your tasks definitions.

Accessing the "current" job
---------------------------

You can access current job in non blocking manner.  Use following code
from coroutine running inside aiorq worker event loop.  To access
current job from regular function running inside rq synchronous worker
use original ``get_current_job`` function from rq package.

.. code:: python

    import asyncio

    from aiorq import get_current_job

    @asyncio.coroutine
    def add(x, y):

        job = yield from get_current_job()
        print('Current job:', job.id)
        return x + y

Storing arbitrary data on jobs
------------------------------

You can store custom pickleable data in the job's ``meta`` property.

.. code:: python

    import asyncio
    import socket

    from aiorq import get_current_job

    @asyncio.coroutine
    def add(x, y):

        job = yield from get_current_job()
        job.meta['handled_by'] = socket.gethostname()
        yield from job.save()
        return x + y

Time to live for job in queue
-----------------------------

To enqueue job that shouldn't be executed after a certain amount of
time, you can define job TTL as such:

.. code:: python

    job = yield from q.enqueue(some_func, ttl=43)

Failed jobs
-----------

If a job fails and raises an exception, the worker will put the job in
a failed job queue.  On the job instance, the ``is_failed`` property
will be true.  To fetch all failed jobs, scan through the
``get_failed_queue()`` queue.

.. code:: python

    from aiorq import get_failed_queue

    job = yield from q.enqueue(div_by_zero)
    assert (yield from job.is_failed())

    fq = get_failed_queue()
    yield from fq.requeue(job.id)
