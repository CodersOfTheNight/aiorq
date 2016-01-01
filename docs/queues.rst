Queues
======

I make assumption that you are already familiar with RQ library.
Queues, jobs and workers preserve its original meanings.  This library
tries to repeat all public API of original package and granite
compatibility with its last release.

Enqueueing jobs
---------------

First of all you need a queue.  To do that create new Redis connection
and pass it into Queue constructor.  We need started event loop for
this task so Queue's creation is possible only inside running
coroutines.  Then simply put job into queue.

.. literalinclude:: ../examples/enqueueing.py

Start old good rq worker with familiar command.

.. code:: bash

    rq worker my_queue

Remember ``mylib`` must be importable to worker, i.e. add ``examples``
directory into python path.
