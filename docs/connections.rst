Connections
===========

There are two different approaches to implicit and explicit connection
management.

Single redis connection
-----------------------

Use connection stored in the global namespace.

.. code:: python

    from aioredis import create_redis
    from aiorq import push_connection

    redis = yield from create_redis(('localhost', 6379))
    push_connection(redis)

Multiple redis connections
--------------------------

You can pass connection directly to Queue, Job or Worker constructors.

.. code:: python

    conn1 = yield from create_redis(('localhost', 6379))
    conn2 = yield from create_redis(('remote.host.org', 6379))

    q1 = Queue('foo', connection=conn1)
    q2 = Queue('bar', connection=conn2)

Or you can use stacked connection context.

.. code:: python

    from aiorq import Queue, Connection

    with Connection((yield from create_redis(('localhost', 6379)))):
        q1 = Queue('foo')
        with Connection((yield from create_redis(('remote.host.org', 6379)))):
            q2 = Queue('bar')
