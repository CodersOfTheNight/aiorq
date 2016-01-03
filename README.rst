
.. |travis| image:: https://img.shields.io/travis/proofit404/aiorq.svg?style=flat-square
    :target: https://travis-ci.org/proofit404/aiorq
    :alt: Build Status

.. |coveralls| image:: https://img.shields.io/coveralls/proofit404/aiorq.svg?style=flat-square
    :target: https://coveralls.io/r/proofit404/aiorq
    :alt: Coverage Status

.. |requires| image:: https://img.shields.io/requires/github/proofit404/aiorq.svg?style=flat-square
    :target: https://requires.io/github/proofit404/aiorq/requirements
    :alt: Requirements Status

.. |codacy| image:: https://img.shields.io/codacy/2ba66fc33f9d482095350cc69b4fc02b.svg?style=flat-square
    :target: https://www.codacy.com/app/proofit404/aiorq
    :alt: Code Quality Status

.. |pypi| image:: https://img.shields.io/pypi/v/aiorq.svg?style=flat-square
    :target: https://pypi.python.org/pypi/aiorq/
    :alt: Python Package Version

=====
aiorq
=====

|travis| |coveralls| |requires| |codacy| |pypi|

asyncio_ client and server for RQ_.

- `Source Code`_
- `Issue Tracker`_
- Documentation_

Features
--------

- Event loop friendly
- Non-blocking job enqueueing and result obtaining

Installation
------------

You can always install last released version from python package
index.

.. code:: bash

    pip install aiorq

Getting started
---------------

Suppose we have a module with slow blocking function like this one.

.. code:: python

    import requests

    def get_json(url):
        response = requests.get(url)
        return response.json()

To schedule deferred jobs create queue and enqueue the function call
within event loop.

.. code:: python

    import asyncio

    from aioredis import create_redis
    from aiorq import Queue

    from my_module import get_json

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def main():
        redis = yield from create_redis(('localhost', 6379), loop=loop)
        queue = Queue(connection=redis)
        job = yield from queue.enqueue(get_json, 'https://www.python.org')
        print((yield from job.result))
        redis.close()
        yield from redis.wait_closed()

    loop.run_until_complete(main())

License
-------

The aiorq is offered under LGPL license.

.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _rq: http://python-rq.org
.. _source code: https://github.com/proofit404/aiorq
.. _issue tracker: https://github.com/proofit404/aiorq/issues
.. _documentation: http://aiorq.readthedocs.org/
