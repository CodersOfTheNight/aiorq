==========
 Examples
==========

This is ``aiorq`` usage example.  It implements distributed internet
crawler build on top of ``aiohttp`` framework.

Installation
------------

Create virtual environment and install examples into it

.. code:: bash

    pip install .

Usage
-----

Start aiorq worker

.. code:: bash

    aiorq worker my_async_queue

Schedule some tasks

.. code:: bash

    python -m enqueue_coroutine
