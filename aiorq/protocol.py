"""
    aiorq.protocol
    ~~~~~~~~~~~~~~

    Redis related state manipulations.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio


@asyncio.coroutine
def empty_queue(connection):
    pass


@asyncio.coroutine
def compact_queue(connection):
    pass


@asyncio.coroutine
def enqueue_job(connection):
    pass


@asyncio.coroutine
def dequeue_job(connection):
    pass


@asyncio.coroutine
def remove_job(connection):
    pass


@asyncio.coroutine
def quarantine_job(connection):
    pass


@asyncio.coroutine
def requeue_job(connection):
    pass


@asyncio.coroutine
def cancel_job(connection):
    pass


@asyncio.coroutine
def start_job(connection):
    pass


@asyncio.coroutine
def finish_job(connection):
    pass


@asyncio.coroutine
def fail_job(connection):
    pass
