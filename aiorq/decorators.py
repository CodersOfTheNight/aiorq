"""
    aiorq.decorators
    ~~~~~~~~~~~~~~~~

    This module contains Celery-like job decorator.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.decorators module written by Vincent
# Driessen and released under 2-clause BSD license.

import asyncio
from functools import wraps

from rq.compat import string_types
from rq.defaults import DEFAULT_RESULT_TTL

from .queue import Queue


def job(queue, connection=None, timeout=None, result_ttl=DEFAULT_RESULT_TTL):
    """A decorator that adds a ``delay`` method to the decorated function.

    Which in turn creates a RQ job when called.  Accepts a required
    ``queue`` argument that can be either a ``Queue`` instance or a
    string denoting the queue name.  For example:

        @job(queue='default')
        def simple_add(x, y):
            return x + y

        yield from simple_add.delay(1, 2) # Puts simple_add function into queue

    """

    def wrapper(f):

        @wraps(f)
        @asyncio.coroutine
        def delay(*args, **kwargs):

            if isinstance(queue, string_types):
                _queue = Queue(name=queue, connection=connection)
            else:
                _queue = queue
            depends_on = kwargs.pop('depends_on', None)
            coroutine = _queue.enqueue_call(
                f, args=args, kwargs=kwargs, timeout=timeout,
                result_ttl=result_ttl, depends_on=depends_on)
            return (yield from coroutine)

        f.delay = delay
        return f

    return wrapper
