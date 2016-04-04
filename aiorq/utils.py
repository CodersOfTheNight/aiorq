"""
    aiorq.utils
    ~~~~~~~~~~~

    This module implements different utility functions.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio
import functools


def pipelined_method(f):
    """Allows to pass transaction easily or create the new one if necessary."""

    @asyncio.coroutine
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):

        if 'pipeline' in kwargs:
            f(self, *args, **kwargs)
        else:
            pipe = self.connection.pipeline()
            kw = {'pipeline': pipe}
            kw.update(kwargs)
            f(self, *args, **kw)
            yield from pipe.execute()

    return wrapper


def pipeline_method(method):
    """Decorates method with access to current `asyncio.Task` pipeline."""

    @asyncio.coroutine
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):

        current_task = asyncio.Task.current_task(loop=self.loop)
        pipeline = self.connection.pipeline()
        current_task.pipeline = pipeline
        try:
            method(self, *args, **kwargs)
        finally:
            del current_task.pipeline  # TODO: test me
        yield from pipeline.execute()

    return wrapper


@property
def pipeline_property(self):
    """Current `asyncio.Task` pipeline property."""

    current_task = asyncio.Task.current_task(loop=self.loop)
    # TODO: test attribute error
    return current_task.pipeline
