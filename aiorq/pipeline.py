"""
    aiorq.pipeline
    ~~~~~~~~~~~~~~

    This module implements redis pipeline access for chained method calls.

    Implied usage:

    .. code:: python

        class Foo:

            pipeline = pipeline_property

            @pipeline_method
            def do(self):

                self.pipeline.set('foo', 'bar')

        foo = Foo()
        multi = redis.multi_exec()

        with Pipeline():
            foo.do()

        yield from multi.execute()

    Foo class should have loop, pipeline and connection attributes.
    This is common convention for aiorq Queue, Job and Worker classes.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio
import contextlib
import functools

from .exceptions import PipelineError


def pipeline_method(method):
    """Decorates method with access to current `asyncio.Task` pipeline."""

    @asyncio.coroutine
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):

        if pipeline_bound(self):
            method(self, *args, **kwargs)
        else:
            pipeline = self.connection.pipeline()
            with Pipeline(pipeline, loop=self.loop):
                method(self, *args, **kwargs)
            yield from pipeline.execute()

    return wrapper


@contextlib.contextmanager
def Pipeline(pipeline, *, loop=None):
    """Bind pipeline for current `asyncio.Task`."""

    current_task = asyncio.Task.current_task(loop=loop)
    current_task.pipeline = pipeline
    try:
        yield
    finally:
        del current_task.pipeline


def pipeline_bound(obj):
    """Check if there is pipeline binding for this object."""

    try:
        obj.pipeline
    except PipelineError:
        return False
    else:
        return True


@property
def pipeline_property(self):
    """Current `asyncio.Task` pipeline property."""

    current_task = asyncio.Task.current_task(loop=self.loop)
    try:
        current_pipeline = current_task.pipeline
    except AttributeError:
        raise PipelineError
    return current_pipeline
