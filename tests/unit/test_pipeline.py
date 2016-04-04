import asyncio

import pytest

from aiorq.connections import resolve_connection
from aiorq.exceptions import PipelineError
from aiorq.pipeline import pipeline_method, pipeline_property, Pipeline


def test_execute_each_method_without_pipeline(loop, redis):
    """If task pipeline was not set each method will be executed."""

    class Test:

        pipeline = pipeline_property

        def __init__(self, *, loop=None):
            self.connection = resolve_connection()
            self.loop = loop

        @pipeline_method
        def set_key(self, value):
            self.pipeline.set('foo', value)

    instance = Test(loop=loop)
    yield from instance.set_key(b'1')
    assert (yield from redis.get('foo')) == b'1'


def test_delay_execution_with_binded_pipeline(loop, redis):
    """Each method will be delayed if pipeline was set."""

    class Test:

        pipeline = pipeline_property

        def __init__(self, *, loop=None):
            self.connection = resolve_connection()
            self.loop = loop

        @pipeline_method
        def set_key(self, value):
            self.pipeline.set('foo', value)

    instance = Test(loop=loop)
    multi = redis.multi_exec()

    with Pipeline(multi, loop=loop):
        yield from instance.set_key(b'1')

    assert not (yield from redis.get('foo'))

    yield from multi.execute()
    assert (yield from redis.get('foo')) == b'1'


def test_cleanup_current_task_pipeline_attribute(loop, redis):
    """There is no pipeline attribute after context manager exit."""

    multi = redis.multi_exec()

    with Pipeline(multi, loop=loop):
        assert hasattr(asyncio.Task.current_task(loop=loop), 'pipeline')

    assert not hasattr(asyncio.Task.current_task(loop=loop), 'pipeline')


def test_pipeline_error(loop):
    """`PipelineError` will be raised if there is no current pipeline."""

    class Test:

        pipeline = pipeline_property

        def __init__(self, *, loop=None):
            self.loop = loop

        def set_key(self, value):
            self.pipeline.set('foo', value)

    instance = Test(loop=loop)
    with pytest.raises(PipelineError):
        instance.set_key(1)
