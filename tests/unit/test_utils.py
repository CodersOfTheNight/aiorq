from aiorq.connections import resolve_connection
from aiorq.utils import pipeline_method, pipeline_property


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
