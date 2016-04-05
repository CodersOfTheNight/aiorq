from aiorq.keys import queue_key
from aiorq.protocol import empty_queue, queue_length


def test_empty_queue(redis):
    """Emptying queue."""

    yield from redis.rpush(queue_key('example'), 'foo')
    yield from redis.rpush(queue_key('example'), 'bar')
    assert (yield from queue_length(redis, 'example'))
    yield from empty_queue(redis, 'example')
    assert not (yield from queue_length(redis, 'example'))


# TODO: test `empty_queue` removes jobs
# TODO: test `empty_queue` removes job dependents
