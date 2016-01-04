from aiorq import Worker, Queue


def test_create_worker():
    """Worker creation using various inputs."""

    # With single string argument
    w = Worker('foo')
    assert w.queues[0].name == 'foo'

    # With list of strings
    w = Worker(['foo', 'bar'])
    assert w.queues[0].name == 'foo'
    assert w.queues[1].name == 'bar'

    # With iterable of strings
    w = Worker(iter(['foo', 'bar']))
    assert w.queues[0].name == 'foo'
    assert w.queues[1].name == 'bar'

    # With single Queue
    w = Worker(Queue('foo'))
    assert w.queues[0].name == 'foo'

    # With iterable of Queues
    w = Worker(iter([Queue('foo'), Queue('bar')]))
    assert w.queues[0].name == 'foo'
    assert w.queues[1].name == 'bar'

    # With list of Queues
    w = Worker([Queue('foo'), Queue('bar')])
    assert w.queues[0].name == 'foo'
    assert w.queues[1].name == 'bar'
