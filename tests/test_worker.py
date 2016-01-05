from aiorq import Worker, Queue
from fixtures import say_hello


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


def test_work_and_quit():
    """Worker processes work, then quits."""

    fooq, barq = Queue('foo'), Queue('bar')
    w = Worker([fooq, barq])
    assert not (yield from w.work(burst=True))

    yield from fooq.enqueue(say_hello, name='Frank')
    assert (yield from w.work(burst=True))


def test_worker_ttl(redis):
    """Worker ttl."""

    w = Worker([])
    yield from w.register_birth()
    [worker_key] = yield from redis.smembers(Worker.redis_workers_keys)
    assert (yield from redis.ttl(worker_key))
    yield from w.register_death()


def test_work_via_string_argument():
    """Worker processes work fed via string arguments."""

    q = Queue('foo')
    w = Worker([q])
    job = yield from q.enqueue('fixtures.say_hello', name='Frank')
    assert (yield from w.work(burst=True))
    assert (yield from job.result) == 'Hi there, Frank!'
