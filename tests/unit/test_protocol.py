from aiorq.keys import queues_key, queue_key, job_key
from aiorq.job import utcparse
from aiorq.protocol import empty_queue, queue_length, enqueue_job


# Queue.


def test_empty_queue(redis):
    """Emptying queue."""

    yield from redis.rpush(queue_key('example'), 'foo')
    yield from redis.rpush(queue_key('example'), 'bar')
    assert (yield from queue_length(redis, 'example'))
    yield from empty_queue(redis, 'example')
    assert not (yield from queue_length(redis, 'example'))


def test_empty_removes_jobs(redis):
    """Emptying a queue deletes the associated job objects."""

    queue = 'default'
    id = '2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        'created_at': '2016-04-05T22:40:35Z',
        'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        'description': 'fixtures.some_calculation(3, 4, z=2)',
        'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    yield from empty_queue(redis, 'default')
    assert not (yield from redis.exists(job_key(id)))


# TODO: test `empty_queue` removes job dependents


# Enqueue job.


def test_enqueue_job_store_job_hash(redis):
    """Storing jobs."""

    queue = 'default'
    id = '2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        'created_at': '2016-04-05T22:40:35Z',
        'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        'description': 'fixtures.some_calculation(3, 4, z=2)',
        'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from redis.type(job_key(id))) == b'hash'
    assert spec['data'] == (yield from redis.hget(job_key(id), 'data'))


def test_enqueue_job_register_queue(redis):
    """Enqueue job will add its queue into queues storage."""

    queue = 'default'
    id = '2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        'created_at': '2016-04-05T22:40:35Z',
        'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        'description': 'fixtures.some_calculation(3, 4, z=2)',
        'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from redis.smembers(queues_key())) == [b'default']


def test_enqueue_job_add_job_key_to_the_queue(redis):
    """Enqueue job must add its id to the queue."""

    queue = 'default'
    id = '2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        'created_at': '2016-04-05T22:40:35Z',
        'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        'description': 'fixtures.some_calculation(3, 4, z=2)',
        'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    queue_content = [b'2a5079e7-387b-492f-a81c-68aa55c194c8']
    assert (yield from redis.lrange(queue_key(queue), 0, -1)) == queue_content


def test_enqueue_job_set_job_status(redis):
    """Enqueue job must set job status to QUEUED."""

    queue = 'default'
    id = '2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        'created_at': '2016-04-05T22:40:35Z',
        'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        'description': 'fixtures.some_calculation(3, 4, z=2)',
        'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from redis.hget(job_key(id), 'status')) == b'queued'


def test_enqueue_job_set_job_origin(redis):
    """Enqueue job must set jobs origin queue."""

    queue = 'default'
    id = '2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        'created_at': '2016-04-05T22:40:35Z',
        'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        'description': 'fixtures.some_calculation(3, 4, z=2)',
        'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    assert (yield from redis.hget(job_key(id), 'origin')) == b'default'


def test_enqueue_job_set_job_enqueued_at(redis):
    """Enqueue job must set enqueued_at time."""

    queue = 'default'
    id = '2a5079e7-387b-492f-a81c-68aa55c194c8'
    spec = {
        'created_at': '2016-04-05T22:40:35Z',
        'data': b'\x80\x04\x950\x00\x00\x00\x00\x00\x00\x00(\x8c\x19fixtures.some_calculation\x94NK\x03K\x04\x86\x94}\x94\x8c\x01z\x94K\x02st\x94.',  # noqa
        'description': 'fixtures.some_calculation(3, 4, z=2)',
        'timeout': 180,
    }
    yield from enqueue_job(redis, queue, id, spec)
    utcparse((yield from redis.hget(job_key(id), 'enqueued_at')))
