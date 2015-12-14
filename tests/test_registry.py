from aiorq.job import Job


def test_add_and_remove(redis, registry, timestamp):
    """Adding and removing job to StartedJobRegistry."""

    job = Job()

    # Test that job is added with the right score
    yield from registry.add(job, 1000)
    assert (yield from redis.zscore(registry.key, job.id)) < timestamp + 1002

    # Ensure that a timeout of -1 results in a score of -1
    yield from registry.add(job, -1)
    assert (yield from redis.zscore(registry.key, job.id)) == -1

    # Ensure that job is properly removed from sorted set
    yield from registry.remove(job)
    assert not (yield from redis.zscore(registry.key, job.id))


def test_get_job_ids(redis, registry, timestamp):
    """Getting job ids from StartedJobRegistry."""

    yield from redis.zadd(registry.key, timestamp + 10, 'foo')
    yield from redis.zadd(registry.key, timestamp + 20, 'bar')
    assert (yield from registry.get_job_ids()) == ['foo', 'bar']
