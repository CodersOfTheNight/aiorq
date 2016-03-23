import pytest
import redis


@pytest.yield_fixture
def flush_redis():

    yield
    connection = redis.StrictRedis()
    connection.flushdb()
