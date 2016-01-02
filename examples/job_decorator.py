import asyncio

from aioredis import create_redis
from aiorq import push_connection

import mylib


@asyncio.coroutine
def go():
    redis = yield from create_redis(('localhost', 6379))
    push_connection(redis)
    job = yield from mylib.summator.delay(1, 2)
    yield from asyncio.sleep(0.2)
    result = yield from job.result
    assert result == 3, '{!r} is not equal 3'.format(result)
    print('Well done, Turner!')
    redis.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
