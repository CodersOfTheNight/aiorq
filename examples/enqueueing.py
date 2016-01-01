import asyncio

from aioredis import create_redis
from aiorq import Queue

import mylib


@asyncio.coroutine
def go():
    redis = yield from create_redis(('localhost', 6379))
    queue = Queue('my_queue', connection=redis)
    job = yield from queue.enqueue(mylib.add, 1, 2)
    result = yield from job.result
    assert result == 3, '{!r} is not equal 3'.format(result)
    print('Well done, Turner!')


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
