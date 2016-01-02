import asyncio

from aioredis import create_redis
from aiorq import Queue

import mylib


@asyncio.coroutine
def go():
    redis = yield from create_redis(('localhost', 6379))
    queue = Queue('my_queue', connection=redis)
    job1 = yield from queue.enqueue(mylib.add, 1, 2)
    job2 = yield from queue.enqueue(mylib.add, 1, 2, depends_on=job1)
    job = yield from queue.enqueue(mylib.job_summator, job1.id, job2.id,
                                   depends_on=job2)
    yield from asyncio.sleep(2)
    result = yield from job.result
    assert result == 6, '{!r} is not equal 6'.format(result)
    print('Well done, Turner!')
    redis.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
