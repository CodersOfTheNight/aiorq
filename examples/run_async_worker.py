import asyncio

from aioredis import create_redis
from aiorq import Worker

@asyncio.coroutine
def go():
    redis = yield from create_redis(('localhost', 6379))
    worker = Worker('my_async_queue', connection=redis)
    yield from worker.work()


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
