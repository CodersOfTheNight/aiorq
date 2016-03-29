import asyncio

from aioredis import create_redis
from aiorq import Queue

import http_client


@asyncio.coroutine
def go():
    redis = yield from create_redis(('localhost', 6379))
    queue = Queue('my_async_queue', connection=redis)
    job = yield from queue.enqueue(
        http_client.fetch_page, 'https://www.python.org')
    yield from asyncio.sleep(5)
    result = yield from job.result
    assert '</html>' in result, 'Given content is not a html page'
    print('Well done, Turner!')
    redis.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
