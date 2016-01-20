import asyncio

import aiohttp

@asyncio.coroutine
def fetch_page(url):
    print('Start request.')
    session = aiohttp.ClientSession()
    response = yield from session.get(url)
    assert response.status == 200
    try:
        content = yield from response.text()
    finally:
        yield from response.release()
    session.close()
    print('Finish request.')
    return content
