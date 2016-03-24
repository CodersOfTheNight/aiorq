import asyncio

import aioredis
import click

from .worker import Worker


@click.group()
def cli():
    """aiorq command line tool."""

    pass


@cli.command()
@click.argument('queues', nargs=-1)
def worker(queues):
    """Starts an aiorq worker."""

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_worker(queues))


@asyncio.coroutine
def run_worker(queues):
    redis = yield from aioredis.create_redis(('localhost', 6379))
    worker = Worker(queues, connection=redis)
    yield from worker.work()
