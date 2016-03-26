import asyncio
import logging

import aioredis
import click

from .worker import Worker


@click.group()
def cli():
    """aiorq command line tool."""

    pass


@cli.command()
@click.argument('queues', nargs=-1)
@click.option('--verbose', '-v', 'log_level', flag_value='DEBUG')
@click.option('--quiet', '-q', 'log_level', flag_value='WARNING')
def worker(queues, log_level):
    """Starts an aiorq worker."""

    level_name = log_level or 'INFO'
    level = getattr(logging, level_name)
    logging.basicConfig(level=level)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_worker(queues))


@asyncio.coroutine
def run_worker(queues):
    redis = yield from aioredis.create_redis(('localhost', 6379))
    worker = Worker(queues, connection=redis)
    yield from worker.work()
