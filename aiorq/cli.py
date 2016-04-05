import asyncio
import logging
import signal

import aioredis
import click

from .compat import ensure_future
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
    ensure_future(run_worker(loop, queues), loop=loop)
    loop.run_forever()
    loop.close()


@asyncio.coroutine
def run_worker(loop, queues):
    redis = yield from aioredis.create_redis(('localhost', 6379))
    worker = Worker(queues, connection=redis, loop=loop)
    loop.add_signal_handler(signal.SIGTERM, worker.request_stop)
    yield from worker.work()
    loop.stop()
