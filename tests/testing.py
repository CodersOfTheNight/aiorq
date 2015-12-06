import asyncio
import functools
import gc

from aioredis import create_redis, Redis
from rq.local import release_local

from aiorq import pop_connection, push_connection
from aiorq.connections import _connection_stack


class Logger:

    def __init__(self, connection):

        self.connection = connection

    def execute(self, command, *args, **kwargs):

        logger_args = []
        for arg in (command,) + args:
            if hasattr(arg, 'decode'):
                try:
                    arg = arg.decode()
                except UnicodeDecodeError:
                    pass
            logger_args.append(arg)
        print('>>>', *logger_args)
        return self.connection.execute(command, *args, **kwargs)

    def __getattr__(self, name):

        return getattr(self.connection, name)


@asyncio.coroutine
def find_connection(loop):
    """Get test redis connection."""

    commands_factory = lambda connection: Redis(Logger(connection))
    kwargs = {'loop': loop, 'commands_factory': commands_factory}
    return (yield from create_redis(('localhost', 6379), **kwargs))


def async_test(f):
    """Run asynchronous tests inside event loop as coroutines."""

    @functools.wraps(f)
    def wrapper(**kwargs):

        @asyncio.coroutine
        def coroutine(loop, kwargs):
            redis = yield from find_connection(loop)
            push_connection(redis)
            if 'redis' in kwargs:
                kwargs['redis'] = redis
            if 'loop' in kwargs:
                kwargs['loop'] = loop
            try:
                yield from asyncio.coroutine(f)(**kwargs)
            except Exception:
                raise
            else:
                connection = pop_connection()
                assert connection == redis, (
                    'Wow, something really nasty happened to the '
                    'Redis connection stack. Check your setup.')
            finally:
                yield from redis.flushdb()
                release_local(_connection_stack)

        assert not len(_connection_stack), \
            'Test require empty connection stack'
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        loop.run_until_complete(coroutine(loop, kwargs))
        loop.stop()
        loop.run_forever()
        loop.close()
        gc.collect()
        asyncio.set_event_loop(None)

    return wrapper
