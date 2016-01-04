"""
    aiorq.connections
    ~~~~~~~~~~~~~~~~~

    This module implement connection resolution mechanism.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""
# This code was adapted from rq.connections module written by Vincent
# Driessen and released under 2-clause BSD license.

from contextlib import contextmanager

from rq.connections import NoRedisConnectionException
from rq.local import LocalStack


@contextmanager
def Connection(connection):
    """All queues created in the inner block will use this connection."""

    push_connection(connection)
    try:
        yield
    finally:
        popped = pop_connection()
        assert popped == connection, \
            'Unexpected Redis connection was popped off the stack. ' \
            'Check your Redis connection setup.'


def pop_connection():
    """Pops the topmost connection from the stack."""

    return _connection_stack.pop()


def push_connection(redis):
    """Pushes the given connection on the stack."""

    _connection_stack.push(redis)


def get_current_connection():
    """Returns the current Redis connection (i.e. the topmost on the
    connection stack).
    """

    return _connection_stack.top


def resolve_connection(connection=None):
    """Convenience function to resolve the given or the current connection.
    Raises an exception if it cannot resolve a connection now.
    """

    if connection is not None:
        return connection

    connection = get_current_connection()
    if connection is None:
        raise NoRedisConnectionException(
            'Could not resolve a Redis connection')
    return connection


_connection_stack = LocalStack()
