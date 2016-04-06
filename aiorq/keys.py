"""
    aiorq.keys
    ~~~~~~~~~~

    Redis keys naming convention.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""


def queues_key():
    """Redis key for all named queues names."""

    return 'rq:queues'


def queue_key(name):
    """Redis key for named queue."""

    return 'rq:queue:' + name


def job_key(id):
    """Redis key for job hash."""

    return 'rq:job:' + id
