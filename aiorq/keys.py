"""
    aiorq.keys
    ~~~~~~~~~~

    Redis keys naming convention.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""


def queue_key(name):
    """Redis key for named queue."""

    return 'rq:queue:' + name
