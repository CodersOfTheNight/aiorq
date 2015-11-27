"""
    aiorq.queue
    ~~~~~~~~~~~

    This module define asyncio compatible job queue.

    :copyright: (c) 2015 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

class Queue:
    """asyncio job queue."""

    def __init__(self, name='default'):

        self.name = name

    def __eq__(self, other):

        return self.name == other.name
