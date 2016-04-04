"""
    aiorq.utils
    ~~~~~~~~~~~

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""

import asyncio
import functools


def pipelined_method(f):
    """Allows to pass transaction easily or create the new one if necessary."""

    @asyncio.coroutine
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):

        if 'pipeline' in kwargs:
            f(self, *args, **kwargs)
        else:
            pipe = self.connection.pipeline()
            kw = {'pipeline': pipe}
            kw.update(kwargs)
            f(self, *args, **kw)
            yield from pipe.execute()

    return wrapper
