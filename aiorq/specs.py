"""
    aiorq.specs
    ~~~~~~~~~~~

    State transition specs.

    :copyright: (c) 2015-2016 by Artem Malyshev.
    :license: LGPL-3, see LICENSE for more details.
"""


class JobStatus:

    QUEUED = b'queued'
    FINISHED = b'finished'
    FAILED = b'failed'
    STARTED = b'started'
    DEFERRED = b'deferred'
