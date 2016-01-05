from contextlib import contextmanager


@contextmanager
def EventLoopDeathPenalty(self, timeout):  # since it used as class attribute
    """Timeout handling context manager."""

    # TODO: really handle timeouts
    try:
        yield
    finally:
        pass
