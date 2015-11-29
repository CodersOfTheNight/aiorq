import pytest

from aiorq.testing import async_test


class CatchMe(Exception):
    """Exception for testing purposes."""

    pass


def test_async_test_on_function():
    """Asynchronous test wraps regular function."""

    @async_test
    def f(*args):
        raise CatchMe

    with pytest.raises(CatchMe):
        f()


def test_async_test_on_generator():
    """Asynchronous test wraps generator."""

    @async_test
    def g(*args):
        yield
        raise CatchMe

    with pytest.raises(CatchMe):
        g()
