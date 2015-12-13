import sys
from os.path import dirname, abspath

import pytest


sys.path.append(dirname(abspath(__file__)))


from testing import async_test


@pytest.fixture
def redis():
    """Suppress pytest errors about missing fixtures.

    Actual redis connection will be inserted by asynchronous test
    decorator instead of this fixture.

    """

    pass


@pytest.fixture
def loop():
    """Suppress pytest errors about missing fixtures.

    Actual asyncio event loop will be inserted by asynchronous test
    decorator instead of this fixture.

    """

    pass


@pytest.fixture
def registry():
    """Suppress pytest errors about missing fixtures.

    Actual StartedJobRegistry instance will be inserted by
    asynchronous test decorator instead of this fixture.

    """

    pass


def pytest_pycollect_makeitem(collector, name, obj):

    # Collect generators as regular tests.
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        return list(collector._genfunctions(name, obj))


def pytest_pyfunc_call(pyfuncitem):

    path = pyfuncitem.location[0]
    # Don't decorate tests written against tests.
    if path != 'tests/test_testing.py':
        funcargs = pyfuncitem.funcargs
        argnames = pyfuncitem._fixtureinfo.argnames
        testargs = {arg: funcargs[arg] for arg in argnames}
        async_test(pyfuncitem.obj)(**testargs)
        return True
