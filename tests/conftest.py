import sys
from os.path import dirname, abspath

import pytest


sys.path.append(dirname(abspath(__file__)))


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
