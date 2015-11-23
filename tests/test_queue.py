from aiorq import Queue


def test_create_queue():
    """We can create queue instance."""

    q = Queue()
    assert q.name == 'default'


def test_create_named_queue():
    """We can create named queue instance."""

    q = Queue('my-queue')
    assert q.name == 'my-queue'
