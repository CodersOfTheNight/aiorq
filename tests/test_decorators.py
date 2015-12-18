from fixtures import decorated_job


def test_decorator_preserves_functionality():
    """Ensure that a decorated function's functionality is still preserved."""

    assert decorated_job(1, 2) == 3
