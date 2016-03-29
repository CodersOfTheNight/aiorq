try:
    from asyncio import ensure_future
except ImportError:
    from asyncio import async as ensure_future


__all__ = ['ensure_future']
