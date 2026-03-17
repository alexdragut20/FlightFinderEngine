from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from functools import wraps
from threading import RLock
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])
_KW_MARKER = object()


def _build_cache_key(args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, ...]:
    if not kwargs:
        return args
    return args + (_KW_MARKER,) + tuple(sorted(kwargs.items()))


def per_instance_lru_cache(maxsize: int | None = 128) -> Callable[[F], F]:
    """Cache method results on each instance instead of on the function object.

    This preserves the behavior we want from `functools.lru_cache` while avoiding
    cross-instance retention of `self`, which is what flake8-bugbear warns about.
    """

    def decorator(func: F) -> F:
        cache_attr = f"__cache_{func.__name__}"
        lock_attr = f"__cache_lock_{func.__name__}"

        @wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            cache: OrderedDict[tuple[Any, ...], Any] | None = getattr(self, cache_attr, None)
            if cache is None:
                cache = OrderedDict()
                setattr(self, cache_attr, cache)

            lock: RLock | None = getattr(self, lock_attr, None)
            if lock is None:
                lock = RLock()
                setattr(self, lock_attr, lock)

            key = _build_cache_key(args, kwargs)
            with lock:
                if key in cache:
                    cache.move_to_end(key)
                    return cache[key]

            result = func(self, *args, **kwargs)

            with lock:
                cache[key] = result
                cache.move_to_end(key)
                if maxsize is not None and maxsize > 0:
                    while len(cache) > maxsize:
                        cache.popitem(last=False)
            return result

        return wrapper  # type: ignore[return-value]

    return decorator
