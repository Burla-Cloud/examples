"""Tiny HTTP retry helper. Exponential backoff with full jitter."""
from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

T = TypeVar("T")


def with_backoff(fn: Callable[[], T], *, attempts: int = 3, base: float = 1.0,
                 cap: float = 30.0, retriable: tuple = (Exception,)) -> T:
    last_exc: BaseException | None = None
    for i in range(attempts):
        try:
            return fn()
        except retriable as e:
            last_exc = e
            if i == attempts - 1:
                break
            sleep = min(cap, base * (2 ** i))
            time.sleep(random.uniform(0, sleep))
    assert last_exc is not None
    raise last_exc
