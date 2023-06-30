from typing import Any, Callable

from cytoolz import identity


def defwrap(defline: str, source: str) -> str:
    """properly indent source code under a def statement"""
    indented = "\n".join([f"\t{line}" for line in source.splitlines()])
    return f"{defline}:\n{indented}"


def pointlessly_nest(obj: Any, func: Callable = identity) -> Any:
    return func(obj)


def segfault():
    import gc
    from hostess.profilers import di
    a_id = id([])
    gc.collect()
    import time
    time.sleep(0.1)
    di(a_id)
