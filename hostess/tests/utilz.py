from typing import Any, Callable

from cytoolz import identity


def defwrap(defline: str, source: str) -> str:
    """properly indent source code under a def statement"""
    indented = "\n".join([f"\t{line}" for line in source.splitlines()])
    return f"{defline}:\n{indented}"


def pointlessly_nest(obj: Any, func: Callable = identity) -> Any:
    return func(obj)
