from typing import Any, Callable

from cytoolz import identity


def defwrap(defline: str, source: str) -> str:
    """properly indent source code under a def statement"""
    indented = "\n".join([f"\t{line}" for line in source.splitlines()])
    return f"{defline}:\n{indented}"


def pointlessly_nest(obj: Any, func: Callable = identity) -> Any:
    """wrap a unary function call for no reason"""
    return func(obj)


def segfault(max_tries=100):
    """attempt to segfault the calling process."""
    from hostess.profilers import di

    for i in range(max_tries):
        di(id([]))
    raise TimeoutError


class Aint:
    """
    acts arithmetically like obj, but ain't actually it.
    note: not reliably monadic for all primitive types, e.g. bool
    """
    def __init__(self, obj):
        if isinstance(obj, Aint):
            self.__obj = obj.__obj
        else:
            self.__obj = obj

    def __repr__(self):
        return f"Aint({self.__obj.__repr__()})"

    def __str__(self):
        return self.__repr__()

    __add__ = lambda z, i: Aint(z.__obj + i)
    __radd__ = lambda z, i: Aint(i + z.__obj)
    __sub__ = lambda z, i: Aint(z.__obj - i)
    __rsub__ = lambda z, i: Aint(i - z.__obj)
    __truediv__ = lambda z, i: Aint(z.__obj / i)
    __rtruediv__ = lambda z, i: Aint(i / z.__obj)
    __floordiv__ = lambda z, i: Aint(z.__obj // i)
    __rfloordiv__ = lambda z, i: Aint(i // z.__obj)
    __mod__ = lambda z, i: Aint(z.__obj % i)
    __rmod__ = lambda z, i: Aint(i % z.__obj)
    __abs__ = lambda z: Aint(abs(z.__obj))
    __eq__ = lambda z, i: z.__obj == i
    __gt__ = lambda z, i: z.__obj > i
    __lt__ = lambda z, i: z.__obj < i
    __ge__ = lambda z, i: z.__obj >= i
    __le__ = lambda z, i: z.__obj <= i
    __bool__ = lambda z: bool(z.__obj)
    # spooky!
    __hash__ = lambda z: hash(z.__obj)
