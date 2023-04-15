"""generic utility objects for hostess"""
import _io
import datetime as dt
import logging
import re
from pathlib import Path
from socket import gethostname
from typing import Callable, MutableMapping, Optional, Union

import rich.console
from cytoolz import first


def stamp() -> str:
    """sorthand for standardized text event stamp"""
    return f"{gethostname()} {dt.datetime.utcnow().isoformat()[:-7]}: "


def filestamp() -> str:
    """shorthand for standardized event stamp that is also a legal filename"""
    return re.sub(r"[-: ]", "_", stamp()[:-2])


def logstamp() -> str:
    """shorthand for standardized text timestamp only (no hostname)"""
    return f"{dt.datetime.utcnow().isoformat()[:-7]}"


hostess_CONSOLE = rich.console.Console()


def console_and_log(message, level="info", style=None):
    hostess_CONSOLE.print(message, style=style)
    getattr(logging, level)(message)


def mb(b, round_to=2):
    value = int(b) / 10 ** 6
    if round_to is not None:
        return round(value, round_to)
    return value


def gb(b, round_to=2):
    value = int(b) / 10 ** 9
    if round_to is not None:
        return round(value, round_to)


def keygrab(mapping_list, key, value):
    """returns first element of mapping_list such that element[key]==value"""
    return next(filter(lambda x: x[key] == value, mapping_list))


# noinspection PyUnresolvedReferences,PyProtectedMember
def infer_stream_length(
    stream: Union[_io.BufferedReader, _io._IOBase, Path]
) -> Optional[int]:
    """
    attempts to infer the size of a potential read from an object.
    Args:
        stream: may be a buffered reader (like the result of calling open()),
            a buffer like io.BytesIO, or a Path
    Returns:
        an estimate of its size based on best available method, or None if
        impossible.
    """
    def filesize() -> Optional[int]:
        try:
            if isinstance(stream, _io.BufferedReader):
                path = Path(stream.name)
            elif isinstance(stream, (str, Path)):
                path = Path(stream)
            else:
                return
            return path.stat().st_size
        except FileNotFoundError:
            pass

    def buffersize() -> Optional[int]:
        if "getbuffer" in dir(stream):
            try:
                return len(stream.getbuffer())
            except (TypeError, ValueError, AttributeError):
                pass

    def responsesize() -> Optional[int]:
        if "headers" in dir(stream):
            try:
                return stream["headers"].get("content-length")
            except (KeyError, TypeError, ValueError, AttributeError):
                pass

    methods = (filesize, buffersize, responsesize)
    length = None
    for method in methods:
        length = method()
        if length is not None:
            break
    return length


def roundstring(string, digits=2):
    return re.sub(
        r"\d+\.\d+", lambda m: str(round(float(m.group()), digits)), string
    )


def my_external_ip():
    import requests

    return requests.get("https://ident.me").content.decode()


def check_cached_results(path, prefix, max_age=7):
    cache_filter = filter(lambda p: p.name.startswith(prefix), path.iterdir())
    try:
        result = first(cache_filter)
        timestamp = re.search(
            r"(\d{4})_(\d{2})_(\d{2})T(\d{2})_(\d{2})_(\d{2})", result.name
        )
        cache_age = dt.datetime.now() - dt.datetime(
            *map(int, timestamp.groups())
        )
        if cache_age.days > max_age:
            return None
    except StopIteration:
        return None
    return result


def clear_cached_results(path, prefix):
    for result in filter(lambda p: p.name.startswith(prefix), path.iterdir()):
        result.unlink()


def record_and_yell(message: str, cache: MutableMapping, loud: bool = False):
    """
    place message into a cache object with a timestamp; optionally print it
    """
    if loud is True:
        print(message)
    cache[logstamp()] = message


def notary(cache: Optional[MutableMapping] = None) -> Callable[
    [str, bool, bool], Optional[MutableMapping]
]:
    """
    create a function that records, timestamps, and optionally prints messages.
    if you pass eject=True to that function, it will return its note cache.
    Args:
        cache: cache for notes (if None, creates a dict)
    Returns:
        note: callable for notetaking
    """
    if cache is None:
        cache = {}

    def note(
        message: str = "", loud: bool = False, eject: bool = False
    ) -> Optional[MutableMapping]:
        """
        Args:
            message: message to record in cache and optionally print.
            loud: print message as well?
            eject: return cache. if eject is True, ignores all other arguments
                (does not log this call)
        Returns:
            cache: only if eject is True
        """
        if eject is True:
            return cache
        return record_and_yell(message, cache, loud)

    return note


def dcom(string, sep=";", bad=(",", "\n")):
    """
    simple string sanitization function. defaults assume that you want to jam
    the string into a CSV field. assumes you don't care about distinguishing
    different forbidden characters from one another in the output.
    """
    return re.sub(rf"[{re.escape(''.join(bad))}]", sep, string.strip())


def unix2dt(epoch: float) -> dt.datetime:
    """shorthand for dt.datetime.fromtimestamp."""
    return dt.datetime.fromtimestamp(epoch)


# noinspection PyArgumentList
def curry(func: Callable, *args, **kwargs) -> Callable:
    """this is a hack to improve PyCharm's static analysis."""
    from cytoolz import curry as _curry

    return _curry(func, *args, **kwargs)