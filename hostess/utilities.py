"""generic utility objects for hostess"""
import _io
import datetime as dt
import logging
import re
from pathlib import Path
from socket import gethostname
from typing import Callable, Iterable, Any, MutableMapping

import rich.console
from cytoolz import first


def stamp() -> str:
    return f"{gethostname()} {dt.datetime.utcnow().isoformat()[:-7]}: "


def filestamp() -> str:
    return re.sub(r"[-: ]", "_", stamp()[:-2])


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


def infer_stream_length(stream):
    def filesize():
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

    def buffersize():
        if "getbuffer" in dir(stream):
            try:
                return len(stream.getbuffer())
            except (TypeError, ValueError, AttributeError):
                pass

    def responsesize():
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


# TODO: temporarily vendored here until the next dustgoggles release
def gmap(
    func: Callable,
    *iterables: Iterable,
    mapper: Callable[[Callable, tuple[Iterable]], Iterable] = map,
    evaluator: Callable[[Iterable], Any] = tuple,
):
    """
    'greedy map' function. map func across iterables using mapper and
    evaluate with evaluator.

    for cases in which you need a terse or configurable way to map and
    immediately evaluate functions.
    """
    return evaluator(mapper(func, *iterables))


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
    cache[dt.datetime.now().isoformat()] = message


def notary(cache):
    def note(message="", loud: bool = False, eject: bool = False):
        if eject is True:
            return cache
        return record_and_yell(message, cache, loud)

    return note


def logstamp() -> str:
    return f"{dt.datetime.utcnow().isoformat()[:-7]}"


def dcom(string, sep=";", bad=(",", "\n")):
    """
    simple string sanitization function. defaults assume that you want to jam
    the string into a CSV field. assumes you don't care about distinguishing
    different forbidden characters from one another in the output.
    """
    return re.sub(rf"[{re.escape(''.join(bad))}]", sep, string.strip())


def unix2dt(epoch):
    return dt.datetime.fromtimestamp(epoch)
