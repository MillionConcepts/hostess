"""generic utility objects for killscreen"""
import _io
import datetime as dt
import logging
import os
import re
from pathlib import Path
from socket import gethostname
from typing import Callable, Iterable, Any
import warnings

import rich.console

LOG_DIR_PATH = None
for path in (os.path.expanduser("~/.killscreen"), "/tmp/killscreen"):
    try:
        LOG_DIR_PATH = Path(path)
        if not LOG_DIR_PATH.exists():
            LOG_DIR_PATH.mkdir(parents=True)
    except OSError:
        pass

if LOG_DIR_PATH is None:
    warnings.warn(
        "No writable log path found, logging will behave unpredictably"
    )


def stamp() -> str:
    return f"{gethostname()} {dt.datetime.utcnow().isoformat()[:-7]}: "


def filestamp() -> str:
    return re.sub(r"[-: ]", "_", stamp()[:-2])


KILLSCREEN_CONSOLE = rich.console.Console()


def console_and_log(message, level="info", style=None):
    KILLSCREEN_CONSOLE.print(message, style=style)
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
