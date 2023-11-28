"""generic utility objects for hostess"""
from __future__ import annotations

import _io
import datetime as dt
from functools import wraps
from importlib import import_module
from importlib.util import spec_from_file_location, module_from_spec
import logging
from pathlib import Path
from operator import is_
import re
from socket import gethostname
import sys
import time
from types import ModuleType
from typing import (
    Any, Callable, Hashable, Iterable, MutableMapping, Optional, Sequence,
    Mapping, Collection, Union
)

from dustgoggles.dynamic import exc_report
import rich.console
import yaml
from rich.style import Style


def stamp() -> str:
    """create standardized text event stamp"""
    return f"{gethostname()} {dt.datetime.now(dt.UTC).isoformat()[:-13]}: "


def filestamp() -> str:
    """shorthand for standardized event stamp that is also a legal filename"""
    return re.sub(r"[-: ]", "_", stamp()[:-2])


def logstamp(extra: int = 0) -> str:
    """shorthand for standardized text timestamp only (no hostname)"""
    return f"{dt.datetime.now(dt.UTC).isoformat()[:(-13 + extra)]}"


HOSTESS_CONSOLE = rich.console.Console()
"""convenient shared rich console"""


def console_and_log(
    message: Any,
    level: str = "info",
    style: Optional[Union[str, Style]] = None
):
    """
    print a message to console and log it with this module's default logger.

    Args:
        message: object to print and log. must be compatible with both default
            logger and rich.console.Console.print. strings or numbers are
            recommended.
        level: logging level as a string ("info", "warning", etc.)
        style: optional rich Style or string description of one, e.g. "red"
    """
    HOSTESS_CONSOLE.print(message, style=style)
    getattr(logging, level)(message)


def mb(b: int, round_to: Optional[int] = 2) -> float:
    """
    utility function to convert B to MB.

    Args:
        b: how many bytes?
        round_to: if not None, round output to this many digits.

    Returns:
        `b` converted from B to MB
    """
    value = int(b) / 10 ** 6
    if round_to is not None:
        return round(value, round_to)
    return value


def gb(b: float, round_to: Optional[int] = 2) -> float:
    """
    utility function to convert B to GB.

    Args:
        b: how many bytes?
        round_to: if not None, round output to this many digits.

    Returns:
        `b` converted from B to GB
    """
    value = int(b) / 10 ** 9
    if round_to is not None:
        return round(value, round_to)
    return value


# noinspection PyUnresolvedReferences,PyProtectedMember
def infer_stream_length(
    stream: Union[
        _io.BufferedReader, _io.BinaryIO, Path, str, requests.Response
    ],
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

    return requests.get("https://4.ident.me").content.decode()


def record_and_yell(
    message: str, cache: MutableMapping, loud: bool = False, extra: int = 0
):
    """
    place message into a cache object with a timestamp; optionally print it
    """
    if loud is True:
        print(message)
    cache[logstamp(extra)] = message


def notary(
    cache: Optional[MutableMapping] = None,
    be_loud: bool = False,
    resolution: int = 0,
) -> Callable[[Any], Optional[MutableMapping]]:
    """
    create a function that records, timestamps, and optionally prints messages.
    if you pass eject=True to that function, it will return its note cache.

    Args:
        cache: cache for notes (if None, creates a dict)
        be_loud: if True, makes output function verbose by default. individual
            calls can override this setting.
        resolution: time resolution in significant digits after the second.
            collisions can occur if entries are sent faster than the time
            resolution.

    Returns:
        note: function for notetaking
    """
    if cache is None:
        cache = {}

    resolution = resolution if resolution == 0 else resolution + 1

    def note(
        message: str = "", loud: bool = be_loud, eject: bool = False
    ) -> Optional[MutableMapping]:
        """
        Args:
            message: message to record in `cache` and optionally print.
            loud: print message as well?
            eject: if True, return `cache` ignore all other arguments, and
                do not log this call.

        Returns:
            usually `None`; if `eject` is True, instead `cache`
        """
        if eject is True:
            return cache
        return record_and_yell(message, cache, loud, resolution)

    return note


def dcom(
    string: str, sep: str = ";", bad: Collection[str] = (",", "\n")
) -> str:
    """
    simple string sanitization function. the default values assume that you
    want to jam the string into a CSV field. always assumes you don't care
    about distinguishing different forbidden characters from one another in
    the output.

    Args:
        string: string to sanitize
        sep: separator to replace 'bad' characters with
        bad: characters to replace with sep

    Returns:
        `string` washed clean of bad characters.
    """
    return re.sub(rf"[{re.escape(''.join(bad))}]", sep, string.strip())


def unix2dt(epoch: float) -> dt.datetime:
    """alias for `dt.datetime.fromtimestamp()`."""
    return dt.datetime.fromtimestamp(epoch)


# noinspection PyArgumentList
def curry(func: Callable, *args, **kwargs) -> Callable:
    """
    alias for cytoolz.curry with type hinting. this is a hack to
    improve PyCharm's static analysis.
    """
    from cytoolz import curry as _curry

    return _curry(func, *args, **kwargs)


class Aliased:
    """
    generic wrapper for aliasing a class method. for instance, if you'd like a
    library function to `append` to a list, but it's only willing to `write`:

    ```
    >>> import json
    >>> my_list = []
    >>> writeable_list = Aliased(my_list, ("write",), "append")
    >>> json.dump([1, 2, 3], writeable_list)
    >>> print(writeable_list)
    Aliased: ('write',) -> append:
    ['[1', ', 2', ', 3', ']']
    ```
    """

    def __init__(self, wrapped: Any, aliases: Sequence[str], referent: str):
        self.obj = wrapped
        self.method = referent
        self.aliases = aliases
        for alias in aliases:
            setattr(self, alias, self._aliased)

    def _aliased(self, *args, **kwargs):
        return getattr(self.obj, self.method)(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self.obj, attr)

    def __str__(self):
        return f"Aliased: {self.aliases} -> {self.method}:\n" + str(self.obj)

    def __repr__(self):
        return f"Aliased: {self.aliases} -> {self.method}:\n" + repr(self.obj)


def timeout_factory(
    raise_timeout: bool = True, timeout: float = 5
) -> tuple[Callable[[], int], Callable[[], None]]:
    """
    returns a tuple of functions. calling the first starts a wait timer if not
    started, and also returns current wait time. calling the second resets the
    wait timer.

    Args:
        raise_timeout: if True, raises TimeoutError if waiting > timeout.
            otherwise, this is basically just a stopwatch.
        timeout: timeout in seconds. Used only if raise_timeout is True.
    """
    starts = []

    def waiting():
        """call me to start and check/raise timeout."""
        if len(starts) == 0:
            starts.append(time.time())
            return 0
        delay = time.time() - starts[-1]
        if (raise_timeout is True) and (delay > timeout):
            raise TimeoutError
        return delay

    def unwait():
        """call me to reset timeout."""
        try:
            starts.pop()
        except IndexError:
            pass

    return waiting, unwait


def signal_factory(
    threads: MutableMapping[Hashable, Optional[int]]
) -> Callable[[Hashable, Optional[int]], None]:
    """
    creates a 'signaler' function that simply assigns values to a mapping
    bound in enclosing scope. this is primarily intended as a simple
    inter-thread communication utility.

    Args:
        threads: mapping from thread names to None or ints. In normal usage,
            named threads will poll the key of this mapping corresponding
            to their name to check for received signals.

    Returns:
        a process that takes a thread name and an optional integer (default
            0) and assigns that integer to the corresponding key of the
            `threads` mapping.
    """

    def signaler(name, signal=0):
        if name == "all":
            for k in threads.keys():
                threads[k] = signal
            return
        if name not in threads.keys():
            raise KeyError
        threads[name] = signal

    return signaler


# TODO, maybe: replace with a dynamic?
@curry
def trywrap(func, name):
    @wraps(func)
    def trywrapped(*args, **kwargs):
        exception, retval = None, None
        try:
            retval = func(*args, **kwargs)
        except Exception as ex:
            exception = ex
        finally:
            return {
                "name": name,
                "retval": retval,
                "time": dt.datetime.now(),
            } | exc_report(exception)

    return trywrapped


@curry
def configured(func: Callable, config: Mapping[str, Any]) -> Callable:
    """
    decorator that permits dynamic partial evaluation of a function.
    `configured` splats `config` into all calls to the decorated
    function, so that its behavior can change along with changes to the
    contents of `config`.

    Args:
        func: function to configure
        config: mapping to use as extra kwargs to func

    Returns:
        version of `func` that splats `config` into every call.
    """
    @wraps(func)
    def with_configuration(*args, **kwargs):
        return func(*args, **kwargs, **config)

    return with_configuration


def get_module(module_name: str) -> ModuleType:
    """
    dynamically import a module by name. check to see if it's already in
    sys.modules; if not, just try to import it; if that doesn't work, try to
    interpret module_name as a path.

    Args:
        module_name: name of or path to a Python module.

    Returns:
        a module, hopefully.
    """
    if module_name in sys.modules:
        return sys.modules[module_name]
    try:
        return import_module(module_name)
    except ModuleNotFoundError:
        pass
    spec = spec_from_file_location(Path(module_name).stem, module_name)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[Path(module_name).stem] = module
    return module


def yprint(
    obj: Any,
    indent: int = 0,
    replace_null: bool = True,
    maxlen: int = 256
) -> str:
    """
    lazy way to pretty-print many objects by using `pyyaml`'s excellent YAML 
    formatter. Doesn't work well for everything.

    Args:
        obj: object to pretty-print
        indent: indentation in spaces
        replace_null: if True, replace the YAML value 'null' with 'None'
        maxlen: maximum length of output

    Returns:
        mildly stylized YAML representation of `obj`
    """
    try:
        text = yaml.dump(obj)
    except TypeError:
        text = f'***pretty-print failed*** {obj}'
    if replace_null is True:
        text = text.replace('null', 'None')
    return "\n".join(
        " " * indent + line[:maxlen] for line in text.splitlines()
    )


def is_any(obj: Any, coll: Iterable) -> bool:
    """
    like `obj in coll`, for use in cases when `obj` and `coll` do not, or 
    might not, support use of `in`.

    Args:
        obj: an object
        coll: a collection

    Returns:
        True if `obj` is in `coll`; False if not
    """
    return any(map(lambda item: is_(obj, item), coll))
