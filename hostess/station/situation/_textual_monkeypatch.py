from functools import lru_cache
from inspect import getfullargspec, isawaitable
from typing import Callable, Any


@lru_cache(maxsize=2048)
def count_parameters(func: Callable) -> int:
    """Count the number of parameters in a callable"""

    # NOTE: PATCHED TO REMOVE POSSIBLE ISSUES WITH KWONLYARGS
    if func.__class__.__name__ == "method":
        return len(getfullargspec(func).args) - 1
    return len(getfullargspec(func).args)


async def _invoke(callback: Callable, *params: object) -> Any:
    """Invoke a callback with an arbitrary number of parameters.

    Args:
        callback: The callable to be invoked.

    Returns:
        The return value of the invoked callable.
    """
    _rich_traceback_guard = True
    # NOTE: PATCHED FOR OPTIMIZATION TO CALLBACK CACHING WITH
    # REPEATEDLY-BOUND METHODS
    try:
        parameter_count = count_parameters(callback.func) - len(callback.args)
    except AttributeError:
        parameter_count = count_parameters(callback)
    result = callback(*params[:parameter_count])
    if isawaitable(result):
        result = await result
    return result


def patch_textual_invoke():
    from textual import _callback

    _callback._invoke = _invoke
