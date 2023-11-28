"""ad-hoc RPC functionality"""
from pathlib import Path
import sys
from typing import Literal, Any, Union, Callable, Optional

CallerSerializationType = Literal["json", "pickle", None]
"""code for payload serialization method"""

CallerCompressionType = Literal["gzip", None]
"""code for payload compression method"""

CallerUnpackingOperator = Literal["", "*", "**"]
"""
string representation of unpacking operator, if any, used to insert 
reconstructed payload into called function
"""


def to_heredoc(
    heredoc_content: str,
    addition: str = "",
    identifier: str = "__BOUNDARYTAG__",
) -> str:
    """
    create a bash heredoc statement.

    Args:
        heredoc_content: content of the heredoc.
        addition: optional additional statement between heredoc identifier
            and body
        identifier: heredoc delimiting identifier.

    Returns:
        bash heredoc statement.

    """
    return f"{identifier} {addition}\n{heredoc_content}\n{identifier}\n"


def encode_payload(
    obj: Any,
    serialization: CallerSerializationType,
    compression: CallerCompressionType,
) -> Union[str, bytes]:
    """
    encode the 'payload' of a remote procedure call.

    Args:
        obj: object to encode
        serialization: serialization method for obj
        compression: how to compress the serialized object (None means
            uncompressed)

    Returns:
        string or bytes containing encoded payload.
    """
    if serialization == "json":
        import json

        serial = json.dumps(obj)
    elif serialization == "pickle":
        import pickle

        serial = pickle.dumps(obj)
    elif serialization is None:
        serial = obj.__repr__()
    else:
        raise NotImplementedError
    if _check_mode(serialization, compression) == "text":
        if serialization == "json":
            return f'"""{serial}"""'
        return serial
    if isinstance(serial, str):
        serial = serial.encode("ascii")
    if compression == "gzip":
        import gzip

        serial = gzip.compress(serial)
    elif compression is not None:
        raise NotImplementedError
    import base64

    return base64.b64encode(serial)


def format_importer(module: Optional[str], func: str) -> str:
    """
    formatting function for import section of RPC script.

    Args:
        module: name of or path to module
        func: name of function in module

    Returns:
        import source code block
    """
    importer = f"""if __name__ == "__main__":
    """
    if module is None:
        importer += f"""target = {func}
    """
        return importer
    if module.endswith(".py"):
        importer += f"""import importlib.util
    import sys
    spec = importlib.util.spec_from_file_location(
        "{Path(module).stem}", "{module}"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["{Path(module).stem}"] = module
    spec.loader.exec_module(module)
    """
    else:
        importer += f"""import {module}
    module = {module}
    """
    if func is not None:
        importer += f"""target = getattr(module, "{func}")
    """
    return importer


def format_decompressor(
    serialized: Union[str, bytes],
    serialization: CallerSerializationType,
    compression: CallerCompressionType,
) -> str:
    """
    create decompression section of RPC script.

    Args:
        serialized: serialized payload
        serialization: name of serialization method used
        compression: name of compression method used

    Returns:
        decompression source code block
    """
    if _check_mode(serialization, compression) == "text":
        return f"""payload = {serialized}
    """
    if compression is None:
        return f"""import base64
    payload = base64.b64decode({serialized})
    """
    if compression == "gzip":
        return f"""import base64
    import gzip
    payload = gzip.decompress(base64.b64decode({serialized}))
    """
    raise NotImplementedError("only gzip compression is currently supported")


def format_deserializer(serialization: CallerSerializationType) -> str:
    """
    create deserialization section of RPC script.

    Args:
        serialization: serialization method used

    Returns:
        deserialization source code block
    """
    if serialization is None:
        return ""
    if serialization == "json":
        return f"import json\npayload = json.loads(payload)"
    elif serialization == "pickle":
        return f"import pickle\npayload = pickle.loads(payload)"
    raise NotImplementedError("Unknown serializer. use 'json' or 'pickle'")


def _check_reconstructable(
    typeobj: type,
    serialization: CallerSerializationType,
    compression: CallerSerializationType,
):
    """
    Raise an error if we are attempting to transfer a compressed, unserialized
    in-memory object with no stable binary representation, which is, for our
    purposes, anything but a string.

    Args:
        typeobj: type of payload object
        serialization: name of serialization method used
        compression: name of compression method used
    """
    if (
        (typeobj is not str)
        and (compression is not None)
        and (serialization is None)
    ):
        raise ValueError(
            "non-string compressed objects will not reconstruct correctly "
            "unless serialized. try compress='gzip', serialization='json' "
            "or serialize='pickle'"
        )


def _check_mode(
    serialization: CallerSerializationType, compression: CallerCompressionType
) -> Literal["text", "binary"]:
    """should we think of our output as in text or binary mode?"""
    if (compression is None) and serialization in (None, "json"):
        return "text"
    return "binary"


def format_kwarg_filter(
    filter_kwargs: bool, splat: CallerUnpackingOperator
) -> str:
    """
    generate kwarg filter section of RPC script, if necessary and requested

    Args:
        filter_kwargs: should we filter unwanted kwargs or not?
        splat: unpacking operator we're using. if it's not '**',
            never generate this block -- it's unnecessary.

    Returns:
        kwarg-filtering source code block.
    """
    if (filter_kwargs is not True) or (splat != "**"):
        return ""
    return """from inspect import getfullargspec
    spec = getfullargspec(target)
    payload = {
        k: v for k, v in payload.items() 
        if k in spec.args + spec.kwonlyargs
    }
    """


# TODO, maybe: validity check to make sure it compiles
def generic_python_endpoint(
    module: str,
    func: Optional[str] = None,
    payload: Any = None,
    *,
    compression: CallerCompressionType = None,
    serialization: CallerSerializationType = None,
    splat: CallerUnpackingOperator = "",
    payload_encoded: bool = False,
    print_result: bool = False,
    filter_kwargs: bool = False,
    interpreter: Optional[str] = None,
    for_bash: bool = True,
    literal_none: bool = False,
) -> str:
    """
    dynamically construct a Python source code snippet that imports a module
    and calls a function from it with a given 'payload' (effectively, an
    argument or arguments, possibly in serialized and/or compressed form). by
    default, wrap it in a shell script that executes the snippet from bash.
    this can be used to perform remote procedure calls, inject code into
    existing applications, etc.

    Args:
        module: name of, or path to, the target module
        func: name of the function to call. must be a member of the target
            module (or explicitly imported by that module). If not specified,
            the generated code simply imports `module` (which is sometimes
            enough, depending on what `module` does when imported). If `func`
            is None, all subsequent arguments other than `for_bash`,
            `interpreter`, and `print_result` have no effect.
        payload: object from which to construct func's call arguments. In many
            cases, this can simply be a Python object or objects you'd like
            to pass to `func`. If the payload is not well-defined by its
            string representation, an appropriate `serialization` must be
            specified for the call to work. For instance, `[1, 2, 3]` is a
            fine payload without serialization;
            `np.random.poisson(5, (100, 100))` is not.
        compression: how to compress the payload. 'gzip' or None. 'gzip' is
            good for jamming larger payloads into a shell command without
            breaking the shell.
        serialization: how to serialize `payload`. 'json' means serialize
            to JSON; 'pickle' means serialize using pickle; None means just
            use the string representation of `payload`. None is only
            suitable for objects that can be reconstructed from their string
            representations.
        splat: Operator for splatting `payload` into the function call.
            Allows you to use `payload` as multiple arguments or keyword
            arguments.`"*"` means `func(*payload)`, `"**"` means
            `func(**payload)`; '' means `func(payload)`.
        payload_encoded: set to True if you have already serialized and/or
            compressed the payload using the specified methods, so the
            generated script should decode it, but this function should not
            re-encode it.
        print_result: if True, the generated script also prints the return
            value of the called function to stdout.
        filter_kwargs: if True, the generated script will attempt to filter
            func-inappropriate kwargs from the payload. Not guaranteed to work
            on functions with complex signatures. Does nothing if
            `splat != '**'`.
        interpreter: path to Python interpreter that should be specified in
            the shell command. can either be a fully-qualified path or
            any name you expect to be on the calling user's $PATH -- e.g., if
            you expect there to be a system Python and want to use it, just
            'python'. If None, assume the path to the desired interpreter is
            the same as the path to the interpreter that is running this
            function. Does nothing if `for_bash` is False.
        for_bash: if True (the default), return a bash command that
            runs the Python script in the specified interpreter. otherwise,
            simply return the generated script.
        literal_none: if False (the default), interpret `payload=None` as
            meaning "run `module.func()`". otherwise, interpret it as meaning
            "run `module.func(None)`".

    Returns:
        Bash command that executes function call in specified interpreter,
        or, if `for_bash` is False, just Python source code for function call.
    """
    if (payload is not None or literal_none is True) and (func is None):
        raise ValueError("Must pass a function name to pass a payload.")
    no_payload = payload is None and literal_none is False
    _check_reconstructable(type(payload), serialization, compression)
    import_ = format_importer(module, func)
    if func is None:
        return import_
    if no_payload is True:
        encoded = ""
    elif payload_encoded is True:
        encoded = repr(payload)
    else:
        encoded = encode_payload(payload, serialization, compression)
    decompress = format_decompressor(encoded, serialization, compression)
    deserialize = format_deserializer(serialization)
    kwarg_filter = format_kwarg_filter(filter_kwargs, splat)
    call = f"target({splat}payload)"
    if print_result is True:
        call = f"print({call})"
    endpoint = import_ + decompress + deserialize + kwarg_filter + call
    if for_bash is True:
        if interpreter is None:
            interpreter = sys.executable
        return f"{interpreter} <<{to_heredoc(endpoint)}"
    return endpoint


def make_python_endpoint_factory(
    module: str,
    func: Optional[str] = None,
    **endpoint_kwargs: Union[
        bool,
        str,
        CallerCompressionType,
        CallerSerializationType,
        CallerUnpackingOperator,
    ],
) -> Union[Callable[[str, Any], str], Callable[[Any], str]]:
    """
    factory function for endpoint factory functions. use this to create
    callables that generate shell scripts that call either a specific Python
    function or Python functions from a specific named module, using specific
    application-correct configurations.

    Args:
        module: name of, or path to, module to use as quasi-namespace of
            endpoint factory.
        func: optional name of function from module. If this is None, the
            returned function can be used to call any function from module.
        endpoint_kwargs: kwargs to partially evaluate / bind to the endpoint
            factory. see generic_python_endpoint() for a full description of
            options.

    Returns:
        a function that, when called, produces shell scripts. If the `func`
            argument was None, this function's call signature is
            (function_name: str, payload: Any). If it was not None, this
            function's call signature is (payload: Any); it always
            generates scripts that call `module.func`.
    """
    if "payload" in endpoint_kwargs.keys():
        raise ValueError("cannot bind a payload to the endpoint factory")

    if func is not None:

        def endpoint_factory(payload):
            return generic_python_endpoint(
                module, func=func, payload=payload, **endpoint_kwargs
            )

    else:

        def endpoint_factory(function_name, payload):
            return generic_python_endpoint(
                module, function_name, payload=payload, **endpoint_kwargs
            )

    return endpoint_factory
