from pathlib import Path


def to_heredoc(heredoc_content, addition="", identifier="__BOUNDARYTAG__"):
    return f"{identifier} {addition}\n{heredoc_content}\n{identifier}"


def encode_payload(obj, serialization, compression):
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
            return f"\"\"\"{serial}\"\"\""
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


def format_importer(module, func):
    importer = f"""if __name__ == "__main__":
    """
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


def format_decompressor(serialized, serialization, compression):
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


def format_deserializer(serialization):
    if serialization is None:
        return ""
    if serialization == "json":
        return f"""import json
    payload = json.loads(payload)
    """
    elif serialization == "pickle":
        return f"""import pickle
    payload = pickle.loads(payload)
    """
    raise NotImplementedError("Unknown serializer. use 'json' or 'pickle'")


def _check_reconstructable(typeobj, serialization, compression):
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


def _check_mode(serialization, compression):
     if (compression is None) and serialization in (None, "json"):
        return "text"
     return "binary"


def generic_python_endpoint(
    module,
    payload=None,
    func=None,
    compression=None,
    serialization=None,
    argument_unpacking="",
    payload_encoded=False,
    print_result=False
):
    if (payload is not None) and (func is None):
        raise ValueError("Must pass a function name to pass a payload.")
    _check_reconstructable(type(payload), serialization, compression)
    import_ = format_importer(module, func)
    if func is None:
        return import_
    if payload_encoded is False:
        payload = encode_payload(payload, serialization, compression)
    else:
        payload = payload.__repr__()
    decompress = format_decompressor(payload, serialization, compression)
    deserialize = format_deserializer(serialization)
    call = f"target({argument_unpacking}payload)"
    if print_result is True:
        call = f"print({call})"
    return import_ + decompress + deserialize + call


def construct_python_call(
    module,
    payload=None,
    func=None,
    interpreter_path="python",
    compression=None,
    serialization=None,
    argument_unpacking="",
    payload_encoded=False,
    print_result=False
):
    in_memory_endpoint = generic_python_endpoint(
        module,
        payload,
        func,
        compression,
        serialization,
        argument_unpacking,
        payload_encoded,
        print_result
    )
    return f"{interpreter_path} <<{to_heredoc(in_memory_endpoint)}"
