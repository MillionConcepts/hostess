from pathlib import Path


def to_heredoc(heredoc_content, addition="", identifier="__BOUNDARYTAG__"):
    return f"{identifier} {addition}\n{heredoc_content}\n{identifier}"


def encode_payload(obj, serializer, compression):
    if serializer == "json":
        import json

        serial = json.dumps(obj)
    elif serializer == "pickle":
        import pickle

        serial = pickle.dumps(obj)
    elif serializer is None:
        serial = obj.__repr__()
    else:
        raise NotImplementedError
    if isinstance(serial, str):
        if compression is None:
            return serial
        serial = serial.encode("ascii")
    if compression == "gzip":
        import gzip

        serial = gzip.compress(serial)
    elif compression is not None:
        raise NotImplementedError
    import base64

    return base64.b64encode(serial)


def format_importer(module, func):
    importer = f"""if __name__ == "__main__":"""
    if module.endswith(".py"):
        importer += f"""
    import importlib.util
    import sys
    spec = importlib.util.spec_from_file_location(
        "{Path(module).stem}", "{module}"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["{Path(module).stem}"] = module
    spec.loader.exec_module(module)
    """
    else:
        importer += f"import {module}\n"
    if func is not None:
        importer += f"""target = getattr(module, "{func}")
    """
    return importer


def format_decompressor(serialized, serialization, compression):
    if (compression is None) and (serialization in (None, "json")):
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


def format_deserializer(serializer):
    if serializer is None:
        return ""
    if serializer == "json":
        return f"""import json
    payload = json.loads(payload)
    """
    elif serializer == "pickle":
        return f"""import pickle
    payload = pickle.loads(payload)
    """
    raise NotImplementedError("Unknown serializer. use 'json' or 'pickle'")


def generic_python_endpoint(
    module,
    payload=None,
    func=None,
    compression=None,
    serialization=None,
    argument_unpacking="",
    payload_encoded=False
):
    if (payload is not None) and (func is None):
        raise ValueError("Must pass a function name to pass a payload.")
    if (
        (compression is not None)
        and (serialization is None)
        and not isinstance(payload, str)
    ):
        raise ValueError(
            "non-string compressed objects will not reconstruct correctly "
            "unless serialized. try compress='gzip', serialization='json' "
            "or serialize='pickle'"
        )
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
    return import_ + decompress + deserialize + call


def construct_python_call(
    module,
    payload=None,
    func=None,
    interpreter_path="python",
    compression=None,
    serializer=None,
    argument_unpacking="",
    payload_encoded=False
):
    in_memory_endpoint = generic_python_endpoint(
        module, 
        payload, 
        func, 
        compression, 
        serializer, 
        argument_unpacking,
        payload_encoded
    )
    return f"{interpreter_path} <<{to_heredoc(in_memory_endpoint)}"
