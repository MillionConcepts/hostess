def defwrap(defline: str, source: str):
    """properly indent source code under a def statement"""
    indented = "\n".join([f"\t{line}" for line in source.splitlines()])
    return f"{defline}:\n{indented}"


def return_this(payload):
    return payload
