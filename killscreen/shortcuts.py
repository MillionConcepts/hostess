"""
shortcuts for composing shell commands.
relies on some bournelike idioms, so output will likely function best in bash.
"""

from typing import Literal, Sequence


def chain(
    cmds: Sequence[str], op: Literal["and", "xor", "then"] = "then",
):
    connector = {"and": "&&", "xor": "||", "then": ";"}[op]
    return f" {connector} ".join(cmds)


def exists(file: str):
    return f"-e {file}"


def sub(cmd: str):
    return f"({cmd})"


def ifthen(if_, then_, else_=None):
    else_ = ":" if else_ is None else else_
    return f"if test {if_}  ; then {then_} ; else {else_} ; fi"


def truthy(cmd):
    return ifthen(cmd, 'echo True', 'echo False')


def ssh_agent(key):
    return chain(("eval `ssh-agent`", f"ssh-add {key}"))
