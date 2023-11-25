"""
shortcuts for composing shell commands.
relies on some bournelike idioms, so output will likely function best in bash.
"""

from typing import Literal, Sequence, Optional


def chain(
    cmds: Sequence[str], op: Literal["and", "xor", "then"] = "then",
) -> str:
    """
    create a multi-part shell command.

    Args:
        cmds: commands to chain together.
        op: logical operator to chain them with.

    Returns:
        multi-part shell command as a string.
    """
    connector = {"and": "&&", "xor": "||", "then": ";"}[op]
    return f" {connector} ".join(cmds)


def sub(cmd: str) -> str:
    """
    shorthand for f"$({cmd})", i.e., instruction to run cmd in a subshell.

    Args:
        cmd: string form of shell command to wrap in subshell instruction

    Returns:
        instruction to run cmd in a subshell
    """
    return f"$({cmd})"


def ternary(
    if_: str,
    then_: str,
    else_: Optional[str] = None
) -> str:
    """
    construct a bash command that serves as a ternary operator.

    Args:
        if_: argument to the shell command "test" to use as predicate
            (see the manpage for test for details)
        then_: shell command to run if predicate is truthy
        else_: shell command to run if predicate is falsy

    Returns:
          string form of ternary shell command
    """
    else_ = ":" if else_ is None else else_
    return f"if test {if_}  ; then {then_} ; else {else_} ; fi"


def truthy(cmd) -> str:
    """
    construct a shell command that tests a predicate and echoes "True" if it's
    truthy and "False" if it's falsy. This is intended as an easy way to
    do tests in bash that output the string representation of a Python bool
    literal.

    Args:
        cmd: predicate to test. will be used as an argument to the shell
            command "test" (see manpage for test for details)

    Returns:
        string form of truthiness-printing shell command
    """
    return ternary(cmd, 'echo True', 'echo False')


def ssh_agent(key: str) -> str:
    """
    construct a shell command that starts an ssh-agent session and adds a
    keyfile.

    Args:
        key: path to SSH keyfile

    Returns:
        string form of shell command.
    """
    return chain(("eval `ssh-agent`", f"ssh-add {key}"))
