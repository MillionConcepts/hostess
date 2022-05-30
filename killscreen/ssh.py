"""
ssh file ops and process functions.
currently only key-based authentication is supported.
"""
import io
from functools import wraps
from itertools import product
from operator import or_
import os
import re
from typing import Callable, Union
import warnings

from dustgoggles.func import are_in, zero
from dustgoggles.structures import listify, separate_by
import pandas as pd
import sh

import killscreen.shortcuts as ks
from killscreen.subutils import Viewer, split_sh_stream
from killscreen.utilities import filestamp


def ssh_key_add(ip_list):
    """
    ssh commands will fail if the remote server is not in the local known
    hosts list. this function attempts to initialize it, but probably is not
    transparently applicable across environments. if it fails, add hosts to
    the known host lists some other way -- logging in once to each server from
    the command line would do the trick.
    """
    ip_list = listify(ip_list)
    home = os.path.expanduser("~")
    with open(home + "/.ssh/known_hosts", "r") as hostfile:
        known_hosts = hostfile.read()
    with open(home + "/.ssh/known_hosts", "a") as hostfile:
        for ip in ip_list:
            if ip in known_hosts:
                continue
            sh.ssh_keyscan("-t", "rsa", ip, _out=hostfile)


def scp_to(source, target, ip, uname, key):
    """
    copy file from memory to remote host using scp.
    currently only key-based authentication is supported.
    """
    return sh.scp(
        "-i" + key,
        source,
        uname + "@" + ip + ":" + target,
    ).stdout.decode()


def scp_from(source, target, ip, uname, key):
    """
    copy file from remote host to file using scp.
    currently only key-based authentication is supported.
    """
    return sh.scp(
        "-i" + key, uname + "@" + ip + ":" + source, target
    ).stdout.decode()


def scp_read(fname, ip, uname, key):
    """
    copy file from remote host into memory using scp.
    currently only key-based authentication is supported.
    """
    return sh.scp(
        "-i" + key, uname + "@" + ip + ":" + fname, "/dev/stdout"
    ).stdout


def scp_read_csv(fname, ip, uname, key, **csv_kwargs):
    """
    reads csv-like file from remote host into pandas DataFrame using csv.
    """
    import pandas as pd

    buffer = io.StringIO()
    buffer.write(scp_read(fname, ip, uname, key).decode())
    buffer.seek(0)
    return pd.read_csv(buffer, **csv_kwargs)


def scp_merge_csv(server_dict, filename, username, ssh_key, **csv_kwargs):
    """
    merges csv files -- logs, perhaps -- from across a defined group
    of remote servers.
    """
    framelist = []
    for name, ip in server_dict.items():
        csv_df = scp_read_csv(filename, ip, username, ssh_key, **csv_kwargs)
        csv_df["server"] = name
        framelist.append(csv_df)
    return pd.concat(framelist).reset_index(drop=True)


def tunnel(ip, key, uname, local_port="8888", remote_port="8888"):
    """
    open ssh tunnel binding local_port to remote_port. returns pid of ssh
    process. default arguments bind to default jupyter port.
    """
    return Viewer.from_command(
        "ssh",
        f"-i{key}",
        f"-L {local_port}:localhost:{remote_port}",
        f"{uname}@{ip}",
        "sleep infinity",
        _bg=True,
        _host=ip
    )


def ssh_command(ip, key, uname):
    """baked ssh command for a server."""
    return sh.ssh.bake(f"-i{key}", f"{uname}@{ip}")


def wrap_ssh(ip, key, uname, caller=None):
    """baked ssh command for a server with extra management."""
    ssh = sh.ssh.bake(f"-i{key}", f"{uname}@{ip}")
    caller = ip if caller is None else caller

    @wraps(interpret_command)
    def run_with_ssh(*args, **kwargs):
        return interpret_command(ssh, *args, _host=caller, **kwargs)
    return run_with_ssh


def interpret_command(
    interpreter,
    *args,
    _host=None,
    _viewer=False,
    _capture=True,
    _disown=False,
    _output_file=None,
    _get_children=False,
    _detach=False,
    _quit=False,
    **kwargs,
) -> Union[sh.RunningCommand, Viewer]:
    if _detach is True:
        kwargs["_bg"] = True
        kwargs["_bg_exc"] = False
        _viewer = True
        _get_children = True
        _capture = True
        _disown = True
    special, unspecial = map(
        dict, separate_by(kwargs.items(), lambda kv: kv[0].startswith("_"))
    )
    if len(unspecial) > 0:
        if (_disown is not True) and (_get_children is True):
            raise NotImplementedError("This doesn't work yet.")
        if any(map(are_in(("&&", "||", ";"), or_), args)):
            warnings.warn(
                f"\n{unspecial} will only be added to the last statement "
                f"in your command. If this isn't what you want, call "
                f"this with explicit option arguments, like:\ncommand("
                f"'ls --size ; cd directory ; ls --size') instead of:\n"
                f"command('ls; cd directory ; ls', size=True).",
                category=SyntaxWarning,
            )

    command_args = list(args)
    if _disown is True:
        if _output_file is None:
            _output_file = f"{filestamp()}.out"

        if isinstance(command_args[0], sh.Command):
            command = command_args[0].bake(*command_args[1:], **unspecial)
            command_string = f"nohup {command}"
        else:
            command_string = f"nohup {' '.join(command_args)} "
            command_string += " ".join(
                [f"--{k}={v}" for k, v in unspecial.items()]
            )
        command_string += f" >> {_output_file}"
        command_args = [command_string]
        kwargs = special
    if _viewer is True:
        return Viewer.from_command(
            interpreter,
            *command_args,
            _host=_host,
            _handlers=_capture,
            _get_children=_get_children,
            _quit=_quit,
            **kwargs,
        )
    # TODO: this works incorrectly without _disown and without
    #  _viewer atm. not sure why.
    return interpreter(*command_args, **kwargs)


def ssh_dict(server_dict, key, uname, namelist=None):
    # returns a dict of ssh commands corresponding to
    # a passed dict of name: ip.
    # if namelist is not passed, names them
    # according to keys in the server_dict.
    if not namelist:
        namelist = list(server_dict.keys())
    return {
        namelist[server_number]: ssh_command(ip, key, uname)
        for server_number, ip in enumerate(server_dict.values())
        if ip is not None
    }


# remote jupyter connection

TOKEN_PATTERN = re.compile(r"(?<=\?token=)([a-z]|\d)+")


def get_jupyter_token(jupyter_command, remote_port):
    for attempt in range(5):
        try:
            jlist = jupyter_command("list", "")
            for line in [out.decode() for out in jlist.stdout.splitlines()]:
                if remote_port in line:
                    return re.search(TOKEN_PATTERN, line).group()
            raise ValueError
        except ValueError:
            continue
    raise ValueError(
        "Token not found. Notebook may not have started on correct port. "
    )


CONDA_NAMES = ("anaconda3", "miniconda3", "miniforge", "mambaforge")
CONDA_PARENTS = ("~", "/opt")
CONDA_SEARCH_PATHS = tuple(
    [f"{root}/{name}" for root, name in product(CONDA_PARENTS, CONDA_NAMES)]
)


def jupyter_connect(
    ip,
    key,
    uname,
    local_port="8888",
    remote_port="8888",
    env=None,
    get_token=True,
    kill_on_exit=True,
):
    command = ssh_command(ip, key, uname)
    if env is not None:
        jupyter = f"{find_conda_env(command, env)}" f"/bin/jupyter notebook"
    else:
        jupyter = "jupyter notebook"
    jupyter_command = sh.ssh.bake(
        f"-i{key}", f"{uname}@{ip}", f"{jupyter}", _bg=True, _bg_exc=False
    )
    if kill_on_exit is True:

        def done(*_):
            jupyter_command("stop", f"--NbserverStopApp.port={remote_port}")

    else:
        done = zero
    jupyter_launch = Viewer.from_command(
        jupyter_command,
        f"--port {remote_port} --no-browser",
        _done=done,
        _host=ip
    )
    if get_token:
        token = get_jupyter_token(jupyter_command, remote_port)
        jupyter_url = f"http://localhost:{local_port}/?token={token}"
    else:
        jupyter_url = f"http://localhost:{local_port}"
    jupyter_tunnel = tunnel(ip, key, uname, local_port, remote_port)
    return jupyter_url, jupyter_tunnel, jupyter_launch


def find_conda_env(cmd: Callable, env: str) -> str:
    env = "base" if env is None else env
    suffix = f"/envs/{env}" if env != "base" else ""
    try:
        envs = filter(
            None,
            str(cmd(f"cat", "~/.conda/environments.txt")).split("\n"),
        )
        if env == "base":
            return next(filter(lambda line: "envs" not in line, envs))
        else:
            return next(filter(lambda line: suffix in line, envs))
    except (sh.ErrorReturnCode, StopIteration):
        pass
    search = cmd(
        ks.chain(
            [ks.truthy(f"-e {path}{suffix}") for path in CONDA_SEARCH_PATHS]
        )
    )
    lines = search.out if "out" in dir(search) else split_sh_stream(search)
    for line, path in zip(lines, CONDA_SEARCH_PATHS):
        if "True" in line:
            return f"{path}/{suffix}"
    raise FileNotFoundError("conda environment not found.")
