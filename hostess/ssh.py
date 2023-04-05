"""
ssh file ops and process functions.
currently only key-based authentication is supported.
"""
import atexit
import io
from functools import wraps
from itertools import product
from operator import or_
import os
import re
from pathlib import Path
from typing import Callable, Union
import warnings

from dustgoggles.func import are_in, zero
from dustgoggles.structures import listify, separate_by
import sh
from magic import Magic

import hostess.shortcuts as ks
from hostess.config import GENERAL_DEFAULTS
from hostess.subutils import Viewer, split_sh_stream, is_sh_command
from hostess.utilities import filestamp


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


def scp_to(source, target, ip, uname=GENERAL_DEFAULTS["uname"], key=None):
    """
    copy file from memory to remote host using scp.
    currently only key-based authentication is supported.
    """
    arguments = [source, uname + "@" + ip + ":" + target]
    if key is not None:
        arguments = ["-i" + key] + arguments
    return sh.scp(*arguments)


def scp_from(source, target, ip, uname=GENERAL_DEFAULTS["uname"], key=None):
    """
    copy file from remote host to file using scp.
    currently only key-based authentication is supported.
    """
    arguments = [uname + "@" + ip + ":" + source, target]
    if key is not None:
        arguments = ["-i" + key] + arguments
    return sh.scp(*arguments)


def scp_read(fname, ip, uname=GENERAL_DEFAULTS["uname"], key=None):
    """
    copy file from remote host into memory using scp.
    currently only key-based authentication is supported.
    """
    arguments = [uname + "@" + ip + ":" + fname, "/dev/stdout"]
    if key is not None:
        arguments = ["-i" + key] + arguments
    return sh.scp(*arguments).stdout


def scp_read_csv(
    fname, ip, uname=GENERAL_DEFAULTS["uname"], key=None, **csv_kwargs
):
    """
    reads csv-like file from remote host into pandas DataFrame using csv.
    """
    import pandas as pd

    buffer = io.StringIO()
    buffer.write(scp_read(fname, ip, uname, key).decode())
    buffer.seek(0)
    # noinspection PyTypeChecker
    return pd.read_csv(buffer, **csv_kwargs)


def scp_merge_csv(
    server_dict,
    filename,
    uname=GENERAL_DEFAULTS["uname"],
    key=None,
    **csv_kwargs,
):
    """
    merges csv files -- logs, perhaps -- from across a defined group
    of remote servers.
    """
    import pandas as pd

    framelist = []
    for name, ip in server_dict.items():
        csv_df = scp_read_csv(filename, ip, uname, key, **csv_kwargs)
        csv_df["server"] = name
        framelist.append(csv_df)
    return pd.concat(framelist).reset_index(drop=True)


# TODO, maybe: add an atexit thing warning users about open tunnels
def tunnel(
    ip,
    uname=GENERAL_DEFAULTS["uname"],
    key=None,
    local_port="8888",
    remote_port="8888",
    kill=True,
):
    """
    open ssh tunnel binding local_port to remote_port. returns pid of ssh
    process. default arguments bind to default jupyter port.
    """
    viewer = Viewer.from_command(
        "ssh",
        f"-nN",
        f"-i{key}",
        f"-L {local_port}:localhost:{remote_port}",
        f"{uname}@{ip}",
        _bg=True,
        _host=ip,
    )
    if kill is True:

        def kill_if_present():
            if viewer.is_alive():
                viewer.kill()

        atexit.register(kill_if_present)
    return viewer


def tunnel_through(
    gateway,
    server,
    uname=GENERAL_DEFAULTS["uname"],
    gateway_key=None,
    tunnel_port="8888",
    gateway_port="22",
):
    """
    open ssh tunnel binding tunnel_port on localhost to tunnel_port on server,
    going through gateway port on gateway. returns Viewer on ssh process.
    default arguments bind to default jupyter port.
    """
    return Viewer.from_command(
        "ssh",
        f"-nN",
        f"-i{gateway_key}",
        f"-L",
        f"{tunnel_port}:{server}:{gateway_port}",
        f"{uname}@{gateway}",
        _bg=True,
        _host=gateway,
    )


def ssh_command(ip, uname=GENERAL_DEFAULTS["uname"], key=None):
    """baked ssh command for a server."""
    arguments = [f"{uname}@{ip}"]
    if key is not None:
        arguments = [f"-i{key}"] + arguments
    return sh.ssh.bake(*arguments)


def wrap_ssh(ip, uname=GENERAL_DEFAULTS["uname"], key=None, caller=None):
    """baked ssh command for a server with extra management."""
    ssh = ssh_command(ip, uname, key)
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
        # TODO: determine if this shares too much responsibility with Viewer
        # baked sh.Command objects instantiate copies of the sh.Command class
        # that cannot be equated to the sh.Command class as imported from sh
        if is_sh_command(command_args[0]):
            command = command_args[0].bake(*command_args[1:], **unspecial)
            command_string = f"(nohup {command}"
        else:
            command_string = f"(nohup {' '.join(command_args)} "
            command_string += " ".join(
                [f"--{k}={v}" for k, v in unspecial.items()]
            )
        command_string += f") >> {_output_file}"
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


def ssh_dict(
    server_dict, uname=GENERAL_DEFAULTS["uname"], key=None, namelist=None
):
    # returns a dict of ssh commands corresponding to
    # a passed dict of name: ip.
    # if namelist is not passed, names them
    # according to keys in the server_dict.
    if not namelist:
        namelist = list(server_dict.keys())
    return {
        namelist[server_number]: ssh_command(ip, uname, key)
        for server_number, ip in enumerate(server_dict.values())
        if ip is not None
    }


# remote jupyter connection

TOKEN_PATTERN = re.compile(r"(?<=\?token=)([a-z]|\d)+")


def get_jupyter_token(command, jupyter_executable, remote_port):
    for attempt in range(5):
        try:
            jlist = command(jupyter_executable, "list", "")
            for line in jlist.splitlines():
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
    uname=GENERAL_DEFAULTS["uname"],
    key=None,
    local_port="8888",
    remote_port="8888",
    env=None,
    get_token=True,
    kill_on_exit=True,
    _bg=True,
    **command_kwargs,
):
    command = wrap_ssh(ip, uname, key)
    if env is not None:
        jupyter = f"{find_conda_env(command, env)}" f"/bin/jupyter notebook"
    else:
        # TODO: this doesn't assemble correctly
        jupyter = "jupyter notebook"
    if kill_on_exit is True:

        def done(*_):
            command(jupyter, "stop", f"--NbserverStopApp.port={remote_port}")

    else:
        done = zero
    jupyter_launch = command(
        jupyter,
        f"--port {remote_port} --no-browser",
        _done=done,
        _viewer=True,
        _bg=True,
        **command_kwargs,
    )
    if get_token:
        token = get_jupyter_token(command, jupyter, remote_port)
        jupyter_url = f"http://localhost:{local_port}/?token={token}"
    else:
        jupyter_url = f"http://localhost:{local_port}"
    jupyter_tunnel = tunnel(ip, uname, key, local_port, remote_port)
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
            return next(filter(lambda l: "envs" not in l, envs))
        else:
            return next(filter(lambda l: suffix in l, envs))
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


def find_ssh_key(keyname, paths=None) -> Union[Path, None]:
    if paths is None:
        paths = list(GENERAL_DEFAULTS["secrets_folders"]) + [os.getcwd()]
    paths = listify(paths)
    for directory in filter(lambda p: p.exists(), map(Path, paths)):
        matching_private_keys = filter(
            lambda x: "private key" in Magic().from_file(x),
            filter(lambda x: keyname in x.name, Path(directory).iterdir()),
        )
        try:
            return next(matching_private_keys)
        except StopIteration:
            continue
    return None
