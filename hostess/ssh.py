import io
from itertools import product
from multiprocessing import Process
import re
import os
from pathlib import Path
import time
from typing import (
    Any, Callable, Collection, Hashable, Mapping, Optional, Union
)

from dustgoggles.func import zero, filtern
from dustgoggles.structures import listify
from fabric import Connection
from invoke import UnexpectedExit
from magic import Magic
import pandas as pd

from hostess.config import GENERAL_DEFAULTS
import hostess.shortcuts as short
from hostess.subutils import RunCommand, Processlike


def open_tunnel(
    host: str,
    uname: str,
    keyfile: Union[str, Path],
    local_port: int,
    remote_port: int
) -> tuple[Process, dict[str, Union[int, str, Path]]]:
    """
    create a child process that maintains an SSH tunnel. NOTE: supports only
    keyfile authentication.

    Args:
        host: remote host ip
        uname: user name on remote host
        keyfile: path to keyfile
        local_port: port on local end of tunnel
        remote_port: port on remote end of tunnel

    Returns:
        tuple whose elements are:
            0: `Process` abstraction for the tunnel process
            1: dict of metadata about the tunnel
    """

    def target():
        conn = SSH.connect(host, uname, keyfile).conn
        try:
            with conn.forward_local(local_port, remote_port):
                while True:
                    time.sleep(1)
        except Exception as ex:
            return conn, ex

    process = Process(target=target)
    process.start()
    metadict = {
        "host": host,
        "uname": uname,
        "keyfile": keyfile,
        "local_port": local_port,
        "remote_port": remote_port,
    }
    # TODO: add a pipe or something
    return process, metadict


class SSH(RunCommand):
    """
    callable interface to an SSH connection to a remote host. basically a
    wrapper for a fabric.connection.Connection object with additional
    functionality for managed command execution. NOTE: supports only keyfile
    authentication.

    Example of use:

        >>> ssh = SSH.connect(
        ...    "1.11.11.111", 'remote_user', '/home/user/.ssh/keyfile.pem'
        ... )
        >>> ssh("echo hi > a.txt")
        >>> tail = ssh("tail -f a.txt")
        >>> for n in range(5):
            ... ssh(f"echo {n} >> a.txt")
        >>> print(','.join([s.strip() for s in tail.out]))
        >>> ssh.con('ls -l / | grep dev')

    output:

        hi, 0, 1, 2, 3
        drwxr-xr-x  15 root   root     3320 Nov 12 01:50 dev
    """

    def __init__(
        self,
        command: Optional[str] = None,
        conn: Optional[Connection] = None,
        key: Optional[str] = None,
        **kwargs
    ):
        """
        Args:
            command: optional shell command to 'curry'  into this object. may
                be omitted if commands will be provided later, or if this
                particular object is not intended to execute commands.
            conn: Fabric `Connection` object
            key: path to keyfile; may be provided after instantiation, but
                must be provided before command is actually executed.
            **kwargs: RunCommand init kwargs (see RunCommand documentation)
        """
        if conn is None:
            raise TypeError("a Connection must be provided")
        super().__init__(command, conn, conn["runners"]["remote"], **kwargs)
        self.host, self.uname, self.key = conn.host, conn.user, key
        self.conn = conn  # effectively an alias for self.ctx
        self.tunnels: list[tuple[Process, dict]] = []

    @classmethod
    def connect(
        cls,
        host: str,
        uname: str = GENERAL_DEFAULTS["uname"],
        key: str = None
    ):
        """
        constructor that creates a connection to the remote host and uses it
        to instantiate the SSH object. convenient in cases when an appropriate
        `Connection` object does not already exist or should not be reused.

        Args:
            host: ip of remote host
            uname: user name on remote host
            key: path to keyfile

        Returns:
            an SSH object with a newly-generated `Connection`.
        """
        connect_kwargs = {"key_filename": key} if key is not None else {}
        conn = Connection(user=uname, host=host, connect_kwargs=connect_kwargs)
        ssh = object().__new__(cls)
        ssh.__init__(conn=conn, key=key)
        return ssh

    def get(self, *args, **kwargs):
        """copy a file from the remote host."""
        return self.conn.get(*args, **kwargs)

    def put(self, *args, **kwargs):
        """copy a file to the remote host."""
        return self.conn.put(*args, **kwargs)

    def read_csv(self, fname: str, **csv_kwargs) -> pd.DataFrame:
        """
        read a CSV file from the remote host into a pandas DataFrame.

        Args:
            fname: path to CSV file on remote host.
            csv_kwargs: kwargs to pass to pd.read_csv.

        Returns:
            DataFrame created from contents of remote CSV file.
        """
        buffer = io.StringIO()
        buffer.write(self.get(fname).decode())
        buffer.seek(0)
        return pd.read_csv(buffer, **csv_kwargs)

    def tunnel(
        self,
        local_port: int,
        remote_port: int
    ):
        """
        create an SSH tunnel between a local port and a remote port; store an
        abstraction for the tunnel process, along with metadata about the
        tunnel, in self.tunnels.

        Args:
            local_port: port number for local end of tunnel.
            remote_port: port number for remote end of tunnel.
        """
        process, meta = open_tunnel(
            self.host, self.uname, self.key, local_port, remote_port
        )
        self.tunnels.append((process, meta))

    def __call__(
        self,
        *args,
        _quiet: bool = True,
        _viewer: bool = True,
        _wait: bool = False,
        **kwargs
    ) -> Processlike:
        """
        run a shell command in the remote host's default interpreter.

        Args:
            *args: args to use to construct the command.
            _viewer: if `True`, return a hostess `Viewer` object. otherwise
                return unwrapped Fabric `Result`.
            _wait: if `True`, block until command terminates (or connection
                fails). _w is an alias.
            _quiet: if `False`, print stdout and stderr, should the process
                return any before this function terminates. Generally best
                used with _wait=True.
            **kwargs: kwargs to pass to command execution. kwarg
                names beginning with '_' specify execution meta-parameters;
                others will be inserted directly into the command as `--`-type
                shell parameters.

        Returns:
            object representing executed process.
        """
        if (_w := kwargs.pop('_w', None)) is not None:
            _wait = _w
        result = super().__call__(*args, _viewer=_viewer, **kwargs)
        if _wait is True:
            result.wait()
        if _quiet is False:
            from rich import print as rp
            if len(result.stdout) > 0:
                rp(*result.stdout)
            if len(result.stderr) > 0:
                rp(*map(lambda t: f"[red]{t}[/red]", result.stderr))
        return result

    def con(self, *args, **kwargs):
        """
        pretend you are running a command on the instance while looking at a
        terminal emulator, pausing for output and pretty-printing it to stdout.

        like SSH.__call__() with _wait=True, _quiet=False, but does not return
        a process abstraction. fun in interactive environments.
        """
        self(*args, _quiet=False, _wait=True, **kwargs)

    def __str__(self):
        return f"{super().__str__()}\n{self.uname}@{self.host}"

    def __del__(self):
        if self.conn is not None:
            self.conn.close()
        for process, _ in self.tunnels:
            process.kill()

    conn = None


# TODO, maybe: try fabric's pooled commands
def merge_csv(
    ssh_dict: Mapping[Hashable, SSH], fn: str, **csv_kwargs
) -> pd.DataFrame:
    """
    merges data from CSV files on multiple remote hosts into a single pandas
    DataFrame.

    Args:
        ssh_dict: mapping whose keys are identifiers for remote hosts and
            whose values are SSH objects connected to those hosts.
        fn: path to file (must be the same on all remote hosts)
        csv_kwargs: kwargs to pass to pd.read_csv()

    Returns:
         a DataFrame containing merged data from all remote CSV files,
         including a "server" column that labels the source hosts using the
         keys of `ssh_dict`.
    """
    framelist = []
    for name, ssh in ssh_dict.items():
        csv_df = ssh.read_csv(fn, **csv_kwargs)
        csv_df["server"] = name
        framelist.append(csv_df)
    return pd.concat(framelist).reset_index(drop=True)


# jupyter / conda utilities

CONDA_NAMES = (
    "miniforge3", "miniforge", "miniconda3", "anaconda3", "mambaforge"
)
CONDA_PARENTS = ("~", "/opt")
CONDA_SEARCH_PATHS = tuple(
    [f"{root}/{name}" for root, name in product(CONDA_PARENTS, CONDA_NAMES)]
)
TOKEN_PATTERN = re.compile(r"(?<=\?token=)([a-z]|\d)+")


def find_conda_env(cmd: RunCommand, env: str = None) -> str:
    """
    find location of a named conda environment. intended primarily for use
    on remote hosts.

    Args:
        cmd: instance of RunCommand or one of its subclasses; most likely an
            SSH instance.
        env: name of conda environment.

    Returns:
        absolute path to root directory of conda environment.
    """
    env = "base" if env is None else env
    suffix = f"/envs/{env}" if env != "base" else ""
    try:
        cat = cmd(
            f"cat ~/.conda/environments.txt", _viewer=True
        )
        cat.wait()
        envs = cat.out
        if env == "base":
            return next(filter(lambda l: "envs" not in l, envs)).strip()
        else:
            return next(filter(lambda l: suffix in l, envs)).strip()
    except (UnexpectedExit, StopIteration):
        pass
    getlines = cmd(
        short.chain(
            [short.truthy(f"-e {path}{suffix}") for path in CONDA_SEARCH_PATHS]
        ),
        _viewer=True
    )
    getlines.wait()
    lines = getlines.out
    for line, path in zip(lines, CONDA_SEARCH_PATHS):
        if "True" in line:
            return f"{path}/{suffix}"
    raise FileNotFoundError("conda environment not found.")


def stop_jupyter_factory(
    command: RunCommand,
    jupyter: str,
    port: int
) -> Callable:
    """
    Create a function that shuts down a Jupyter server when some other task
    or process completes.

    Args:
        command: an instance of RunCommand or one of its subclasses, likely
            an SSH object.
        jupyter: absolute path to jupyter executable
        port: port on which jupyter server is running

    Returns:
        function that, when passed anything with a `wait` method, calls that
        method, and once it finishes, attempts to stop the Jupyter server
        running on the specified port.
    """
    def stop_it(waitable: Any):
        waitable.wait()
        command(f"{jupyter} stop --NbserverStopApp.port={port}")

    return stop_it


def get_jupyter_token(
    command: RunCommand,
    jupyter_executable: str,
    port: int
) -> str:
    """
    Get the access token of a Jupyter server running on the specified port.

    Args:
        command: an instance of RunCommand or one of its subclasses, likely
            an SSH object.
        jupyter_executable: path to Jupyter executable.
        port: port on which Jupyter server is running.

    Returns:
        the Jupyter server's access token.
    """
    for attempt in range(5):
        try:
            jlister = command(f"{jupyter_executable} list", _viewer=True)
            jlister.wait()
            line = filtern(lambda l: str(port) in l, jlister.out)
            return re.search(TOKEN_PATTERN, line).group()
        except (StopIteration, AttributeError):
            time.sleep(0.1)
            continue
    raise ValueError(
        "Token not found. Notebook may not have started on correct port. "
    )


NotebookConnection = tuple[str, tuple[Process, dict], Processlike]
"""
structure containing results of a tunneled Jupyter Notebook execution.

1. URL for Jupyter server
2. SSH tunnel process + metadata
3. Jupyter execution process
"""


def jupyter_connect(
    ssh: SSH,
    local_port: int = 22222,
    remote_port: int = 8888,
    env: Optional[str] = None,
    get_token: bool = True,
    kill_on_exit: bool = True,
    working_directory: Optional[str] = None,
    **command_kwargs,
) -> NotebookConnection:
    """
    Launch a Jupyter server on a remote host over an SSH tunnel.

    Args:
        ssh: `SSH` object connected to remote host
        local_port: port number for local end of tunnel
        remote_port: port number for remote Jupyter server / remote end of
            tunnel
        env: conda env from which to launch Jupyter server; if none is
            specified, use the remote host's default `jupyter`
        get_token: get the access token from the server?
        kill_on_exit: attempt to kill the Jupyter server when the
            `jupyter_launch` process terminates?
        working_directory: working directory for jupyter server
        **command_kwargs: additional kwargs to pass to `jupyter notebook`

    Returns:
        structure containing results of tunneled notebook execution
    """
    if env is not None:
        jupyter = f"{find_conda_env(ssh, env)}" f"/bin/jupyter notebook"
    else:
        jupyter = "jupyter notebook"
    if kill_on_exit is True:
        done = stop_jupyter_factory(ssh, jupyter, remote_port)
    else:
        done = zero
    cmd = f"{jupyter} --port {remote_port} --no-browser"
    if working_directory is not None:
        cmd = f"cd {working_directory} && {cmd}"
    # TODO, maybe: return a Viewer here
    jupyter_launch = ssh(cmd, _done=done, _bg=True, **command_kwargs)
    jupyter_url_base = f"http://localhost:{local_port}"
    if get_token:
        token = get_jupyter_token(ssh, jupyter, remote_port)
        jupyter_url = f"{jupyter_url_base}/?token={token}"
    else:
        jupyter_url = jupyter_url_base
    ssh.tunnel(local_port, remote_port)
    return jupyter_url, ssh.tunnels[-1], jupyter_launch


def find_ssh_key(
    keyname, paths: Optional[Collection[Union[str, Path]]] = None
) -> Union[Path, None]:
    """
    look for private SSH keyfile.

    Args:
        keyname: full or partial name of keyfile
        paths: paths in which to search for key file. if not specified, look
            in hostess.config.GENERAL_DEFAULTS['secrets_folders']

    Returns:
        path to keyfile, or None if not found.
    """
    if paths is None:
        paths = list(GENERAL_DEFAULTS["secrets_folders"]) + [os.getcwd()]
    for directory in filter(lambda p: p.exists(), map(Path, listify(paths))):
        # TODO: public key option
        matching_private_keys = filter(
            lambda x: "private key" in Magic().from_file(x),
            filter(lambda x: keyname in x.name, Path(directory).iterdir()),
        )
        try:
            return next(matching_private_keys)
        except StopIteration:
            continue
    return None
