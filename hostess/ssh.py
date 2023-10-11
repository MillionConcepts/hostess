import io
import os
import time
from itertools import product
from multiprocessing import Process
import re
from pathlib import Path
from typing import Hashable, Mapping, Optional, Union

from dustgoggles.func import zero, filtern
from dustgoggles.structures import listify
from fabric import Connection
from invoke import UnexpectedExit
from magic import Magic

from hostess.config import GENERAL_DEFAULTS
import hostess.shortcuts as short
from hostess.subutils import RunCommand


def open_tunnel(host, uname, keyfile, local_port, remote_port):
    """abstraction to open a tunnel with Fabric in a child process."""

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
    """wrapper for managed command execution via fabric.Connection"""

    def __init__(
        self,
        command="",
        conn: Optional[Connection] = None,
        key: Optional[str] = None,
        **kwargs
    ):
        if conn is None:
            raise TypeError("a Connection must be provided")
        super().__init__(command, conn, conn["runners"]["remote"], **kwargs)
        self.host, self.uname, self.key = conn.host, conn.user, key
        self.conn = conn  # effectively an alias for self.ctx
        self.tunnels = []

    @classmethod
    def connect(cls, host, uname=GENERAL_DEFAULTS["uname"], key=None):
        connect_kwargs = {"key_filename": key} if key is not None else {}
        conn = Connection(user=uname, host=host, connect_kwargs=connect_kwargs)
        ssh = object().__new__(cls)
        ssh.__init__(conn=conn, key=key)
        return ssh

    def get(self, *args, **kwargs):
        return self.conn.get(*args, **kwargs)

    def put(self, *args, **kwargs):
        return self.conn.put(*args, **kwargs)

    def read_csv(self, fname, **csv_kwargs):
        import pandas as pd

        buffer = io.StringIO()
        buffer.write(self.get(fname).decode())
        buffer.seek(0)
        return pd.read_csv(buffer, **csv_kwargs)

    def tunnel(self, local_port, remote_port):
        process, meta = open_tunnel(
            self.host, self.uname, self.key, local_port, remote_port
        )
        self.tunnels.append((process, meta))

    def __str__(self):
        return f"{super().__str__()}\n{self.uname}@{self.host}"


# TODO: try fabric's pooled commands
def merge_csv(
    ssh_dict: Mapping[Hashable, SSH], fn: str, **csv_kwargs
) -> "pd.DataFrame":
    """
    merges csv files -- logs, perhaps -- from across a defined group
    of remote servers.
    """
    import pandas as pd

    framelist = []
    for name, ssh in ssh_dict.items():
        csv_df = ssh.read_csv(fn, **csv_kwargs)
        csv_df["server"] = name
        framelist.append(csv_df)
    return pd.concat(framelist).reset_index(drop=True)


# jupyter / conda utilities

CONDA_NAMES = ("anaconda3", "miniconda3", "miniforge", "mambaforge")
CONDA_PARENTS = ("~", "/opt")
CONDA_SEARCH_PATHS = tuple(
    [f"{root}/{name}" for root, name in product(CONDA_PARENTS, CONDA_NAMES)]
)
TOKEN_PATTERN = re.compile(r"(?<=\?token=)([a-z]|\d)+")


def find_conda_env(cmd: RunCommand, env: str = None) -> str:
    env = "base" if env is None else env
    suffix = f"/envs/{env}" if env != "base" else ""
    try:
        envs = str(cmd(f"cat ~/.conda/environments.txt").stdout).splitlines()
        if env == "base":
            return next(filter(lambda l: "envs" not in l, envs))
        else:
            return next(filter(lambda l: suffix in l, envs))
    except (UnexpectedExit, StopIteration):
        pass
    lines = cmd(
        short.chain(
            [short.truthy(f"-e {path}{suffix}") for path in CONDA_SEARCH_PATHS]
        )
    ).stdout.splitlines()
    for line, path in zip(lines, CONDA_SEARCH_PATHS):
        if "True" in line:
            return f"{path}/{suffix}"
    raise FileNotFoundError("conda environment not found.")


def stop_jupyter_factory(command, jupyter, remote_port):
    def stop_it(waitable):
        waitable.wait()
        command(f"{jupyter} stop --NbserverStopApp.port={remote_port}")

    return stop_it


def get_jupyter_token(
    command: RunCommand,
    jupyter_executable: str,
    port: int
):
    for attempt in range(5):
        try:
            jlist = command(f"{jupyter_executable} list").stdout
            line = filtern(lambda l: str(port) in l, jlist.splitlines())
            return re.search(TOKEN_PATTERN, line).group()
        except (StopIteration, AttributeError):
            time.sleep(0.1)
            continue
    raise ValueError(
        "Token not found. Notebook may not have started on correct port. "
    )


def jupyter_connect(
    ssh: SSH,
    local_port: int = 22222,
    remote_port: int = 8888,
    env: Optional[str] = None,
    get_token: bool = True,
    kill_on_exit: bool = True,
    **command_kwargs,
):
    if env is not None:
        jupyter = f"{find_conda_env(ssh, env)}" f"/bin/jupyter notebook"
    else:
        jupyter = "jupyter notebook"
    if kill_on_exit is True:
        done = stop_jupyter_factory(ssh, jupyter, remote_port)
    else:
        done = zero
    jupyter_launch = ssh(
        f"{jupyter} --port {remote_port} --no-browser",
        _done=done,
        #         TODO: work on yielding a viewer i guess?
        #         _viewer=True,
        _bg=True,
        **command_kwargs,
    )
    jupyter_url_base = f"http://localhost:{local_port}"
    if get_token:
        token = get_jupyter_token(ssh, jupyter, remote_port)
        jupyter_url = f"{jupyter_url_base}/?token={token}"
    else:
        jupyter_url = jupyter_url_base
    ssh.tunnel(local_port, remote_port)
    return jupyter_url, ssh.tunnels[-1], jupyter_launch


def find_ssh_key(keyname, paths=None) -> Union[Path, None]:
    """look for private SSH key in common folders"""
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
