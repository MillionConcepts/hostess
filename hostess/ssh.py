from concurrent.futures import ThreadPoolExecutor
import io
import warnings
from itertools import product
import re
import os
from pathlib import Path
import time
from typing import (
    Any,
    Callable,
    Collection,
    Hashable,
    Mapping,
    Optional,
    Union,
    Literal,
    IO,
)

import fabric.transfer
from dustgoggles.func import zero, filtern
from dustgoggles.structures import listify
from fabric import Connection
from invoke import UnexpectedExit
from magic import Magic
import pandas as pd

from hostess.config import GENERAL_DEFAULTS
import hostess.shortcuts as short
from hostess.subutils import Processlike, RunCommand, Viewer
from hostess.utilities import timeout_factory

 
def launch_tunnel_thread(
    host: str, 
    uname: str, 
    keyfile: str,
    local_port: int,
    remote_port: int,
    signalbuf: Optional[list] = None
) -> Union[tuple[Connection, Exception], Any]:
    """
    launch an SSH tunnel. primarily intended as a helper function 
    for `open_tunnel()`, but can be used on its own. blocks until
    it hits an exception or it receives a signal, so should generally
    be run in a thread.

    Args:
        host: hostname of tunnel target
        uname: username on remote host
        keyfile: path to local SSH key file
        local_port: port for proximal end of tunnel
        remote_port: port for distal end of tunnel
        signalbuf: list to receive close-tunnel signal

    Returns:
        signal received if closed gracefully; tuple of the Connection
        object and the Exception if it hits an exception.
    """
    conn = SSH.connect(host, uname, keyfile).conn
    try:
        with conn.forward_local(local_port, remote_port):
            while True:
                if signalbuf is not None:
                    if len(signalbuf) > 0:
                        conn.close()
                        return signalbuf[0]
                time.sleep(1)
    except Exception as ex:
        return conn, ex


def open_tunnel(
    host: str,
    uname: str,
    keyfile: Union[str, Path],
    local_port: int,
    remote_port: int,
) -> tuple[Callable[[Any], None], dict[str, Union[int, str, Path]]]:
    """
    launch a thread that maintains an SSH tunnel. NOTE: supports only
    keyfile authentication.

    Args:
        host: remote host ip
        uname: user name on remote host
        keyfile: path to keyfile
        local_port: port on local end of tunnel
        remote_port: port on remote end of tunnel

    Returns:
        signaler: function that shuts down tunnel
        tunnel_metadata: dict of metadata about the tunnel
    """

    exc = ThreadPoolExecutor(1)
    signalbuf = []
    exc.submit(
        launch_tunnel_thread, 
        host, 
        uname, 
        keyfile, 
        local_port, 
        remote_port, 
        signalbuf
    )
    metadict = {
        "host": host,
        "uname": uname,
        "keyfile": keyfile,
        "local_port": local_port,
        "remote_port": remote_port,
    }
    
    def signaler(sig=0):
        signalbuf.append(sig)
        
    return signaler, metadict


def _move_print_heads(err_head, out_head, process):
    from rich import print as rp

    has_new_output = False
    if (outlen := len(process.out)) > out_head:
        has_new_output = True
        for outline in process.out[out_head:]:
            rp(outline)
        out_head = outlen
    if (errlen := len(process.err)) > err_head:
        has_new_output = True
        for errline in process.err[err_head:]:
            rp(f"[red]{errline}[/red]")
        err_head = errlen
    return has_new_output, out_head, err_head


class SSH(RunCommand):
    """
    callable interface to an SSH connection to a remote host. basically a
    wrapper for a fabric.connection.Connection object with additional
    functionality for managed command execution. NOTE: supports only keyfile
    authentication.

    Examples:
        >>> ssh = SSH.connect(
        ...    "1.11.11.111", 'remote_user', '/home/user/.ssh/keyfile.pem'
        ... )
        >>> ssh("echo hi > a.txt")
        >>> tail = ssh("tail -f a.txt")
        >>> for n in range(5):
            ... ssh(f"echo {n} >> a.txt")
        >>> print(','.join([s.strip() for s in tail.out]))
        >>> ssh.con('ls -l / | grep dev')

        expected output:

            hi, 0, 1, 2, 3
            drwxr-xr-x  15 root   root     3320 Nov 12 01:50 dev
    """

    def __init__(
        self,
        command: Optional[str] = None,
        conn: Optional[Connection] = None,
        key: Optional[str] = None,
        **kwargs: Union[int, float, str, bool],
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
        self.tunnels: list[tuple[Callable, dict]] = []

    @classmethod
    def connect(
        cls, host: str, uname: str = GENERAL_DEFAULTS["uname"], key: str = None
    ) -> "SSH":
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

    def put(
        self,
        source: Union[str, Path, IO, bytes],
        target: Union[str, Path],
        *args: Any,
        literal_str: bool = False,
        **kwargs: Any,
    ) -> dict:
        """
        write local file or object to target file on remote host.

        Args:
            source: filelike object or path to local file
            target: write path on remote host
            args: additional arguments to pass to underlying put method
            literal_str: if True and `source` is a `str`, write `source`
                into `target` as text rather than interpreting `source` as a
                path
            kwargs: additional kwargs to pass to underlying put command

        Returns:
            dict giving transfer metadata: local, remote, host, and port
        """
        if isinstance(source, str) and (literal_str is True):
            source = io.StringIO(source)
        if isinstance(source, bytes):
            source = io.BytesIO(source)
        elif not isinstance(source, (str, Path, io.StringIO, io.BytesIO)):
            raise TypeError("Source must be a string, Path, or IO.")
        return unpack_transfer_result(
            self.conn.put(source, target, *args, **kwargs)
        )

    def get(
        self,
        source: Union[str, Path],
        target: Union[str, Path, IO],
        *args: Any,
        **kwargs: Any,
    ) -> dict:
        """
        copy file from remote to local.

        Args:
            source: path to file on remote host
            target: path to local file, or a filelike object (such as
                io.BytesIO)
            *args: args to pass to underlying get method
            **kwargs: kwargs to pass to underlying get method

        Returns:
            dict giving transfer metadata: local, remote, host, and port
        """
        return unpack_transfer_result(
            self.conn.get(str(source), target, *args, **kwargs)
        )

    def read(
        self,
        source: str,
        mode: Literal["r", "rb"] = "r",
        encoding: str = "utf-8",
        as_buffer: bool = False,
    ) -> Union[io.BytesIO, io.StringIO, bytes, str]:
        """
        read a file from the remote host directly into memory.

        Args:
            source: path to file on remote host.
            mode: 'r' to read file as text; 'rb' to read file as bytes
            encoding: encoding for text, used only if `mode` is 'r'
            as_buffer: if True, return BytesIO/StringIO; if False, return
                bytes/str

        Returns:
            contents of remote file as str, bytes, or IO
        """
        if mode not in ("r", "rb"):
            raise TypeError("mode must be 'r' or 'rb'")
        buffer = io.BytesIO()
        self.get(source, buffer)
        buffer.seek(0)
        if mode == "r":
            stringbuf = io.StringIO()
            stringbuf.write(buffer.read().decode(encoding))
            stringbuf.seek(0)
            buffer = stringbuf
        if as_buffer is True:
            return buffer
        return buffer.read()

    def read_csv(
        self,
        source: Union[str, Path],
        encoding: str = "utf-8",
        **csv_kwargs: Any,
    ) -> pd.DataFrame:
        """
        read a CSV-like file from the remote host into a pandas DataFrame.

        Args:
            source: path to CSV-like file on remote host
            encoding: encoding for text
            csv_kwargs: kwargs to pass to pd.read_csv

        Returns:
            DataFrame created from contents of remote CSV file
        """
        return pd.read_csv(
            self.read(str(source), "r", encoding, True), **csv_kwargs
        )

    def tunnel(self, local_port: int, remote_port: int):
        """
        create an SSH tunnel between a local port and a remote port; store an
        abstraction for the tunnel process, along with metadata about the
        tunnel, in self.tunnels.

        Args:
            local_port: port number for local end of tunnel.
            remote_port: port number for remote end of tunnel.
        """
        signaler, meta = open_tunnel(
            self.host, self.uname, self.key, local_port, remote_port
        )
        self.tunnels.append((signaler, meta))

    def __call__(
        self,
        *args: Union[int, float, str],
        _quiet: bool = True,
        _viewer: bool = True,
        _wait: bool = False,
        **kwargs: Union[int, float, str, bool],
    ) -> Processlike:
        """
        run a shell command in the remote host's default interpreter. See
        `RunCommand.__call__()` for details on calling conventions and options.

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
        if (_w := kwargs.pop("_w", None)) is not None:
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

    def con(
        self,
        *args: Union[int, float, str],
        _poll: float = 0.05,
        _timeout: Optional[float] = None,
        _return_viewer: bool = False,
        **kwargs: Union[int, float, str, bool],
    ) -> Optional[Viewer]:
        """
        pretend you are running a command on the remote host while looking at a
        terminal emulator. pauses for output and pretty-prints it to stdout.

        Does not return a process abstraction by default (pass
        _return_viewer=True if you want one). Fun in interactive environments.

        Only arguments unique to con() are described here; others are as
        SSH.__call__().

        Args:
            *args: additional args to pass to self.__call__.
            _poll: polling rate for process output, in seconds
            _timeout: if not None, raise a TimeoutError if this many seconds
                pass before receiving additional output from process (or
                process exit).
            _return_viewer: if True, return a Viewer for the process once it
                exits. Otherwise, return None.
            **kwargs: additional kwargs to pass to self.__call__.

        Returns:
            A Viewer if _return_viewer is True; otherwise None.
        """
        if kwargs.get("_viewer") is False:
            raise TypeError("Cannot call con() with _viewer=False")
        process = self(*args, _viewer=True, **kwargs)
        if _timeout is not None:
            waiting, unwait = timeout_factory(True, _timeout)
        else:
            waiting, unwait = zero, zero
        out_head, err_head = 0, 0
        try:
            while process.running:
                has_new_output, out_head, err_head = _move_print_heads(
                    err_head, out_head, process
                )
                if has_new_output is True:
                    unwait()
                else:
                    waiting()
                time.sleep(_poll)
            _move_print_heads(err_head, out_head, process)
        except KeyboardInterrupt:
            process.kill()
            print("^C")
        if _return_viewer is True:
            return process

    def close(self):
        for process, _ in self.tunnels:
            process.kill()
        if self.conn is not None:
            self.conn.close()

    def __str__(self):
        return f"{super().__str__()}\n{self.uname}@{self.host}"

    def __del__(self):
        self.close()

    conn = None


# TODO, maybe: try fabric's pooled commands
def merge_csv(
    ssh_dict: Mapping[Hashable, SSH], fn: str, **csv_kwargs: Any
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
    "miniforge3",
    "miniforge",
    "miniconda3",
    "anaconda3",
    "mambaforge",
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

    Raises:
        FileNotFoundError: if environment cannot be found.
    """
    env = "base" if env is None else env
    suffix = f"/envs/{env}" if env != "base" else ""
    try:
        cat = cmd(f"cat ~/.conda/environments.txt", _viewer=True)
        cat.wait()
        envs = "".join(cat.out).split("\n")
        if env == "base":
            return next(
                filter(lambda l: "envs" not in l and len(l) > 0, envs)
            ).strip()
        else:
            return next(filter(lambda l: suffix in l, envs)).strip()
    except (UnexpectedExit, StopIteration):
        pass
    getlines = cmd(
        short.chain(
            [short.truthy(f"-e {path}{suffix}") for path in CONDA_SEARCH_PATHS]
        ),
        _viewer=True,
    )
    getlines.wait()
    lines = getlines.out
    for line, path in zip(lines, CONDA_SEARCH_PATHS):
        if "True" in line:
            return f"{path}/{suffix}"
    raise FileNotFoundError("conda environment not found.")


def stop_jupyter_factory(
    command: RunCommand, jupyter: str, port: int
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
            running on the specified port. If called with no arguments or with
            an object with no `wait` method, attempts to stop the server
            immediately.
    """

    def stop_it(waitable: Any = None):
        if waitable is not None:
            waitable.wait()
        command(f"{jupyter} stop {port}")

    return stop_it


def get_jupyter_token(
    command: RunCommand, jupyter_executable: str, port: int
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


NotebookConnection = tuple[str, Callable, dict, Processlike, Callable[[], None]]
"""
structure containing results of a tunneled Jupyter Notebook execution.

1. URL for Jupyter server
2. function that shuts down tunnel
3. SSH tunnel metadata
3. Jupyter execution process
4. Callable for gracefully shutting down Notebook
"""


def jupyter_connect(
    ssh: SSH,
    local_port: int = 22222,
    remote_port: int = 8888,
    env: Optional[str] = None,
    get_token: bool = True,
    kill_on_exit: bool = False,
    working_directory: Optional[str] = None,
    lab: bool = False,
    **command_kwargs: Union[int, str, bool],
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
        lab: launch JupyterLab instead of Jupyter Notebook
        **command_kwargs: additional kwargs to pass to `jupyter notebook`

    Returns:
        structure containing results of tunneled notebook execution,
            including a callable to terminate the Notebook and another
            to close the tunnel
    """
    booktype = "notebook" if lab is False else "lab"
    if env is not None:
        jupyter = f"{find_conda_env(ssh, env)}" f"/bin/jupyter {booktype}"
    else:
        jupyter = f"jupyter {booktype}"
    stopper = stop_jupyter_factory(ssh, jupyter, remote_port)
    done = stopper if kill_on_exit is True else zero
    cmd = f"{jupyter} --port {remote_port} --no-browser"
    if working_directory is not None:
        cmd = f"cd {working_directory} && {cmd}"
    launch_process = ssh(cmd, _done=done, _bg=True, **command_kwargs)
    jupyter_url_base = f"http://localhost:{local_port}"
    if get_token:
        try:
            token = get_jupyter_token(ssh, jupyter, remote_port)
            jupyter_url = f"{jupyter_url_base}/?token={token}"
        except ValueError as ve:
            warnings.warn(str(ve))
            jupyter_url = None
    else:
        jupyter_url = jupyter_url_base
    if jupyter_url is not None:
        ssh.tunnel(local_port, remote_port)
        tunnel, tunnel_meta = ssh.tunnels[-1]
    else:
        tunnel, tunnel_meta = None, None
    return jupyter_url, tunnel, tunnel_meta, launch_process, stopper


def find_ssh_key(
    keyname: str, paths: Optional[Collection[Union[str, Path]]] = None
) -> Union[Path, None]:
    """
    look for private SSH keyfile.

    Args:
        keyname: full or partial name of keyfile
        paths: paths in which to search for key file. if not specified, look
            in hostess.config.GENERAL_DEFAULTS['secrets_folders']

    Returns:
        path to keyfile

    Raises:
        FileNotFoundError: if no key found
    """
    checked = []
    if paths is None:
        paths = list(GENERAL_DEFAULTS["secrets_folders"]) + [os.getcwd()]
    for directory in filter(lambda p: p.exists(), map(Path, listify(paths))):
        # TODO: public key option
        try:
            matching_private_keys = filter(
                lambda x: "private key" in Magic().from_file(x),
                filter(lambda x: keyname in x.name, Path(directory).iterdir()),
            )
            return next(matching_private_keys)
        except StopIteration:
            checked.append(f"{directory}")
        except PermissionError:
            checked.append(f"(permission denied) {directory}")
    raise FileNotFoundError(f"Looked in: {'; '.join(checked)}")


def unpack_transfer_result(result: fabric.transfer.Result) -> dict:
    """
    summarize a fabric transfer Result.

    Args:
        result: Result of a get, put, or similar SSH operation.

    Returns:
        dict giving local and remote transfer targets, hostname, and port.
    """
    return {
        "local": result.local,
        "remote": result.remote,
        "host": result.connection.host,
        "port": result.connection.port,
    }
