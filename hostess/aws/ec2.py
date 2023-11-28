from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
from functools import cache, partial, wraps
import io
from itertools import chain
import os
from multiprocessing import Process
from pathlib import Path
import pickle
from random import choices
import re
from string import ascii_lowercase
import time
from typing import (
    Callable,
    Collection,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union, IO, Any
)

import boto3.resources.base
import botocore.client

import botocore.exceptions
import fabric.transfer
from cytoolz.curried import get
import dateutil.parser as dtp
from dustgoggles.func import gmap, zero
from dustgoggles.structures import listify
from invoke import UnexpectedExit
from paramiko.ssh_exception import NoValidConnectionsError, SSHException

import hostess.shortcuts as ks
from hostess.aws.pricing import (
    get_cpu_credit_price,
    get_ec2_basic_price_list,
    get_on_demand_price,
)
from hostess.aws.utilities import (
    autopage,
    clarify_region,
    init_client,
    init_resource,
    tag_dict,
    tagfilter, _check_cached_results, _clear_cached_results,
)
from hostess.caller import generic_python_endpoint, CallerCompressionType, \
    CallerSerializationType, CallerUnpackingOperator
from hostess.config import EC2_DEFAULTS, GENERAL_DEFAULTS
from hostess.ssh import (
    find_ssh_key, find_conda_env, jupyter_connect, NotebookConnection, SSH
)
from hostess.subutils import Processlike, Viewer
from hostess.utilities import (
    filestamp,
    my_external_ip,
    timeout_factory,
)

InstanceState = Literal[
    "running", "stopped", "terminated", "stopping", "pending", "shutting-down"
]
"""valid EC2 instance state names"""

InstanceIdentifier = str
"""
stringified IP (e.g. `'111.11.11.1'` or full instance id 
(e.g. `i-0243d3f8g0a85cb18`), used as an explicit instance identifier by some 
functions in this module.
"""

InstanceDescription = dict[str, Union[dict, str]]
"""
concise version of an EC2 API Instance data structure 
(see https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_Instance.html)
"""


class NoKeyError(OSError):
    """we're trying to do things over SSH, but can't find a valid keyfile."""


def connectwrap(func):
    @wraps(func)
    def tryconnect(instance, *args, **kwargs):
        # noinspection PyProtectedMember
        instance._prep_connection()
        try:
            return func(instance, *args, **kwargs)
        except (SSHException, NoValidConnectionsError):
            instance.connect()
            return func(instance, *args, **kwargs)

    return tryconnect


def summarize_instance_description(
    description: Mapping
) -> InstanceDescription:
    """
    convert a dictionary produced from an EC2 API Instance object to a more 
    concise format. Likely sources for this dictionary include `boto3` or 
    parsing JSON responses from the AWS CLI or HTTP API.
    """
    return {
        "name": tag_dict(description.get("Tags", {})).get("Name"),
        "ip": description.get("PublicIpAddress"),
        "id": description.get("InstanceId"),
        "state": description.get("State")["Name"],
        "type": description.get("InstanceType"),
        "ip_private": description.get("PrivateIpAddress"),
        "keyname": description.get("KeyName"),
    }


def ls_instances(
    identifier: Optional[InstanceIdentifier] = None,
    states: Sequence[str] = ("running", "pending", "stopping", "stopped"),
    raw_filters: Optional[Sequence[Mapping[str, str]]] = None,
    client=None,
    session=None,
    long: bool = False,
    tag_regex: bool = True,
    **tag_filters,
) -> tuple[Union[InstanceDescription, dict]]:
    """
    `ls` for EC2 instances.

    Args:
        identifier: string specifying a particular instance. may be a 
            stringified IP address or instance id.
        states: strings specifying legal states for listed instances.
        raw_filters: search filters to pass directly to the EC2 API.
        client: optional boto3 Client object
        session: optional boto3 Session object
        long: if not True, return InstanceDescriptions, including only the
            most regularly-pertinent information, flattened, with succinct
            field names. Otherwise return a flattened version of the full API
            response.
        tag_regex: regex patterns for tag matching.
        tag_filters: filters to interpret as tag name / value pairs before 
            passing to the EC2 API.

    Returns:
        tuple of records describing all matching EC2 instances owned by 
            caller.
    """
    client = init_client("ec2", client, session)
    filters = [] if raw_filters is None else raw_filters
    if identifier is not None:
        identifier = listify(identifier)
        if "." in identifier[0]:
            filters.append({"Name": "ip-address", "Values": identifier})
        else:
            filters.append({"Name": "instance-id", "Values": identifier})
    filters.append({"Name": "instance-state-name", "Values": list(states)})
    response = client.describe_instances(Filters=filters)
    descriptions = chain.from_iterable(
        map(get("Instances"), response["Reservations"])
    )
    # TODO: examine newer Filter API functionality (see 
    # https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_Filter.html)
    
    # the EC2 api does not support string inclusion or other fuzzy filters.
    # we would like to be able to fuzzy-filter, so we apply our tag filters to
    # the structure returned by boto3 from its DescribeInstances call.
    descriptions = filter(
        partial(tagfilter, filters=tag_filters, regex=tag_regex), descriptions
    )
    if long is False:
        return gmap(summarize_instance_description, descriptions)
    return tuple(descriptions)


def instances_from_ids(
    ids, resource=None, session=None, **instance_kwargs
):
    resource = init_resource("ec2", resource, session)
    instances = []
    # TODO: make this asynchronous
    for instance_id in ids:
        instance = Instance(instance_id, resource=resource, **instance_kwargs)
        instances.append(instance)
    return instances


class Instance:
    """
    Interface to an EC2 instance. Enables permitting state control,
    monitoring, and remote procedure calls.
    """

    def __init__(
        self,
        description: Union[InstanceIdentifier, InstanceDescription],
        uname: str = GENERAL_DEFAULTS["uname"],
        key: Optional[Path] = None,
        client: Optional[botocore.client.BaseClient] = None,
        resource: Optional[boto3.resources.base.ServiceResource] = None,
        session: Optional[boto3.Session] = None,
        use_private_ip: bool = False,
    ):
        """
        Args:
            description: unique identifier for the instance, either its public
                / private IP (whichever is accessible from where you're
                initializing this object), the full instance identifier, or
                an InstanceDescription as returned by ls_instances().
            uname: username for SSH access to the instance.
            key: path to keyfile. You don't usually need to explicitly specify
                it. The constructor can find it automatically given the
                following conditions:

                1. You keep the keyfile in a 'standard' location (like
                ~/.ssh/; see config.config.GENERAL_DEFAULTS['secrets_folders']
                for a list) or a directory you specify in
                config.user_config.GENERAL_DEFAULTS['secrets_folders'],
                2. its filename matches the key name given in the API
                response describing the instance.

                If those aren't both true, you'll need to pass this value to
                connect to the instance via SSH.
            client: boto client. creates default client if not given.
            resource: boto resource. creates default resource if not given.
            session: boto session. creates default session if not given.
            """
        resource = init_resource("ec2", resource, session)
        if isinstance(description, str):
            # if it's got periods in it, assume it's a public IPv4 address
            if "." in description:
                client = init_client("ec2", client, session)
                instance_id = ls_instances(description, client=client)[0]["id"]
            # otherwise assume it's the instance id
            else:
                instance_id = description
        # otherwise assume it's a full description like from
        # ls_instances / ec2.describe_instance
        elif "id" in description.keys():
            instance_id = description["id"]
        elif "InstanceId" in description.keys():
            instance_id = description["InstanceId"]
        else:
            raise ValueError("can't interpret this description.")
        instance_ = resource.Instance(instance_id)
        self.instance_id = instance_id
        self.address_type = "private" if use_private_ip is True else "public"
        if f"{self.address_type}_ip_address" in dir(instance_):
            self.ip = getattr(instance_, f"{self.address_type}_ip_address")
        self.instance_type = instance_.instance_type
        self.tags = tag_dict(instance_.tags)
        self.launch_time = instance_.launch_time
        self.state = instance_.state["Name"]
        self.request_cache = []
        if "Name" in self.tags.keys():
            self.name = self.tags["Name"]
        else:
            self.name = None
        self.zone = instance_.placement["AvailabilityZone"]
        self.uname, self.passed_key, self._ssh = uname, key, None
        self.instance_ = instance_

    @classmethod
    def launch(
        cls,
        template=None,
        options=None,
        tags=None,
        client=None,
        session=None,
        wait=True,
        connect: bool = False,
        maxtries: int = 40,
        **instance_kwargs
    ):
        """
        launch a single instance. This is a thin wrapper for
        `Cluster.launch()` with `count=0` and an optional `connect`
        argument. See that function's documentation for a description
        of arguments other than `connect` and `maxtries`

        Args:
            connect: if True, block until establishing an SSH connection
                with the instance.
            maxtries: how many times to try to connect to the instance if
                connect=True (waiting 1s between each attempt).

        Returns:
            an Instance associated with a newly-launched instance.
        """
        instance = Cluster.launch(
            1,
            template,
            options,
            tags,
            client,
            session,
            wait,
            **instance_kwargs
        )[0]
        if connect is True:
            instance._wait_on_connection(maxtries)
        return instance

    def _wait_on_connection(self, maxtries):
        print("waiting until instance is connectable...", end="")
        while not self.is_connected:
            try:
                self.connect(maxtries=maxtries)
                break
            except ConnectionError:
                continue
        print('connection established')

    # TODO: pull more of these command / connect behaviors up to SSH.

    def connect(self, maxtries: int = 5, delay: float = 1):
        """
        establish SSH connection to the instance, prepping a new connection
        if none currently exists, but not replacing an existing one.

        Args:
            maxtries: maximum times to re-attempt failed connections
            delay: how many seconds to wait after failed attempts
        """
        self._prep_connection(lazy=False, maxtries=maxtries, delay=delay)

    def reconnect(self, maxtries: int = 5, delay: float = 1):
        """
        create and attempt to establish a new SSH connection to the instance,
        closing any existing one.
        Note that this will immediately terminate any non-daemonized processes
        previously executed over the existing connection.

        Args:
            maxtries: maximum times to re-attempt failed connections
            delay: how many seconds to wait after failed attempts
        """
        if self._ssh is not None:
            self._drop_ssh()
        self._prep_connection(lazy=False, maxtries=maxtries, delay=delay)

    @property
    def is_connected(self):
        if self._ssh is None:
            return False
        if self._ssh.conn.is_connected:
            return True
        return False

    def _maybe_find_key(self):
        if self.key is not None:
            return
        if self.passed_key is None:
            try:
                found = find_ssh_key(self.instance_.key_name)
            except FileNotFoundError as fe:
                self.key_errstring = (
                    f"Couldn't find a keyfile for the instance. The keyfile "
                    f"may not be in the expected path, or its filename might "
                    f"not match the AWS key name ({self.instance_.key_name}). "
                    f"Try explicitly specifying the path by setting the .key "
                    f"attribute or passing the key= argument. {fe}"
                )
            else:
                self.key, self.key_errstring = str(found), None
            return
        if not Path(self.passed_key).exists():
            self.key_errstring = (
                f"The specified key file ({self.passed_key}) does not exist."
            )
            return
        self.key, self.key_errstring = str(self.passed_key), None

    def _prep_connection(
        self, *, lazy: bool = True, maxtries: int = 5, delay: float = 1
    ):
        """
        try to prep, and optionally establish, a SSH connection to the
        instance. if no closed / latent connection exists, create one;
        otherwise, use the existing one. if the instance isn't running,
        automatically replace any existing connection (which will be closed
        anyway by then, or should be).

        Args:
            lazy: don't establish the connection immediately; wait until some
                method needs it. other arguments do nothing if this is True.
            maxtries: maximum times to re-attempt failed connections
            delay: how many seconds to wait after subsequent attempts
        """
        if self.is_connected:  # nothing to do
            return
        self._maybe_find_key()
        if self.key_errstring is not None:
            # we want to raise this error immediately
            raise NoKeyError(self.key_errstring)
        self._update_ssh_info()
        if self._ssh is None:
            self._ssh = SSH.connect(self.ip, self.uname, self.key)
        if lazy is True:
            return
        connection_error = None
        for attempt in range(maxtries):
            try:
                self._ssh.conn.open()
                return
            except (
                AttributeError, SSHException, NoValidConnectionsError
            ) as ce:
                connection_error = ce
                time.sleep(delay)
                self.update()
        raise ConnectionError(
            f"Unable to establish SSH connection to instance. It may not yet "
            f"be ready to accept SSH connections, or something might be wrong "
            f"with configuration. Reported error: {connection_error}"
        )

    def _check_unready(self) -> Union[str, bool]:
        """
        Is the instance obviously not ready for SSH connections?

        Returns:
            "state" if it is not running or transitioning to running;
                comma-separated list of "ip"/"uname"/"key" if any are missing.
                Otherwise False (meaning it looks ready).
        """
        if self.state not in ("running", "pending"):
            return "state"
        none = [p for p in ("ip", "uname", "key") if getattr(self, p) is None]
        if len(none) > 0:
            return ", ".join(none)
        return False

    def _update_ssh_info(self):
        """
        update SSH connectability info about Instance. raise an error if
        required info is not available. automatically remove any existing
        prepped connection if instance is not running.
        """
        self.update()
        if (unready := self._check_unready()) is False:
            return
        errstring = f"Unable to execute commands on {self.instance_id}. "
        number = iter(range(1, 5))
        if "state" in unready:
            errstring += (
                f"{next(number)}. It is currently not running. Try starting "
                f"the instance with .start()."
            )
            if self._ssh is not None:
                self._drop_ssh()
        # only mention missing IP if instance is running -- we don't expect
        # a stopped instance to have an IP.
        elif "ip" in unready:
            errstring += (
                f"{next(number)}. Cannot find IP for instance. It "
                f"may be in the process of IP assignment; try waiting a "
                f"moment. It may also be configured to have no appropriate IP."
            )
        if "key" in unready:
            errstring += f"{next(number)}. {self.key_errstring}"
        raise ConnectionError(errstring)

    def _drop_ssh(self):
        """
        close an existing SSH connection and attempt to purge it from memory.
        should only be called by other methods of Instance.
        """
        self._ssh.close()
        del self._ssh
        self._ssh = None

    @connectwrap
    def command(
        self,
        command: str,
        *args,
        _viewer: bool = True,
        _wait: bool = False,
        _quiet: bool = True,
        **kwargs
    ) -> Processlike:
        """
        run a command in the instance's default interpreter.

        Args:
            command: command name or full text of command
                (see hostess.subutils.RunCommand.__call__() for detailed
                calling conventions).
            *args: args to pass to `self._ssh.__call__()`.
            _viewer: if `True`, return a `Viewer` object. otherwise return
                unwrapped result from `self._ssh.__call__()`.
            _wait: if `True`, block until command terminates (or connection
                fails). _w is an alias.
            _quiet: if `False`, print stdout and stderr, should the process
                return any before this function terminates. Generally best
                used with _wait=True.
            **kwargs: kwargs to pass to `self._ssh.__call__()`.

        Returns:
            object representing executed process.
        """
        return self._ssh(
            command,
            *args,
            _viewer=_viewer,
            _wait=_wait,
            _quiet=_quiet,
            **kwargs
        )

    @connectwrap
    def con(
        self,
        command: str,
        *args,
        _poll: float = 0.05,
        _timeout: Optional[float] = None,
        _return_viewer: bool = False,
        **kwargs
    ) -> Optional[Viewer]:
        """
        pretend you are running a command on the instance while looking at a
        terminal emulator. pauses for output and pretty-prints it to stdout.

        Does not return a process abstraction by default (pass
        _return_viewer=True if you want one). Fun in interactive environments.

        Args:
            command: command name or full text of command
                (see hostess.subutils.RunCommand.__call__() for detailed
                calling conventions).
            _poll: polling rate for process output, in seconds
            _timeout: if not None, raise a TimeoutError if this many seconds
                pass before receiving additional output from process (or
                process exit).
            _return_viewer: if True, return a Viewer for the process once it
                exits. Otherwise, return None.
            **kwargs: kwargs to pass to Instance.command().

        Returns:
            A Viewer if _return_viewer is True; otherwise None.
        """
        return self._ssh.con(
            command,
            *args,
            _poll=_poll,
            _timeout=_timeout,
            _return_viewer=_return_viewer,
            **kwargs
        )

    @connectwrap
    def commands(
        self,
        commands: Sequence[str],
        op: Literal["and", "xor", "then"] = "then",
        _con: bool = False,
        **kwargs,
    ) -> Optional[Processlike]:
        """
        Remotely run a multi-part shell command. Convenience method
        for constructing long shell instructions like
        `this && that && theother && etcetera`.

        Args:
            commands: commands to chain together.
            op: logical operator to connect commands.
            _con: run 'console-style', pretty-printing rather than
                returning output

        Returns:
            abstraction representing executed process, or None if
                _con is True.
        """
        if _con is True:
            return self.con(ks.chain(commands, op), **kwargs)
        return self.command(ks.chain(commands, op), **kwargs)

    def notebook(self, **connect_kwargs) -> NotebookConnection:
        """
        execute a Jupyter Notebook on the instance and establish a tunnel for
        local access.

        Args:
            connect_kwargs: arguments for notebook execution/connection. see
                `ssh.jupyter_connect()` for complete signature.

        Returns:
            structure containing results of tunneled Notebook execution.
        """
        self._prep_connection()
        return jupyter_connect(self._ssh,  **connect_kwargs)

    def start(
        self,
        return_response: bool = False,
        wait: bool = True,
        connect: bool = False,
        maxtries: int = 40
    ) -> Optional[dict]:
        """
        Start the instance.

        Args:
            return_response: if True, return API response.
            wait: if True, wait until instance is running.
            connect: if True, wait until the instance is connectable via SSH
                or we have tried to connect `maxtries` times.
            maxtries: max number of times to attempt connection (5s delay in
                between).

        Returns:
            API response if `return_response` is True; otherwise None.
        """
        response = self.instance_.start()
        self.update()
        if (wait is True) and (connect is False):
            print('waiting until instance is running...', end='')
            self.wait_until_running()
            print('running.')
        if connect is True:
            self.wait_until_running()
            self._wait_on_connection(maxtries)
        if return_response is True:
            return response

    def stop(self, return_response: bool = False) -> Optional[dict]:
        """
        Stop the instance.

        Args:
            return_response: if True, return API response.

        Return:
            API response if `return_response` is True; otherwise None.
        """
        response = self.instance_.stop()
        self.update()
        if return_response is True:
            return response

    def terminate(self, return_response: bool = False) -> Optional[dict]:
        """
        Terminate (aka delete) the instance. The royal road to cloud cost
        management. Please note that this action is permanent and cannot be
        undone.

        Args:
            return_response: if True, return the API response.

        Returns:
            API response if `return_response` is True; otherwise None.
        """
        response = self.instance_.terminate()
        self.update()
        if return_response is True:
            return response

    @connectwrap
    def put(
        self,
        source: Union[str, Path, IO],
        target: Union[str, Path],
        *args,
        literal_str: bool = False,
        **kwargs
    ) -> dict:
        """
        write local file or object to target file on instance.

        Args:
            source: filelike object or path to local file
            target: write path on instance
            args: additional arguments to pass to underlying put method
            literal_str: if True and `source` is a `str`, write `source`
                into `target` as text rather than interpreting `source` as a
                path
            kwargs: additional kwargs to pass to underlying put command

        Returns:
            dict giving transfer metadata: local, remote, host, and port
        """
        return self._ssh.put(
            source, target, *args, literal_str=literal_str, **kwargs
        )

    @connectwrap
    def get(
        self,
        source: Union[str, Path],
        target: Union[str, Path, IO],
        *args,
        **kwargs
    ) -> dict:
        """
        copy file from instance to local.

        Args:
            source: path to file on instance
            target: path to local file, or a filelike object (such as
                io.BytesIO)
            *args: args to pass to underlying get method
            **kwargs: kwargs to pass to underlying get method

        Returns:
            dict giving transfer metadata: local, remote, host, and port
        """
        return self._ssh.get(source, target, *args, **kwargs)

    @connectwrap
    def read(
        self,
        source,
        mode: Literal['r', 'rb'] = 'r',
        encoding='utf-8'
    ) -> Union[io.BytesIO, io.StringIO]:
        """
        read a file from the instance directly into memory.

        Args:
            source: path to file on instance
            mode: 'r' to read file as text; 'rb' to read file as bytes
            encoding: encoding for text, used only if `mode` is 'r'

        Returns:
            Buffer containing contents of remote file
        """
        return self._ssh.read(source, mode, encoding)

    @connectwrap
    def read_csv(self, source, encoding='utf-8', **csv_kwargs):
        """
        read a CSV-like file from the instance into a pandas DataFrame.

        Args:
            source: path to CSV-like file on instance
            encoding: encoding for text
            csv_kwargs: kwargs to pass to pd.read_csv

        Returns:
            DataFrame created from contents of remote CSV file
        """
        return self._ssh.read_csv(source, encoding, csv_kwargs)

    @cache
    def conda_env(self, env: str = "base") -> str:
        """
        Find the root directory of a named conda environment on the instance.

        Args:
            env: name of conda environment

        Returns:
            absolute path to root directory of conda environment.

        Raises:
            FileNotFoundError if environment cannot be found.
        """
        return find_conda_env(self._ssh, env)

    @cache
    @connectwrap
    def find_package(self, package: str, env: Optional[str] = None) -> str:
        """
        Find the location of an installed Python package on the instance using
        `pip show`.

        Args:
            package: name of package (e.g. 'requests')
            env: optional name of conda environment. If None, uses whatever
                `pip` is on the remote user's $PATH, if any.

        Returns:
            Absolute path to parent directory of package (e.g.
                "/home/ubuntu/miniforge3/lib/python3.12/site-packages")

        Raises:
            OSError if unable to execute `pip show`; FileNotFoundError if
                package doesn't appear to be installed
        """
        if env is None:
            pip = "pip"
        else:
            pip = f"{self.conda_env(env)}/bin/pip"
        try:
            result = self.command(
                f"{pip} show {package}", _wait=True
            )
            if len(result.stderr) > 0:
                raise OSError(
                    f"pip show did not run successfully: {result.stderr[0]}"
                )
            return re.search(
                r"Location:\s+(.*?)\n", ''.join(result.stdout)
            ).group(1)
        except AttributeError:
            raise FileNotFoundError("package not found")

    def compile_env(self):
        """"""
        pass

    def update(self):
        """Refresh information about the instance's state and ip address."""
        self.instance_.load()
        self.state = self.instance_.state["Name"]
        self.ip = getattr(self.instance_, f"{self.address_type}_ip_address")

    def wait_until(self, state: Literal[InstanceState], timeout: float = 65):
        """
        Pause execution until the instance reaches the specified state.
        Automatically updates `state` and `ip` attributes.

        Args:
            state: name of target instance state
            timeout: how long, in seconds, to wait before timing out
        """

        # noinspection PyUnresolvedReferences
        assert state in InstanceState.__args__
        waiting, _ = timeout_factory(timeout=timeout)
        while self.state != state:
            waiting()
            self.update()

    def wait_until_running(self, timeout: float = 65):
        """
        Alias for Instance.wait_until('running')

        Args:
            timeout: how long, in seconds, to wait until timing out
        """
        self.wait_until("running", timeout)

    def wait_until_started(self, timeout: float = 65):
        """
        Additional alias for Instance.wait_until('running')

        Args:
            timeout: how long, in seconds, to wait until timing out
        """
        self.wait_until("running", timeout)

    def wait_until_stopped(self, timeout: float = 65):
        """
        Alias for Instance.wait_until('stopped')

        Args:
            timeout: how long, in seconds, to wait before timing out
        """
        self.wait_until("stopped", timeout)

    def wait_until_terminated(self, timeout: float = 65):
        """
        Alias for Instance.wait_until('terminated')

        Args:
            timeout: how long, in seconds, to wait before timing out
        """
        self.wait_until("terminated", timeout)

    def reboot(
        self,
        wait: bool = True,
        hard: bool = False,
        timeout: float = 65
    ):
        """
        Reboot or hard-restart the instance. Note that a hard
        restart will change the instance's ip unless it has been assigned a
        static ip. The Instance object will automatically handle this, but
        other code/processes using it will need to be informed.

        Args:
            wait: if True, block until instance state reaches 'running' again.
            hard: if True, perform a 'hard' restart: fully shut the instance
                down and start it up again. if False, perform a 'soft' restart.
                Note that AWS will automatically switch to a 'hard' restart if
                its attempt at a soft restart fails.
            timeout: seconds to wait for state transitions before timing out.
        """
        if hard is True:
            self.stop()
            self.wait_until_stopped(timeout)
            self.start()
        else:
            self.instance_.reboot()
        if wait is True:
            self.wait_until_running(timeout)

    def restart(
        self, wait: bool = True, hard: bool = False, timeout: float = 65
    ):
        """alias for Instance.reboot()."""
        self.reboot(wait, hard, timeout)

    @connectwrap
    def tunnel(
        self,
        local_port: int,
        remote_port: int
    ) -> tuple[Process, dict[str, Union[int, str, Path]]]:
        """
        create an SSH tunnel between a local port and a remote port.

        Args:
            local_port: port number for local end of tunnel.
            remote_port: port number for remote end of tunnel.

        Returns:
            tunnel_process: `Process` abstraction for the tunnel process
            tunnel_metadata: dict of metadata about the tunnel
        """
        self._ssh.tunnel(local_port, remote_port)
        return self._ssh.tunnels[-1]

    @connectwrap
    def call_python(
        self,
        module: str,
        func: Optional[str] = None,
        payload: Any = None,
        *,
        compression: CallerCompressionType = None,
        serialization: CallerSerializationType = None,
        interpreter: Optional[str] = None,
        env: Optional[str] = None,
        splat: CallerUnpackingOperator = "",
        payload_encoded: bool = False,
        print_result: bool = True,
        filter_kwargs: bool = True,
        **command_kwargs,
    ) -> Viewer:
        """
        call a Python function on the instance. See
        `hostess.caller.generic_python_endpoint()` for more verbose
        documentation and technical discussion.

        Args:
            module: name of, or path to, the target module
            func: name of the function to call. must be a member of the target
                module (or explicitly imported by that module).
            payload: object from which to construct func's call arguments.
                must specify appropriate `serialization` if it cannot be
                reconstructed from its string representation.
            interpreter: path to Python interpreter that should be specified in
                the shell command.
            env: optional name of conda environment. both `interpreter` and
                `env` cannot be specified. If neither are specified, simply
                uses the first `python` binary on the remote user's $PATH,
                if any.
            compression: compression for payload. 'gzip' or None.
            serialization: how to serialize `payload`. 'json' means serialize
                to JSON; 'pickle' means serialize using pickle; None means just
                use the string representation of `payload`.
            splat: Operator for splatting the payload into the function call.
                `"*"` means `func(*payload)`, `"**"` means `func(**payload)`;
                None means `func(payload)`.
            payload_encoded: set to True if you have already
                compressed/serialized `payload` with the specified methods.
            print_result: if True, the function call will print its result
                to stdout, so it will be available in the `.out` attribute of
                the returned Viewer.
            filter_kwargs: Attempt to filter `func`-inappropriate kwargs from
                `payload`? Does nothing if `splat != "**"`.
            **command_kwargs: additional kwargs to pass to `self.command()`.
                Note that `_viewer=False` is invalid; this function always
                returns a `Viewer`.

        Returns:
            `Viewer` wrapping executed Python process.
        """
        if (interpreter is not None) and (env is not None):
            raise TypeError(
                "Please pass either the name of a conda environment or the "
                "path to a Python interpreter (one or the other, not both)."
            )
        if env is not None:
            interpreter = f"{self.conda_env(env)}/bin/python"
        if interpreter is None:
            interpreter = "python"
        python_command_string = generic_python_endpoint(
            module,
            func,
            payload,
            compression=compression,
            serialization=serialization,
            splat=splat,
            payload_encoded=payload_encoded,
            print_result=print_result,
            filter_kwargs=filter_kwargs,
            interpreter=interpreter,
            for_bash=True
        )
        return self.command(
            python_command_string, _viewer=True, **command_kwargs
        )

    # TODO: It is permitted to have more than one security group apply to
    #  an instance. This method will currently modify _all_ security
    #  groups attached to a particular instance. We only really ever use
    #  one security group per instance... but this should be fixed
    #  somehow.
    def rebase_ssh_ingress_ip(
        self,
        ip: Optional[str] = None,
        force: bool = False,
        revoke: bool = True
    ):
        """
        Modify this instance's security group(s) to permit SSH access from
        an IP (by default, the caller's external IP). By default, revoke all
        other access rules.

        Args:
            ip: permit SSH access from this IP. if None, use the caller's
                external IP.
            force: if True, will force modification even of default security
                groups.
            revoke: if True, will revoke all other inbound permissions.
        """

        for sg_index in self.instance_.security_groups:
            sg = init_resource("ec2").SecurityGroup(sg_index["GroupId"])
            if revoke is True:
                revoke_ssh_ingress_from_all_ips(
                    sg, force_modification_of_default_sg=force
                )
            authorize_ssh_ingress_from_ip(
                sg, ip=ip, force_modification_of_default_sg=force
            )

    def __repr__(self):
        string = f"{self.instance_type} in {self.zone} "
        if self.ip is None:
            string += "(no ip)"
        else:
            string += f"at {self.ip}"
        if self.name is None:
            return f"{self.instance_id}: {string}"
        return f"{self.name} ({self.instance_id}): {string}"

    def __str__(self):
        return self.__repr__()

    key = None
    """path to SSH keyfile"""
    key_errstring = None
    """
    detailed description of what's wrong with our attempt to find an SSH 
    keyfile, if anything
    """


class Cluster:
    """Class offering an interface to multiple EC2 instances at once."""
    def __init__(self, instances: Collection[Instance]):
        """
        Args:
            instances: Instance objects to incorporate into this Cluster.
        """
        self.instances = tuple(instances)
        self.fleet_request = None

    def _async_method_call(
        self, method_name: str, *args, **kwargs
    ) -> list[Any]:
        """
        Internal wrapper function: make multithreaded calls to a specified
        method of all this Cluster's Isntances.

        Args:
             method_name: named method of Instance to call on all our Instances
             *args: args to pass to these method calls
             **kwargs: kwargs to pass to these method calls

        Returns:
            list containing result of method call from each Instance
        """
        exc = ThreadPoolExecutor(len(self.instances))
        futures = []
        for instance in self.instances:
            futures.append(
                exc.submit(getattr(instance, method_name), *args, **kwargs)
            )
        while not all(f.done() for f in futures):
            time.sleep(0.01)
        return [f.result() for f in futures]

    def command(self, command: str, *args, **kwargs) -> list[Processlike, ...]:
        """
        Call a shell command on all this Cluster's Instances. See
        `Instance.command()` for further documentation.

        Args:
            command: command name/string
            *args: args to pass to `Instance.command()`
            **kwargs: kwargs to pass to `Instance.command()`

        Returns:
            list containing results of `command()` from each Instance.
        """
        return self._async_method_call("command", command, *args, **kwargs)

    def commands(
        self, commands: Sequence[str], *args, **kwargs
    ) -> list[Processlike, ...]:
        """
        Call a sequence of shell commands on all this Cluster's Instances. See
        `Instance.commands()` for further documentation.

        Args:
            commands: command names/strings
            *args: args to pass to `Instance.commands()`
            **kwargs: kwargs to pass to `Instance.commands()`

        Returns:
            list containing results of `commands()` from each Instance.
        """
        return self._async_method_call("commands", commands, *args, **kwargs)

    def call_python(self, *args, **kwargs) -> list[Processlike, ...]:
        """
        Call a Python function on all this Cluster's Instances. See
        `Instance.call_python()` for further documentation.

        Args:
            *args: args to pass to `Instance.call_python()`
            **kwargs: kwargs to pass to `Instance.call_python()`

        Returns:
            list containing results of `call_python()` from each Instance.
        """
        return self._async_method_call("call_python", *args, **kwargs)

    def start(self, *args, **kwargs):
        """
        Start all Instances. See `Instance.start()` for further documentation.

        Args:
            *args: args to pass to `Instance.start()`
            **kwargs: kwargs to pass to `Instance.start()`

        Returns:
            list containing results of `start()` from each Instance.
        """
        return self._async_method_call("start", *args, **kwargs)

    def stop(self, *args, **kwargs):
        """
        Stop all Instances. See `Instance.stop()` for further documentation.

        Args:
            *args: args to pass to `Instance.stop()`
            **kwargs: kwargs to pass to `Instance.stop()`

        Returns:
            list containing results of `stop()` from each Instance.
        """
        return self._async_method_call("stop", *args, **kwargs)

    def terminate(self, *args, **kwargs):
        """
        Terminate all Instances. See `Instance.terminate()` for further
        documentation.

        Args:
            *args: args to pass to `Instance.terminate()`
            **kwargs: kwargs to pass to `Instance.terminate()`

        Returns:
            list containing results of `terminate()` from each Instance.
        """
        return self._async_method_call("terminate", *args, **kwargs)

    # TODO: update this

    # def gather_files(
    #     self,
    #     source_path,
    #     target_path: str = ".",
    #     process_states: Optional[
    #         MutableMapping[str, Literal["running", "done"]]
    #     ] = None,
    #     callback: Callable = zero,
    #     delay=0.02,
    # ):
    #     status = {instance.instance_id: "ready" for instance in self.instances}
    #
    #     def finish_factory(instance_id):
    #         def finisher(*args, **kwargs):
    #             status[instance_id] = transition_state
    #             callback(*args, **kwargs)
    #
    #         return finisher
    #
    #     if process_states is None:
    #         return {
    #             instance.instance_id: instance.get(
    #                 f"{source_path}/*", target_path, _bg=True, _viewer=True
    #             )
    #             for instance in self.instances
    #         }
    #     commands = defaultdict(list)
    #
    #     while any(s != "complete" for s in status.values()):
    #         for instance in self.instances:
    #             if status[instance.instance_id] in ("complete", "fetching"):
    #                 continue
    #             if process_states[instance.instance_id] == "done":
    #                 transition_state = "complete"
    #             else:
    #                 transition_state = "ready"
    #             status[instance.instance_id] = "fetching"
    #             command = instance.get(
    #                 f"{source_path}/*",
    #                 target_path,
    #                 _bg=True,
    #                 _bg_exc=False,
    #                 _viewer=True,
    #                 _done=finish_factory(instance.instance_id),
    #             )
    #             commands[instance.instance_id].append(command)
    #         time.sleep(delay)
    #     return commands

    @classmethod
    def from_descriptions(cls, descriptions: Collection[InstanceDescription], *args, **kwargs):
        """
        Construct a Cluster from InstanceDescriptions, as produced by
        `ls_instances()`.

        Args:
            descriptions: InstanceDescriptions to initialize Instances from
                and subsequently collect into a Cluster.
            *args: args to pass to the Instance constructor
            **kwargs: kwargs to pass to the Instance constructor
        """
        instances = [Instance(d, *args, **kwargs) for d in descriptions]
        return cls(instances)

    @classmethod
    def launch(
        cls,
        count: int,
        template: Optional[str] = None,
        options: Optional[dict] = None,
        tags: Optional[dict] = None,
        client: Optional[botocore.client.BaseClient] = None,
        session: Optional[boto3.session] = None,
        wait: bool = True,
        **instance_kwargs,
    ):
        """
        Launch a fleet of Instances and collect them into a Cluster. See
        hostess documentation Notebooks for examples of how to construct
        options, tags, etc.

        Args:
            count: number of instances to launch
            template: name of preexisting EC2 launch template to construct
                instances from. if not specified, will use a 'scratch'
                template.
            options: optional dict of specifications for Instances. if not
                specified, will use default values for everything. Currently,
                if a template is specified, it will override all values in
                options. This will change in the future.
            tags: optional tags to apply to the launched instances and their
                associated EBS volumes (if any).
            client: optional preexisting EC2 client.
            session: optional preexisting boto session.
            wait: if True, block after launch until all instances are running.
            **instance_kwargs: kwargs to pass to the Instance constructor.

        Returns:
            a Cluster created from the newly-launched fleet.
        """
        client = init_client("ec2", client, session)
        options = {} if options is None else options
        # TODO: add a few more conveniences, clean up
        if instance_kwargs.get('type_') is not None:
            options['instance_type'] = instance_kwargs.pop('type_')
        if instance_kwargs.get('name') is not None:
            options['instance_name'] = instance_kwargs.pop('name')
        if template is None:
            using_scratch_template = True
            template = create_launch_template(**options)["LaunchTemplateName"]
            if options.get('image_id') is None:
                # we're always using a stock Canonical image in this case, so
                # note that we're forcing uname to 'ubuntu':
                print(
                    "Using stock Canonical image, so setting uname to "
                    "'ubuntu'."
                )
                instance_kwargs['uname'] = 'ubuntu'
        else:
            using_scratch_template = False
        if tags is not None:
            tagrecs = [{"Key": k, "Value": v} for k, v in tags.items()]
            tag_kwarg = {
                'TagSpecifications': [
                    {"ResourceType": "instance", "Tags": tagrecs},
                    {"ResourceType": "volume", "Tags": tagrecs},
                ]
            }
        else:
            tag_kwarg = {}
        try:
            fleet = client.create_fleet(
                LaunchTemplateConfigs=[
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateName": template,
                            "Version": "$Default",
                        }
                    }
                ],
                TargetCapacitySpecification={
                    "TotalTargetCapacity": count,
                    "OnDemandTargetCapacity": count,
                    "DefaultTargetCapacityType": "on-demand",
                },
                Type="instant",
                **tag_kwarg
            )
        finally:
            if using_scratch_template is True:
                client.delete_launch_template(LaunchTemplateName=template)
        # note that we do not want to raise these all the time, because the
        # API frequently dumps a lot of harmless info in here.
        launch_errors = len(fleet.get('Errors', []))
        try:
            n_instances = len(fleet['Instances'][0]['InstanceIds'])
            assert n_instances > 0
        except (KeyError, IndexError, AssertionError):
            raise ValueError(
                f"No instances appear to have launched. "
                f"Client returned error(s):\n\n{launch_errors}"
            )
        if n_instances != count:
            print(
                f"warning: fewer instances appear to have launched than "
                f"requested ({n_instances} vs. {count}). Check the 'Errors' "
                f"key of this Cluster's 'fleet_request' attribute."
            )

        def instance_hook():
            return instances_from_ids(
                fleet["Instances"][0]["InstanceIds"],
                client=client,
                **instance_kwargs,
            )

        instances = []
        for _ in range(5):
            try:
                instances = instance_hook()
                break
            except botocore.exceptions.ClientError as ce:
                if "does not exist" in str(ce):
                    time.sleep(0.2)
            raise TimeoutError(
                "launched instances successfully, but unable to run "
                "DescribeInstances. Perhaps permissions are wrong. "
                "Reported instance ids:\n"
                + "\n".join(fleet["Instances"][0]["InstanceIds"])
            )
        cluster = Cluster(instances)
        cluster.fleet_request = fleet
        noun = "fleet" if count > 1 else "instance"
        if wait is False:
            print(f"launched {noun}; _wait=False passed, not checking status")
            return cluster
        print(f"launched {noun}; waiting until running")
        for instance in cluster.instances:
            instance.wait_until_running()
            print(f"{instance} is running")
        return cluster

    def rebase_ssh_ingress_ip(self, ip=None, force=False, revoke=False):
        """Revoke all SSH ingress rules and add one for the current IP.
        To issue commands to instances, you must have SSH ingress access
        and it is good security practice to not white list the entire world."""
        return self._async_method_call(
            "rebase_ssh_ingress_ip", ip=ip, force=force, revoke=revoke
        )

    def __getitem__(self, item):
        return self.instances[item]

    def __repr__(self):
        return "\n".join([inst.__repr__() for inst in self.instances])

    def __str__(self):
        return "\n".join([inst.__str__() for inst in self.instances])


def get_canonical_images(
    architecture: Literal["x86_64", "arm64", "i386"] = "x86_64",
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.session] = None,
) -> list[dict]:
    """
    fetch the subset of official (we refuse to make the obvious pun) Ubuntu
    Amazon Machine Images from Canonical that we might plausibly want to offer
    as defaults to users. This will generally return hundreds of images and
    take > 1.5 seconds because of the number of unsupported daily builds
    available, so we cache the results with a one-week shelf life.

    Args:
        architecture: get images for this system architecture
        client: optional preexisting boto client
        session: optional preexisting boto session

    Returns:
        list of metadata dictionaries for all matching Canonical AMIs
    """
    client = init_client("ec2", client, session)
    # this perhaps excessive-looking optimization is intended to reduce not
    # only call time but also the chance that we will pick a 'bad' image --
    # Canonical generally drops LTS images at a quicker cadence than 7 months.
    month_globs = [
        f"{(dt.datetime.now() - dt.timedelta(days=30 * n)).isoformat()[:7]}*"
        for n in range(7)
    ]
    return client.describe_images(
        Filters=[
            {"Name": "creation-date", "Values": list(set(month_globs))},
            {"Name": "block-device-mapping.volume-size", "Values": ["8"]},
            {"Name": "architecture", "Values": [architecture]},
        ],
        Owners=["099720109477"],
    )["Images"]


def get_stock_ubuntu_image(
    architecture: Literal["x86_64", "arm64", "i386"] = "x86_64",
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.session] = None,
) -> str:
    """
    retrieve image ID of the most recent officially-supported
    Canonical Ubuntu Server LTS AMI for the provided architecture.

    Args:
        architecture: get images for this system architecture
        client: optional preexisting boto client
        session: optional preexisting boto session

    Returns:
        AWS image ID for most recent matching Ubuntu Server LTS AMI.
    """
    available_images = get_canonical_images(architecture, client, session)
    supported_lts_images = [
        i
        for i in available_images
        if (
            ("UNSUPPORTED" not in i["Description"])
            and ("LTS" in i["Description"])
            and ("FIPS" not in i["Description"])
        )
    ]
    dates = {
        ix: date
        for ix, date in enumerate(
            map(dtp.parse, map(get("CreationDate"), supported_lts_images))
        )
    }
    release_date = max(dates.values())
    idxmax = [ix for ix, date in dates.items() if date == release_date][0]
    return supported_lts_images[idxmax]["ImageId"]


# noinspection PyTypedDict
def summarize_instance_type_structure(structure: dict) -> dict:
    """
    summarize an individual instance type description from a wrapped
    description call
    """
    proc = structure["ProcessorInfo"]
    attributes = {
        "instance_type": structure["InstanceType"],
        "architecture": proc["SupportedArchitectures"][0],
        "cpus": structure["VCpuInfo"]["DefaultVCpus"],
        "cpu_speed": proc.get("SustainedClockSpeedInGhz"),
        "ram": structure["MemoryInfo"]["SizeInMiB"] / 1024,\
        "bw": structure["NetworkInfo"]["NetworkPerformance"],
    }
    if "EbsOptimizedInfo" in structure["EbsInfo"].keys():
        ebs = structure["EbsInfo"]["EbsOptimizedInfo"]
        attributes["ebs_bw_min"] = ebs["BaselineThroughputInMBps"]
        attributes["ebs_bw_max"] = ebs["MaximumThroughputInMBps"]
        attributes["ebs_iops_min"] = ebs["BaselineIops"]
        attributes["ebs_iops_max"] = ebs["MaximumIops"]
    if structure["InstanceStorageSupported"] is True:
        attributes["disks"] = structure["InstanceStorageInfo"]["Disks"]
        attributes["local_storage"] = structure["InstanceStorageInfo"][
            "TotalSizeInGB"
        ]
    else:
        attributes["disks"] = []
        attributes["local_storage"] = 0
    attributes["cpu_surcharge"] = structure["BurstablePerformanceSupported"]
    return attributes


def summarize_instance_type_response(response) -> tuple[dict]:
    """
    summarize a series of instance type descriptions as returned from a
    boto3-wrapped
    DescribeInstanceTypes or DescribeInstanceTypeOfferings call
    """
    types = response["InstanceTypes"]
    return gmap(summarize_instance_type_structure, types)


# TODO: maybe we need to do something fancier to support passing a session
#  or region/credentials around to support the pricing features here

# TODO: fail only partially if denied permissions for Price List
def describe_instance_type(
    instance_type: str,
    pricing: bool = True,
    ec2_client=None,
    pricing_client=None,
    session=None,
) -> dict:
    """
    note that this function will report i386 architecture for the
    very limited number of instance types that support both i386
    and x86_64.
    """
    ec2_client = init_client("ec2", ec2_client, session)
    response = ec2_client.describe_instance_types(
        InstanceTypes=[instance_type]
    )
    summary = summarize_instance_type_response(response)[0]
    if pricing is False:
        return summary
    pricing_client = init_client("pricing", pricing_client, session)
    summary["on_demand_price"] = get_on_demand_price(
        instance_type, None, pricing_client
    )
    if instance_type.startswith("t"):
        summary["cpu_credit_price"] = get_cpu_credit_price(
            instance_type, None, pricing_client
        )
    return summary


def get_all_instance_types(client=None, session=None, reset_cache=False):
    client = init_client("ec2", client, session)
    region = clarify_region(None, client)
    cache_path = Path(GENERAL_DEFAULTS["cache_path"])
    prefix = f"instance_types_{region}"
    if reset_cache is False:
        cached_results = _check_cached_results(cache_path, prefix, max_age=7)
        if cached_results is not None:
            return pickle.load(cached_results.open("rb"))
    results = autopage(client, "describe_instance_types")
    _clear_cached_results(cache_path, prefix)
    with Path(cache_path, f"{prefix }_{filestamp()}.pkl").open("wb") as stream:
        pickle.dump(results, stream)
    return results


# TODO: allow cleaner region specification
# TODO: even with caching this still takes like 100ms, probably
#  I'm assembling the df too much inside this function; probably this doesn't
#  actually matter
def instance_catalog(family=None, client=None, session=None):
    types = get_all_instance_types(client, session)
    summaries = gmap(summarize_instance_type_structure, types)
    if family is not None:
        summaries = [
            s for s in summaries if s["instance_type"].split(".")[0] == family
        ]
    import pandas as pd

    summary_df = pd.DataFrame(summaries)
    pricing = get_ec2_basic_price_list(session=session)["ondemand"]
    pricing_df = pd.DataFrame(pricing)
    return summary_df.merge(pricing_df, on="instance_type", how="left")


def _interpret_ebs_args(
    volume_type, volume_size, iops, throughput, volume_list
):
    if ((volume_type is not None) or (volume_size is not None)) and (
        volume_list is not None
    ):
        raise ValueError(
            "Please pass either a list of volumes (volume_list) or "
            "volume_type and size, not both."
        )
    if volume_list is None:
        if (volume_type is None) or (volume_size is None):
            raise ValueError(
                "If a list of volumes (volume_list) is not specified, "
                "volume_type and volume_size cannot be None."
            )
        return [
            _ebs_device_mapping(volume_type, volume_size, 0, iops, throughput)
        ]
    return [
        _ebs_device_mapping(index=index, **specification)
        for index, specification in enumerate(volume_list)
    ]


EBS_VOLUME_TYPES = ("gp2", "gp3", "io1", "io2", "st1", "sc1")


def _ebs_device_mapping(
    volume_type,
    volume_size,
    index=0,
    iops=None,
    throughput=None,
    device_name=None,
):
    if volume_type not in EBS_VOLUME_TYPES:
        raise ValueError(f"{volume_type} is not a recognized EBS volume type.")
    if volume_type.startswith("io"):
        raise NotImplementedError(
            "Handling for io1 and io2 volumes is not yet supported."
        )
    if volume_type not in ("io1", "io2", "gp3") and (
        (iops is not None) or (throughput is not None)
    ):
        raise ValueError(
            f"{volume_type} does not support specifying explicit values for "
            f"throughput or IOPS."
        )
    if device_name is None:
        device_name = f"/dev/sd{ascii_lowercase[index]}"
    if device_name == "/dev/sda":
        device_name = "/dev/sda1"
    mapping = {
        "DeviceName": device_name,
        "Ebs": {"VolumeType": volume_type, "VolumeSize": volume_size},
    }
    if iops is not None:
        mapping["Ebs"]["Iops"] = iops
    if throughput is not None:
        mapping["Ebs"]["Throughput"] = throughput
    return mapping


def create_security_group(
    name=None, description=None, client=None, resource=None, session=None
):
    client = init_client("ec2", client, session)
    try:
        default_vpc_id = client.describe_vpcs(
            Filters=[{"Name": "is-default", "Values": ["true"]}]
        )["Vpcs"][0]["VpcId"]
    except IndexError:
        raise EnvironmentError(
            "Could not find a default VPC for automated security group "
            "creation."
        )
    resource = init_resource("ec2", resource, session)
    if name is None:
        name = hostess_placeholder()
    if description is None:
        description = "hostess-generated security group"
    sg = resource.create_security_group(
        Description=description,
        GroupName=name,
        VpcId=default_vpc_id,
        TagSpecifications=[
            {
                "ResourceType": "security-group",
                "Tags": [{"Key": "hostess-generated", "Value": "True"}],
            },
        ],
    )
    my_ip = my_external_ip()
    sg.authorize_ingress(
        IpPermissions=[
            {
                "FromPort": 22,
                "ToPort": 22,
                "IpProtocol": "tcp",
                "IpRanges": [
                    {
                        "CidrIp": f"{my_ip}/32",
                        "Description": "SSH access from creating IP",
                    }
                ],
            },
        ]
    )
    sg.authorize_ingress(SourceSecurityGroupName=sg.group_name)
    return sg


def hostess_placeholder():
    return f"hostess-{''.join(choices(ascii_lowercase, k=10))}"


def create_ec2_key(key_name=None, save_key=True, resource=None, session=None):
    if key_name is None:
        key_name = hostess_placeholder()
    resource = init_resource("ec2", resource, session)
    key = resource.create_key_pair(KeyName=key_name)
    keydir = Path(os.path.expanduser("~/.ssh"))
    keydir.mkdir(exist_ok=True)
    if save_key is True:
        keyfile = Path(keydir, f"{key_name}.pem")
        with keyfile.open("w") as stream:
            stream.write(key.key_material)
        keyfile.chmod(0o700)
    return key


def create_launch_template(
    template_name=None,
    instance_type=EC2_DEFAULTS['instance_type'],
    volume_type=None,
    volume_size=None,
    image_id=None,
    iops=None,
    throughput=None,
    volume_list=None,
    instance_name=None,
    security_group_name=None,
    tags=None,
    key_name=None,
    client=None,
    session=None,
):
    default_name = hostess_placeholder()
    if volume_list is None:
        volume_type = (
            EC2_DEFAULTS["volume_type"] if volume_type is None else volume_type
        )
        volume_size = (
            EC2_DEFAULTS["volume_size"] if volume_size is None else volume_size
        )
    client = init_client("ec2", client, session)
    if (image_id is not None) and not image_id.startswith("ami-"):
        try:
            image_id = client.describe_images(
                Filters=[{"Name": "name", "Values": [image_id]}]
            )['Images'][0]["ImageId"]
        except KeyError:
            raise ValueError(
                f"Can't find an image corresponding to the name {image_id}."
            )
    if image_id is None:
        description = describe_instance_type(
            instance_type, pricing=False, ec2_client=client
        )
        image_id = get_stock_ubuntu_image(description["architecture"], client)
        print(
            f"No AMI specified, using most recent Ubuntu Server LTS "
            f"image from Canonical ({image_id})."
        )
    block_device_mappings = _interpret_ebs_args(
        volume_type, volume_size, iops, throughput, volume_list
    )
    if tags is None:
        tags = []
    elif isinstance(tags, Mapping):
        tags = [{"Key": k, "Value": v} for k, v in tags.items()]
    tags.append({"Key": "hostess-generated", "Value": "True"})
    resource_tags = tags.copy()
    if instance_name is not None:
        resource_tags.append({"Key": "Name", "Value": instance_name})
    if security_group_name is not None:
        sg_response = client.describe_security_groups(
            GroupNames=[security_group_name]
        )["SecurityGroups"]
        if len(sg_response) == 0:
            create_security_group(security_group_name, None, client)
            print(
                f"No security group named {security_group_name} exists; "
                f"created one."
            )
    else:
        security_group_name = create_security_group(
            default_name, client=client
        ).group_name
        print(
            f"No security group specified; created a new one named "
            f"{default_name}."
        )
    if key_name is not None:
        key_response = client.describe_key_pairs(KeyNames=[key_name])[
            "KeyPairs"
        ]
        if len(key_response) == 0:
            create_ec2_key(key_name)
            print(f"No key pair named {key_name} exists; created one.")
    else:
        key_name = create_ec2_key(default_name).key_name
        print(
            f"No key pair specified; created one named {default_name} "
            "and saved key material to disk in ~/.ssh."
        )
    launch_template_data = {
        "BlockDeviceMappings": block_device_mappings,
        "ImageId": image_id,
        "TagSpecifications": [
            {"ResourceType": "instance", "Tags": resource_tags},
            {"ResourceType": "volume", "Tags": resource_tags},
        ],
        "SecurityGroups": [security_group_name],
        "InstanceType": instance_type,
        "KeyName": key_name,
    }
    if template_name is None:
        template_name = default_name
    return client.create_launch_template(
        LaunchTemplateName=template_name,
        LaunchTemplateData=launch_template_data,
        TagSpecifications=[{"ResourceType": "launch-template", "Tags": tags}],
    )["LaunchTemplate"]


def revoke_ssh_ingress_from_all_ips(
    sg,
    force_modification_of_default_sg=False,
    ports=(22,),
    protocols=('tcp',)
):
    print(f'Revoking SSH ingress from all IPs for security group {sg.id}')
    if 'default' in sg.id and not force_modification_of_default_sg:
        print(
            '\tRefusing to modify permissions of a default security group. '
            'Pass flag to override.'
        )
        return
    for rule in sg.ip_permissions:
        # do not modify ingress rules based on security group (and not just IP)
        if not (rule['FromPort'] in ports and
                rule['IpProtocol'] in protocols and
                any(sg.ip_permissions[1]['UserIdGroupPairs'])):
            continue
        sg.revoke_ingress(IpPermissions=[rule])


def authorize_ssh_ingress_from_ip(
    sg, ip=None, force_modification_of_default_sg=False,
):
    """
    Args:
        sg: ec2.SecurityGroup object to apply authorization to
        ip: ip to authorize
        force_modification_of_default_sg: apply modification even to a default
            security group?
    """
    if ip is None:
        # automatically select the user's ip
        ip = my_external_ip()
    print(f"Authorizing SSH ingress from {ip} for security group {sg.id}")
    if 'default' in sg.id and not force_modification_of_default_sg:
        print(
            '\tRefusing to modify permissions of a default security group. '
            'Pass flag to override.'
        )
        return
    try:
        sg.authorize_ingress(
            IpPermissions=[
                {
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpProtocol": "tcp",
                    "IpRanges": [
                        {
                            "CidrIp": f"{ip}/32",
                            "Description": "SSH access from specified IP",
                        }
                    ],
                },
            ],
        )
    except botocore.client.ClientError as ce:
        if 'InvalidPermission.Duplicate' in str(ce):
            print(f"** {ip} already authorized for SSH ingress for {sg.id} **")
        else:
            raise
