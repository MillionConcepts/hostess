import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from functools import cache, partial, wraps
import io
from itertools import chain, cycle
import os
from pathlib import Path
import pickle
from random import choices
import re
from string import ascii_lowercase
import time
from types import NoneType
from typing import (
    Any,
    Callable,
    Collection,
    IO,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Union,
)
import warnings

from cytoolz.curried import get
import boto3.resources.base
import botocore.client
import botocore.exceptions
import dateutil.parser as dtp
from dustgoggles.func import gmap
from dustgoggles.structures import listify
import pandas as pd
from paramiko.ssh_exception import NoValidConnectionsError, SSHException

from hostess.aws.pricing import (
    get_cpu_credit_price,
    get_ec2_basic_price_list,
    get_on_demand_price,
    HOURS_PER_MONTH,
)
from hostess.aws.utilities import (
    _check_cached_results,
    _clear_cached_results,
    autopage,
    clarify_region,
    init_client,
    init_resource,
    tag_dict,
    tagfilter,
    make_boto_session,
)
from hostess.caller import (
    CallerCompressionType,
    CallerSerializationType,
    CallerUnpackingOperator,
    generic_python_endpoint,
)
from hostess.config import CONDA_DEFAULTS, EC2_DEFAULTS, GENERAL_DEFAULTS
import hostess.shortcuts as hs
from hostess.serverpool import ServerPool
from hostess.ssh import (
    find_conda_env,
    find_ssh_key,
    jupyter_connect,
    NotebookConnection,
    SSH,
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


def connectwrap(
    func: Callable[["Instance", ...], Any]
) -> Callable[["Instance", ...], Any]:
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
    description: Mapping,
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
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    long: bool = False,
    tag_regex: bool = True,
    **tag_filters: str,
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
            field names. Otherwise, return a flattened version of the full API
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


def _instances_from_ids(
    ids,
    resource: Optional[boto3.resources.base.ServiceResource] = None,
    session: Optional[boto3.Session] = None,
    **instance_kwargs: Union[
        str,
        botocore.client.BaseClient,
        boto3.resources.base.ServiceResource,
        boto3.Session,
        Path,
        bool,
    ],
):
    """helper function for cluster launch."""
    resource = init_resource("ec2", resource, session)
    instances = []
    # TODO: make this asynchronous
    for instance_id in ids:
        instance = Instance(instance_id, resource=resource, **instance_kwargs)
        instances.append(instance)
    return instances


class Instance:
    """
    Interface to an EC2 instance. Enables remote procedure calls, state
    control, and monitoring.
    """

    def __init__(
        self,
        description: Union[InstanceIdentifier, InstanceDescription],
        *,
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
        self.session = session if session is not None else make_boto_session()
        self.resource = init_resource("ec2", resource, self.session)
        self.client = init_client("ec2", client, self.session)

        if isinstance(description, str):
            # if it's got periods in it, assume it's a public IPv4 address
            if "." in description:
                instance_id = ls_instances(description, client=self.client)[0][
                    "id"
                ]
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
        instance_ = self.resource.Instance(instance_id)
        self.instance_id = instance_id
        self.address_type = "private" if use_private_ip is True else "public"
        if f"{self.address_type}_ip_address" in dir(instance_):
            self.ip = getattr(instance_, f"{self.address_type}_ip_address")
        self.instance_type = instance_.instance_type
        self.tags = tag_dict(instance_.tags)
        self.launch_time = instance_.launch_time
        self.state = instance_.state["Name"]
        self.request_cache = []
        self.name = self.tags.get("Name")
        self.zone = instance_.placement["AvailabilityZone"]
        self.uname, self.passed_key, self._ssh = uname, key, None
        self.instance_ = instance_

    def rename(self, name: str):
        """
        Rename the instance. Does not rename volumes or network interfaces.
        Updates local instance state cache when called.

        Args:
            name: new name for instance.
        """
        self.client.create_tags(
            Resources=[self.instance_id], Tags=[{"Key": "Name", "Value": name}]
        )
        self.update()

    @classmethod
    def launch(
        cls,
        template=None,
        options=None,
        tags=None,
        client=None,
        session=None,
        wait=True,
        connect=False,
        maxtries: int = 40,
        **instance_kwargs: Union[
            str,
            botocore.client.BaseClient,
            boto3.resources.base.ServiceResource,
            boto3.Session,
            Path,
            bool,
        ],
    ) -> "Instance":
        """
        launch a single instance. This is a thin wrapper for
        `Cluster.launch()` with `count=1`. See that function for full
        documentation.

        Returns:
            an Instance associated with a newly-launched EC2 instance.
        """
        return Cluster.launch(
            1,
            template,
            options,
            tags,
            client,
            session,
            wait,
            connect,
            maxtries,
            **instance_kwargs,
        )[0]

    def wait_on_connection(self, maxtries: int):
        """block until an SSH connection to the instance is established"""
        while not self.is_connected:
            try:
                self.connect(maxtries=maxtries)
                break
            except ConnectionError:
                continue
        print("connection established")

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
                AttributeError,
                SSHException,
                NoValidConnectionsError,
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
        *args: Union[int, str, float],
        _viewer: bool = True,
        _wait: bool = False,
        _quiet: bool = True,
        **kwargs: Union[int, str, float, bool],
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
            **kwargs,
        )

    @connectwrap
    def con(
        self,
        command: str,
        *args: Union[int, str, float],
        _poll: float = 0.05,
        _timeout: Optional[float] = None,
        _return_viewer: bool = False,
        **kwargs: Union[int, str, float, bool],
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
            **kwargs,
        )

    # noinspection PyTypeChecker
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
            return self.con(hs.chain(commands, op), **kwargs)
        return self.command(hs.chain(commands, op), **kwargs)

    @connectwrap
    def notebook(
        self, **connect_kwargs: Union[int, str, bool]
    ) -> NotebookConnection:
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
        return jupyter_connect(self._ssh, **connect_kwargs)

    @connectwrap
    def install_conda(
        self,
        installer_url=CONDA_DEFAULTS['installer_url'],
        prefix=CONDA_DEFAULTS['prefix'],
        **kwargs: bool
    ) -> Processlike:
        """
        install a Conda Python distribution on the instance.

        Args:
            installer_url: url of install script; by default, the latest
                miniforge3 Linux x86_64 installer.
            prefix: path for Conda installation. If a Conda installation
                already exists at this path, it will be updated. Defaults
                to $HOME/miniforge3.
            kwargs: kwargs to pass to `self.commands()`. Only meta-options are
                recommended.

        Returns:
            Output of `self.commands()` for installer script fetch/execution.
        """
        # noinspection PyArgumentList
        return self.commands(
            [
                f"wget {installer_url}",
                f"sh {Path(installer_url).name} -b -u -p {prefix}"
            ],
            "and",
            **kwargs
        )

    def start(
        self,
        return_response: bool = False,
        wait: bool = True,
        connect: bool = False,
        maxtries: int = 40,
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
            print("waiting until instance is running...", end="")
            self.wait_until_running()
            print("running.")
        if connect is True:
            self.wait_until_running()
            self.wait_on_connection(maxtries)
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
        source: Union[str, Path, IO, bytes],
        target: Union[str, Path],
        *args: Any,
        literal_str: bool = False,
        **kwargs: Any,
    ) -> dict:
        """
        write local file or in-memory data to target file on instance.

        Args:
            source: filelike object or path to local file. note that filelike
                objects will be closed during put operation.
            target: write path on instance
            args: additional arguments to pass to underlying put method
            literal_str: if True and `source` is a `str`, write `source`
                into `target` as text rather than interpreting `source` as a
                path to a local file
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
        *args: Any,
        **kwargs: Any,
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
            dict giving transfer metadata: local, remote, host, port, and this
                instance's name, if any
        """
        result = self._ssh.get(source, target, *args, **kwargs)
        if self.name is not None:
            result["name"] = self.name
        return result

    @connectwrap
    def read(
        self,
        source: Union[str, Path],
        mode: Literal["r", "rb"] = "r",
        encoding: str = "utf-8",
        as_buffer: bool = False,
    ) -> Union[io.BytesIO, io.StringIO, bytes, str]:
        """
        read a file from the instance directly into memory.

        Args:
            source: path to file on instance
            mode: 'r' to read file as text; 'rb' to read file as bytes
            encoding: encoding for text, used only if `mode` is 'r'
            as_buffer: if True, return BytesIO/StringIO; if False, return
                bytes/str

        Returns:
            Buffer containing contents of remote file
        """
        return self._ssh.read(source, mode, encoding, as_buffer)

    @connectwrap
    def read_csv(
        self,
        source: Union[str, Path],
        encoding: str = "utf-8",
        **csv_kwargs: Any,
    ) -> pd.DataFrame:
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
            FileNotFoundError: if environment cannot be found.
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
            OSError: if unable to execute `pip show`
            FileNotFoundError: if package doesn't appear to be installed
        """
        if env is None:
            pip = "pip"
        else:
            pip = f"{self.conda_env(env)}bin/pip"
        try:
            result = self.command(f"{pip} show {package}", _wait=True)
            if len(result.stderr) > 0:
                raise OSError(
                    f"pip show did not run successfully: {result.stderr[0]}"
                )
            return re.search(
                r"Location:\s+(.*?)\n", "".join(result.stdout)
            ).group(1)
        except AttributeError:
            raise FileNotFoundError("package not found")

    def compile_env(self):
        """"""
        pass

    def update(self):
        """Refresh basic state and identification information."""
        self.instance_.load()
        self.state = self.instance_.state["Name"]
        self.ip = getattr(self.instance_, f"{self.address_type}_ip_address")
        self.tags = tag_dict(self.instance_.tags)
        self.name = self.tags.get("Name")

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
        self, wait: bool = True, hard: bool = False, timeout: float = 65
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
        self, local_port: int, remote_port: int
    ) -> tuple[Callable, dict[str, Union[int, str, Path]]]:
        """
        create an SSH tunnel between a local port and a remote port.

        Args:
            local_port: port number for local end of tunnel.
            remote_port: port number for remote end of tunnel.

        Returns:
            signaler: function to shut down tunnel
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
        **command_kwargs: bool,
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
                returns a `Viewer`. Only `RunCommand` meta-option are valid,
                you can't pass extra command-line-type kwargs. If you try, it
                will break the call.

        Returns:
            `Viewer` wrapping executed Python process.
        """
        if (interpreter is not None) and (env is not None):
            raise TypeError(
                "Please pass either the name of a conda environment or the "
                "path to a Python interpreter (one or the other, not both)."
            )
        if env is not None:
            path = self.conda_env(env)
            path = path + "/" if not path.endswith("/") else path
            interpreter = f"{path}bin/python"
        interpreter = "python" if interpreter is None else interpreter
        python_command_string = generic_python_endpoint(
            module,
            func,
            payload,
            compression=compression,
            serialization=serialization,
            splat=splat,
            payload_encoded=payload_encoded,
            return_result=print_result,
            filter_kwargs=filter_kwargs,
            interpreter=interpreter,
            for_bash=True,
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
        revoke: bool = True,
    ):
        """
        Modify this instance's security group(s) to permit SSH access from
        an IP (by default, the caller's external IP). IMPORTANT: by default,
        this method revokes all other inbound access permissions, because it
        is good security practice to not slowly whitelist the entire world.
        Pass `revoke=False `if there are permissions you need to retain.

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
                revoke_ingress(sg, force_modification_of_default_sg=force)
            authorize_ssh_ingress_from_ip(
                sg, ip=ip, force_modification_of_default_sg=force
            )

    def price_per_hour(self):
        return instance_price_per_hour(self)

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
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> list[Any]:
        """
        Internal wrapper function: make multithreaded calls to a specified
        method of all this Cluster's Instances with shared arguments.

        Args:
             method_name: named method of Instance to call on all our Instances
             *args: args to pass to these method calls
             **kwargs: kwargs to pass to these method calls

        Returns:
            list containing results of method call from each Instance, 
                including raised Exceptions for failed calls
        """
        exc = ThreadPoolExecutor(len(self))
        futures = []
        for instance in self.instances:
            futures.append(
                exc.submit(getattr(instance, method_name), *args, **kwargs)
            )
        while not all(f.done() for f in futures):
            time.sleep(0.01)
        return [
            f.exception() if f.exception() is not None else f.result()
            for f in futures
        ]
    
    def _async_method_map(
        self,
        method: str,
        argseq: Optional[Union[Sequence[Sequence], cycle]] = None,
        kwargseq: Optional[Union[Sequence[Mapping[str, Any]], cycle]] = None,
        max_concurrent: int = 1,
        poll: float = 0.03
    ) -> ServerPool:
        """
        Internal wrapper function: make multithreaded calls to a specified
        method of all this Cluster's Instances with shared arguments.

        Args:
             method: name of method of Instance to call on Instances
             argseq: optional args to pass to these method calls -- one
                sequence of args per Instance. either `args` or `kwargs` must
                be defined.
             kwargseq: optional kwargs to pass to these method calls -- one
                `dict` or other `Mapping` of kwargs per Instance.

        Returns:
            list containing result of method call from each Instance,
                including raised Exceptions for failed calls
        """
        if (argseq is None) and (kwargseq is None):
            raise TypeError("Must pass at least one of argseq or kwargseq.")
        if not any(
            map(lambda x: isinstance(x, (cycle, NoneType)), (argseq, kwargseq))
        ):
            if len(argseq) != len(kwargseq):
                raise ValueError(
                    "sequences of args and kwargs must have matching lengths."
                )
        argseq = cycle([()]) if argseq is None else argseq
        kwargseq = cycle([{}]) if kwargseq is None else kwargseq
        pool = ServerPool(self.instances, max_concurrent, poll)
        # attempt to prevent accidentally mapping an infinite number of tasks
        if isinstance(argseq, cycle) and isinstance(kwargseq, cycle):
            pool.max_concurrent = 1
            for _, args, kwargs in zip(self.instances, argseq, kwargseq):
                pool.apply(method, args, kwargs)
        else:
            for args, kwargs in zip(argseq, kwargseq):
                pool.apply(method, args, kwargs)
        return pool

    @staticmethod
    def _dispatch_cycle_arguments(argseq, kwargseq):
        if isinstance(argseq, str):
            argseq = cycle(((argseq,),))
        elif (argseq is not None) and (len(argseq) > 0):
            if all(isinstance(a, str) for a in argseq):
                argseq = tuple((a,) for a in argseq)
            elif not isinstance(argseq[0], Sequence):
                argseq = cycle((argseq,))
            elif not isinstance(argseq, Sequence):
                raise TypeError("Malformed argseq.")
        if isinstance(kwargseq, Mapping):
            kwargseq = cycle([kwargseq])
        return argseq, kwargseq

    @staticmethod
    def _check_exceptions(results, _permissive, _warn):
        """internal function for selective Exception-raising on async calls."""
        if (_warn is False) and (_permissive is True):
            return
        for exception in filter(lambda x: isinstance(x, Exception), results):
            if _permissive is False:
                raise exception
            if _warn is True:
                warnings.warn(f"{type(exception)}: {exception}")

    def commandmap(
        self,
        argseq: Union[str, Sequence[Any]],
        kwargseq: Optional[
            Union[Mapping[str, Any], Sequence[Mapping[str, Any]]]
        ] = None,
        wait: bool = True,
        max_concurrent: int = 1
    ) -> Union[list[Viewer], ServerPool]:
        """
        Map a shell command or commands across this `Cluster's` `Instances`,
        asynchronously calling `Instance.command()` with optionally-variable
        args and kwargs. This method enables a wide variety of dispatch/map
        behaviors, and as such, has a very flexible signature.

        Notes:
            * Unlike `Cluster.command()`, this method blocks by default until
                all tasks have completed. If you do not wish it to block, pass
                `wait=False`, which will cause it to return a `ServerPool` you
                can later poll or join for output.
            * If neither `argseq` and `kwargseq` specify a finite number of
                tasks (e.g., `argseq` is a `str` and `kwargseq` is `None`),
                this method will execute the command once on each instance,
                much as if you had passed the same arguments to
                `Cluster.command()`.
            * If both `argseq` and `kwargseq` specify a finite number of tasks
                (e.g., `argseq` is a `list` of `tuples` and `kwargseq` is a
                `list` of `dicts`), they must have equal length.
            * Task order is always preserved in output, but if the number of
                tasks is greater than `len(self) * max_concurrent` (e.g.,
                `argseq` is a `list` of 30 `tuples`, `max_concurrent` is 1,
                and this `Cluster` has 4 `Instances`), there is no guarantee
                that tasks past the first `len(self) * max_concurrent` tasks
                will execute on any particular instance -- the underlying
                `ServerPool` will dispatch pending tasks as instances complete
                older ones. First come, first serves.
            * This method ignores the `_viewer=False` meta-option. It always
                returns either `Viewers` or a `ServerPool` that creates
                `Viewers`.
            * This method ignores the `_disown=True` meta-option.

        Args:
            argseq: Positional argument(s). May be:

                1. A sequence of sequences of args, like:
                    `[("ls", "/home"), ...]`; each of its elements will be
                    `*`-splatted into a single `Instance.command()` call.
                2. A single sequence of args, like `("ls", "/home")`. This will
                    be `*`-splatted into every `Instance.command()` call.
                3. A sequence of strings, like `("ls -a", "cat f", "echo 1")`;
                    each of these strings will be passed directly to a single
                    `Instance.command()` call.
                3. A single string, like `"ls"`; this string will be passed
                    directly to every `Instance.command()` call.
            kwargseq: Optional keyword argument(s). May be:

                1. A sequence of mappings of kwargs, like:
                    `[{'-a': True, '-l': False}, ...]`; each of its elements
                    will be `**`-splatted into a single `command()` call.
                2. A single mapping of kwargs, like:
                    `{'-a': True, '-l': False}`; these kwargs will be
                    `**`-splatted into every `command()` call.
                3. `None`: no kwargs for anyone.
            wait: if `False`, return a `ServerPool` object that asynchronously
                polls the running processes. Otherwise, block until all
                processes complete and return a list of `Viewers`.
            max_concurrent: maximum number of commands to simultaneously run
                on each instance.

        Returns:
            If `wait` is `True`, a list of `Viewers` produced from
                `Instance.command()` executions. If `wait` is
                `False`, a `ServerPool` object that can be used to interact
                with and retrieve the results of the mapped commands.
        """
        argseq, kwargseq = self._dispatch_cycle_arguments(argseq, kwargseq)
        pool = self._async_method_map(
            "command", argseq, kwargseq, max_concurrent
        )
        pool.close()
        if wait is False:
            return pool
        return pool.gather()

    def pythonmap(
        self,
        argseq: Union[str, Sequence[Any]],
        kwargseq: Optional[
            Union[Mapping[str, Any], Sequence[Mapping[str, Any]]]
        ] = None,
        wait: bool = True,
        max_concurrent: int = 1
    ) -> Union[list[Union[Viewer, Exception]], ServerPool]:
        """
        Map Python calls across this `Cluster's` `Instances`, asynchronously
        calling `Instance.call_python()` with optionally-variable args and
        kwargs. This method has the same flexible calling conventions as
        `Cluster.commandmap()`; refer to that method's documentation for more
        detail.

        Args:
            argseq: Positional argument(s) to `Instance.call_python()`.
            kwargseq: Optional keyword argument(s) to `Instance.call_python()`.
            wait: if `False`, return a `ServerPool` object that asynchronously
                polls the running processes. Otherwise, block until all
                processes complete and return a list of `Viewers`.
            max_concurrent: maximum number of calls to simultaneously perform
                on each instance.

        Returns:
            If `wait` is `True`, a list of `Viewers` produced from
                `Instance.call_python()` calls.  If `wait` is `False`, a
                `ServerPool` object that can be used to interact with and
                retrieve the results of the mapped calls.
        """
        argseq, kwargseq = self._dispatch_cycle_arguments(argseq, kwargseq)
        pool = self._async_method_map(
            "call_python", argseq, kwargseq, max_concurrent
        )
        pool.close()
        if wait is False:
            return pool
        return pool.gather()

    def command(
        self,
        command: str,
        *args: Union[str, int, float],
        _permissive: bool = False,
        _warn: bool = True,
        **kwargs: bool
    ) -> list[Union[Processlike, Exception]]:
        """
        Call a shell command on all this Cluster's Instances. See
        `Instance.command()` for further documentation.

        Args:
            command: command name/string
            _permissive: if False, raise first Exception encountered, if any.
            _warn: if True and `permissive` is True, raise a UserWarning for
                each encountered Exception.
            *args: args to pass to `Instance.command()`
            **kwargs: kwargs to pass to `Instance.command()`.

        Returns:
            list containing result of `command()` from each Instance, 
                including raised Exceptions for failed calls if `permissive`
                is True
        """
        results = self._async_method_call("command", command, *args, **kwargs)
        self._check_exceptions(results, _permissive, _warn)
        return results

    def con(
        self,
        command: str,
        *args: Union[str, int, float],
        _permissive: bool = False,
        _warn: bool = True,
        **kwargs: bool
    ) -> list[Optional[Viewer]]:
        """
        Run a command 'console-style' on all this cluster's instances. See
        `Instance.con()` for further documentation. Note that this doesn't
        perform any kind of managed separation of outputs from different
        instances, so it can get pretty visually messy for commands that write
        to stdout/stderr multiple times.

        Args:
            command: command name/string
            _permissive: if False, raise first Exception encountered, if any.
            _warn: if True and `permissive` is True, raise a UserWarning for
                each encountered Exception.
            *args: args to pass to `Instance.con()`
            **kwargs: kwargs to pass to `Instance.con()`.

        Returns:
            list containing results of `con()` from each Instance, including
                raised Exceptions for failed calls if `permissive` is True
        """
        results = self._async_method_call("con", command, *args, **kwargs)
        self._check_exceptions(results, _permissive, _warn)
        return results

    def commands(
        self,
        commands: Sequence[str],
        op: Literal["and", "xor", "then"] = "then",
        _con: bool = False,
        _permissive: bool = False,
        _warn: bool = True,
        **kwargs: bool,
    ) -> list[Union[Processlike, Exception]]:
        """
        Call a sequence of shell commands on all this Cluster's Instances. See
        `Instance.commands()` for further documentation.

        Args:
            commands: command names/strings
            op: logical operator to connect commands.
            _con: run 'console-style', pretty-printing rather than
                returning output (will look message with lots of Instances)
            _permissive: if False, raise first Exception encountered, if any.
            _warn: if True and `permissive` is True, raise a UserWarning for
                each encountered Exception.
            **kwargs: kwargs to pass to `Instance._ssh()`.
                Only meta-options are recommended.

        Returns:
            list containing result of `commands()` from each Instance, 
                including raised Exceptions for failed calls if `permissive`
                is True
        """
        results = self._async_method_call(
            "commands", commands, op, _con, **kwargs
        )
        self._check_exceptions(results, _permissive, _warn)
        return results

    def call_python(
        self,
        module: str,
        func: Optional[str] = None,
        payload: Any = None,
        _permissive: bool = False,
        _warn: bool = True,
        **kwargs: Union[
            bool,
            str,
            CallerCompressionType,
            CallerSerializationType,
            CallerUnpackingOperator,
        ],
    ) -> list[Processlike]:
        """
        Call a Python function on all this Cluster's Instances. See
        `Instance.call_python()` for further documentation.

        Args:
            module: name of, or path to, the target module
            func: name of the function to call.
            payload: object from which to constrct func's call arguments.
            _permissive: if False, raise first Exception encountered, if any.
            _warn: if True and `permissive` is True, raise a 
                UserWarning for each encountered Exception.
            **kwargs: kwargs to pass to `Instance.call_python()`

        Returns:
            list containing results of `call_python()` from each Instance,
                including raised Exceptions for failed calls if `permissive`
                is True
        """
        results = self._async_method_call(
            "call_python", module, func, payload, **kwargs
        )
        self._check_exceptions(results, _permissive, _warn)
        return results

    def connect(self, maxtries: int = 10, delay: float = 1):
        """
        establish SSH connections to all instances, prepping new connections
        when none currently exist, but not replacing existing ones.

        Args:
            maxtries: maximum times to re-attempt failed connections
            delay: how many seconds to wait after failed attempts
        """
        return self._async_method_call(
            "_prep_connection", lazy=False, maxtries=maxtries, delay=delay
        )

    def update(self) -> list:
        """update basic information for instances."""
        return self._async_method_call("update")

    def start(self, *args, **kwargs) -> list:
        """
        Start all Instances. See `Instance.start()` for further documentation,
        including valid arguments.

        Returns:
            list containing results of `start()` from each Instance.
        """
        return self._async_method_call("start", *args, **kwargs)

    def stop(self, *args, **kwargs) -> list:
        """
        Stop all Instances. See `Instance.stop()` for further documentation,
        including valid arguments.

        Returns:
            list containing results of `stop()` from each Instance.
        """
        return self._async_method_call("stop", *args, **kwargs)

    def terminate(self, *args, **kwargs) -> list:
        """
        Terminate all Instances. See `Instance.terminate()` for further
        documentation  / valid arg and kwargs.

        Returns:
            list containing results of `terminate()` from each Instance.
        """
        return self._async_method_call("terminate", *args, **kwargs)

    def put(
        self,
        source: Union[str, Path, IO, bytes],
        target: Union[str, Path],
        *args: Any,
        literal_str: bool = False,
        _permissive: bool = False,
        _warn: bool = True,
        **kwargs: Any,
    ) -> list[Union[dict, Exception]]:
        """
        write local files or in-memory data to target files on instances.

        Args:
            source: filelike object, path to local file, string, or bytestring
                (shared between all instances), or a sequence of such objects,
                one per instance. note that if this is a single filelike
                object, it will be read into memory and closed before sending
                its contents to the instances.
            target: write path (shared between all instances), or a sequence
                of write paths, one per instance.
            _permissive: if False, raise first Exception encountered, if any.
            _warn: if True and `permissive` is True, raise a UserWarning for
                each encountered Exception.
            args: additional arguments to pass to underlying put method
            literal_str: if True and `source` is a `str`, write `source`
                into `target` as text rather than interpreting `source` as a
                path to a local file.
            **kwargs: kwargs to pass to underlying get method

        Returns:
            list of dicts giving transfer metadata: local, remote, host, port,
                including Exceptions for falled puts if _permissive is True.
        """
        if isinstance(target, (str, Path)):
            target = cycle((target,))
        elif len(target) != len(self.instances):
            raise ValueError(
                "a sequence of targets must have the same length as instances"
            )
        # upstream implementation makes it impossible to reuse buffers
        if isinstance(source, IO):
            contents = cycle((source.read(),))
            source.close()
            source = contents
        elif isinstance(source, (str, Path, bytes)):
            source = cycle((source,))
        elif len(source) != len(self.instances):
            raise ValueError(
                "a sequence of sources must have the same length as instances"
            )
        kwargs['literal_str'] = literal_str
        results = self._async_method_map(
            "put",
            # we need self.instances to bound length; s & t can both be cycles.
            [(s, t, *args) for s, t, _ in zip(source, target, self.instances)],
            cycle((kwargs,))
        )
        self._check_exceptions(results, _permissive, _warn)
        return results

    def get(
        self,
        source: Union[Sequence[Union[str, Path]], str, Path],
        target: Union[str, Path, IO, Sequence[Union[str, Path, IO]]],
        _permissive: bool = False,
        _warn: bool = True,
        **kwargs: Any,
    ) -> list[Union[dict, Exception]]:
        """
        copy files from instances to local.

        Args:
            source: path to file (shared between all instances), or a sequence
                of paths to files on instances, one per instance
            target: path to local file, or a filelike object (such as
                io.BytesIO), or a sequence of such things, one per instance.
                if `target` is a path to a local file, one separate file, with
                incrementing suffixes, will be written per instance.
            _permissive: if False, raise first Exception encountered, if any.
            _warn: if True and `permissive` is True, raise a UserWarning for
                each encountered Exception.
            **kwargs: kwargs to pass to underlying get method

        Returns:
            list of dicts giving transfer metadata: local, remote, host, port,
                including Exceptions for falled gets if _permissive is True.
        """
        if isinstance(target, (str, Path)):
            t = Path(target)
            target = [
                f"{t.absolute().parent}/{t.stem}_{i}{t.suffix}"
                for i in range(len(self))
            ]
        elif len(target) != len(self.instances):
            raise ValueError(
                "a sequence of targets must have the same length as instances"
            )
        if isinstance(source, (str, Path)):
            source = cycle((source,))
        elif len(source) != len(self.instances):
            raise ValueError(
                "a sequence of sources must have the same length as instances"
            )
        results = self._async_method_map(
            "get", [(s, t) for s, t in zip(source, target)], cycle((kwargs,))
        )
        self._check_exceptions(results, _permissive, _warn)
        return results

    # TODO: a bit messy
    def read(
        self,
        source: Union[Sequence[Union[str, Path]], str, Path],
        mode: Union[Literal["r", "rb"], Sequence[Literal["r", "rb"]]] = "r",
        encoding: str = "utf-8",
        as_buffer: bool = False,
        concatenate: bool = False,
        separator: Optional[Union[str, bytes]] = None,
        _permissive: bool = True,
        _warn: bool = True,
        **kwargs: Any,
    ) -> Union[
        io.BytesIO,
        io.StringIO,
        bytes,
        str,
        Sequence[Union[io.BytesIO, io.StringIO, bytes, str]],
    ]:
        """
        read files from instances directly into memory.

        Args:
            source: path to file (same path on all instances), or a sequence
                of paths to files on instances, one per instance
            mode: "r" for text, "rb" for binary, or a sequence of those, one
                per instance. must be a single value if `concatenate` is True.
            encoding: encoding for text files. ignored when mode is "rb".
            as_buffer: if True, return BytesIO/StringIO instead of bytes/str
            concatenate: if True, concatenate contents of all files into a
                single object (preserving order), rather than returning them
                as a list
            separator: if `concatenate` is True, separate results from
                different files with this string/bytestring. ignored if
                `concatenate` is False. if None, just stick them together.
            _permissive: if False, raise the first Exception encountered, if 
                any. When True, and concatenating, simply concatenate only 
                successful reads (meaning Exceptions will be discarded).
            _warn: if True and `permissive` is True, raise a UserWarning for
                each encountered Exception.
            **kwargs: kwargs to pass to underlying get method

        Returns:
            if `concatenate` is False: a list of contents, one element per
                instance, including raised Exceptions for failed calls if
                `permissive` is True. if `concatenate` is True: a single object
                containing concatenated contents, with failed reads omitted
                if `permissive` is True. (This will be a 0-length
                string/bytestring/buffer if all reads failed.)  type of
                elements/concatenated object depends on `mode` and `as_buffer`.
        """
        if concatenate is True and not isinstance(mode, str):
            raise TypeError("specify a single mode if concatenate is True.")
        if isinstance(source, (str, Path)):
            source = cycle((source,))
        elif len(source) != len(self.instances):
            raise ValueError(
                "a sequence of sources must have the same length as instances"
            )
        if isinstance(mode, str):
            mode = cycle((mode,))
        elif len(mode) != len(self.instances):
            raise ValueError(
                "a sequence of modes must have the same length of instances"
            )
        results = self._async_method_map(
            "read",
            [
                (s, m, encoding, True)
                for s, m, _ in zip(source, mode, self.instances)
            ],
            cycle((kwargs,)),
        )
        self._check_exceptions(results, _permissive, _warn)
        if concatenate is False:
            if as_buffer is True:
                return results
            return [
                r if isinstance(r, Exception) else r.read() for r in results
            ]
        output = [r.read() for r in results if not isinstance(r, Exception)]
        if separator is None:
            if next(mode) == "rb":
                separator = b""
            else:
                separator = ""
        if next(mode) == "rb" and isinstance(separator, str):
            separator = separator.encode(encoding)
        if next(mode) == "r" and isinstance(separator, bytes):
            separator = separator.decode(encoding)
        output = separator.join(output)
        if as_buffer is False:
            return output
        if mode == "rb":
            # noinspection PyTypeChecker
            return io.BytesIO(output)
        return io.StringIO(output)

    def read_csv(
        self,
        source: Union[Sequence[Union[str, Path]], str, Path],
        encoding: str = "utf-8",
        add_identifiers: bool = True,
        reset_index: bool = True,
        _permissive: bool = False,
        _warn: bool = False,
        **csv_kwargs: Any,
    ) -> pd.DataFrame:
        """
        read CSV-like files from all instances and concatenate them along the 
            column axis into a pandas DataFrame, optionally adding identifier
            columns.
            
        Args:
            source: path to CSV-like file (same path on all instances), or
                a sequence of such paths, one per instance
            encoding: text encoding for CSV-like files
            add_identifiers: if True, add "name" and "id" columns to the
                returned DataFrame indicating which instances produced which
                rows.
            reset_index: if True, reset index and drop original indices.
            _permissive: if False, raise first Exception encountered during
                read, if any. When True, simply ignore read Exceptions,
                meaning the `DataFrame` will contain no output from instances
                on which calls failed and all Exceptions will be discarded.
                (This will result in an empty `DataFrame` if all reads failed.)
                This option does *not* suppress Exceptions encountered during
                `DataFrame` construction or concatenation.
            _warn: if True and `permissive` is True, raise a UserWarning for
                each encountered read Exception.
            **csv_kwargs: kwargs to pass to pd.read_csv

        Returns:
            A `DataFrame` containing concatenated contents of all files,
                optionally including identifying information for source
                instances.
        """
        buffers = self.read(
            source,
            encoding=encoding,
            _permissive=_permissive,
            _warn=_warn,
            as_buffer=True
        )
        frames = []
        for buffer, instance in zip(buffers, self.instances):
            if isinstance(buffer, Exception):
                continue
            df = pd.read_csv(buffer, **csv_kwargs)
            if add_identifiers is True:
                df[['name', 'id']] = instance.name, instance.instance_id
            frames.append(df)
        concatenated = pd.concat(frames)
        if reset_index is False:
            return concatenated
        return concatenated.reset_index(drop=True)

    def install_conda(
        self,
        installer_url=CONDA_DEFAULTS['installer_url'],
        prefix=CONDA_DEFAULTS['prefix'],
        _permissive: bool = False,
        _warn: bool = False,
        **kwargs: bool
    ) -> list[Union[Processlike, Exception]]:
        """
        install a Conda Python distribution on all instances.

        Args:
            installer_url: url of install script; by default, the latest
                miniforge3 Linux x86_64 installer.
            prefix: path for Conda installation. If a Conda installation
                already exists at this path, it will be updated. Defaults
                to $HOME/miniconda3.
            _permissive: if False, raise first Exception encountered, if any.
            _warn: if True and `permissive` is True, raise a UserWarning for
                each encountered Exception.
            kwargs: kwargs to pass to `Instance.commands()`. Only meta-options
                are recommended.

        Returns:
            List containing outputs of `Instance.commands()` for installer
                script fetch/execution, including Exceptions for failed
                calls if `_permissive` is True.
        """
        results = self._async_method_call(
            "install_conda", installer_url, prefix, **kwargs
        )
        self._check_exceptions(results, _permissive, _warn)
        return results

    @classmethod
    def from_descriptions(
        cls,
        descriptions: Collection[InstanceDescription],
        **kwargs: Union[
            str,
            Path,
            botocore.client.BaseClient,
            boto3.resources.base.ServiceResource,
            boto3.Session,
            bool,
        ],
    ) -> "Cluster":
        """
        Construct a Cluster from InstanceDescriptions, as produced by
        `ls_instances()`.

        Args:
            descriptions: InstanceDescriptions to initialize Instances from
                and subsequently collect into a Cluster.
            **kwargs: kwargs to pass to the Instance constructor

        Returns:
            a Cluster including an Instance for each description.
        """
        instances = [Instance(d, **kwargs) for d in descriptions]
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
        connect: bool = False,
        maxtries: int = 40,
        increment_names: bool = True,
        **instance_kwargs: Union[
            str,
            botocore.client.BaseClient,
            boto3.resources.base.ServiceResource,
            boto3.Session,
            Path,
            bool,
        ],
    ) -> "Cluster":
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
                overrides connect=True if False.
            connect: if True, block after launch until all instances are
                connectable.
            maxtries: how many times to try to connect to the instances if
                connect=True (waiting 1s between each attempt)
            increment_names: if True and count > 1, suffix incrementing
                integers to the names of each instance in the Cluster,
                and, if they had Name tags already, add a "ClusterName" tag
                indicating the base name
            **instance_kwargs: kwargs to pass to the Instance constructor.

        Returns:
            a Cluster created from the newly-launched fleet.
        """
        if count < 1:
            raise ValueError(f"count must be >= 1.")
        client = init_client("ec2", client, session)
        options = {} if options is None else options
        # TODO: add a few more conveniences, clean up
        if instance_kwargs.get("type_") is not None:
            options["instance_type"] = instance_kwargs.pop("type_")
        if instance_kwargs.get("name") is not None:
            options["instance_name"] = instance_kwargs.pop("name")
        if template is None:
            using_scratch_template = True
            template = create_launch_template(**options)["LaunchTemplateName"]
            if options.get("image_id") is None:
                # we're always using a stock Canonical image in this case, so
                # note that we're forcing uname to 'ubuntu':
                print(
                    "Using stock Canonical image, so setting uname to "
                    "'ubuntu'."
                )
                instance_kwargs["uname"] = "ubuntu"
        else:
            using_scratch_template = False
        if tags is not None:
            tagrecs = [{"Key": k, "Value": v} for k, v in tags.items()]
            tag_kwarg = {
                "TagSpecifications": [
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
                **tag_kwarg,
            )
        finally:
            if using_scratch_template is True:
                client.delete_launch_template(LaunchTemplateName=template)
        # note that we do not want to raise these all the time, because the
        # API frequently dumps a lot of harmless info in here.
        launch_errors = len(fleet.get("Errors", []))
        try:
            n_instances = len(fleet["Instances"][0]["InstanceIds"])
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
            return _instances_from_ids(
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
        # TODO: async?
        if (increment_names is True) and (count > 1):
            if (basename := instances[0].tags.get("Name")) is not None:
                client.create_tags(
                    Resources=[i.instance_id for i in instances],
                    Tags=[{"Key": "ClusterName", "Value": basename}],
                )
            for i, instance in enumerate(instances):
                instance.rename(instance.tags.get("Name", "") + str(i))
        if wait is False:
            print(f"launched {noun}; wait=False passed, not checking status")
            return cluster
        if connect is True:
            print(f"launched {noun}; waiting until connectable")
        else:
            print(f"launched {noun}; waiting until running")
        # TODO: also async?
        for instance in cluster.instances:
            if connect is False:
                instance.wait_until_running()
                print(f"{instance} is running")
            else:
                instance.wait_on_connection(maxtries)
                print(f"connected to {instance}")
        return cluster

    def price_per_hour(self):
        prices = [i.price_per_hour() for i in self.instances]
        return {
            "running": sum(p["running"] for p in prices),
            "stopped": sum(p["stopped"] for p in prices),
        }

    def rebase_ssh_ingress_ip(
        self,
        ip: Optional[str] = None,
        force: bool = False,
        revoke: bool = True,
    ) -> list[None]:
        """
        Modify all security groups associated with all of this Cluster's
        instances to permit SSH access from an IP IMPORTANT: by default,
        this method revokes all other inbound access permissions, because it
        is good security practice to not slowly whitelist the entire world.
        Pass `revoke=False` if there are permissions you need to retain.

        Args:
            ip: permit SSH access from this IP. if None, use the caller's
                external IP.
            force: if True, will force modification even of default security
                groups.
            revoke: if True, will revoke all other inbound permissions.

        Returns:
            list of None (since the Instance method doesn't return anything)
        """
        return self._async_method_call(
            "rebase_ssh_ingress_ip", ip=ip, force=force, revoke=revoke
        )

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return self.instances[item]
        for attr in ("name", "ip", "instance_id"):
            matches = [i for i in self.instances if getattr(i, attr) == item]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 0:
                return matches
        raise KeyError

    def __repr__(self):
        return "\n".join([inst.__repr__() for inst in self.instances])

    def __str__(self):
        return "\n".join([inst.__str__() for inst in self.instances])

    def __len__(self):
        return len(self.instances)


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
def summarize_instance_type_structure(itinfo: dict) -> dict:
    """
    summarize an [EC2 InstanceTypeInfo](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_InstanceTypeInfo.html)
    object.

    Args:
        itinfo: dict created from an InstanceTypeInfo structure, as returned
            by boto3 functions like `describe_instance_types()`

    Returns:
        succinct version of InstanceTypeInfo structure
    """
    proc = itinfo["ProcessorInfo"]
    attributes = {
        "instance_type": itinfo["InstanceType"],
        "architecture": proc["SupportedArchitectures"][0],
        "cpus": itinfo["VCpuInfo"]["DefaultVCpus"],
        "cpu_speed": proc.get("SustainedClockSpeedInGhz"),
        "ram": itinfo["MemoryInfo"]["SizeInMiB"] / 1024,
        "bw": itinfo["NetworkInfo"]["NetworkPerformance"],
    }
    if "EbsOptimizedInfo" in itinfo["EbsInfo"].keys():
        ebs = itinfo["EbsInfo"]["EbsOptimizedInfo"]
        attributes["ebs_bw_min"] = ebs["BaselineThroughputInMBps"]
        attributes["ebs_bw_max"] = ebs["MaximumThroughputInMBps"]
        attributes["ebs_iops_min"] = ebs["BaselineIops"]
        attributes["ebs_iops_max"] = ebs["MaximumIops"]
    if itinfo["InstanceStorageSupported"] is True:
        attributes["disks"] = itinfo["InstanceStorageInfo"]["Disks"]
        attributes["local_storage"] = itinfo["InstanceStorageInfo"][
            "TotalSizeInGB"
        ]
    else:
        attributes["disks"] = []
        attributes["local_storage"] = 0
    attributes["cpu_surcharge"] = itinfo["BurstablePerformanceSupported"]
    return attributes


def summarize_instance_type_response(response: dict) -> tuple[dict]:
    """
    extract a series of [EC2 InstanceTypeInfo](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_InstanceTypeInfo.html)
    from a boto3-wrapped DescribeInstanceTypes or DescribeInstanceTypeOfferings
    call and produce succinct summaries of them

    Args:
        response: boto3-wrapped DescribeInstanceType* API response

    Returns:
        summaries of each described instance type
    """
    types = response["InstanceTypes"]
    return gmap(summarize_instance_type_structure, types)


# TODO: maybe we need to do something fancier to support passing a session
#  or region/credentials around to support the pricing features here

# TODO: fail only partially if denied permissions for Price List
def describe_instance_type(
    instance_type: str,
    pricing: bool = True,
    ec2_client: Optional[botocore.client.BaseClient] = None,
    pricing_client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.session.Session] = None,
) -> dict:
    """
    Retrieve a succinct description of an EC2 instance type.

    NOTE: this function will report i386 architecture for the
    very limited number of instance types that support both i386
    and x86_64.

    Args:
        instance_type: instance type name, e.g. 'm6i.large'
        pricing: if True, also retrieve pricing information
        ec2_client: optional preexisting boto ec2 client
        pricing_client: optional preexisting boto pricing client
        session: optional preexisting boto session

    Returns:
        dict containing summary information for this instance type
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


def _retrieve_instance_type_info(
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.session.Session] = None,
    reset_cache: bool = False,
) -> tuple[dict]:
    """
    Retrieve full descriptions of all instance types available in the client's
    AWS Region, either from the API or from an on-disk cache, and cache them
    to disk if retrieved from API.

    This is primarily intended to be used as a helper function for higher-level
    instance type summarization and tabulation functions.

    Args:
        client: optional preexisting boto ec2 client
        session: optional preexisting boto session
        reset_cache: if True, always retrieve all descriptions from the API,
            even if 'fresh' cached descriptions are available on disk. If
            False, do so only if there are no cached descriptions or they are
            more than 5 days old.

    Returns:
        dicts produced from [EC2 InstanceTypeInfo](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_InstanceTypeInfo.html)
            API objects, one for every instance type available in the AWS
            Region.
    """
    client = init_client("ec2", client, session)
    region = clarify_region(None, client)
    cache_path = Path(GENERAL_DEFAULTS["cache_path"])
    prefix = f"instance_types_{region}"
    if reset_cache is False:
        cached_results = _check_cached_results(cache_path, prefix, max_age=5)
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
def instance_catalog(
    family: Optional[str] = None,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.session.Session] = None,
) -> pd.DataFrame:
    """
    Construct a catalog of available instance types, including their
    technical specifications and on-demand pricing.

    Args:
        family: optional name of instance family, e.g. 'm6i'. If not specified,
            catalog includes all available instance types.
        client: optional preexisting boto ec2 client
        session: optional preexisting boto session

    Returns:
        Instance catalog DataFrame.
    """
    types = _retrieve_instance_type_info(client, session)
    summaries = gmap(summarize_instance_type_structure, types)
    if family is not None:
        summaries = [
            s for s in summaries if s["instance_type"].split(".")[0] == family
        ]
    summary_df = pd.DataFrame(summaries)
    pricing = get_ec2_basic_price_list(session=session)["ondemand"]
    pricing_df = pd.DataFrame(pricing)
    return summary_df.merge(pricing_df, on="instance_type", how="left")


def _interpret_ebs_args(
    volume_type: Optional[Literal["gp2", "gp3", "io1", "io2"]] = None,
    volume_size: Optional[int] = None,
    iops: Optional[int] = None,
    throughput: Optional[int] = None,
    volume_list: Optional[Sequence[dict[str, Union[int, str]]]] = None,
) -> list[dict]:
    """
    helper function for `create_launch_template()`. Parse user-provided volume
    specifications into a list of dicts formatted like EC2 API
    [LaunchTemplateBlockDeviceMappingRequest objects](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_LaunchTemplateBlockDeviceMappingRequest.html).

    This function permits specification of _either_ a volume_list or
    volume_type + volume_size + (optional) iops + (optional) throughput, which
    all implicitly refer to a single boot volume.

    Args:
        volume_type: EBS volume type for instance boot volum.
        volume_size: Size in GB for instance boot volume.
        iops: IOPS for instance boot volume, if other than default
        throughput: throughput for instance boot volume, if other than default
        volume_list: sequence of dicts that could be passed as kwargs to this
            function, giving all volumes that will be attached to the instance
            at creation. the first one is the boot volume. If specified,
            volume_type and volume_size must be None.

    Returns:
        LaunchTemplateBlockDeviceMappingRequest objects from specs
    """
    if ((volume_type is not None) or (volume_size is not None)) and (
        volume_list is not None
    ):
        raise TypeError(
            "Please pass either a list of volumes (volume_list) or "
            "volume_type and _size, not both."
        )
    if volume_list is None:
        if (volume_type is None) or (volume_size is None):
            raise TypeError(
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
"""all current-generation EBS volume types"""


def _ebs_device_mapping(
    volume_type: str,
    volume_size: int,
    index: int = 0,
    iops: Optional[int] = None,
    throughput: Optional[int] = None,
    device_name: Optional[str] = None,
) -> dict:
    """
    reformat the passed specification as a legal EC2 API
    [LaunchTemplateBlockDeviceMappingRequest object](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_LaunchTemplateBlockDeviceMappingRequest.html)

    Args:
        volume_type: EBS volume type, e.g. 'gp3'
        volume_size: volume size in GB
        index: index of volume among all specified volumes (0 means root)
        iops: I/O operations per second, if other than default and volume type
            supports IOPS specification
        throughput: throughput in MiB/s, if other than default and volume type
            supports throughput specification
        device_name: optional block device name; if not specified, will
            automatically assign one based on index

    Returns:
        LaunchTemplateBlockDeviceMappingRequest-formatted dict
    """
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


def _hostess_placeholder() -> str:
    """create a random hostess placeholder name"""
    return f"hostess-{''.join(choices(ascii_lowercase, k=10))}"


def create_security_group(
    name: Optional[str] = None,
    description: Optional[str] = None,
    client: Optional[botocore.client.BaseClient] = None,
    resource: Optional[boto3.resources.base.ServiceResource] = None,
    session: Optional[boto3.session.Session] = None,
) -> "SecurityGroup":
    """
    Create a new EC2 security group in the caller's AWS account with default
    hostess settings.

    Args:
        name: optional name for new security group. If not specified, will
            randomly assign a name.
        description: optional description for new security group. will give a
            default description.
        client: optional preexisting boto ec2 client
        resource: optional preexisting boto ec2 resource
        session: optional preexisting boto session

    Returns:
        A boto3 SecurityGroup resource providing an interface to the newly-
            created security group.
    """
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
        name = _hostess_placeholder()
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


def create_ec2_key(
    key_name: Optional[str] = None,
    save_key: bool = True,
    resource: Optional[boto3.resources.base.ServiceResource] = None,
    session: Optional[boto3.session.Session] = None,
) -> "KeyPair":
    """
    Create a new EC2 SSH key pair in the caller's AWS account. Optionally also
    save the key material to disk.

    Args:
        key_name: optional name for key pair (if not specified, a name is
            randomly assigned)
        save_key: if True, save the key material to disk in ~/.ssh, in a .pem
            file with the same filename stem as the key pair
        resource: optional preexisting boto ec2 resource
        session: optional preexisting boto session

    Returns:
        a boto3 KeyPair resource providing an interface to the newly-created
            key pair.
    """
    if key_name is None:
        key_name = _hostess_placeholder()
    resource = init_resource("ec2", resource, session)
    key = resource.create_key_pair(KeyName=key_name)
    keydir = Path(os.path.expanduser("~/.ssh"))
    keydir.mkdir(exist_ok=True)
    if save_key is True:
        keyfile = Path(keydir, f"{key_name}.pem")
        with keyfile.open("w") as stream:
            stream.write(key.key_material)
        # many programs will not permit you to use a key file with read/write
        # permissions for other users
        keyfile.chmod(0o700)
    return key


def create_launch_template(
    template_name: Optional[str] = None,
    instance_type: str = EC2_DEFAULTS["instance_type"],
    volume_type: Optional[Literal["gp2", "gp3", "io1", "io2"]] = None,
    volume_size: Optional[int] = None,
    image_id: Optional[str] = None,
    iops: Optional[int] = None,
    throughput: Optional[int] = None,
    volume_list: Optional[list[dict]] = None,
    instance_name: Optional[str] = None,
    security_group_name: Optional[str] = None,
    tags: Optional[dict] = None,
    key_name: Optional[str] = None,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.session.Session] = None,
) -> dict:
    """
    Create a new EC2 launch template in the caller's AWS account (see
    https://docs.aws.amazon.com/autoscaling/ec2/userguide/launch-templates.html).

    Args:
        template_name: optional name for template. if none is specified, a
            random name is assigned.
        instance_type: instance type name (e.g. 'm6i.large')
        volume_type: EBS volume type for boot volume (e.g. 'gp3'). If not
            specified and `volume_list` is not passed, defaults to
            `EC2_DEFAULTS['volume_type']`
        volume_size: volume size in GB for boot volume. If not specified and
            `volume_list` is not passed, defaults to
            `EC2_DEFAULTS['volume_size']`
        image_id: ID for Amazon Machine Image (AMI) to create instance from.
            if not specified, uses the most recently-released Ubuntu Server
            LTS AMI.
        iops: I/O operations per second for boot volume, if other than default
            and volume type supports IOPS specification; ignored if volume_list
            passed
        throughput: throughput in MiB/s for boot volume, if other than default
            and volume type supports throughput specification; ignored if
            volume_list passed
        volume_list: alternative to specifying volume_type/volume_size (and
            optional iops/throughput). a list of dicts with 'volume_type',
            'volume_size', and optionally 'iops' and 'throughput' keys. Each
            of these dicts specifies a separate EBS volume for the instance;
            the first will be the boot volume. It must not be passed along
            with volume_size or volume_type.
        instance_name: optional name for instances created from template
        security_group_name: optional name of preexisting security group. if
            not specified, a new security group will be created.
        tags: optional dict like {tag name: tag value} specifying resource
            tags for instances and volumes created from this template.
        key_name: optional name of preexisting EC2 key pair object known to
            AWS. if not specified, a new EC2 key pair will be created and a
            corresponding key file will be saved to disk in ~/.ssh.
        client: optional preexisting boto ec2 client
        session: optional preexisting boto session

    Returns:
        dict created from an AWS
            [LaunchTemplate API response](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_LaunchTemplate.html).
    """
    default_name = _hostess_placeholder()
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
            )["Images"][0]["ImageId"]
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


def revoke_ingress(
    sg: "SecurityGroup",
    force_modification_of_default_sg: bool = False,
    ports: Collection[int] = (22,),
    protocols: Collection[Literal["tcp", "udp", "icmp"]] = ("tcp",),
):
    """
    Remove inbound permission rules from a security group. The default
    settings revoke permissions on the default SSH port.

    Args:
        sg: boto3 SecurityGroup resource object
        force_modification_of_default_sg: if True, modify rules even if
            `sg` is a default security group
        ports: revoke only those rules granting ingress on one of these ports
        protocols: revoke only those rules granting ingress via one of these
            protocols
    """
    if "default" in sg.id and not force_modification_of_default_sg:
        print(
            "\tRefusing to modify permissions of a default security group. "
            "Pass flag to override."
        )
        return
    print(
        f"Revoking ingress from all IPs on port(s) {ports} to security group "
        f"{sg.id}"
    )
    for rule in sg.ip_permissions:
        # do not modify ingress rules based on security group (and not just IP)
        if not (
            rule["FromPort"] in ports
            and rule["IpProtocol"] in protocols
            and any(sg.ip_permissions[1]["UserIdGroupPairs"])
        ):
            continue
        sg.revoke_ingress(IpPermissions=[rule])


def authorize_ssh_ingress_from_ip(
    sg: "SecurityGroup",
    ip: Optional[str] = None,
    force_modification_of_default_sg: bool = False,
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
    if "default" in sg.id and not force_modification_of_default_sg:
        print(
            "\tRefusing to modify permissions of a default security group. "
            "Pass flag to override."
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
        if "InvalidPermission.Duplicate" in str(ce):
            print(f"** {ip} already authorized for SSH ingress for {sg.id} **")
        else:
            raise


def instance_price_per_hour(instance: Instance):
    ec2_pricelist = get_ec2_basic_price_list()
    try:
        instance_rates = [
            r
            for r in ec2_pricelist["ondemand"]
            if r["instance_type"] == instance.instance_type
        ][0]
    except IndexError:
        raise ValueError("cannot retrieve pricing for this instance type.")
    stopped_price = 0
    volumes = list(instance.instance_.volumes.iterator())
    ebs_pricelist = ec2_pricelist["ebs"]
    for volume in volumes:
        match = [
            r for r in ebs_pricelist if r["volume_type"] == volume.volume_type
        ]
        # instance storage case
        if len(match) == 0:
            continue
        ebs_rates = match[0]
        storage_cost = volume.size * ebs_rates["storage"] / HOURS_PER_MONTH
        # currently, this is only for gp3
        if ebs_rates["throughput"] is not None:
            extra = volume.throughput - 125
            throughput_cost = extra * ebs_rates["throughput"] / HOURS_PER_MONTH
        else:
            throughput_cost = 0
        # gp3, io1, io2
        if ebs_rates["iops"] is not None:
            if volume.volume_type == "gp3":
                extra = volume.iops - 3000
            else:
                extra = volume.iops
            # technically io2 iops are tiered, but the pricing
            # API doesn't describe this well
            iops_cost = extra * ebs_rates["iops"] / HOURS_PER_MONTH
        else:
            iops_cost = 0
        stopped_price += iops_cost + throughput_cost + storage_cost
    return {
        "stopped": stopped_price,
        "running": stopped_price + instance_rates["usd_per_hour"],
    }
