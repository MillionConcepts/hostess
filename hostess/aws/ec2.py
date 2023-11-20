from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
from functools import cache, partial, wraps
import io
from itertools import chain
import os
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
    Union, IO
)

import boto3.resources.base
import botocore.client
import botocore.exceptions
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
    tagfilter,
)
from hostess.caller import generic_python_endpoint
from hostess.config import EC2_DEFAULTS, GENERAL_DEFAULTS
from hostess.ssh import (
    find_ssh_key, find_conda_env, jupyter_connect, NotebookConnection, SSH
)
from hostess.subutils import Processlike
from hostess.utilities import (
    check_cached_results,
    filestamp,
    my_external_ip,
    timeout_factory,
)

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
    states: Sequence[str] = ("running", "pending", "stopped"),
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


class Instance:
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
        Interface for an EC2 instance, permitting state control, monitoring,
        and remote process calls. Uses a combination of direct SSH access and
        EC2 API calls.

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
        **instance_kwargs,
    ):
        return Cluster.launch(
            1,
            template,
            options,
            tags,
            client,
            session,
            wait,
            **instance_kwargs
        )[0]

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
            del self._ssh
            self._ssh = None
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
            found = find_ssh_key(self.instance_.key_name)
            if found is None:
                self.key_errstring = (
                    f"Couldn't find a keyfile for the instance. The keyfile "
                    f"may not be in the expected path, or its filename might "
                    f"not match the AWS key name ({self.instance_.key_name}). "
                    f"Try explicitly specifying the path by setting the .key "
                    f"attribute or passing the key= argument."
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
        self, *, lazy=True, maxtries: int = 5, delay: float = 1
    ):
        """
        try to prep, and optionally establish, a SSH connection to the
        instance. if no closed / latent connection exists, create one;
        otherwise, use the existing one. if the instance isn't running,
        automatically replace any existing connection (which will be closed
        anyway by then, or should be).

        Args:
            lazy: don'y establish the connection immediately;  wait until some
                method needs it. other arguments do nothing if this is True.
            maxtries: maximum times to re-attempt failed connections
            delay: how many seconds to wait after subsequent attempts
        """
        if self.is_connected:  # nothing to do
            return
        self._maybe_find_key()
        self._update_ssh_info()
        if self._ssh is None:
            self._ssh = SSH.connect(self.ip, self.uname, self.key)
        if lazy is True:
            return
        for attempt in range(maxtries):
            try:
                self._ssh.conn.open()
                return
            except (AttributeError, SSHException, NoValidConnectionsError):
                time.sleep(delay)
                self.update()
        raise ConnectionError(
            "Unable to establish SSH connection to instance. It may not yet "
            "be ready to accept SSH connections, or something might "
            "be wrong with configuration."
        )

    def _check_unready(self) -> Union[str, bool]:
        """
        Is the instance obviously not ready for SSH connections?

        Returns:
            "state" if it is not running or transitioning to running;
                comma-separated list of missing ip/uname/key if any;
                Otherwise False.
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
            del self._ssh
            self._ssh = None
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

    @connectwrap
    def command(
        self,
        *args,
        _viewer: bool = True,
        _wait: bool = False,
        _quiet: bool = True,
        **kwargs
    ) -> Processlike:
        """
        run a command in the instance's default interpreter via SSH.

        Args:
            *args: args to pass to `self._ssh`.
            _viewer: if `True`, return a `Viewer` object. otherwise return
                unwrapped result from `self._ssh`.
            _wait: if `True`, block until command terminates (or connection
                fails). _w is an alias.
            _quiet: if `False`, print stdout and stderr, should the process
                return any before this function terminates. Generally best
                used with _wait=True.
            **kwargs: kwargs to pass to `self.ssh`.

        Returns:
            object representing executed process.
        """
        return self._ssh(
            *args, _viewer=_viewer, _wait=_wait, _quiet=_quiet, **kwargs
        )

    @connectwrap
    def con(self, *args, _poll=0.05, _timeout=None, **kwargs):
        """
        pretend you are running a command on the instance while looking at a
        terminal emulator, pausing for output and pretty-printing it to stdout.

        like Instance.command with _wait=True, _quiet=False, but does not
        return a process abstraction. fun in interactive environments.
        """
        process = self.command(*args, **kwargs)
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
            print("^C")

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
            structure containing results of the tunneled Jupyter Notebook
                execution.
        """
        self._prep_connection()
        return jupyter_connect(self._ssh,  **connect_kwargs)

    def start(self, return_response: bool = False) -> Optional[dict]:
        """
        Start the instance and attempt to establish an SSH connection.

        Args:
            return_response: if True, return API response.
        """
        response = self.instance_.start()
        self.update()
        if return_response is True:
            return response

    def stop(self, return_response: bool = False) -> Optional[dict]:
        """
        Stop the instance.

        Args:
            return_response: if True, return API response.
        """
        response = self.instance_.stop()
        self.update()
        if return_response is True:
            return response

    def terminate(self, return_response=False):
        """Terminate (aka delete) the instance."""
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
    ):
        """
        write local file or object over SSH to target file on instance.

        Args:
            source: filelike object or path to file
            target: write path on instance
            args: additional arguments to pass to underlying put command
            literal_str: if True and `source` is a `str`, write `source`
                into `target` as text rather than interpreting `source` as a
                path
            kwargs: additional kwargs to pass to underlying put command
        """
        if isinstance(source, str) and (literal_str is True):
            source = io.StringIO(source)
        elif not isinstance(source, (str, Path, io.StringIO, io.BytesIO)):
            raise TypeError("Object must be a string, Path, or filelike.")
        return self._ssh.put(source, target, *args, **kwargs)

    @connectwrap
    def get(self, source, target, *args, **kwargs):
        """copy source file from instance to local target file with scp."""
        return self._ssh.get(source, target, *args, **kwargs)

    @connectwrap
    def read(self, source, *args, **kwargs):
        """copy source file from instance into memory using scp."""
        buffer = io.BytesIO()
        self._ssh.get(source, buffer, *args, **kwargs)
        buffer.seek(0)
        return buffer

    @connectwrap
    def read_csv(self, source, encoding='utf-8', **csv_kwargs):
        """
        reads csv-like file from remote host into pandas DataFrame using scp.
        """
        import pandas as pd

        buffer = io.StringIO()
        buffer.write(self.read(source).read().decode(encoding))
        buffer.seek(0)
        return pd.read_csv(buffer, **csv_kwargs)

    @cache
    def conda_env(self, env):
        return find_conda_env(self._ssh, env)

    @cache
    def find_package(self, package, env=None):
        if env is None:
            pip = "pip"
        else:
            pip = f"{self.conda_env(env)}/bin/pip"
        try:
            result = self.command(
                f"{pip} show package-name {package}"
            ).stdout
            return re.search(r"Location:\s+(.*?)\n", result).group(1)
        except UnexpectedExit:
            raise OSError("pip show did not run successfully")
        except (AttributeError, IndexError):
            raise FileNotFoundError("package not found")

    def compile_env(self):
        """"""
        pass

    def update(self):
        """Update locally available information about the instance."""
        self.instance_.load()
        self.state = self.instance_.state["Name"]
        self.ip = getattr(self.instance_, f"{self.address_type}_ip_address")

    def wait_until(self, state):
        """Pause execution until the instance state is met.
        Will update locally available information."""
        assert state in [
            "running",
            "stopped",
            "terminated",  # likely useful
            "stopping",
            "pending",
            "shutting-down",
        ]  # unlikely
        while self.state != state:
            self.update()
            continue

    def wait_until_running(self):
        """Pause execution until the instance state=='running'
        Will update the locally available information by default."""
        self.wait_until("running")

    def wait_until_stopped(self):
        """Pause execution until the instance state=='stopped'
        Will update the locally available information by default."""
        self.wait_until("stopped")

    def wait_until_terminated(self):
        """Pause execution until the instance state=='stopped'
        Will update the locally available information by default."""
        self.wait_until("terminated")

    def reboot(self, wait_until_running=True):
        """Reboot the instance. Pause execution until done."""
        self.stop()
        self.wait_until_stopped()
        self.start()
        if wait_until_running:
            self.wait_until_running()

    def restart(self, wait_until_running=True):
        # an alias for reboot()
        self.reboot(wait_until_running=wait_until_running)

    def tunnel(self, local_port, remote_port):
        self._prep_connection()
        self._ssh.tunnel(local_port, remote_port)
        return self._ssh.tunnels[-1]

    @connectwrap
    def call_python(
        self,
        module=None,
        func=None,
        payload=None,
        interpreter_path=None,
        env=None,
        compression=None,
        serialization=None,
        splat="",
        payload_encoded=False,
        print_result=True,
        filter_kwargs=True,
        **command_kwargs,
    ):
        if (interpreter_path is not None) and (env is not None):
            raise ValueError(
                "Please pass either the name of a conda environment or the "
                "path to a Python interpreter (one or the other, not both)."
            )
        if env is not None:
            interpreter_path = f"{self.conda_env(env)}/bin/python"
        if interpreter_path is None:
            interpreter_path = "python"
        python_command_string = generic_python_endpoint(
            module,
            func,
            payload,
            compression,
            serialization,
            splat,
            payload_encoded,
            print_result,
            filter_kwargs,
            interpreter_path,
            for_bash=True
        )
        return self._ssh(python_command_string, **command_kwargs)

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
    key_errstring = None


class Cluster:
    def __init__(self, instances: Collection[Instance]):
        self.instances = tuple(instances)
        self.fleet_request = None

    def _async_method_call(self, method_name: str, *args, **kwargs):
        exc = ThreadPoolExecutor(len(self.instances))
        futures = []
        for instance in self.instances:
            futures.append(
                exc.submit(getattr(instance, method_name), *args, **kwargs)
            )
        while not all(f.done() for f in futures):
            time.sleep(0.01)
        return [f.result() for f in futures]

    def command(
        self, command, *args, _viewer=True, **kwargs
    ) -> list[Processlike, ...]:
        return self._async_method_call(
            "command", command, *args, _viewer=_viewer, **kwargs
        )

    def commands(
        self, commands: Sequence[str], *args, _viewer=True, **kwargs
    ) -> list[Processlike, ...]:
        return self._async_method_call(
            "commands", commands, *args, _viewer=_viewer, **kwargs
        )

    def call_python(
        self, module, *args, _viewer=True, **kwargs
    ) -> list[Processlike, ...]:
        return self._async_method_call(
            "call_python", module, *args, _viewer=_viewer, **kwargs
        )

    def start(self, return_response=False):
        """Start the instances."""
        return self._async_method_call("start", return_response)

    def stop(self, return_response=False):
        """Stop the instances."""
        return self._async_method_call("stop", return_response)

    def terminate(self, return_response=False):
        """Terminate (aka delete) the instances."""
        return self._async_method_call("terminate", return_response)

    def gather_files(
        self,
        source_path,
        target_path: str = ".",
        process_states: Optional[
            MutableMapping[str, Literal["running", "done"]]
        ] = None,
        callback: Callable = zero,
        delay=0.02,
    ):
        status = {instance.instance_id: "ready" for instance in self.instances}

        def finish_factory(instance_id):
            def finisher(*args, **kwargs):
                status[instance_id] = transition_state
                callback(*args, **kwargs)

            return finisher

        if process_states is None:
            return {
                instance.instance_id: instance.get(
                    f"{source_path}/*", target_path, _bg=True, _viewer=True
                )
                for instance in self.instances
            }
        commands = defaultdict(list)

        while any(s != "complete" for s in status.values()):
            for instance in self.instances:
                if status[instance.instance_id] in ("complete", "fetching"):
                    continue
                if process_states[instance.instance_id] == "done":
                    transition_state = "complete"
                else:
                    transition_state = "ready"
                status[instance.instance_id] = "fetching"
                command = instance.get(
                    f"{source_path}/*",
                    target_path,
                    _bg=True,
                    _bg_exc=False,
                    _viewer=True,
                    _done=finish_factory(instance.instance_id),
                )
                commands[instance.instance_id].append(command)
            time.sleep(delay)
        return commands

    @classmethod
    def from_descriptions(cls, descriptions, *args, **kwargs):
        instances = [Instance(d, *args, **kwargs) for d in descriptions]
        return cls(instances)

    @classmethod
    def launch(
        cls,
        count,
        template=None,
        options=None,
        tags=None,
        client=None,
        session=None,
        wait=True,
        **instance_kwargs,
    ):
        client = init_client("ec2", client, session)
        tags = [] if tags is None else tags
        options = {} if options is None else options
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
                TagSpecifications= [
                    {"ResourceType": "instance", "Tags": tags},
                    {"ResourceType": "volume", "Tags": tags},
                ],
                Type="instant",
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

    def __getitem__(self, item):
        return self.instances[item]

    def __repr__(self):
        return "\n".join([inst.__repr__() for inst in self.instances])

    def __str__(self):
        return "\n".join([inst.__str__() for inst in self.instances])


def get_canonical_images(
    architecture: Literal["x86_64", "arm64", "i386"] = "x86_64",
    client=None,
    session=None,
) -> list[dict]:
    """
    fetch the subset of official Canonical images we might
    plausibly want to offer as defaults to users. this will still
    generally return hundreds of images and take > 1.5 seconds because
    of the number of unsupported daily builds available.
    """
    client = init_client("ec2", client, session)
    # this perhaps excessive-looking optimization is intended to reduce the
    # time of the API call and the chance we will pick a 'bad' image.
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
    client=None,
    session=None,
) -> str:
    """
    retrieve image ID of the most recent officially-supported
    Canonical Ubuntu Server LTS AMI for the provided architecture.
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
def summarize_instance_type_structure(structure) -> dict:
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
        "ram": structure["MemoryInfo"]["SizeInMiB"] / 1024,
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
        cached_results = check_cached_results(cache_path, prefix, max_age=7)
        if cached_results is not None:
            return pickle.load(cached_results.open("rb"))
    results = autopage(client, "describe_instance_types")
    clear_cached_results(cache_path, prefix)
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
        device_name = f"/dev/sd{ascii_lowercase[1:][index]}"
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
