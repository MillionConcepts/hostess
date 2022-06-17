import io
import re
import time
from collections import defaultdict
from functools import cache, wraps
from pathlib import Path
from typing import (
    Union, Collection, Literal, Sequence, MutableMapping, Optional, Callable
)

import sh
from botocore.exceptions import ClientError
from cytoolz import concat
from dustgoggles.func import zero

import killscreen.shortcuts as ks
from killscreen.aws.utilities import tag_dict, init_client, init_resource
from killscreen.ssh import (
    jupyter_connect,
    wrap_ssh,
    ssh_key_add,
    find_conda_env,
    interpret_command
)
from killscreen.subutils import Viewer, Processlike, Commandlike


def ls_instances(
    states=None, raw_filters=None, client=None, session=None, flatten=True
):
    """
    return list describing all instances accessible to aws user,
    or optionally dict containing instances nested by reservation
    """
    # TODO, maybe: implement abstractions to other parts of the filter system
    client = init_client("ec2", client, session)
    filters = [] if raw_filters is None else raw_filters
    if states is not None:
        filters.append({"Name": "instance-state-name", "Values": list(states)})
    instances = client.describe_instances(Filters=filters)
    if flatten is False:
        return instances
    return tuple(concat(res["Instances"] for res in instances["Reservations"]))


def describe(
    tag_filters=None,
    states=("running",),
    raw_filters=None,
    client=None,
    session=None,
):
    """
    return dict of key:description for all instances accessible to aws user
    with tags matching tag_filters (using simple string inclusion for now)
    and state in states. post-filters on the tag filters so that we can use
    string inclusion rather than exact matching (which the ec2 api does not
    support).
    """
    if tag_filters is None:
        tag_filters = {}
    if raw_filters is None:
        raw_filters = []
    descriptions = {}
    for instance in ls_instances(states, raw_filters, client, session):
        tags = tag_dict(instance.get("Tags", []))
        match = True
        for key, value in tag_filters.items():
            if key not in tags.keys():
                match = False
                break
            if value not in tags[key]:
                match = False
                break
        if match is True:
            descriptions[instance["InstanceId"]] = instance
    return descriptions


def instance_ips(
    tag_filters=None,
    states=("running",),
    raw_filters=None,
    client=None,
    session=None,
):
    """
    dict of key:ip for all instances accessible to aws user
    with tags matching tag_filters (using simple string inclusion for now)
    and state in states. post-filters on the tag filters so that we can use
    string inclusion rather than exact matching (which the ec2 api does not
    support).
    """
    descriptions = describe(
        tag_filters, states, raw_filters, client, session
    )
    return {
        instance_id: description.get("PublicIpAddress")
        for instance_id, description in descriptions.items()
    }


def instance_ids(
    tag_filters=None,
    states=("running",),
    raw_filters=None,
    client=None,
    session=None,
):
    """
    tuple of instance ids for all instances accessible to aws user
    with tags matching tag_filters (using simple string inclusion for now)
    and state in states. post-filters on the tag filters so that we can use
    string inclusion rather than exact matching (which the ec2 api does not
    support).
    """
    descriptions = describe(
        tag_filters, states, raw_filters, client, session
    )
    return tuple(descriptions.keys())


def instances_from_ids(
    ids, *instance_args, resource=None, session=None, **instance_kwargs
):
    resource = init_resource("ec2", resource, session)
    instances = []
    # TODO: make this asynchronous
    for instance_id in ids:
        instance = Instance(
            instance_id, *instance_args, resource=resource, **instance_kwargs
        )
        instances.append(instance)
    return instances


class Instance:
    def __init__(
        self,
        description,
        key=None,
        uname=None,
        client=None,
        resource=None,
        session=None,
        use_private_ip=False
    ):
        resource = init_resource("ec2", resource, session)
        if isinstance(description, str):
            # if it's got periods in it, assume it's a public IPv4 address
            if "." in description:
                client = init_client("ec2", client, session)
                instance_id = ls_instances(
                    raw_filters=[
                        {"Name": "ip-address", "Values": [description]}
                    ],
                    client=client
                )
            # otherwise assume it's the instance id
            else:
                instance_id = description
        # otherwise assume it's a full description like from
        # ls_instances / ec2.describe_instance*
        else:
            instance_id = description["InstanceId"]
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
        self.key, self.uname = key, uname
        self.instance_ = instance_
        self._command = wrap_ssh(self.ip, self.key, self.uname, self)

    @wraps(interpret_command)
    def command(self, *args, **kwargs) -> Union[Viewer, sh.RunningCommand]:
        if any(p is None for p in (self.ip, self.key, self.uname)):
            raise AttributeError(
                "ip, key, and uname must all be set to run ssh commands."
            )
        return self._command(*args, **kwargs)

    def commands(
        self,
        commands: Sequence[Commandlike],
        op: Literal["and", "xor", "then"] = "then",
        **kwargs
    ) -> Processlike:
        return self.command(ks.chain(commands, op), **kwargs)

    def notebook(self, **connect_kwargs):
        return jupyter_connect(self.ip, self.key, self.uname, **connect_kwargs)

    def start(self, return_response=False):
        response = self.instance_.start()
        if return_response is True:
            return response

    def stop(self, return_response=False):
        response = self.instance_.stop()
        if return_response is True:
            return response

    def terminate(self, return_response=False):
        response = self.instance_.terminate()
        if return_response is True:
            return response

    def add_key(self):
        ssh_key_add(self.ip)

    def put(self, source, target, *args, _literal_str=False, **kwargs):
        """
        copy file from local disk or object from memory to target file on
        instance using scp.
        """
        if isinstance(source, str) and (_literal_str is True):
            source_file = "/dev/stdin"
        elif not isinstance(source, (str, Path)):
            source_file = "/dev/stdin"
        else:
            source_file = source
        command_parts = [
            f"-i{self.key}", source_file, f"{self.uname}@{self.ip}:{target}"
        ]
        if source_file == "/dev/stdin":
            command_parts.append(source)
        return interpret_command(
            sh.scp, *command_parts, *args, _host=self, **kwargs
        )

    def get(self, source, target, *args, **kwargs):
        """copy source file from instance to local target file with scp."""
        command_parts = [
            f"-i{self.key}", f"{self.uname}@{self.ip}:{source}", target
        ]
        return interpret_command(
            sh.scp, *command_parts, *args, _host=self, **kwargs
        )

    def read(self, source, *args, **kwargs):
        """copy source file from instance into memory using scp."""
        command_parts = [
            f"-i{self.key}", f"{self.uname}@{self.ip}:{source}", "/dev/stdout"
        ]
        return interpret_command(
            sh.scp, *command_parts, *args, _host=self, **kwargs
        ).stdout

    def read_csv(self, source, **csv_kwargs):
        """
        reads csv-like file from remote host into pandas DataFrame using scp.
        """
        import pandas as pd

        buffer = io.StringIO()
        buffer.write(self.read(source).decode())
        buffer.seek(0)
        return pd.read_csv(buffer, **csv_kwargs)

    @cache
    def conda_env(self, env):
        return find_conda_env(self.command, env)

    @cache
    def find_package(self, package, env=None):
        if env is None:
            pip = "pip"
        else:
            pip = f"{self.conda_env(env)}/bin/pip"
        result = self.command(
            f"{pip} show package-name {package}"
        ).stdout.decode()
        return re.search(r"Location:\s+(.*?)\n", result).group(1)

    def update(self):
        self.instance_.load()
        self.ip = getattr(self.instance_, f"{self.address_type}_ip_address")
        self._command = wrap_ssh(self.ip, self.key, self.uname, self)

    def wait_until_running(self, update=True):
        if self.state == "running":
            return
        self.instance_.wait_until_running()
        if update is True:
            self.update()

    def __repr__(self):
        string = f"{self.instance_type} in {self.zone} at {self.ip}"
        if self.name is None:
            return f"{self.instance_id}: {string}"
        return f"{self.name} ({self.instance_id}): {string}"

    def __str__(self):
        return self.__repr__()


class Cluster:
    def __init__(self, instances: Collection[Instance]):
        self.instances = tuple(instances)
        self.fleet_request = None

    def command(
        self,
        *args,
        _viewer=True,
        **kwargs
    ) -> tuple[Processlike, ...]:
        return tuple(
            [
                instance.command(*args, _viewer=_viewer, **kwargs)
                for instance in self.instances
            ]
        )

    def commands(
        self, commands: Sequence[Commandlike], _viewer=True, **kwargs
    ) -> tuple[Processlike, ...]:
        return tuple(
            (
                instance.commands(commands, _viewer=_viewer, **kwargs)
                for instance in self.instances
            )
        )

    def add_keys(self):
        ssh_key_add(
            list(filter(None, [instance.ip for instance in self.instances]))
        )

    # TODO: make these run asynchronously
    def start(self, return_response=False):
        responses = []
        for instance in self.instances:
            responses.append(instance.start(return_response))
        if return_response is True:
            return responses

    def stop(self, return_response=False):
        responses = []
        for instance in self.instances:
            responses.append(instance.stop(return_response))
        if return_response is True:
            return responses

    def terminate(self, return_response=False):
        responses = []
        for instance in self.instances:
            responses.append(instance.terminate(return_response))
        if return_response is True:
            return responses

    def gather_files(
        self,
        source_path,
        target_path: str = ".",
        process_states: Optional[
            MutableMapping[str, Literal["running", "done"]]
        ] = None,
        callback: Callable = zero,
        delay=0.02
    ):
        status = {
            instance.instance_id: "ready" for instance in self.instances
        }

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
                    _done=finish_factory(instance.instance_id)
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
        template,
        tags=None,
        client=None,
        session=None,
        *instance_args,
        wait=True,
        **instance_kwargs,
    ):
        client = init_client("ec2", client, session)
        tags = [] if tags is None else tags
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
            TagSpecifications=[{"Tags": tags}],
            Type="instant",
        )

        def instance_hook():
            return instances_from_ids(
                fleet["Instances"][0]["InstanceIds"],
                *instance_args,
                client=client,
                **instance_kwargs,
            )
        instances = []
        for _ in range(5):
            try:
                instances = instance_hook()
                break
            except ClientError as ce:
                if "does not exist" in str(ce):
                    time.sleep(0.2)
            raise TimeoutError(
                "launched instances successfully, but unable to run "
                "DescribeInstances. Perhaps permissions are wrong."
            )
        cluster = Cluster(instances)
        cluster.fleet_request = fleet
        if wait is False:
            print("launched fleet; _wait=False passed, not checking status")
            return cluster
        print("launched fleet; waiting until instances are running")
        for instance in cluster.instances:
            instance.wait_until_running()
            print(f"{instance} is running")
        print("scanning instance ssh keys")
        added = False
        tries = 0
        while (tries < 10) and (added is False):
            try:
                cluster.add_keys()
                added = True
            except sh.ErrorReturnCode as error:
                time.sleep(max(6 - tries, 2))
                [instance.update() for instance in cluster.instances]
                tries += 1
        if added is False:
            print("warning: timed out adding keys for one or more instances")
        return cluster

