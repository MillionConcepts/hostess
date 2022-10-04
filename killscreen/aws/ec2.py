import datetime as dt
import io
import os
import pickle
import re
import time
from collections import defaultdict
from functools import cache, wraps, partial
from itertools import chain
from pathlib import Path
from random import choices
from string import ascii_lowercase
from typing import (
    Union,
    Collection,
    Literal,
    Sequence,
    MutableMapping,
    Optional,
    Callable,
    Mapping,
)

from botocore.exceptions import ClientError
from cytoolz.curried import get
import dateutil.parser as dtp
from dustgoggles.func import zero
from dustgoggles.structures import listify
import sh

from killscreen.aws.pricing import (
    get_on_demand_price,
    get_cpu_credit_price,
    get_ec2_basic_price_list,
)
from killscreen.caller import generic_python_endpoint
from killscreen.config import EC2_DEFAULTS, GENERAL_DEFAULTS
import killscreen.shortcuts as ks
from killscreen.aws.utilities import (
    init_client,
    init_resource,
    tag_dict,
    tagfilter,
    autopage,
    clarify_region,
)
from killscreen.ssh import (
    jupyter_connect,
    wrap_ssh,
    ssh_key_add,
    find_conda_env,
    interpret_command,
    find_ssh_key,
    tunnel,
)
from killscreen.subutils import Viewer, Processlike, Commandlike
from killscreen.utilities import (
    gmap,
    my_external_ip,
    filestamp,
    check_cached_results,
    clear_cached_results,
)


def summarize_instance_description(description):
    """
    convert an Instance element of a JSON structure returned by a
    DescribeInstances operation to a more concise format.
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
    identifier: Optional[str] = None,
    states: Sequence[str] = ("running", "pending", "stopped"),
    raw_filters: Optional[Sequence[Mapping[str, str]]] = None,
    client=None,
    session=None,
    long: bool = False,
    tag_regex: bool = True,
    **tag_filters,
):
    """
    return tuple of records describing all instances accessible to aws user.
    if long=True is not passed, includes only the most regularly-pertinent
    information, flattened, with succint field names.
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
    # the ec2 api does not support string inclusion or other fuzzy filters.
    # we would like to be able to fuzzy-filter, so we apply our tag filters to
    # the structure returned by boto3 from its DescribeInstances call.
    descriptions = filter(
        partial(tagfilter, filters=tag_filters, regex=tag_regex), descriptions
    )
    if long is False:
        return gmap(summarize_instance_description, descriptions)
    return tuple(descriptions)


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
        uname=GENERAL_DEFAULTS["uname"],
        key=None,
        client=None,
        resource=None,
        session=None,
        use_private_ip=False,
    ):
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
        # ls_instances / ec2.describe_instance*
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
        if key is None:
            key = find_ssh_key(instance_.key_name)
        self.uname, self.key = uname, key
        self.instance_ = instance_
        self._command = wrap_ssh(self.ip, self.uname, self.key, self)

    def _is_unready(self):
        return (self.state not in ("running", "pending")) or any(
            p is None for p in (self.ip, self.uname, self.key)
        )

    def _raise_unready(self):
        if not self._is_unready():
            return
        self.update()
        if self._is_unready():
            raise ConnectionError(
                f"Unable to execute commands on {self.instance_id}. This is "
                f"most likely because it is not running or because its "
                f"identity keyfile could not be located. Please try "
                f"explicitly setting the path to the keyfile, like: "
                f"instance.key = '/path/to/key.pem', or passing it to the "
                f"class constructor when creating a new Instance."
            )

    @wraps(interpret_command)
    def command(self, *args, **kwargs) -> Union[Viewer, sh.RunningCommand]:
        self._raise_unready()
        return self._command(*args, **kwargs)

    def commands(
        self,
        commands: Sequence[Commandlike],
        op: Literal["and", "xor", "then"] = "then",
        **kwargs,
    ) -> Processlike:
        return self.command(ks.chain(commands, op), **kwargs)

    def notebook(self, **connect_kwargs):
        self._raise_unready()
        return jupyter_connect(self.ip, self.uname, self.key, **connect_kwargs)

    def start(self, return_response=False):
        """Start the instance."""
        response = self.instance_.start()
        self.update()
        if return_response is True:
            return response

    def stop(self, return_response=False):
        """Stop the instance."""
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

    def add_public_key(self, tries=5, delay=1.5):
        """
        attempts to add the instance's public key to the local known
        hosts file so that we can interact with it via ssh.
        """
        for _ in range(tries):
            if self.ip is None:
                self.update()
            if self.ip is not None:
                try:
                    ssh_key_add(self.ip)
                    return
                except sh.ErrorReturnCode:
                    pass
            time.sleep(delay)
        raise TimeoutError("timed out adding key. try again in a moment.")

    def put(self, source, target, *args, _literal_str=False, **kwargs):
        """
        copy file from local disk or object from memory to target file on
        instance using scp.
        """
        self._raise_unready()
        if isinstance(source, str) and (_literal_str is True):
            source_file = "/dev/stdin"
        elif not isinstance(source, (str, Path)):
            source_file = "/dev/stdin"
        else:
            source_file = source
        command_parts = [
            f"-i{self.key}",
            source_file,
            f"{self.uname}@{self.ip}:{target}",
        ]
        if source_file == "/dev/stdin":
            command_parts.append(source)
        return interpret_command(
            sh.scp, *command_parts, *args, _host=self, **kwargs
        )

    def get(self, source, target, *args, **kwargs):
        """copy source file from instance to local target file with scp."""
        self._raise_unready()
        command_parts = [
            f"-i{self.key}",
            f"{self.uname}@{self.ip}:{source}",
            target,
        ]
        return interpret_command(
            sh.scp, *command_parts, *args, _host=self, **kwargs
        )

    def read(self, source, *args, **kwargs):
        """copy source file from instance into memory using scp."""
        self._raise_unready()
        command_parts = [
            f"-i{self.key}",
            f"{self.uname}@{self.ip}:{source}",
            "/dev/stdout",
        ]
        return interpret_command(
            sh.scp, *command_parts, *args, _host=self, **kwargs
        ).stdout

    def read_csv(self, source, **csv_kwargs):
        """
        reads csv-like file from remote host into pandas DataFrame using scp.
        """
        self._raise_unready()
        import pandas as pd

        buffer = io.StringIO()
        buffer.write(self.read(source).decode())
        buffer.seek(0)
        # noinspection PyTypeChecker
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

    def compile_env(self):
        """"""
        pass

    def update(self):
        """Update locally available information about the instance."""
        self.instance_.load()
        self.state = self.instance_.state["Name"]
        self.ip = getattr(self.instance_, f"{self.address_type}_ip_address")
        self._command = wrap_ssh(self.ip, self.uname, self.key, self)

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

    def tunnel(self, local_port, remote_port, kill=True):
        self._raise_unready()
        return tunnel(
            self.ip, self.uname, self.key, local_port, remote_port, kill
        )

    def call_python(
        self,
        module,
        payload=None,
        func=None,
        interpreter_path=None,
        env=None,
        compression=None,
        serialization=None,
        argument_unpacking="",
        payload_encoded=False,
        print_result=False,
        **command_kwargs
    ):
        if (interpreter_path is None) == (env is None):
            raise ValueError(
                "Please pass either the name of a conda environment or the "
                "path to "
                "a Python interpreter (one or the other, not both)."
            )
        if interpreter_path is None:
            interpreter_path = f"{self.conda_env(env)}/bin/python"
        python_command_string = generic_python_endpoint(
            module,
            payload,
            func,
            interpreter_path,
            compression,
            serialization,
            argument_unpacking,
            payload_encoded,
            print_result,
            for_bash=True
        )
        return self.command(python_command_string, **command_kwargs)

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
        self, *args, _viewer=True, **kwargs
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

    def add_public_keys(self):
        ssh_key_add(
            list(filter(None, [instance.ip for instance in self.instances]))
        )

    # TODO: make these run asynchronously
    def start(self, return_response=False):
        """Start the instances."""
        responses = []
        for instance in self.instances:
            responses.append(instance.start(return_response))
        if return_response is True:
            return responses

    def stop(self, return_response=False):
        """Stop the instances."""
        responses = []
        for instance in self.instances:
            responses.append(instance.stop(return_response))
        if return_response is True:
            return responses

    def terminate(self, return_response=False):
        """Terminate (aka delete) the instances."""
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
        *instance_args,
        wait=True,
        **instance_kwargs,
    ):
        client = init_client("ec2", client, session)
        tags = [] if tags is None else tags
        options = {} if options is None else options
        if template is None:
            using_scratch_template = True
            template = create_launch_template(**options)["Template"][
                "LaunchTemplateName"
            ]
        else:
            using_scratch_template = False
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
        if using_scratch_template is True:
            client.delete_launch_template("LaunchTemplateName")

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
        while (tries < 12) and (added is False):
            try:
                cluster.add_public_keys()
                added = True
            except sh.ErrorReturnCode:
                time.sleep(max(6 - tries, 2))
                [instance.update() for instance in cluster.instances]
                tries += 1
        if added is False:
            print(
                "warning: timed out adding keys for one or more instances. "
                "try running .add_public_keys() again in a moment."
            )
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
        name = killscreen_placeholder()
    if description is None:
        description = "killscreen-generated security group"
    sg = resource.create_security_group(
        Description=description,
        GroupName=name,
        VpcId=default_vpc_id,
        TagSpecifications=[
            {
                "ResourceType": "security-group",
                "Tags": [{"Key": "killscreen-generated", "Value": "True"}],
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
            {
                "FromPort": 6000,
                "ToPort": 6000,
                "IpProtocol": "tcp",
                "IpRanges": [
                    {
                        "CidrIp": f"{my_ip}/32",
                        "Description": "default killscreen port access from "
                        "creating IP",
                    }
                ],
            },
        ]
    )
    sg.authorize_ingress(SourceSecurityGroupName=sg.group_name)
    return sg


def killscreen_placeholder():
    return f"killscreen-{''.join(choices(ascii_lowercase, k=10))}"


def create_ec2_key(key_name=None, save_key=True, resource=None, session=None):
    if key_name is None:
        key_name = killscreen_placeholder()
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
    instance_type=EC2_DEFAULTS["instance_type"],
    volume_type=EC2_DEFAULTS["volume_type"],
    volume_size=EC2_DEFAULTS["volume_size"],
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
    default_name = killscreen_placeholder()
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
                Filters=[{"Name": "tag:Name", "Values": [image_id]}]
            )[0]["ImageId"]
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
    tags.append({"Key": "killscreen-generated", "Value": "True"})
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
