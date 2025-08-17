import re
import time
from typing import Optional, Any, Sequence, Collection, Literal, NamedTuple

import boto3
import botocore.client
import pandas as pd

from hostess.aws.utilities import tagfilter, tag_dict, init_client


class ECRImage(NamedTuple):
    registry: str  # e.g. "123456789012.dkr.ecr.us-east-1.amazonaws.com"
    repository: str  # e.g. "my/app"
    ref_type: str  # "tag" | "digest" | "none"
    ref_value: Optional[
        str
    ]  # tag string or "sha256:..." (without leading "@")
    is_public: bool
    region: Optional[
        str
    ]  # us-east-1, cn-north-1, etc (public uses us-east-1 for login)
    account: Optional[str]  # None for public
    partition: str  # "aws" | "aws-cn" | "aws-us-gov"


_ECR_PRIVATE = re.compile(
    r"^(?P<account>\d+)\.dkr\.ecr\.(?P<region>[\w-]+)\.amazonaws\.com(?P<suffix>\.cn)?/(?P<repo>[^:@]+)(?::(?P<tag>[^@]+))?(?:@(?P<digest>sha256:[0-9a-fA-F]{64}))?$"
)

# public.ecr.aws/<alias-or-namespace>/<repo>(:tag)?(@sha256:...)?
_ECR_PUBLIC = re.compile(
    r"^public\.ecr\.aws/(?P<repo>[^:@]+)(?::(?P<tag>[^@]+))?(?:@(?P<digest>sha256:[0-9a-fA-F]{64}))?$"
)


def parse_ecr_image_uri(image_uri: str) -> ECRImage:
    """
    Parse an ECR image URI (private or public). Supports tags and digest pins.

    Examples:
      123456789012.dkr.ecr.us-east-1.amazonaws.com/my/app:abc123
      123456789012.dkr.ecr.us-gov-west-1.amazonaws.com/thing@sha256:...
      public.ecr.aws/abc1d2e3/myimg:latest
    """
    m = _ECR_PRIVATE.match(image_uri)
    if m:
        account = m.group("account")
        region = m.group("region")
        suffix = m.group("suffix") or ""
        partition = (
            "aws-cn"
            if suffix == ".cn"
            else ("aws-us-gov" if "us-gov" in region else "aws")
        )
        repo = m.group("repo")
        tag = m.group("tag")
        digest = m.group("digest")
        ref_type, ref_value = (
            ("tag", tag)
            if tag
            else (("digest", digest) if digest else ("none", None))
        )
        return ECRImage(
            registry=f"{account}.dkr.ecr.{region}.amazonaws.com{suffix}",
            repository=repo,
            ref_type=ref_type,
            ref_value=ref_value,
            is_public=False,
            region=region,
            account=account,
            partition=partition,
        )

    m = _ECR_PUBLIC.match(image_uri)
    if m:
        repo = m.group("repo")
        tag = m.group("tag")
        digest = m.group("digest")
        ref_type, ref_value = (
            ("tag", tag)
            if tag
            else (("digest", digest) if digest else ("none", None))
        )
        # ECR Public logins use us-east-1 region with ecr-public
        return ECRImage(
            registry="public.ecr.aws",
            repository=repo,
            ref_type=ref_type,
            ref_value=ref_value,
            is_public=True,
            region="us-east-1",
            account=None,
            partition="aws",
        )

    raise ValueError(f"Not an ECR image URI I recognize: {image_uri!r}")


def docker_push_commands_for_ecr(
    image_uri: str, local_tag: Optional[str] = None
) -> str:
    """
    Generate a login → build → tag → push command sequence for an ECR image URI.

    - If `image_uri` has a tag, that tag is used for local build unless `local_tag` is provided.
    - If `image_uri` is pinned by digest, a tag is required to push; we synthesize a
      local/remote tag like 'repro-<shortdigest>' unless `local_tag` is provided.

    Returns a newline-joined shell snippet.
    """
    info = parse_ecr_image_uri(image_uri)
    # Pick a safe local image name (no slashes). Use repo basename locally.
    repo_basename = info.repository.rsplit("/", 1)[-1]

    if info.ref_type == "tag":
        chosen_tag = local_tag or info.ref_value  # type: ignore[arg-type]
        remote_ref = f"{info.registry}/{info.repository}:{chosen_tag}"
    else:
        # Digest or none -> must pick a tag to push
        if local_tag:
            chosen_tag = local_tag
        elif info.ref_type == "digest" and info.ref_value:
            short = info.ref_value.split(":")[1][:12]
            chosen_tag = f"repro-{short}"
        else:
            chosen_tag = "latest"
        remote_ref = f"{info.registry}/{info.repository}:{chosen_tag}"

    # Login command differs for public vs private ECR
    if info.is_public:
        login = (
            f"aws ecr-public get-login-password --region {info.region} | "
            f"docker login --username AWS --password-stdin {info.registry}"
        )
    else:
        login = (
            f"aws ecr get-login-password --region {info.region} | "
            f"docker login --username AWS --password-stdin {info.registry}"
        )

    build = f"docker build -t {repo_basename}:{chosen_tag} ."

    # If original had a tag and the tag matches chosen_tag and the registry/repo are the same,
    # we *could* build directly to remote_ref to skip the extra `docker tag`. Keeping the
    # explicit tag step for clarity/parity with AWS console snippets.
    tag_cmd = f"docker tag {repo_basename}:{chosen_tag} {remote_ref}"
    push = f"docker push {remote_ref}"

    # If the original reference included a digest, add a comment to signal the mismatch.
    digest_note = ""
    if info.ref_type == "digest":
        digest_note = (
            "# Note: original task definition pinned by digest; pushing by tag.\n"
            "#       Consider updating the task definition to a tag or re-pinning to the new digest after push.\n"
        )

    return "\n".join([login, digest_note + build, tag_cmd, push]).strip()


def summarize_task_description(task):
    container = task["containers"][0]
    summary = {
        "az": task["availabilityZone"],
        "id": container.get("runtimeId"),
        "group": task["group"],
        "status": container["lastStatus"],
        "container_arn": container["containerArn"],
        "task_arn": container["taskArn"],
        "cluster_arn": task["clusterArn"],
        "definition_arn": task["taskDefinitionArn"],
        "image": container["image"],
        "start": task.get("startedAt"),
        "container_name": container["name"],
        "name": tag_dict(task.get("tags", [])).get("Name"),
        "launch_type": task["launchType"],
        "tags": tag_dict(task.get("tags", []))
    }
    if summary["start"] is not None:
        summary["start"] = summary["start"].isoformat()
    return summary


def definition_from_summary(client, summary):
    return client.describe_task_definition(
        taskDefinition=summary["definition_arn"]
    )["taskDefinition"]


def summarize_task_def(def_):
    cont = def_["containerDefinitions"][0]
    return {"family": def_["family"], "log_config": cont["logConfiguration"]}


def ls_tasks(
    cluster: str | None = None,
    task_arn: str | None = None,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    status: (
        Collection[Literal["RUNNING", "STOPPED"]]
        | Literal["RUNNING", "STOPPED"]
    ) = ("RUNNING", "STOPPED"),
    tag_regex: bool = True,
    raw_filters: dict[str, Any] | None = None,
    **tag_filters: str,
) -> Sequence:
    """
    Note: this assumes the common one-container-per-task structure.
    If that's not true, you'll only get information about the first
    container associated with the task.
    """
    client = init_client("ecs", client, session)
    if cluster is None:
        clusters = [c for c in client.list_clusters()["clusterArns"]]
    else:
        clusters = [cluster]
    filts = {} if raw_filters is None else raw_filters
    status = (status,) if isinstance(status, str) else status
    summaries, task_defs = [], {}
    for c in clusters:
        arns = []
        # NOTE: yes, these must be separate calls
        for s in status:
            if s not in ("RUNNING", "STOPPED"):
                raise TypeError(
                    "Only 'RUNNING' and 'STOPPED' may be included in `status`"
                )
            arns += client.list_tasks(cluster=c, **filts, desiredStatus=s)[
                "taskArns"
            ]
        if not arns:
            continue
        if task_arn is not None and task_arn not in arns:
            continue
        elif task_arn is not None:
            arns = [a for a in arns if a == task_arn]
        tasks = client.describe_tasks(cluster=c, tasks=arns, include=["TAGS"])[
            "tasks"
        ]
        tasks = [t for t in tasks if tagfilter(t, tag_filters, tag_regex)]
        csums = [summarize_task_description(t) for t in tasks]
        filtered = []
        for cs in csums:
            if cs["definition_arn"] in task_defs:
                def_ = task_defs[cs["definition_arn"]]
            else:
                def_ = definition_from_summary(client, cs)
                task_defs[cs["definition_arn"]] = def_
            dsum = summarize_task_def(def_)
            if dsum.get("log_config", {}).get("logDriver") == "awslogs":
                opts = dsum["log_config"]["options"]
                cs["log_group_name"] = opts["awslogs-group"]
                cs["log_stream_name"] = "/".join(
                    [
                        opts["awslogs-stream-prefix"],
                        cs["container_name"],
                        cs["task_arn"].rsplit("/")[-1],
                    ]
                )
            else:
                cs["log_stream_name"] = None
                cs["log_group_name"] = None
            filtered.append(cs)
        summaries += filtered
    return summaries


class ECSTask:
    """
    Class providing a minimal interface to a running task.

    NOTE: like other objects in this module, this class assumes that one and
        only one container is associated with each task.
    """

    def __init__(self, summary, session=None):
        self.summary = summary.copy()
        self.cluster_arn = summary["cluster_arn"]
        self.task_arn = summary["task_arn"]
        self.ecs = init_client("ecs", None, session)
        # TODO: there are several redundant API calls here.
        self.logs = init_client("logs", None, session)
        self.update()
        if self.status == "MISSING":
            raise RuntimeError(
                "Could not find specified task. It may have been discarded "
                "from the ECS 'buffer' of recently-stopped tasks, or the "
                "provided information may be incorrect."
            )
        self.definition = definition_from_summary(self.ecs, self.summary)

    def update(self):
        listing = ls_tasks(cluster=self.cluster_arn, task_arn=self.task_arn)
        if not listing:
            self.status = "MISSING"
            self.summary["status"] = "MISSING"
            return
        summary = listing[0]
        self.summary = summary
        self.name = summary["name"]
        self.launch_type = summary["launch_type"]
        self.group = summary["group"]
        self.id_ = summary["id"]
        self.az = summary["az"]
        self.definition_arn = summary["definition_arn"]
        self.status = summary["status"]
        self.log_group_name = summary["log_group_name"]
        self.log_stream_name = summary["log_stream_name"]

    def get_logs(
        self,
        from_head: bool = False,
        formatting: Literal["simple", "df", "records", "raw"] = "simple",
    ) -> dict | list[str] | pd.DataFrame:
        """
        NOTES:

        1. this currently only returns the last 10000 / 1 MB of
          log entries (whichever is smaller). Paginated and asynchronous
          versions are planned.

        2. It supports only logs created using the `awslogs` driver
          (i.e. 'normal' CloudWatch logs).
        """
        if self.log_group_name is None:
            raise ValueError("This task does not have readable logs.")
        response = self.logs.get_log_events(
            logGroupName=self.log_group_name,
            logStreamName=self.log_stream_name,
            startFromHead=from_head,
        )
        if formatting == "raw":
            return response
        if formatting == "records":
            return response["events"]
        if formatting == "simple":
            return [e["message"].strip() for e in response["events"]]
        if formatting == "df":
            return pd.DataFrame(response["events"])
        raise TypeError(
            f"unknown formatting {formatting}. Expected one of: "
            f"raw, records, messages, df."
        )

    @property
    def cluster(self):
        return self.cluster_arn

    @property
    def task(self):
        return self.task_arn

    def docker_cmd_strings(self) -> str:
        """
        Returns a string containing `awscli` and `docker` shell commands
        to rebuild and push the image associated with this container.
        This is a convenience method only, and these commands are not
        guaranteed to work with all configurations. They will _definitely_
        not work unless all these assumptions are true:

        1. The image associated with this task is hosted on ECR
        2. You run these commands from the working directory containing
           the associated Dockerfile
        3. These commands are executed from an account with permissions
           to push this image
        """
        image_uri = self.definition["containerDefinitions"][0]["image"]
        return docker_push_commands_for_ecr(image_uri)

    def stop(self):
        """
        Attempts to stop the task using the StopTask action. This action sends
        SIGTERM, then, after a delay, SIGKILL (the delay is not configurable
        at call time, but defaults to 30s).

        See API documentation for details:
        https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_StopTask.html
        """
        self.ecs.stop_task(cluster=self.cluster, task=self.task)

    def wait_while_pending(self, poll=0.1, timeout=60):
        """
        Blocks until the task's status is not 'PENDING' or `timeout` seconds
        have passed.
        """
        self.update()
        start = time.time()
        while self.status == "PENDING":
            if time.time() - start > timeout:
                raise TimeoutError(f"Task still pending after {timeout} s")
            time.sleep(poll)
            self.update()

    def __str__(self):
        parts = [
            f"group: {self.group}",
            f"task: {self.task}",
            f"cluster: {self.cluster}",
            f"launch type: {self.launch_type}",
            f"az: {self.az}",
            f"status: {self.status}",
        ]
        if self.name is not None:
            parts = [f"name: {self.name}"] + parts
        return "\n".join(parts)

    def __repr__(self):
        return self.__str__()

    name: str | None
    launch_type: str
    id_: str
    az: str
    definition_arn: str
    status: str
    log_group_name: str | None
    log_stream_name: str | None
    group: None
