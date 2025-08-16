import time
from typing import Optional, Any, Sequence, Collection, Literal

import boto3
import botocore.client
import pandas as pd

from hostess.aws.utilities import tagfilter, tag_dict, init_client


def summarize_task_description(task):
    container = task["containers"][0]
    return {
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
        "name": tag_dict(task.get("tags", {})).get("Name"),
        "launch_type": task["launchType"],
    }


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
    status: Collection[Literal["RUNNING", "STOPPED"]]
    | Literal["RUNNING", "STOPPED"] = ("RUNNING", "STOPPED"),
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
        if arns is None:
            continue
        if task_arn is not None and task_arn not in arns:
            continue
        elif task_arn is not None:
            arns = [a for a in arns if a == task_arn]
        tasks = []

        tasks = client.describe_tasks(cluster=c, tasks=arns, include=["TAGS"])[
            "tasks"
        ]
        csums = [summarize_task_description(t) for t in tasks]
        filtered = []
        for cs in csums:
            if not tagfilter(cs, tag_filters, tag_regex):
                continue
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
        image_name = image_uri.split("/")[-1].split(":")[0]
        image_tag = image_uri.split("/")[-1]
        region = self.ecs._client_config.region_name
        endpoint = image_uri.split('/')[0]
        auth_token_cmd = (
            f"aws ecr get-login-password --region {region} | "
            f"docker login --username AWS --password-stdin {endpoint}"
        )
        build_cmd = f"docker build -t {image_name} ."
        tag_cmd = f"docker tag {image_tag} {image_uri}"
        push_cmd = f"docker push {image_uri}"
        return "\n".join([auth_token_cmd, build_cmd, tag_cmd, push_cmd])

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
