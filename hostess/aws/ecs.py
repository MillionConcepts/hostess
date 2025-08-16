from typing import Optional, Any, Sequence

import boto3
import botocore.client

from hostess.aws.utilities import tagfilter, tag_dict, init_client


def summarize_task_description(task):
    container = task['containers'][0]
    return {
        'az': task['availabilityZone'],
        'id': container.get('runtimeId'),
        'group': task['group'],
        'status': container['lastStatus'],
        'container_arn': container['containerArn'],
        'task_arn': container['taskArn'],
        'cluster_arn': task['clusterArn'],
        'definition_arn':  task['taskDefinitionArn'],
        'image': container['image'],
        'start': task.get('startedAt'),
        'container_name': container['name'],
        'name': tag_dict(task.get('tags', {})).get('Name'),
        'launch_type': task['launchType']
    }


def definition_from_summary(client, summary):
    return client.describe_task_definition(
        taskDefinition=summary['definition_arn']
    )['taskDefinition']


def summarize_task_def(def_):
    cont = def_['containerDefinitions'][0]
    return {
        'family': def_['family'],
        'log_config': cont['logConfiguration']
    }


def ls_tasks(
    cluster: str | None = None,
    task_arn: str | None = None,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
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
        clusters = [c for c in client.list_clusters()['clusterArns']]
    else:
        clusters = [cluster]
    filts = {} if raw_filters is None else raw_filters
    summaries, task_defs = [], {}
    for c in clusters:
        if not (arns := client.list_tasks(cluster=c, **filts)['taskArns']):
            continue
        if task_arn is not None and task_arn not in arns:
            continue
        elif task_arn is not None:
            arns = [a for a in arns if a == task_arn]
        tasks = client.describe_tasks(
            cluster=c, tasks=arns, include=['TAGS']
        )['tasks']
        csums = [summarize_task_description(t) for t in tasks]
        filtered = []
        for cs in csums:
            if not tagfilter(cs, tag_filters, tag_regex):
                continue
            if cs['definition_arn'] in task_defs:
                def_ = task_defs[cs['definition_arn']]
            else:
                def_ = definition_from_summary(client, cs)
                task_defs[cs['definition_arn']] = def_
            dsum = summarize_task_def(def_)
            if dsum.get('log_config', {}).get('logDriver') == 'awslogs':
                opts = dsum['log_config']['options']
                cs['log_group_name'] = opts['awslogs-group']
                cs['log_stream_name'] = '/'.join([
                    opts['awslogs-stream-prefix'],
                    cs['container_name'],
                    cs['task_arn'].rsplit('/')[-1],
                ])
            else:
                cs['log_stream_name'] = None
                cs['log_group_name'] = None
            filtered.append(cs)
        summaries += filtered
    return summaries


class ECSTask:
    def __init__(self, summary, session=None):
        self.summary = summary.copy()
        self.cluster_arn = summary['cluster_arn']
        self.task_arn = summary['task_arn']
        self.ecs = init_client('ecs', None, session)
        self.logs = init_client('logs', None, session)
        self.update()

    def update(self):
        listing = ls_tasks(
            cluster=self.cluster_arn,
            task_arn=self.task_arn
        )
        if not listing:
            self.status = 'MISSING'
            self.summary['status'] = 'MISSING'
            return
        summary = listing[0]
        self.summary = summary
        self.name = summary['name']
        self.launch_type = summary['launch_type']
        self.id_ = summary['id']
        self.az = summary['az']
        self.definition_arn = summary['definition_arn']
        self.status = summary['status']
        self.log_group_name = summary['log_group_name']
        self.log_stream_name = summary['log_stream_name']

    def tail_log(self):
        if self.log_group_name is None:
            raise ValueError("This task does not have readable logs.")
        return self.logs.get_log_events(
            logGroupName=self.log_group_name,
            logStreamName=self.log_stream_name,
            startFromHead=False
        )

    name: str | None
    launch_type: str
    id_: str
    az: str
    definition_arn: str
    status: str
    log_group_name: str | None
    log_stream_name: str | None