import atexit
import random
import signal
from pathlib import Path
from string import ascii_lowercase

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from hostess.aws.utilities import init_client

AWS_CLEANUP_TASKS = {}


def randstr(k: int):
    return ''.join(random.choices(ascii_lowercase, k=k))





def empty_bucket(name, *, delete_bucket=False):
    """Delete everything in a bucket."""
    try:
        session = boto3.Session()
        s3 = session.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in iter(paginator.paginate(Bucket=name)):
            objects = page.get("Contents", [])
            keys += [o["Key"] for o in objects]
        for key in keys:
            s3.delete_object(Bucket=name, Key=key)
        if delete_bucket is True:
            s3.delete_bucket(Bucket=name)
    except ClientError as ce:
        raise OSError(
            f"Failed to clean up test bucket {name}: {ce}. This may "
            f"indicate missing DeleteObject or DeleteBucket permissions. "
            f"Manual cleanup of this bucket is required."
        )

def _termination_error(instance_identifier, exc):
    return OSError(
        f"Failed to terminate test instance {instance_identifier} : {type(exc)}: {exc}. This may indicate "
        f"missing TerminateInstances permissions. Manual cleanup of this "
        f"instance is required."
    )


def terminate_instance(instance_name):
    from hostess.aws.ec2 import Instance
    try:
        inst = Instance.find(name=instance_name, verbose=False)
    except Exception as ex:
        raise _termination_error(instance_name, ex)
    try:
        inst.terminate()
    except Exception as ex:
        raise _termination_error(
            f"{inst.name} ({inst.instance_.id}) @ {inst.ip}", ex
        )


def delete_security_group(security_group_name):
    try:
        client = init_client("ec2")
        client.delete_security_group(GroupName=security_group_name)
    except ClientError as ce:
        raise OSError(
            f"Failed to delete test security group {security_group_name}: "
            f"{ce}. This may indicate missing DeleteSecurityGroup "
            f"permissions. Manual cleanup of this security group is required."
        )


# TODO: we could implement writing keys to tempfiles to remove the necessity
#  for this, but it would really only be useful for tests
def delete_keyfile(keyfile_path):
    try:
        Path(keyfile_path).unlink()
    except (OSError, PermissionError) as oe:
        raise OSError(
            f"Failed to delete test keyfile {keyfile_path}. Manual cleanup of "
            f"this file is required."
        )


def do_aws_fallback_cleanup(signum=None, _frame=None):
    # TODO: log or something
    for task_name in tuple(AWS_CLEANUP_TASKS.keys()):
        AWS_CLEANUP_TASKS.pop(task_name)()
    if signum == signal.SIGTERM:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.raise_signal(signal.SIGTERM)


@pytest.fixture(scope="session")
def aws_fallback_cleanup():
    """
    prep cleanup for temp AWS resources on SIGTERM or unexpected system exit
    """
    atexit.register(do_aws_fallback_cleanup)
    # NOTE: we don't need to install a signal handler for SIGINT; pytest
    #  handles that for fixtures
    # TODO, maybe: other signals are still going to leave resources hanging
    #  around and prevent us from saying anything about it. Addressing this is
    #  nontrivial and imposes tradeofffs.
    signal.signal(signal.SIGTERM, do_aws_fallback_cleanup)


@pytest.fixture(scope="session")
def aws_cleanup_tasks(aws_fallback_cleanup):
    return AWS_CLEANUP_TASKS
