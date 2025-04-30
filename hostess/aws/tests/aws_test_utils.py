import atexit
import random
import signal
import sys
from string import ascii_lowercase

import boto3
import pytest
from botocore.exceptions import ClientError

AWS_CLEANUP_TASKS = {}


def randstr(k: int):
    return ''.join(random.choices(ascii_lowercase, k=k))


@pytest.fixture(scope="session")
def aws_reachable():
    """
    Check that we can access AWS at all. Avoids use of hostess
    code. This calls STS GetCallerIdentity, which is always available to every
    AWS account even if an administrator attempts to explicitly remove
    permissions for it. If this fails, there could be a regression in boto3,
    but it probably indicates an expired account, missing or mangled local
    config/credential files, or a network issue.
    """
    session = boto3.Session()
    sts = session.client("sts")
    try:
        ident = sts.get_caller_identity()
    except ClientError as ce:
        raise OSError(
            f"Can't reach AWS: {ce}. Skipping all live AWS tests. Check "
            f"network status and local account configuration."
        )
    return ident


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
            f"Failed to clean up test bucket {name}: {ce}. This likely "
            f"indicates missing permissions for object or bucket deletion. "
            f"Manual cleanup of this bucket is required."
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
    prep cleanup for temp AWS resources on unexpected system exit or SIGTERM
    """
    atexit.register(do_aws_fallback_cleanup)
    # NOTE: we don't need to install a signal handler for SIGINT; pytest
    #  handles that in fixtures
    # TODO, maybe: other signals are still going to leave resources hanging
    #  around and prevent us from saying anything about it
    signal.signal(signal.SIGTERM, do_aws_fallback_cleanup)


@pytest.fixture(scope="session")
def aws_cleanup_tasks(aws_fallback_cleanup):
    return AWS_CLEANUP_TASKS




#
# @pytest.fixture(scope="session")
# def do_aws_cleanup():
#