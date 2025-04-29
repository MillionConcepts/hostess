import random
from string import ascii_lowercase

import boto3
import pytest
from botocore.exceptions import ClientError


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
