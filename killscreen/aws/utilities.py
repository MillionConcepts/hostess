import csv
import os
from typing import Optional

import boto3
import boto3.resources.base
import botocore.client


def clear_constructor(semaphore, chunkfile, part_number):
    """
    utility function for multipart upload process.
    deletes file and releases semaphore when chunk
    completes upload.
    """

    def clear_when_done(_command, _success, _exit_code):
        print("part number " + str(part_number) + " upload complete.")
        os.unlink(chunkfile)
        semaphore.release()
        return 0

    return clear_when_done


def tag_dict(tag_records):
    return {tag["Key"]: tag["Value"] for tag in tag_records}


def read_aws_config_file(path):
    with open(path) as config_file:
        lines = config_file.readlines()
    if "," in lines[0]:
        parsed = next(csv.DictReader(iter(lines)))
        return {"_".join(k.lower().split(" ")): v for k, v in parsed.items()}
    parsed = {}
    # TODO: allow selection of non-default profiles
    for line in lines:
        try:
            parameter, value = map(str.strip, line.split("="))
            parsed[parameter] = value
        except ValueError:
            continue
    return parsed


def make_boto_session(profile=None, credential_file=None, region=None):
    if credential_file is None:
        return boto3.Session(profile_name=profile, region_name=region)
    creds = read_aws_config_file(credential_file)
    for disliked_kwarg in ('user_name', 'password'):
        if disliked_kwarg in creds.keys():
            del creds[disliked_kwarg]
    return boto3.Session(**creds, region_name=region)


def make_boto_client(
    service, profile=None, credential_file=None, region=None
):
    session = make_boto_session(profile, credential_file, region)
    return session.client(service)


def make_boto_resource(
    service, profile=None, credential_file=None, region=None
):
    session = make_boto_session(profile, credential_file, region)
    return session.resource(service)


def init_client(
    service: str,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
) -> botocore.client.BaseClient:
    if client is not None:
        return client
    if session is not None:
        return session.client(service)
    return make_boto_client(service)


def init_resource(
    service: str,
    resource: Optional[boto3.resources.base.ServiceResource] = None,
    session: Optional[boto3.Session] = None,
):
    if resource is not None:
        return resource
    if session is not None:
        return session.resource(service)
    return make_boto_resource(service)
