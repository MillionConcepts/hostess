import csv
from operator import contains
import os
import re
from typing import Optional

import boto3
import boto3.resources.base
import botocore.client
from cytoolz import identity, flip
from cytoolz.curried import mapcat, get


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


def tag_dict(tag_records, lower=False):
    formatter = str.lower if lower is True else identity
    return {formatter(tag["Key"]): tag["Value"] for tag in tag_records}


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
    for disliked_kwarg in ("user_name", "password"):
        if disliked_kwarg in creds.keys():
            del creds[disliked_kwarg]
    return boto3.Session(**creds, region_name=region)


def make_boto_client(service, profile=None, credential_file=None, region=None):
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


def tagfilter(description, filters, regex=True):
    tags = tag_dict(description.get("Tags", []), lower=True)
    # noinspection PyArgumentList
    matcher = flip(contains) if regex is False else re.search
    for key, value in filters.items():
        if key.lower() not in tags.keys():
            return False
        if not matcher(value, tags[key.lower()]):
            return False
    return True


# noinspection PyProtectedMember
def clarify_region(region=None, boto_obj=None):
    if region is not None:
        return region
    if "region_name" in dir(boto_obj):
        return boto_obj.region_name
    elif "_client_config" in dir(boto_obj):
        return boto_obj._client_config.region_name
    raise AttributeError(f"Don't know how to read region from {boto_obj}.")


def autopage(client, operation, agg=None, *args, **kwargs):
    assert client.can_paginate(operation)
    if isinstance(agg, str):
        agg = mapcat(get(agg))
    pager = iter(client.get_paginator(operation).paginate(*args, **kwargs))
    if agg is not None:
        return tuple(agg(pager))
    page = next(pager)
    lengths = [(k, len(v)) for k, v in page.items() if isinstance(v, list)]
    aggkey = [k for k, v in lengths if v == max([v for _, v in lengths])][0]
    return tuple(get(aggkey)(page) + list(mapcat(get(aggkey))(pager)))


def whoami(client=None, session=None):
    sts = init_client("sts", client, session)
    response = sts.get_caller_identity()
    return {
        'user_id': response['UserId'],
        'account': response['Account'],
        'arn': response['Arn']
    }