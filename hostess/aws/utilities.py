from __future__ import annotations

import csv
import datetime as dt
from operator import contains
from pathlib import Path
import re
from typing import Optional, Union, Callable, Any, Sequence

import boto3
import boto3.resources.base
import botocore.client
from cytoolz import identity, flip, first
from cytoolz.curried import mapcat, get


def tag_dict(tag_records, lower=False):
    formatter = str.lower if lower is True else identity
    return {formatter(tag["Key"]): tag["Value"] for tag in tag_records}


def parse_aws_identity_file(
    path: Union[str, Path], profile: Optional[str] = None
) -> dict[str, str]:
    """
    Parse an AWS config, credentials, or downloaded IAM secrets file.

    Args:
        path: path to file
        profile: if specified, look for settings for this profile
            specifically. Otherwise, use the first profile in the file.
            Ignored if file appears to be an IAM secrets file.

    Returns:
        `dict` of parsed key-value pairs from identity file.

    Raises:
        OSError: if `profile` is specified but not in identity file, or if
            identity file is obviously malformatted.
    """
    with open(path) as config_file:
        lines = config_file.readlines()
    if "," in lines[0]:
        # this is a downloaded secret key file.
        parsed = next(csv.DictReader(iter(lines)))
        return {"_".join(k.lower().split(" ")): v for k, v in parsed.items()}
    parsed = {}
    if profile is not None:
        err, search = f"{profile} not described in identity file", f"[{profile}"
    else:
        err, search = "Identity file empty or malformatted", "["
    # TODO: I think this is a bug
    try:
        lineno = first(
            i for i, l in enumerate(lines) if l.strip().startswith(search)
        )
    except StopIteration:
        raise OSError(err)
    else:
        try:
            lineno = first(
                i for i, l in enumerate(lines) if l.strip().startswith("[")
            )
        except StopIteration:
            raise OSError("Identity file empty or malformatted")
    for line in lines[lineno + 1 :]:
        if line.strip().startswith("["):
            break
        try:
            parameter, value = map(str.strip, line.split("="))
            parsed[parameter] = value
        except ValueError:
            continue
    return parsed


def make_boto_session(
    profile: Optional[str] = None,
    credential_file: Optional[Union[str, Path]] = None,
    region: Optional[str] = None,
    **session_kwargs
) -> boto3.Session:
    """
    Create a new boto session.

    Args:
        profile: name of AWS profile to use (default profile if not specified)
        credential_file: path to credential file (looks in default credential
            path if not specified)
        region: name of AWS region, e.g. "us-east-1". (uses profile's default
            region if not specified)
        session_kwargs: passed directly to botocore Session constructor

    Returns:
        boto session.
    """
    if credential_file is None:
        return boto3.Session(profile_name=profile, region_name=region)
    creds = parse_aws_identity_file(credential_file, profile)
    for disliked_kwarg in ("user_name", "password"):
        if disliked_kwarg in creds.keys():
            del creds[disliked_kwarg]
    return boto3.Session(**creds, region_name=region)


def make_boto_client(
    service: str,
    profile: Optional[str] = None,
    credential_file: Optional[Union[str, Path]] = None,
    region: Optional[str] = None,
    **client_kwargs
) -> botocore.client.BaseClient:
    """
    Create a new boto client.

    Args:
        service: service to create client for, e.g. "ec2"
        profile: optional name of AWS profile to use (default profile if not
            specified)
        credential_file: optional path to credential file (looks in default
            credential path if not specified)
        region: optional name of AWS region, e.g. "us-east-1". (uses profile's
            default region if not specified)
        client_kwargs: passed directly to botocore client constructor

    Returns:
        boto client for service.
    """
    # a little redundant but whatever
    session = make_boto_session(profile, credential_file, region)
    return session.client(service, **client_kwargs)


def make_boto_resource(
    service: str,
    profile: Optional[str] = None,
    credential_file: Optional[Union[str, Path]] = None,
    region: Optional[str] = None,
    **resource_kwargs
) -> boto3.resources.base.ServiceResource:
    """
    Create a new boto resource.

    Args:
        service: service to create resource for, e.g. "ec2"
        profile: optional name of AWS profile to use (default profile if not
            specified)
        credential_file: optional path to credential file (looks in default
            credential path if not specified)
        region: optional name of AWS region, e.g. "us-east-1". (uses profile's
            default region if not specified)
        resource_kwargs: passed directly to boto resource constructor

    Returns:
        boto resource for service.
    """
    if "region_name" in resource_kwargs:
        region = resource_kwargs.pop("region_name")
    session = make_boto_session(profile, credential_file, region)
    return session.resource(service, **resource_kwargs)


def init_client(
    service: str,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    **client_kwargs
) -> botocore.client.BaseClient:
    """
    Utility function used throughout `hostess.aws` to selectively initialize
    boto clients.

    Args:
        service: service to produce client for (e.g. "ec2")
        client: if not None, simply return `client`
        session: if not None and `client` is None, initialize newly-made
            client using this session. Otherwise use a default session. Does
            nothing if `client` is not None.
        client_kwargs: passed to make_boto_client() or boto client constructor

    Returns:
        boto client for `service`.
    """
    if client is not None:
        return client
    if session is not None:
        if "region" in client_kwargs:
            client_kwargs["region_name"] = client_kwargs.pop("region")
        return session.client(service, **client_kwargs)
    return make_boto_client(service, **client_kwargs)


def init_resource(
    service: str,
    resource: Optional[boto3.resources.base.ServiceResource] = None,
    session: Optional[boto3.Session] = None,
) -> boto3.resources.base.ServiceResource:
    """
    Utility function used throughout `hostess.aws` to selectively initialize
    boto resources.

    Args:
        service: service to produce resource for (e.g. "ec2")
        resource: if not None, simply return `resource`
        session: if not None and `resource` is None, initialize newly-made
            resource using this session. Otherwise use a default session. Does
            nothing if `resource` is not None.

    Returns:
        boto resource for `service`.
    """
    if resource is not None:
        return resource
    if session is not None:
        return session.resource(service)
    return make_boto_resource(service)


def tagfilter(
    description: dict[str, Any], filters: dict[str, str], regex: bool = True
) -> bool:
    """
    Simple predicate function that permits resource matching based on Arrays of
    Tags in API responses related to that resource.
    See https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_Tag.html.

    Args:
        description: dict produced from an AWS API response.
        filters: dict of {tag_name: tag_value}. All tag names must be present,
            and their values must match the associated tag_values.
        regex: if True, treat the values of `filters` as regex expressions.
            if False, treat them as required substrings of tag values.

    Returns:
        True if all filters pass; False if any fail. Note that a filter will
            always fail if the API response simply lacks a Tags array.
    """
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
def clarify_region(region: Optional[str] = None, boto_obj: Any = None) -> str:
    """
    attempt to determine the AWS region associated with an object (presumably
    some kind of boto object).

    Args:
        region: if not None, this acts as a strict override: the function
            simply returns `region`.
        boto_obj: object whose AWS region to determine.

    Returns:
        name of AWS region (e.g. 'us-east-2').

    Raises:
        AttributeError: if `region` is None and we can't figure out how to read
            a region from `boto_obj`.
    """
    if region is not None:
        return region
    if "region_name" in dir(boto_obj):
        return boto_obj.region_name
    elif "_client_config" in dir(boto_obj):
        return boto_obj._client_config.region_name
    raise AttributeError(f"Don't know how to read region from {boto_obj}.")


def autopage(
    client: botocore.client.BaseClient,
    operation: str,
    agg: Optional[Union[str, Sequence[str], Callable]] = None,
    **api_kwargs: Any,
) -> tuple:
    """
    Perform an AWS API call that returns paginated results, greedily page
    through all of them, and return the aggregated results.

    Args:
        client: boto Client object to make API call
        operation: name of API call to perform
        agg: optional special aggregator. If `agg` is a `str`, it means:
            'concatenate the values of the key named `agg` from all pages of
            the response'. If `agg` is a sequence of `str`, it means:
            'concatenate the values of each of the named keys in `agg` from
            all pages of the response in separate lists'.  if `agg` is
            callable, it means: 'just feed the pager to `agg` and let it do
            its thing'. If not specified, aggregate the values of the key
            whose value is longest in the first response. (This is a heuristic
            for naively getting the actual responses and ignoring pagination
            metadata.)
        **api_kwargs: kwargs to pass to the API call.

    Returns:
        Tuple of aggregated responses, format depending on `agg`.
    """
    assert client.can_paginate(operation)
    if isinstance(agg, (str, Sequence)):
        agg = mapcat(get(agg))
    pager = iter(client.get_paginator(operation).paginate(**api_kwargs))
    if agg is not None:
        return tuple(agg(pager))
    page = next(pager)
    lengths = [(k, len(v)) for k, v in page.items() if isinstance(v, list)]
    aggkey = [k for k, v in lengths if v == max([v for _, v in lengths])][0]
    return tuple(get(aggkey)(page) + list(mapcat(get(aggkey))(pager)))


def whoami(
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
) -> dict[str, str]:
    """
    Make an STS GetCallerIdentity request and return just the essentials from
    the response.

    Note that _every_ AWS account is allowed to make this request: attempts to
    deny access to the sts:GetCallerIdentity action don't do anything. That
    means that if other AWS operations are failing, you can call this function
    to rule out a couple of very basic failure modes. If it fails, it means
    that either your credentials for the account are invalid or you can't
    connect to AWS at all (your network is down or blocking access to the API
    endpoint).

    Args:
        client: optional STS client.
        session: optional boto Session.

    Returns:
        `dict` with keys 'user_id', 'account', and 'arn'.
    """
    sts = init_client("sts", client, session)
    response = sts.get_caller_identity()
    return {
        "user_id": response["UserId"],
        "account": response["Account"],
        "arn": response["Arn"],
    }


def _check_cached_results(
    path: Path, prefix: str, max_age: float = 5
) -> Optional[Path]:
    """
    check for a 'fresh' cached API call by string-matching against filename
    prefix and our standardized in-filename timestamp format.

    Args:
        path: path to check for results
        prefix: filename prefix for results
        max_age: age, in days, after which a result is no longer 'fresh'

    Returns:
        Path for first matching result, if it is 'fresh'. None if it is not
            fresh or there is no matching result.
    """
    cache_filter = filter(lambda p: p.name.startswith(prefix), path.iterdir())
    try:
        result = first(cache_filter)
        timestamp = re.search(
            r"(\d{4})_(\d{2})_(\d{2})T(\d{2})_(\d{2})_(\d{2})", result.name
        )
        cache_age = dt.datetime.now() - dt.datetime(
            *map(int, timestamp.groups())
        )
        if cache_age.days > max_age:
            return None
    except StopIteration:
        return None
    return result


def _clear_cached_results(folder: Path, pre: str):
    """
    quick way to delete all files in folder whose names begin with `pre`.

    Args:
        folder: folder to clear.
        pre: filename prefix that triggers deletion.
    """
    for result in filter(lambda p: p.name.startswith(pre), folder.iterdir()):
        result.unlink()
