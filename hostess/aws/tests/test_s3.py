from itertools import chain
from pathlib import Path
import random
from tempfile import NamedTemporaryFile

import boto3
from botocore.exceptions import ClientError
import pytest

from hostess.aws.s3 import Bucket
from hostess.aws.tests.aws_test_utils import aws_reachable, randstr


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


@pytest.fixture(scope="module")
def temp_bucket_name():
    """
    Make a temporary S3 bucket for use by tests in this module, and clean it
    up when tests are done. Avoids use of hostess code. If this fails, it
    could be a regression in boto3, but there is probably something wrong
    with account permissions. Note that it is possible for this fixture to
    fail to clean up the temp bucket through no fault of its own if the account
    has permissions to create buckets but not to delete buckets, or to
    create objects but not delete objects. There is nothing we can do about
    this.
    """
    name = f"hostess-test-{randstr(20)}"
    try:
        session = boto3.Session()
        s3 = session.client("s3")
        response = s3.create_bucket(Bucket=name)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    except ClientError as ce:
        raise OSError(f"Unable to set up bucket: {ce}. Check AWS permissions.")
    except AssertionError:
        # noinspection PyUnboundLocalVariable
        raise OSError(
            f"Unable to set up bucket: "
            f"status {response['ResponseMetadata']['HTTPStatusCode']}."
            f"Check AWS permissions."
        )
    yield name
    empty_bucket(name, delete_bucket=True)


@pytest.fixture(scope="function")
def clean_temp_bucket(temp_bucket_name):
    """pytest fixture wrapper for empty_bucket()"""
    empty_bucket(temp_bucket_name)


@pytest.fixture(scope="module")
def temp_data():
    contents = tuple(
        random.randbytes(i * 100) for i in range(1, 10)
    )
    files = []
    for c in contents:
        f = NamedTemporaryFile()
        f.write(c)
        f.flush()
        f.seek(0)
        files.append(f)
    yield {"contents": contents, "files": files}

def test_roundtrip_disk(
    aws_reachable, temp_bucket_name, temp_data, clean_temp_bucket
):
    bucket = Bucket(temp_bucket_name)
    bucket.put(
        [f.name for f in temp_data["files"]],
        [Path(f.name).name for f in temp_data["files"]]
    )
    targets = [NamedTemporaryFile() for _ in temp_data["files"]]
    _results = bucket.get(
        [Path(f.name).name for f in temp_data["files"]],
        [t.name for t in targets]
    )
    for t, b in zip(targets, temp_data["contents"]):
        with open(t.name, "rb") as stream:
            assert stream.read() == b


# noinspection PyUnresolvedReferences
def test_roundtrip_mem(
    aws_reachable, temp_bucket_name, temp_data, clean_temp_bucket
):
    bucket = Bucket(temp_bucket_name)
    bucket.put(
        [c for c in temp_data["contents"]],
        [str(i) for i in range(len(temp_data["contents"]))]
    )
    bufs = bucket.get(
        [str(i) for i in range(len(temp_data["contents"]))]
    )
    for io_, b in zip(bufs, temp_data["contents"]):
        io_.seek(0)
        assert io_.read() == b


def test_ls_cases(
    aws_reachable, temp_bucket_name, temp_data, clean_temp_bucket
):
    bucket = Bucket(temp_bucket_name)
    bucket.put(
        [f.name for f in temp_data["files"]],
        [str(i) for i in range(len(temp_data["files"]))]
    )
    simple = bucket.ls(formatting="simple")
    contents = bucket.ls(formatting="contents")
    df = bucket.ls(formatting="df")
    for i, f in enumerate(temp_data["files"]):
        path = Path(f.name)
        assert simple[i] == str(i)
        assert contents[i]["Key"] == str(i)
        size = path.stat().st_size
        # TODO: this could be wrong
        assert contents[i]["Size"] == size
        row = df.loc[i]
        assert row["Key"] == str(i)
        assert row["Size"] == size


def test_df(aws_reachable, temp_bucket_name, clean_temp_bucket):
    bucket = Bucket(temp_bucket_name)
    prefixes = {
        randstr(15): [randstr(25) for _ in range(5)] for _ in range(2)
    }
    all_keys = tuple(
        chain(*([f"{p}/{n}" for n in ns] for p, ns in prefixes.items()))
    )
    bucket.put([None for _ in all_keys], all_keys)
    df = bucket.df()
    assert set(df['Key']) == set(all_keys)
    for p, ns in prefixes.items():
        preslice = df.loc[df['Key'].str.startswith(p), 'Key']
        assert set(preslice.str.replace(f"{p}/", "")) == set(ns)
    assert (df['Size'] == 0).all()




# TODO: test various streaming features

# TODO: testing Bucket.freeze() or other storage-class manipulating
#  things is difficult. Not sure about charges for deleting a
#  0-byte GDA object, for one thing.

