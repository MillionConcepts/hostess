import datetime as dt
import io
import json
import sys
from functools import partial
from inspect import getmembers
from multiprocessing import Process
from pathlib import Path
from typing import (
    Union,
    Optional,
    Literal,
    Iterator,
    MutableMapping,
    Mapping,
    Callable,
    IO,
    Any,
)
import warnings

import boto3
import boto3.resources.base
import boto3.s3.transfer
import botocore.client
import requests
from dustgoggles.func import naturals

from killscreen.aws.utilities import init_client

# general note: we're largely avoiding use of the boto3 s3.Bucket object,
# as it performs similar types of abstraction which collide with ours -- for
# instance, it often prevents us from getting the API's responses, which we
# may in some cases want.
from killscreen.subutils import piped, clean_process_records
from killscreen.utilities import stamp, console_and_log, infer_stream_length


class Bucket:
    def __init__(
        self, bucket_name: str, client=None, session=None, config=None
    ):
        self.client = init_client("s3", client, session)
        self.name = bucket_name
        self.contents = []
        if config is None:
            config = boto3.s3.transfer.TransferConfig()
        self.config = config
        # populate s3 operations
        for name, thing in getmembers(sys.modules[__name__]):
            if name not in Bucket.__annotations__.keys():
                continue
            setattr(self, name, partial(thing, self, config=self.config))

    # annotations to aid both population and static analysis
    abort_multipart_upload: Callable
    complete_multipart_upload: Callable
    create_multipart_upload: Callable
    freeze: Callable
    get: Callable
    ls: Callable
    put: Callable
    put_stream: Callable
    put_stream_chunk: Callable
    rm: Callable

    def update_contents(self, prefix="", cache=None):
        self.contents = self.ls(
            recursive=True, prefix=prefix, cache=cache, formatting="contents"
        )

    def chunk_putter_factory(
        self,
        key: str,
        upload_threads: Optional[int] = 4,
        download_threads: Optional[int] = None,
        config=None,
        verbose: bool = False,
    ):
        """
        construct a callable chunk uploader. this can be used in relatively
        direct ways or passed to complex pipelines as a callback.
        """
        if config is None:
            config = self.config
        if download_threads is not None:
            raise NotImplementedError(
                "Asynchronous downloads are not yet implemented; "
                "please pass download_threads=None"
            )
        # threaded = any(
        #     map(lambda x: isinstance(x, int),
        #         [upload_threads, download_threads])
        # )
        parts = {}
        multipart = self.create_multipart_upload(key, config=config)
        kwargs = {
            "config": self.config,
            "download_cache": [b""],
            "multipart": multipart,
            "upload_numerator": naturals(),
            "parts": parts,
            "upload_threads": upload_threads,
            "verbose": verbose,
        }
        return partial(self.put_stream_chunk, **kwargs), parts, multipart

    @property
    def df(self):
        import pandas as pd

        if len(self.contents) == 0:
            self.update_contents()
        return pd.DataFrame(self.contents)

    @classmethod
    def bind(
        cls,
        bucket,
        client: Optional[botocore.client.BaseClient] = None,
        session: Optional[boto3.Session] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
    ):
        if isinstance(bucket, Bucket):
            return bucket
        return Bucket(bucket, client=client, session=session, config=config)


def put(
    bucket: Union[str, Bucket],
    obj: Union[str, Path, io.IOBase, bytes, None] = None,
    key: Optional[str] = None,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.s3.transfer.TransferConfig] = None,
    pass_string: bool = False,
    config=None,
):
    """
    Upload a file or buffer to an S3 bucket

    :param bucket: bucket name as str, or Bucket object
    :param obj: str, Path, or filelike / buffer object to upload; None for
        'touch' behavior
    :param client: name of bucket to upload it to, or bound boto Bucket object
    :param key: S3 key. If not specified then str(
        file_or_buffer) is used -- will most likely look bad if it's a buffer
    :param client: boto s3 client; makes a default client if None; used
        only if bucket is a bucket name as string and not an already-bound
        Bucket
    :param session: botocore.session.Session instance; used only if resource
        is None, makes a default resource from that session
    :param pass_string -- write passed string directly to file instead of
        interpreting as a path
    :param config: boto3.s3.transfer.TransferConfig; default if None
    :return: API response
    """
    bucket = Bucket.bind(bucket, client, session)
    if config is None:
        config = bucket.config
    # If S3 key was not specified, use string rep of
    # passed object, up to 1024 characters
    key = str(obj)[:1024] if key is None else key
    # 'touch' - type behavior
    if obj is None:
        obj = io.BytesIO()
    # directly upload file from local storage
    if (isinstance(obj, str) and not pass_string) or (isinstance(obj, Path)):
        return bucket.client.upload_file(
            Bucket=bucket.name, Filename=str(obj), Key=key, Config=config
        )
    # or: upload in-memory objects
    # encode string to bytes if we're writing it to S3 object instead
    # of interpreting it as a path
    if isinstance(obj, str) and pass_string:
        obj = io.BytesIO(obj.encode("utf-8"))
    elif isinstance(obj, bytes):
        obj = io.BytesIO(obj)
    return bucket.client.upload_fileobj(
        Bucket=bucket.name, Fileobj=obj, Key=key, Config=config
    )


def get(
    bucket: Union[str, Bucket],
    key: str,
    destination: Union[str, Path, io.IOBase, None] = None,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    config=None,
) -> tuple[Union[Path, str, io.IOBase], Any]:
    # TODO: add more useful error messages for streams opened in text mode
    bucket = Bucket.bind(bucket, client, session)
    if config is None:
        config = bucket.config
    if destination is None:
        destination = io.BytesIO()
    if isinstance(destination, io.IOBase):
        response = bucket.client.download_fileobj(
            bucket.name, key, destination, Config=config
        )
    else:
        response = bucket.client.download_file(
            bucket.name, key, destination, Config=config
        )
    if "seek" in dir(destination):
        destination.seek(0)
    return destination, response


# TODO: scary, refactor
def ls(
    bucket: Union[str, Bucket],
    prefix: str = "",
    recursive=False,
    formatting: Literal["simple", "contents", "df", "raw"] = "simple",
    cache: Union[str, Path, io.IOBase, None] = None,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    start_after: str = "",
    cache_only=False,
    config=None,
) -> Union[tuple, "pd.DataFrame"]:
    # TODO: add more useful error messages for streams opened in text mode
    bucket = Bucket.bind(bucket, client, session)
    truncated = True
    responses = []
    while truncated is True:
        if len(responses) > 0:
            if "Contents" in responses[-1]:
                start_after = responses[-1]["Contents"][-1]["Key"]
            elif "CommonPrefixes" in responses[-1]:
                start_after = responses[-1]["CommonPrefixes"][-1]["Prefix"]
            else:
                raise ValueError("Can't interpret this truncated response.")
        list_objects = partial(
            bucket.client.list_objects_v2,
            Bucket=bucket.name,
            Prefix=prefix,
            StartAfter=start_after,
        )
        if recursive is False:
            response = list_objects(Delimiter="/")
            # try to automatically treat 'directories' as directories
            if (
                    ("Contents" not in response.keys())
                    and ("CommonPrefixes" in response.keys())
                    and not (prefix.endswith("/"))
                    and (prefix != "")
            ):
                response = list_objects(Prefix=f"{prefix}/", Delimiter="/")
        else:
            # note 'recursive' behavior is default -- the ListObjects* methods
            # don't respect prefix boundaries by default -- and this is
            # different from the default s3 behavior in awscli (though not
            # awscli s3api)
            response = list_objects()
        responses.append(response)
        truncated = response.get("IsTruncated")
        if (cache is not None) and (response.get("Contents") is not None):
            cache = Path(cache) if isinstance(cache, str) else cache
            if isinstance(cache, Path):
                stream = cache.open("a+")
            else:
                stream = cache
            try:
                contents = response['Contents']
                if len(responses) == 1:
                    stream.write(
                        ",".join([k for k in contents[0].keys()]) + "\n"
                    )
                if isinstance(contents[0].get("LastModified"), dt.datetime):
                    for rec in contents:
                        rec['LastModified'] = rec['LastModified'].isoformat()
                        stream.write(",".join(map(str, rec.values())) + "\n")
            finally:
                if isinstance(cache, Path):
                    stream.close()
                if cache_only and (len(responses) > 2):
                    del responses[0]
    if formatting == "raw":
        return responses
    contents, prefixes = [], []
    for r in responses:
        if "Contents" in r.keys():
            contents += r["Contents"]
        if "CommonPrefixes" in r.keys():
            prefixes += r["CommonPrefixes"]
    if formatting == "simple":
        return tuple(
            [obj["Key"] for obj in contents]
            + [obj["Prefix"] for obj in prefixes]
        )
    if formatting == "contents":
        return tuple(contents)
    if formatting == "df":
        import pandas as pd

        return pd.DataFrame(contents)
    warnings.warn(
        f"formatting class {formatting} not recognized, returning as 'simple'"
    )
    return tuple([obj["Key"] for obj in contents])


def rm(
    bucket: Union[str, Bucket],
    key: str,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    config=None,
):
    bucket = Bucket.bind(bucket, client, session)
    return bucket.client.delete_object(Bucket=bucket.name, Key=key)


def ls_multipart(
    bucket: Union[str, Bucket],
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    config=None
):
    bucket = Bucket.bind(bucket, client, session, config)
    return bucket.client.list_multipart_uploads(Bucket=bucket.name)


def create_multipart_upload(
    bucket: Union[str, Bucket],
    key: str,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    config=None,
):
    bucket = Bucket.bind(bucket, client, session, config)
    return bucket.client.create_multipart_upload(Bucket=bucket.name, Key=key)


def abort_multipart_upload(
    bucket: Union[str, Bucket],
    multipart: Mapping,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    config=None,
):
    bucket = Bucket.bind(bucket, client, session, config)
    return bucket.client.abort_multipart_upload(
        Bucket=bucket.name,
        Key=multipart["Key"],
        UploadId=multipart["UploadId"],
    )


def complete_multipart_upload(
    bucket: Union[str, Bucket],
    multipart: Mapping,
    parts: Mapping,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    config=None,
):
    bucket = Bucket.bind(bucket, client, session, config)
    return bucket.client.complete_multipart_upload(
        Bucket=bucket.name,
        Key=multipart["Key"],
        UploadId=multipart["UploadId"],
        MultipartUpload={
            "Parts": [
                {"ETag": part["ETag"], "PartNumber": number}
                for number, part in parts.items()
            ]
        },
    )


def freeze(
    bucket: Union[str, Bucket],
    key: str,
    client: Optional[botocore.client.BaseClient] = None,
    session: Optional[boto3.Session] = None,
    config=None,
):
    # unlike awscli s3, storage class can't be changed for objects > 5 GB in
    # boto3 / s3api without using explicit unmanaged multipart copy, which
    # I'll need to implement separately.
    raise NotImplementedError


def put_stream(
    bucket: Union[str, Bucket],
    obj: Union[Iterator, Callable, IO, str, Path],
    key: str,
    client: Optional[botocore.client.BaseClient] = None,
    config=None,
    session: Optional[boto3.Session] = None,
    upload_threads: Optional[int] = 4,
    download_threads: Optional[int] = None,
    verbose: bool = False,
    explicit_length: Optional[int] = None,
    # TODO: overrides chunksize in config -- maybe make an easier interface
    #  to this
    chunksize: Optional[int] = None,
):
    """
    :param bucket:
    :param key:
    :param obj:
    :param client:
    :param config:
    :param session:
    :param upload_threads:
    :param download_threads:
    :return:
    """
    bucket = Bucket.bind(bucket, client, session, config)
    if config is None:
        config = bucket.config
    if chunksize is not None:
        config.multipart_chunksize = chunksize
    stream = obj
    if isinstance(stream, str):
        if stream.startswith("http") and ("//" in stream):
            stream = requests.get(stream, stream=True)
        elif stream.startswith("s3") and ("//" in stream):
            raise NotImplementedError(
                "dispatch for handling s3 urls is not yet implemented. "
                "please use something else for now."
            )
    if isinstance(stream, requests.Response):
        stream.raise_for_status()
    target_chunksize = config.multipart_chunksize
    if explicit_length is not None:
        length = explicit_length
    else:
        length = infer_stream_length(stream)
    if (length is not None) and (length < config.multipart_chunksize):
        if verbose:
            console_and_log(
                "Stream shorter than chunksize, falling back to basic put op."
            )
        return bucket.put(obj=stream, key=key, config=config)
    if isinstance(stream, (str, Path)):
        stream = Path(stream).open("rb")
    if isinstance(stream, requests.Response):
        stream = stream.iter_content(chunk_size=target_chunksize)
    if "read" in dir(stream):
        reader = partial(stream.read, target_chunksize)
    elif "__next__" in dir(stream):
        reader = stream.__next__
    else:
        raise TypeError("can't determine how to consume bytes from stream.")
    put_chunk, parts, multipart_upload = bucket.chunk_putter_factory(
        key, upload_threads, download_threads, config, verbose
    )
    try:
        chunk = reader()
        if len(chunk) == 0:
            raise ValueError("Empty stream.")
        while len(chunk) > 0:
            chunk = reader()
            put_chunk(chunk)
    except StopIteration:
        pass
    except ValueError:
        bucket.abort_multipart_upload(multipart=multipart_upload)
        raise
    del chunk
    put_chunk(b"", flush=True)
    if upload_threads is not None:
        clean_process_records(parts)
    del put_chunk
    parts = {number: part["result"] for number, part in parts.items()}
    return bucket.complete_multipart_upload(
        multipart=multipart_upload, parts=parts
    )


def put_stream_chunk(
    bucket: Bucket,
    blob: bytes,
    download_cache: list[bytes],
    parts: MutableMapping,
    multipart: Mapping,
    upload_numerator: Iterator[int],
    config: boto3.s3.transfer.TransferConfig,
    upload_threads: Optional[int],
    verbose: bool = False,
    flush: bool = False,
):
    download_cache[0] += blob
    if (len(download_cache[0]) < config.multipart_chunksize) and (
        flush is False
    ):
        return
    number = next(upload_numerator)
    if verbose is True:
        infix = "received" if flush is False else "flushing buffer as"
        console_and_log(
            f"{stamp()}: {infix} chunk {number} for {multipart['Key']}, "
            f"initiating upload"
        )
    kwargs = {
        "Body": download_cache.pop(),
        "Bucket": bucket.name,
        "Key": multipart["Key"],
        "PartNumber": number,
        "UploadId": multipart["UploadId"],
    }
    download_cache.append(b"")
    if upload_threads is not None:
        clean_process_records(parts, block_threshold=upload_threads)
        pipe, remote = piped(bucket.client.upload_part)
        process = Process(target=remote, kwargs=kwargs)
        process.start()
        parts[number] = {"process": process, "pipe": pipe}
    else:
        parts[number] = {"result": bucket.client.upload_part(**kwargs)}
