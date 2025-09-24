"""
This module provides managed operations on AWS S3 objects. Its centerpiece
is a Bucket object providing a high-level interface to operations on a single
S3 bucket. This module was originally motivated by the need to quickly
construct pandas DataFrames containing inventories of large buckets.

Much of this module wraps lower-level methods of boto3, so it is in some sense
an alternative implementation of boto3's own high-level managed S3 methods
and objects, designed for cases in which deeper indexing, more response
introspection, or more flexible I/O stream manipulation are required. We also
like the syntax better.
"""
import datetime as dt
import inspect
import re
import time
import warnings
from collections.abc import MutableSequence
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from functools import partial, wraps, cached_property
from io import BytesIO, IOBase, StringIO
from itertools import chain, repeat
from pathlib import Path
from typing import (
    Any,
    IO,
    Iterator,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Union,
    Sequence,
    Callable,
    TypeVar,
    ParamSpec, BinaryIO, TextIO, Concatenate, overload
)

import boto3
import boto3.resources.base
import boto3.s3.transfer
import botocore.client
from botocore.exceptions import ClientError
from cytoolz import keyfilter, valfilter
from dustgoggles.func import naturals
import pandas as pd
import requests
from s3transfer.manager import TransferManager

from hostess.aws.s3transfer_extensions.threadpool_range import (
    TransferManagerWithRange
)
from hostess.aws.utilities import init_client, init_resource
from hostess.config.config import S3_DEFAULTS
from hostess.utilities import (
    console_and_log, infer_stream_length, stamp, StoppableFuture
)

Puttable = Union[str, Path, IOBase, bytes, None]
"""type alias for Python objects Bucket will write to S3 """

P = ParamSpec("P")
R = TypeVar("R")
B = TypeVar("B")
M = TypeVar("M")
N = TypeVar("N")

BMethOne = Callable[Concatenate[B, M, P], R]
BMethTwo = Callable[Concatenate[B, M, N, P], R]
BMethOneList = Callable[
    Concatenate[B, M | Sequence[M], P],
    R | list[R]
]
BMethTwoList = Callable[
    Concatenate[B, M | Sequence[M], N | Sequence[N], P],
    R | list[R]
]


def split_threaded(method, bucket, seq_iter, bound):
    output = []
    with ThreadPoolExecutor(bucket.n_threads) as exc:
        futures = [
            exc.submit(method, bucket, *pre, **bound) for pre in seq_iter
        ]
        concurrent.futures.wait(futures)
        # loop over the original list rather than what .wait returns,
        # to preserve the original order
        for f in futures:
            try:
                output.append(f.result())
            except BaseException as ex:
                output.append(ex)
    return output


def split_sequential(method, bucket, seq_iter, bound):
    output = []
    for pre in seq_iter:
        try:
            output.append(method(bucket, *pre, **bound))
        except Exception as ex:
            output.append(ex)
    return output


def splitwrap_arity_1(method: BMethOne) -> BMethOneList:
    sig = inspect.signature(method)
    params = list(sig.parameters)
    defaults = {n: p.default for n, p in sig.parameters.items()}

    @wraps(method)
    def maybesplit_arity_1(*args, **kwargs):
        bound = sig.bind(*args, **kwargs).arguments
        source = bound.get(params[1], defaults[params[1]])
        sseq = hasattr(source, "index") and not isinstance(
            source, (str, bytes)
        )
        if sseq is False:
            return method(*args, **kwargs)
        seq_iter = ((x,) for x in source)
        bound.pop(params[1])
        bucket = bound.pop(params[0])
        if bucket.n_threads is None:
            return split_sequential(method, bucket, seq_iter, bound)
        else:
            return split_threaded(method, bucket, seq_iter, bound)
    return maybesplit_arity_1


def splitwrap_arity_2(method: BMethTwo) -> BMethTwoList:
    sig = inspect.signature(method)
    params = list(sig.parameters)
    defaults = {n: p.default for n, p in sig.parameters.items()}

    @wraps(method)
    def maybesplit_arity_2(*args, **kwargs):
        bound = sig.bind(*args, **kwargs).arguments
        source = bound.get(params[1], defaults[params[1]])
        target = bound.get(params[2], defaults[params[2]])
        sseq = hasattr(source, "index") and not isinstance(
            source, (str, bytes)
        )
        sseq_len = len(source) if sseq else -1
        if target is None:
            tseq = sseq
            tseq_len = sseq_len
            if sseq:
                target = repeat(target, tseq_len)
        else:
            tseq = hasattr(target, "index") and not isinstance(
                target, (str, bytes)
            )
            tseq_len = len(target) if tseq else -1
        if tseq != sseq:
            raise TypeError("Mismatched sequence/nonsequence call")
        if not sseq:
            return method(*args, **kwargs)
        if tseq_len != sseq_len:
            raise ValueError("Mismatched source/target sequence lengths")
        seq_iter = zip(source, target)
        bound.pop(params[1])
        bound.pop(params[2], None)
        bucket = bound.pop(params[0])
        if bucket.n_threads is None:
            return split_sequential(method, bucket, seq_iter, bound)
        else:
            return split_threaded(method, bucket, seq_iter, bound)
    return maybesplit_arity_2


@overload
def splitwrap(
    *, seq_arity: Literal[1]
) -> Callable[[BMethOne], BMethOneList]: ...
@overload
def splitwrap(
    *, seq_arity: Literal[2]
) -> Callable[[BMethTwo], BMethTwoList]: ...


def splitwrap(*, seq_arity: Literal[1, 2]) -> (
    Callable[[BMethOne], BMethOneList] | Callable[[BMethTwo], BMethTwoList]
):
    """
    Decorator for methods of Bucket that permits them to accept either single
    source and/or destination arguments or sequences of them. Automatically
    maps sequences into a thread pool, unless instructed to run serially,
    and returns all results in a list. Fails gracefully, returning Exceptions
    raised by any individual function call rather than raising them.
    """
    if seq_arity == 1:
        return splitwrap_arity_1
    elif seq_arity == 2:
        return splitwrap_arity_2
    raise ValueError(f'seq_arity must be 1 or 2, not {seq_arity!r}')


DIRECTORY_BUCKET_NAMEPAT = re.compile(
    r"[a-z0-9-.]{1,45}--[a-z]{3}\d-az\d--x-s3"
)
"""Legal name for zonal directory bucket"""
BUCKET_NAMEPAT = re.compile(r"[a-z0-9-.]{3,63}")
"""Legal name for general-purpose bucket"""
AZ_NAMEPAT = re.compile(r"[a-z]{2}-[a-z]{3,10}-\d(?P<letter>[a-z])")
"""Legal AZ name"""
AZ_IDPAT = re.compile(r"[a-z]{3}\d-az(?P<number>\d)")
"""Legal AZ id"""
BUCKET_TYPE = Literal["general", "directory"]
"""Pull account ID of owner out of directory bucket ARN"""
DIRECTORY_BUCKET_ARN_ACCOUNTPAT = re.compile(r".*?:(\d+):bucket/")

def _should_be_file(obj, literal_str):
    return (
        (isinstance(obj, str) and literal_str is False)
        or (isinstance(obj, Path))
    )


def _poll_obj(
    bucket: "Bucket",
    key: str,
    start_pos: int | None,
    _sigdict: dict[int, int | None],
    _id: int,
    text_mode: bool,
    destination: MutableSequence | BinaryIO | TextIO | IOBase | Path | str,
    poll: float,
    permit_missing: bool
):
    def _mayberaise(func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as ce:
            if permit_missing is True and "not found" in str(ce).lower():
                return None
            raise ce

    if start_pos is None:
        head = _mayberaise(bucket.head, key)
        start_pos = 0 if head is None else head['ContentLength']
    if isinstance(destination, str):
        destination = Path(destination)
    pos = start_pos
    while _sigdict.get(0) is None:
        result = _mayberaise(bucket.get, key, start_byte=pos)
        result = "" if result is None else result.read()
        if len(result) == 0:
            time.sleep(poll)
            continue
        pos += len(result)
        if text_mode is True:
            result = result.decode("utf-8")
        if isinstance(destination, MutableSequence):
            destination.append(result)
        elif isinstance(destination, Path):
            mode = "ab" if text_mode is False else "a"
            with destination.open(mode) as stream:
                stream.write(result)
        else:
            stream.write(result)
        time.sleep(poll)


def _attach_directory_bucket_suffix(name, azid):
    return f"{name}--{azid}--x-s3"


def _raise_for_owned_use1_bucket(client, name, bucket_type: BUCKET_TYPE):
    if bucket_type == "general":
        names = [b["Name"] for b in client.list_buckets()["Buckets"]]
    else:
        names = [b["Name"] for b in client.list_directory_buckets()["Buckets"]]
    if name in names:
        raise ValueError(
            f"You already own a bucket named {name} in us-east-1. "
            f"hostess does not support the legacy ACL-clearing behavior "
            f"associated with repeat CreateBucket operations in us-east-1."
        )


def check_az(az: str | int, region_name: str):
    """
    Validate existence of an AvailabilityZone in a particular region, and
    return its canonical Zone ID.

    Args:
        az: identifier for Availability Zone. May be its Zone Name, Zone ID,
            number, or letter.
        region_name: canonical, unabbreviated region name (e.g. 'us-east-1')

    Returns:
        Unabbreviated Zone ID for referenced Availability Zone.
    """
    if isinstance(az, int):
        az, ident = str(az), "number"
    elif not isinstance(az, str):
        raise TypeError(f"'az_name' must be int or string, got {type(az)}")
    elif len(az) == 1:
        ident = "number" if re.match(r"\d", az) else "letter"
    elif AZ_NAMEPAT.match(az):
        ident = "name"
    elif AZ_IDPAT.match(az):
        ident = "id"
    else:
        raise ValueError(f"Unrecognized pattern for 'az'.")
    ec2 = init_client("ec2", region=region_name)
    zones = ec2.describe_availability_zones()["AvailabilityZones"]
    if ident == "number":
        match = [z for z in zones if z['ZoneId'][-1] == az]
    elif ident == "name":
        match = [z for z in zones if z['ZoneName'] == az]
    elif ident == "letter":
        match = [z for z in zones if z['ZoneName'][-1] == az]
    else:
        match = [z for z in zones if z['ZoneId'] == az]
    if len(match) == 0:
        raise ValueError(f"No AZ in {region_name} found matching {az}")
    elif len(match) > 1:
        raise ValueError(
            "Multiple AZ matches. This may indicate a bug or an API error."
        )
    return match[0]['ZoneId']


def _dtstr(thing: Any):
    """
    convert thing to its isoformat() if it's a datetime, otherwise return
    its string representation. helper function for Bucket.ls() in cached mode.
    """
    if isinstance(thing, dt.datetime):
        return thing.isoformat()
    return str(thing)


class Bucket:
    """
    Interface to and representation of an S3 bucket.

    Note:
        Bucket supports only general-purpose and directory buckets, not
        table buckets.
    """

    def __init__(
        self,
        bucket_name: str,
        client: Optional[botocore.client.BaseClient] = None,
        resource: Optional[boto3.resources.base.ServiceResource] = None,
        session: Optional[boto3.session.Session] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
        n_threads: Optional[int] = 4,
    ):
        """
        Args:
            bucket_name: name of bucket
            client: optional boto3 s3 Client. if not specified, creates a
                default client.
            resource: optional boto3 s3 Resource. if not specified, creates a
                default resource.
            session: optional boto3 Session. if not specified, creates a
                default session.
            config: optional boto3 TransferConfig. if not specified, creates a
                default config.
            n_threads: if not None, automatically multithread some operations.
                note that this is not a hard cap on the number of threads used
                by a single bucket operation. it provides a cap on concurrency
                across operations on multiple objects, not on concurrency on
                operations per object. if you wish to cap concurrency within
                operations on individual objects, modify the `max_concurrency`
                attribute of `config`.
        """
        self.client = init_client("s3", client, session)
        self.resource = init_resource("s3", resource, session)
        self.session = session
        self.name = bucket_name
        self.contents = []
        if config is None:
            config = boto3.s3.transfer.TransferConfig(**S3_DEFAULTS["config"])
        self.config = config
        self.n_threads = n_threads

    @classmethod
    def create(
        cls,
        name: str,
        client: botocore.client.BaseClient | None = None,
        session: boto3.session.Session = None,
        *,
        bucket_type: BUCKET_TYPE = "general",
        az: str | int | None = None,
        tags: dict[str, str] | None = None,
        bucket_config: Mapping | None = None,
        **bucket_kwargs
    ):
        """
        Create a new bucket on S3 and return it as a Bucket object.

        Args:
            name: Name of bucket. If creating a directory bucket, do not
                include the AZ suffix (e.g. pass "something" instead of
                "something--use1-az4--x-s3"). `Bucket` will automatically add
                the correct suffix.
            client: optional boto3 s3 Client. if not specified, creates a
                default client.
            session: optional boto3 Session. if not specified, creates a
                default session.
            bucket_type: "general" (default, meaning a general-purpose bucket)
                or "directory" (meaning a directory bucket). Note that this
                method only supports zonal directory buckets.
            az: Name, letter, ID, or number of the Availability Zone (AZ) in
                which to create a directory bucket. For instance, if `session`
                is associated with the us-east-1 region, 'us-east-1c',
                `'use1-az4'`, `'c'`, and `4` all refer to the same AZ. Note
                that creating a bucket in a region other than the one `client`
                (or `session`, if `client` is not passed) is not supported.

                This argument is ignored for general-purpose buckets.

                Note that not all AZs support directory buckets, and there is
                no mechanism to discover which do and do not via the API
                (other than actually attempting to create one). See:
                https://docs.aws.amazon.com/AmazonS3/latest/userguide/endpoint-directory-buckets-AZ.html
            tags: Keys and values of bucket tags to set after successful
                  bucket creation. If not None, there must be at least one
                  item in this dict. Note that directory buckets do not
                  support tags; this method will raise a ValueError if
                  provided tags for a directory bucket.
            bucket_config: passed to the botocore `create_bucket()` method as
                the 'CreateBucketConfig' argument.
            bucket_kwargs: passed directly to `Bucket.__init__()`.

        Caution:
            In all regions other than us-east-1, the S3 API returns a
            'bucket aready exists and is owned by you' error if a user
            attempts to create a bucket with the same name as a bucket they
            already own. In us-east-1, it instead returns a standard success
            response and silently erases all ACLs associated with that bucket.
            We are unwilling to spring this on users, and for this reason,
            `Bucket.create()` behaves slightly differently in us-east-1. It
            checks the user already owns a bucket with the requested name,
            and raises an exception if so. This means that an account must
            have the ListBuckets permission to use `Bucket.create()` in
            us-east-1.
        """
        client = init_client("s3", client, session)
        if bucket_type not in ("general", "directory"):
            raise ValueError("'bucket_type' must be 'general' or 'directory'.")
        if bucket_type == "directory" and az is None:
            raise TypeError("'az' must not be None for a directory bucket.")
        elif bucket_type == "directory":
            azid = check_az(az, region_name=client.meta.region_name)
            name = _attach_directory_bucket_suffix(name, azid)
            pat = DIRECTORY_BUCKET_NAMEPAT
        else:
            pat, azid = BUCKET_NAMEPAT, None
        if pat.match(name) is None:
            raise ValueError(f"{name} is not a valid bucket name.")
        if client.meta.region_name == 'us-east-1':
            _raise_for_owned_use1_bucket(client, name, bucket_type)
        conf = dict(bucket_config) if bucket_config is not None else {}
        if bucket_type == "directory":
            if len({'Location', 'Bucket'}.intersection(conf.keys())) > 0:
                raise ValueError(
                    "Please do not specify custom 'Location' or 'Bucket' "
                    "values in bucket config for directory buckets."
                )
            conf |= {
                'Location': {'Type': "AvailabilityZone", 'Name': azid},
                'Bucket': {
                    'Type': 'Directory',
                    'DataRedundancy': 'SingleAvailabilityZone'
                }
            }

        # note that we're just relying on botocore for exceptions at this
        # stage. If it doesn't raise one, we assume it worked.
        kwargs = {'Bucket': name}
        if len(conf) > 0:
            kwargs['CreateBucketConfiguration'] = conf
        try:
            client.create_bucket(**kwargs)
        except ClientError as ce:
            if "InvalidBucketName" in str(ce) and bucket_type == "directory":
                raise ValueError(
                    f"Although {name} is a valid directory bucket name, the "
                    f"S3 API returned an InvalidBucketName error. This "
                    f"typically indicates that Availability Zone {az} in "
                    f"{client.meta.region_name} does not support directory "
                    f"buckets."
                )
            else:
                raise ce
        bucket = Bucket(name, client=client, **bucket_kwargs)
        if tags is not None:
            bucket.set_tags(**tags)
        return bucket

    def delete(self):
        """
        Delete this bucket.

        Notes:
            S3 will not delete a bucket that contains any objects, and
            `hostess` does not provide a 'force'-type operation that
            auto-empties a bucket before deletion.
        """
        self.client.delete_bucket(Bucket=self.name)

    def update_contents(
        self,
        prefix: Optional[str] = None,
        cache: Optional[Union[str, Path, IOBase]] = None,
        fetch_owner: bool = False
    ):
        """
        recursively scan the contents of the bucket and store the result in
        self.contents.

        Args:
            prefix: prefix at which to begin scan. if not passed, scans the
                entire bucket.
            cache: optional file or filelike object to write scan results to
                in addition to storing them in self.contents.
            fetch_owner: if True, include owner of objects in response.
        """
        self.contents = self.ls(
            recursive=True,
            prefix=prefix,
            cache=cache,
            formatting="contents",
            fetch_owner=fetch_owner,
        )

    def chunk_putter_factory(
        self,
        key: str,
        upload_threads: Optional[int] = 4,
        download_threads: Optional[int] = None,
        verbose: bool = False,
    ):
        """
        construct a callable chunk uploader. this can be used in relatively
        direct ways or passed to complex pipelines as a callback.
        """
        if download_threads is not None:
            raise NotImplementedError(
                "Asynchronous downloads are not yet implemented; "
                "please pass download_threads=None"
            )
        parts = {}
        multipart = self.create_multipart_upload(key)
        if upload_threads is None:
            exc = None
        else:
            exc = ThreadPoolExecutor(upload_threads)
        kwargs = {
            "config": self.config,
            "download_cache": [b""],
            "multipart": multipart,
            "upload_numerator": naturals(),
            "parts": parts,
            "exc": exc,
            "verbose": verbose,
        }
        return partial(self._put_stream_chunk, **kwargs), parts, multipart

    def df(self) -> pd.DataFrame:
        """
        Construct a manifest of all known objects in bucket as a pandas
        DataFrame. If update_contents() has never been called, greedily scan
        the contents of the bucket rather than returning an empty DataFrame.

        Returns:
            Manifest of all known objects in bucket.
        """
        if len(self.contents) == 0:
            self.update_contents()
        return pd.DataFrame(self.contents)

    def put_stream(
        self,
        obj: Union[Iterator, IO, str, Path],
        key: str,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
        upload_threads: Optional[int] = 4,
        # download_threads: Optional[int] = None,
        verbose: bool = False,
        explicit_length: Optional[int] = None,
        # TODO: overrides chunksize in config -- maybe make an easier interface
        #  to this
        chunksize: Optional[int] = None,
    ) -> Optional[dict]:
        """
        Create an S3 object from a byte stream via a managed multipart upload.
        Intended primarily for intermittent streams, incremental writes of
        larger-than-memory data, direct streams from remote resources, and
        streams of unknown length. If you are just uploading on-disk files
        or discrete in-memory objects, Bucket.put() is usuallygh preferable.

        Args:
            obj: source of stream to upload. May be a path, a URL, a filelike
                object, or any iterator that yields `bytes` objects.
            key: key of object to create from stream (fully-qualified 'path'
                relative to bucket root)
            config: optional transfer config
            upload_threads: number of subprocesses to use for upload (None
                means upload serially)
            verbose: print and log progress of streaming upload
            explicit_length: optional explicit length specification, for
                streams of known length
            chunksize: size of individual upload chunks; overrides any setting
                in config. if stream length is explicitly specified or inferred
                to be less than chunksize, this function will fall back to a
                simple put operation.

        Returns:
            API response to multipart upload completion, or None if stream
                length < chunksize and we fell back to simple upload
        """
        if config is None:
            config = self.config
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
                    "Stream shorter than chunksize, falling back to basic put."
                )
            return self.put(obj=stream, key=key, config=config)
        if isinstance(stream, (str, Path)):
            stream = Path(stream).open("rb")
        if isinstance(stream, requests.Response):
            stream = stream.iter_content(chunk_size=target_chunksize)
        if "read" in dir(stream):
            reader = partial(stream.read, target_chunksize)
        elif "__next__" in dir(stream):
            reader = stream.__next__
        else:
            raise TypeError(
                "can't determine how to consume bytes from stream."
            )
        put_chunk, parts, multipart_upload = self.chunk_putter_factory(
            key, upload_threads, None, verbose
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
            self.abort_multipart_upload(multipart=multipart_upload)
            raise
        del chunk
        put_chunk(b"", flush=True)
        del put_chunk
        if upload_threads is not None:
            while not all(f.done() for f in parts.values()):
                time.sleep(0.05)
            parts = {number: f.result() for number, f in parts.items()}
        return self.complete_multipart_upload(
            multipart=multipart_upload, parts=parts
        )

    def _put_stream_chunk(
        self,
        blob: bytes,
        download_cache: list[bytes],
        parts: MutableMapping,
        multipart: Mapping,
        upload_numerator: Iterator[int],
        config: boto3.s3.transfer.TransferConfig,
        exc: Optional[ThreadPoolExecutor],
        verbose: bool = False,
        flush: bool = False,
    ):
        """helper function for Bucket.put_stream()"""
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
            "Bucket": self.name,
            "Key": multipart["Key"],
            "PartNumber": number,
            "UploadId": multipart["UploadId"],
        }
        download_cache.append(b"")
        if exc is not None:
            parts[number] = exc.submit(self.client.upload_part, **kwargs)
        else:
            parts[number] = self.client.upload_part(**kwargs)

    @splitwrap(seq_arity=1)
    def freeze(
        self,
        key: Union[str, Sequence[str]],
        storage_class: str = "DEEP_ARCHIVE",
    ) -> Union[str, list[Union[str, Exception]]]:
        """
        Modify the storage class of an object or objects. Intended primarily
        for moving objects from S3 Standard to one of the Glacier classes.

        Args:
            key: object key(s) (fully-qualified 'path' relative to bucket root)
            storage_class: target storage class

        Returns:
            uri: URI of frozen object, or list containing URI of each
                frozen object if its freeze succeeded and an Exception if not
        """
        return self.cp(key, StorageClass=storage_class)

    @splitwrap(seq_arity=1)
    def restore(
        self,
        key: Union[str, Sequence[str]],
        tier: Literal["Expedited", "Standard", "Bulk"] = "Bulk",
        days: int = 5,
    ) -> Union[dict, list[Union[dict, Exception]]]:
        """
        Issue a request to temporarily restore one or more objects from S3
        Glacier Flexible Retrival or Deep Archive to S3 Standard. Note that
        object restoration is not instantaneous. Depending on retrieval tier
        and storage class, AWS guarantees retrieval times ranging from 5
        minutes to 48 hours. See https://docs.aws.amazon.com/AmazonS3/latest
        /userguide/restoring-objects-retrieval-options.html for details.

        You can check the progress of restore requests using Bucket.head().

        Args:
            key: key(s) of object(s) to restore (fully-qualified 'paths')
            tier: retrieval tier. In order of speed and expense, high to low,
                options are "Expedited", "Standard", and "Bulk". "Expedited" is
                not available for Deep Archive.
            days: number of days object(s) should remain restored before
                reverting to Glaciered state

        Returns:
            RestoreObject API response, or list of responses and/or Exceptions
        """
        restore_request = {
            "Days": days,
            "GlacierJobParameters": {"Tier": tier},
        }
        return self.client.restore_object(
            Bucket=self.name, Key=key, RestoreRequest=restore_request
        )

    @splitwrap(seq_arity=2)
    def put(
        self,
        obj: Union[Puttable, Sequence[Puttable]] = b"",
        key: Optional[Union[str, Sequence[str]]] = None,
        literal_str: bool = False,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
        **extra_args: str
    ) -> Union[None, list[Optional[Exception]]]:
        """
        Upload files or buffers to an S3 bucket

        Args:
            obj: An individual str, Path, or filelike / buffer object to
                upload, or a sequence of such objects
            key: S3 key (fully-qualified 'path' from bucket root); or, if `obj`
                is a sequence, a sequence of keys of the same length of `obj`.
                If `key` is not specified, key(s) are generated from the
                string representation(s) of the uploaded object(s), truncated
                to 1024 characters (maximum length of an S3 key).
            literal_str: If True, and `obj` is a string or a sequence
                containing strings, write all such strings directly to objects.
                Otherwise, interpret them as paths to local files
            config: boto3.s3.transfer.TransferConfig; bucket's default if None
            extra_args: ExtraArgs for boto3 bucket object

        Returns:
            None, or, for multi-upload, a list containing None for each
                successful put and an Exception for each failed one
        """
        config = self.config if config is None else config
        # If S3 key was not specified, use string rep of
        # passed object, up to 1024 characters
        key = str(obj)[:1024] if key is None else key
        base_kwargs = {
            "Bucket": self.name,
            "Key": key,
            "Config": config,
            "ExtraArgs": extra_args
        }
        # 'touch' - type behavior
        if obj is None:
            obj = BytesIO()
        # directly upload file from local storage
        if _should_be_file(obj, literal_str):
            return self.client.upload_file(Filename=str(obj), **base_kwargs)
        # or: upload in-memory objects
        # encode string to bytes if we're writing it to an S3 object instead
        # of interpreting it as a path
        if isinstance(obj, str):
            obj = obj.encode("utf-8")
        if isinstance(obj, bytes):
            obj = BytesIO(obj)
        # if it's not string or bytes, it has to be buffer/file-like.
        # this isn't a perfect heuristic, of course!
        elif not hasattr(obj, "read"):
            raise TypeError(f"Cannot put object of type {type(obj)}")
        return self.client.upload_fileobj(Fileobj=obj, **base_kwargs)

    @splitwrap(seq_arity=2)
    def get(
        self,
        key: Union[str, Sequence[str]],
        destination: Union[
            Union[str, Path, IOBase, None],
            Sequence[Union[str, Path, IOBase, None]],
        ] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
        start_byte: Optional[int] = None,
        end_byte: Optional[int] = None,
        **extra_args: str
    ) -> Union[Path, str, IOBase] | list[Union[Path, str, IOBase, Exception]]:
        """
        write S3 object(s) into file(s) or filelike object(s).

        Args:
            key: object key(s) (fully-qualified 'path(s)' from root)
            destination: where to write the retrieved object(s). May be path(s)
                or filelike object(s). If not specified, constructs new BytesIO
                buffer(s).
            config: optional transfer config
            start_byte: Byte index at which to begin read. None (default) or
                0 means the first byte of the object. Negative integers are
                interpreted as Python-style negative slice indices. e.g.,
                `start_byte=0` and `end_byte=-1` means 'read all the bytes
                but the last one', analogous to `my_list[0:-1]`.
            end_byte: Byte index at which to end read. None (default) means
                the last byte of the object.
            extra_args: passed directly to `TransferManager.download()`
        Returns:
            outpath: the path, string, or buffer we wrote the object to, or,
                for multi-get, a list containing one such outpath for each
                successful write and an Exception for each failed write

        Caution:
            This does not currently support specifying different byte ranges
            for different objects. In other words, this is not legal:
            ```
            Bucket.get(
                [k1, k2], [f1, f2], start_byte=[s1, s2], end_byte=[e1, e2]
            )
            ```
            If specified in a multi-object call to `get()`, `start_byte` and
            `end_byte` will fetch the same range from each object.

            This may change in the future.
        """
        # TODO: add more useful error messages for streams opened in text mode
        config = self.config if config is None else config
        if destination is None:
            dest = BytesIO()
        elif isinstance(destination, Path):
            dest = str(destination)
        else:
            dest = destination
        start_byte = None if start_byte == 0 else start_byte
        args, kwargs = (self.name, key, dest), {'extra_args': extra_args}
        if start_byte is not None or end_byte is not None:
            manager_class = TransferManagerWithRange
            kwargs |= {'start_byte': start_byte, 'end_byte': end_byte}
        else:
            manager_class = TransferManager
        with manager_class(self.client, config) as manager:
            dirs_we_made = []
            if not isinstance(dest, IOBase):
                for p in reversed(Path(dest).parents):
                    if p.exists() is False:
                        p.mkdir()
                        dirs_we_made.append(p)
            future = manager.download(*args, **kwargs)
            ok = False
            try:
                # this call is strictly intended to raise exceptions,
                # e.g. attempts to get objects that don't exist.
                future.result()
                ok = True
            finally:
                if ok is False:
                    for d in reversed(dirs_we_made):
                        d.rmdir()
        if hasattr(dest, "seek"):
            dest.seek(0)
        return dest

    def read(
        self,
        key: str,
        mode: Literal["r", "rb"] = "r",
        return_buffer: bool = False,
        start_byte: Optional[int] = None,
        end_byte: Optional[int] = None
    ) -> Union[bytes, str, BytesIO, StringIO]:
        """
        Read an S3 object into memory.

        Args:
            key: fully-qualified 'path' to object
            mode: 'r' to read as text, 'rb' as bytes
            return_buffer: if True, return object as a StringIO/BytesIO; if
                False, return it as str/bytes
            start_byte: if not None, start reading at this byte
            end_byte: if not None, stop reading at this byte

        Returns:
            Contents of object, in format specified by `mode` and
                `return_buffer`.
        """
        buf = self.get(key, start_byte=start_byte, end_byte=end_byte)
        if return_buffer is True:
            if mode == "rb":
                return buf
            strbuf = StringIO()
            strbuf.write(buf.read().decode())
            strbuf.seek(0)
            return strbuf
        if mode == "rb":
            return buf.read()
        return buf.read().decode()

    # TODO, maybe: rewrite this using lower-level methods. This may not be
    #  required, because flexibility with S3 -> S3 copies is less often useful
    #  than with uploads.
    @splitwrap(seq_arity=2)
    def cp(
        self,
        source: Union[str, Sequence[str]],
        destination: Union[Optional[str], Sequence[Optional[str]]] = None,
        destination_bucket: Optional[str] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
        **extra_args: str
    ) -> Union[str, list[Union[str, Exception]]]:
        """
        Copy S3 object(s) to another location on S3.

        Args:
            source: key(s) of object(s) to copy (fully-qualified 'path(s)')
            destination: key(s) to copy object(s) to (by default, uses
                source key(s))
            destination_bucket: bucket to copy object(s) to. if not
                specified, uses source bucket. this means that if destination
                and destination_bucket are both None, it overwrites the object
                inplace. this is useful for things like storage class changes.
            config: optional transfer config
            extra_args: ExtraArgs for boto3 bucket object

        Returns:
            uri: S3 URI of newly-created copy, or, for multi-copy, a list
                containing an S3 URI for each successful copy and an Exception
                for each failed
        """
        config = self.config if config is None else config
        if destination_bucket is None:
            destination_bucket = self.name
        # supporting copy-self-to-self for storage class changes etc.
        destination = source if destination is None else destination

        # use boto3's high-level Bucket object to perform a managed transfer
        # (in order to easily support objects > 5 GB)
        destination_bucket_object = self.resource.Bucket(destination_bucket)
        copy_source = {"Bucket": self.name, "Key": source}
        destination_bucket_object.copy(
            copy_source, destination, ExtraArgs=extra_args, Config=config
        )
        return f"s3://{destination_bucket}:{destination}"

    def append(
        self,
        obj: Puttable,
        key: str,
        literal_str: bool = False,
        offset: int | None = None
    ) -> None:
        """
        Write data at an offset to an S3 Express One Zone object. If no offset
        is specified, appends data to the end of the object.

        Args:
            obj: string, Path, bytes, or filelike / buffer object to write.
                If None and `key` does not yet exist, performs "touch"-type
                behavior (creates an empty object).
            key: key of S3 object in this bucket to write `obj` to.
            literal_str: if True and `obj` is a `str`, write that string to
                `key`. Otherwise, interpret it as a path to a local file.
            offset:

        Returns:
            None.

        Cautions:
            `append()` does not currently perform managed multipart uploads.
            This means that if `obj` is > 5 GB, the operation will fail, and
            also that `append()` is generally inefficient for large writes.
            In its current state, it is primarily intended for tasks like
            tail-writes to logs. This may change in the future.
        """
        touch = True
        try:
            head = self.head(key)
            if offset in (None, 0):
                offset = head["ContentLength"]
            touch = False
        except ClientError as ce:
            if "not found" not in str(ce).lower():
                raise ce
        file = None
        try:
            if obj is None:
                file = BytesIO()
            elif _should_be_file(obj, literal_str):
                file = Path(obj).open("rb")
            elif isinstance(obj, str):
                file = BytesIO(obj.encode('utf-8'))
            elif isinstance(obj, bytes):
                file = BytesIO(obj)
            elif not hasattr(obj, "read") and hasattr(obj, "seek"):
                raise TypeError(f"Cannot put object of type {type(obj)}")
            else:
                file = obj
            file.seek(0)
            kwargs = {"Bucket": self.name, "Body": file, "Key": key}
            if touch is False:
                kwargs["WriteOffsetBytes"] = offset
            resp = self.client.put_object(**kwargs)
        finally:
            if file is not None:
                file.close()
        return resp

    @cached_property
    def _buckethead(self):
        """
        Cached response to HeadBucket call against this bucket. Not currently
        formatted in an interesting way for public use.
        """
        return self.client.head_bucket(Bucket=self.name)

    @cached_property
    def bucket_type(self) -> BUCKET_TYPE:
        """
        Top-level type of bucket, 'general' or 'directory'. Does not
        distinguish directory bucket subtypes.
        """
        if 'BucketLocationType' in self._buckethead:
            return 'directory'
        return 'general'

    def _s3c_kwargs(self):
        """Produces a dict containing kwargs s3control actions want."""
        arn = self._buckethead["BucketArn"]
        account_id = DIRECTORY_BUCKET_ARN_ACCOUNTPAT.search(arn).group(1)
        return {'AccountId': account_id, 'ResourceArn': arn}

    def _directory_bucket_tagrecs(self):
        if self.bucket_type != "directory":
            raise ValueError(
                "Don't call this method on general-purpose buckets"
            )
        s3c = init_client("s3control", None, self.session)
        return s3c.list_tags_for_resource(**self._s3c_kwargs()).get('Tags', [])

    @cached_property
    def tags(self) -> dict:
        """
        Bucket tags represented as a dict. If there are no tags, returns
        an empty dict.
        """
        if self.bucket_type == "directory":
            recs = self._directory_bucket_tagrecs()
        else:
            recs = self.client.get_bucket_tagging(Bucket=self.name)['TagSet']
        return {rec["Key"]: rec["Value"] for rec in recs}

    def set_tags(self, *, replace_all: bool = False, **tags: str):
        """
        Set tags for this bucket.

        Args:
            replace_all: if True, removes _all_ existing tags along with
                setting passed tag values (this is the default behavior of the
                PutBucketTagging API operation). If False, leaves existing
                tags unchanged unless they are redefined in `tags`. Raises
                a ValueError if passed for a directory bucket, because the
                API call required to set tags behaves entirely differently,
                and we do not wish to implement that behavior here.
            **tags: Argument names are tag keys; argument values are tag
                values. At least one tag must be defined.

        Returns:
            API response for PutBucketTagging operation.
        """
        if len(tags) == 0:
            raise ValueError("No tags to set")
        if not all(isinstance(x, str) for x in tags.values()):
            raise TypeError("Tag values must be strings")
        if replace_all is True and self.bucket_type == 'directory':
            raise ValueError(
                "replace_all=True not supported for directory buckets."
            )
        if replace_all is False:
            tags = self.tags | tags
        tagset = [{'Key': k, 'Value': v} for k, v in tags.items()]
        if self.bucket_type == "directory":
            s3c = init_client("s3control", None, self.session)
            response = s3c.tag_resource(
                **self._s3c_kwargs(), Tags=tagset
            )
        else:
            response = self.client.put_bucket_tagging(
                Bucket=self.name, Tagging={'TagSet': tagset}
            )
        try:
            # noinspection PyPropertyAccess
            del self.tags
        except AttributeError:
            pass
        return response

    # TODO: verify types of returned dict values
    @splitwrap(seq_arity=1)
    def head(
        self, key: Union[str, Sequence[str]]
    ) -> Union[dict[str, str], list[Union[dict[str, str], Exception]]]:
        """
        Get basic information about S3 objects in a nicely formatted dict.

        Args:
            key: object key(s) (fully-qualified 'path(s)' from bucket root)

        Returns:
            dict containing a curated selection of object headers, or, for
            a multi-object call, a list containing a dict for each
            successful head and an Exception for each failed
        """
        response = self.client.head_object(Bucket=self.name, Key=key)
        headers = response["ResponseMetadata"].get("HTTPHeaders", {})
        interesting_responses = (
            "ContentLength",
            "ContentType",
            "ETag",
            "LastModified",
            "Metadata",
            "Restore",
            "StorageClass",
        )
        interesting_headers = (
            "x-amz-restore-request-date",
            "x-amz-restore-expiry-days",
            "x-amz-restore-tier",
        )
        head_dict = {}
        head_dict |= keyfilter(lambda k: k in interesting_responses, response)
        head_dict |= keyfilter(lambda k: k in interesting_headers, headers)
        if "LastModified" in head_dict:
            head_dict["LastModified"] = head_dict["LastModified"].isoformat()
        return head_dict

    def tail(
        self,
        key: str,
        destination: MutableSequence | BinaryIO | TextIO | IOBase | Path | str,
        start_pos: int | None = None,
        poll: float = 1,
        text_mode: bool = True,
        permit_missing: bool = False
    ):
        """
        Provides asynchronous `tail -f`-like functionality for S3 objects.

        Args:
            key: object key (fully-qualified 'path' from root)
            destination: where to write the tail chunks. If a sequence,
                appends each chunk as a new item.
            start_pos: start reading from where? If None, start at the length
                of the object when `tail()` is called.
            poll: poll rate in seconds
            text_mode: if True, decode each chunk as utf-8 text
            permit_missing: if True, don't raise an error if the object
                doesn't exist; instead, periodically check to see if the
                object comes into existence, and if it does, start tailing it.

        Returns:
            A `StoppableFuture` for the poll loop. Call its `stop()` method to
                stop tailing. Note that this future's result will always be
                `None`.

        Warnings:
            This function is primarily intended for following append-writes
            to SEOZ objects, and if the size of the object _decreases_ while
            tailing, the method may no longer accurately fetch subsequent
            writes to the file. In the future, we may add an option to
            perform an additional HEAD request to check object length before
            each normal HEAD / GET pair.

        Notes:
            Exclusive of egress costs, running this for a full day at the
            default 1-second poll rate on a single S3 Standard object costs
            approximately $0.07. On a SEOZ object, it costs approximately
            $0.005. Egress costs in this application should be equal to
            egress for (size of object when tailing ends) minus (the smaller
            of start_pos or size of object when tailing starts)
        """
        return StoppableFuture.launch_into(
            ThreadPoolExecutor(1),
            _poll_obj,
            bucket=self,
            key=key,
            start_pos=start_pos,
            text_mode=text_mode,
            destination=destination,
            poll=poll,
            permit_missing=permit_missing
        )

    def ls(
        self,
        prefix: Optional[str] = None,
        recursive: bool = False,
        formatting: Literal["simple", "contents", "df", "raw"] = "simple",
        cache: Union[str, Path, IOBase, None] = None,
        start_after: Optional[str] = None,
        cache_only: bool = False,
        fetch_owner: bool = False
    ) -> Union[tuple, pd.DataFrame, None]:
        """
        list objects in a bucket.

        Args:
            prefix: prefix ('folder') to list (if not specified, defaults to
                bucket root)
            recursive: recursively list all objects in tree rooted at prefix?
            formatting: how to format list results
                * "simple": tuple containing object names only
                * "contents": tuple of dicts containing object names,
                    modification times, etc.
                * "df": pandas DataFrame produced from contents
                * "raw": API response as reported by boto3
            cache: optional file or filelike object to write results to.
            start_after: if specified, begin listing objects only "after" this
                prefix. intended principally for sequential calls.
            cache_only: _only_ write results into the specified cache; do not
                retain them in memory. intended mainly for cases in which the
                full list would be larger than available memory.
            fetch_owner: if True, include owner of objects in output. Not
                enabled by default due to permissioning and performance issues.

        Returns:
            Manifest of contents, format dependent on `formatting`; or None
                if `cache_only` is True.
        """
        kwargs = {"Bucket": self.name}
        if recursive is False:
            # try to treat prefixes like directories.
            # note that 'recursive' behavior is default -- the
            # ListObjects* methods don't treat prefixes as anything but parts
            # of an object key by default. _also_ note that this is different
            # from the default awscli s3 behavior, but not awscli s3api.
            kwargs["Delimiter"] = "/"
        if recursive is False and prefix is None:
            kwargs["Prefix"] = ""
        elif prefix is not None:
            kwargs["Prefix"] = prefix.lstrip("/")
        if start_after is not None:
            kwargs["StartAfter"] = start_after
        kwargs["FetchOwner"] = fetch_owner
        # pagination is typically slightly faster than iteratively passing
        # StartAfter based on the last key of a truncated response
        paginator = self.client.get_paginator("list_objects_v2")
        cache = Path(cache) if isinstance(cache, str) else cache
        self._maybe_prep_ls_cache(cache)
        pages = []
        for page in iter(paginator.paginate(**kwargs)):
            self._maybe_write_ls_cache(page, cache)
            if cache_only is False:
                pages.append(page)
        if cache_only is True:
            return None
        if formatting not in ("raw", "simple", "df", "contents"):
            warnings.warn(
                f"invalid formatting '{formatting}', defaulting to 'simple'"
            )
            formatting = "simple"
        if formatting == "raw":
            return tuple(pages)
        objects = chain(*(p.get("Contents", []) for p in pages))
        prefixes = chain(*(p.get("CommonPrefixes", []) for p in pages))
        if formatting == "simple":
            return tuple(
                [obj["Key"] for obj in objects]
                + [obj["Prefix"] for obj in prefixes]
            )
        if formatting == "contents":
            return tuple(objects)
        if fetch_owner is False:
            return pd.DataFrame(objects)
        objects = tuple(objects)
        owner_info = pd.DataFrame([o.pop('Owner') for o in objects])
        return pd.concat(map(pd.DataFrame, (objects, owner_info)), axis=1)

    @staticmethod
    def _maybe_prep_ls_cache(cache: Optional[Union[Path, IO]]):
        if cache is None:
            return
        columns = "Key,LastModified,ETag,Size,StorageClass\n"
        if isinstance(cache, Path):
            if cache.exists() and cache.stat().st_size > 0:
                return
            with cache.open("w") as stream:
                stream.write(columns)
        elif cache.tell() == 0:
            cache.write(columns)

    @staticmethod
    def _maybe_write_ls_cache(page: dict, cache: Optional[Union[Path, IO]]):
        """helper function for Bucket.ls()"""
        if (cache is None) or (objects := page.get("Contents")) is None:
            return
        stream = cache if not isinstance(cache, Path) else cache.open("a+")
        try:
            for rec in objects:
                stream.write(",".join(map(_dtstr, rec.values())) + "\n")
        finally:
            if isinstance(cache, Path):
                stream.close()

    @splitwrap(seq_arity=1)
    def rm(self, key: str) -> Union[None, list[Optional[None]]]:
        """
        Delete an S3 object.

        Args:
            key: key of object to delete (fully-qualified 'path' from root)

        Returns:
            None, or, for multi-delete, a list containing None for successful
                deletes and Exceptions for failed
        """
        return self.client.delete_object(Bucket=self.name, Key=key)

    def ls_multipart(self) -> dict:
        """
        List all multipart uploads associated with this bucket.

        Returns:
            API response
        """
        return self.client.list_multipart_uploads(Bucket=self.name)

    def create_multipart_upload(self, key: str) -> dict:
        """
        Prepare an S3 multipart upload. Note that this is not itself an upload
        operation! It merely _sets up_ an upload.

        Args:
            key: upload target key (fully-qualified 'path' from bucket root)

        Returns:
            API response
        """
        return self.client.create_multipart_upload(Bucket=self.name, Key=key)

    def abort_multipart_upload(self, multipart: Mapping) -> dict:
        """
        Abort a multipart upload operation.

        Args:
            multipart: API response received when multipart upload operation
                was created

        Returns:
            API response

        """
        return self.client.abort_multipart_upload(
            Bucket=self.name,
            Key=multipart["Key"],
            UploadId=multipart["UploadId"],
        )

    def complete_multipart_upload(
        self,
        multipart: Mapping,
        parts: Mapping,
    ) -> dict:
        """
        Notify S3 that all parts of an object have been uploaded and it may
        close the multipart upload operation and actually create the object.

        Args:
            multipart: API response received when multipart upload was created
            parts: API responses received after uploading each part

        Returns:
            API response
        """
        return self.client.complete_multipart_upload(
            Bucket=self.name,
            Key=multipart["Key"],
            UploadId=multipart["UploadId"],
            MultipartUpload={
                "Parts": [
                    {"ETag": part["ETag"], "PartNumber": number}
                    for number, part in parts.items()
                ]
            },
        )

    def __str__(self):
        return f"s3 bucket {self.name} ({self.client.meta.region_name})"

    def __repr__(self):
        return self.__str__()


# noinspection PyProtectedMember
def _clean_putter_process_records(
    records, block=True, threshold=1, poll_delay=0.05
):
    """helper function for multithreaded put_stream()"""
    i_am_actively_blocking = True
    while i_am_actively_blocking is True:
        process_records = valfilter(lambda v: "process" in v.keys(), records)
        alive_count = 0
        for record in process_records.values():
            if not record["process"]._closed:
                if record["process"].is_alive():
                    alive_count += 1
                    continue
            if "result" not in record.keys():
                record["result"] = record["pipe"].recv()
                record["pipe"].close()
            if not record["process"]._closed:
                record["process"].close()
        i_am_actively_blocking = (alive_count >= threshold) and block
        if i_am_actively_blocking is True:
            time.sleep(poll_delay)
