"""
This module provides managed operations on AWS S3 objects. Its centerpiece
is a Bucket object providing a high-level interface to operations on a single
S3 bucket. The original motivation for this module was quickly constructing
inventories of large buckets in pandas DataFrames.

Much of this module wraps lower-level methods of boto3, so it is in some sense
an alternative implementation of boto3's own high-level managed S3 methods
and objects, designed for cases in which deeper indexing, more response
introspection, or more flexible I/O stream manipulation are required. We also
like the syntax better.
"""
import datetime as dt
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import io
from inspect import getmembers
from itertools import chain
from multiprocessing import Process
from pathlib import Path
import sys
from typing import (
    Any,
    IO,
    Iterator,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Union,
)
import warnings

import boto3
import boto3.resources.base
import boto3.s3.transfer
import botocore.client
import pandas as pd
from cytoolz import keyfilter, valfilter
from dustgoggles.func import naturals
import requests

from hostess.aws.utilities import init_client, init_resource
from hostess.subutils import piped
from hostess.utilities import console_and_log, infer_stream_length, stamp


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
    """

    def __init__(
        self,
        bucket_name: str,
        client: Optional[botocore.client.BaseClient] = None,
        resource: Optional[boto3.resources.base.ServiceResource] = None,
        session: Optional[boto3.session.Session] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
        n_threads: Optional[int] = 4
    ):
        """
        Args:
            bucket_name: name of bucket
            client: optional boto3 s3 Client. if not specified, creates a
                default client.
            resource: optional boto3 s3 Resource. if not specified, creates a
                default resource.
            session: optional boto3 s3 Session. if not specified, creates a
                default session.
            config: optional boto3 TransferConfig. if not specified, creates a
                default config.
            n_threads: if not None, automatically multithreads some operations.
                cannot be modified after Bucket creation.
        """
        self.client = init_client("s3", client, session)
        self.resource = init_resource("s3", resource, session)
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
        if n_threads is not None:
            self._n_threads = n_threads
            self.exc = ThreadPoolExecutor(n_threads)

    def update_contents(
        self,
        prefix: str = "",
        cache: Optional[Union[str, Path, io.IOBase]] = None,
    ):
        """
        recursively scan the contents of the bucket and store the result in
        self.contents.

        Args:
            prefix: prefix at which to begin scan. if not passed, scans the
                entire bucket.
            cache: optional file or filelike object to write scan results to
                in addition to storing them in self.contents.
        """
        self.contents = self.ls(
            recursive=True, prefix=prefix, cache=cache, formatting="contents"
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
        kwargs = {
            "config": self.config,
            "download_cache": [b""],
            "multipart": multipart,
            "upload_numerator": naturals(),
            "parts": parts,
            "upload_threads": upload_threads,
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
    ) -> dict:
        """
        Create an S3 object from a byte stream via a managed multipart upload.
        Useful for uploading large files, but can also handle intermittent
        streams, larger-than-memory transfers, direct streams from remote URLs,
        and streams of unknown length.

        Args:
            self: Bucket or name of bucket
            obj: source of stream to upload. May be a path, a URL, a filelike
                object, or any iterator that yields bytes.
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
            API response to multipart upload completion, or API response from
                simple upload if stream length < chunksize, if Any
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
                "can't determine how to consume bytes from stream.")
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
        if upload_threads is not None:
            _clean_putter_process_records(parts)
        del put_chunk
        parts = {number: part["result"] for number, part in parts.items()}
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
        upload_threads: Optional[int],
        verbose: bool = False,
        flush: bool = False,
    ):
        """helper function for Bucket.put_stream()"""
        download_cache[0] += blob
        if (
            (len(download_cache[0]) < config.multipart_chunksize)
            and (flush is False)
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
        if upload_threads is not None:
            _clean_putter_process_records(parts, threshold=upload_threads)
            pipe, remote = piped(self.client.upload_part)
            process = Process(target=remote, kwargs=kwargs)
            process.start()
            parts[number] = {"process": process, "pipe": pipe}
        else:
            parts[number] = {"result": self.client.upload_part(**kwargs)}

    def freeze(
        self,
        key: str,
        storage_class: str = "DEEP_ARCHIVE",
    ) -> tuple[str, Optional[dict]]:
        """
        Modify the storage class of an object. Intended primarily for
        moving objects from S3 Standard to one of the Glacier classes.

        Args:
            self: Bucket or name of bucket
            key: object key (fully-qualified 'path' relative to bucket root)
            storage_class: target storage class

        Returns:
            uri: URI of frozen object
            response: API response, if any
        """
        return self.cp(key, extra_args={"StorageClass": storage_class})

    def restore(
        self,
        key: str,
        tier: Literal["Expedited", "Standard", "Bulk"] = "Bulk",
        days: int = 5,
    ) -> dict:
        """
        Issue a request to temporarily restore an object from S3 Glacier
        Flexible Retrival or Deep Archive to S3 Standard. Note that object
        restoration is not instantaneous. Depending on retrieval tier and
        storage class, AWS guarantees retrieval times ranging from 5 minutes
        to 48 hours. See https://docs.aws.amazon.com/AmazonS3/latest
        /userguide/restoring-objects-retrieval-options.html for details.

        You can check the progress of restore requests using Bucket.head().

        Args:
            key: key of object to restore (fully-qualified path from root)
            tier: retrieval tier. In order of speed and expense, high to low,
                options are "Expedited", "Standard", and "Bulk". Only
                "Standard" and "Bulk" are available for Deep Archive.
            days: number of days object should remain restored before
                reverting to its Glaciered state

        Returns:
            RestoreObject API response
        """
        restore_request = {
            "Days": days, "GlacierJobParameters": {"Tier": tier}
        }
        return self.client.restore_object(
            Bucket=self.name, Key=key, RestoreRequest=restore_request
        )

    @classmethod
    def bind(
        cls,
        bucket: Union[str, "Bucket"],
        client: Optional[botocore.client.BaseClient] = None,
        session: Optional[boto3.Session] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
    ) -> "Bucket":
        """
        Conservative constructor for Bucket. Does not construct a new
        Bucket if passed a Bucket. Intended primarily as a convenience for
        other functions in this module that might be either partially
        evaluated with a Bucket or simply passed the name of a bucket.

        Args:
            bucket: Bucket or name of bucket
            client: optional client
            session: optional session
            config: optional transferconfig

        Returns:
            newly-initialized Bucket if bucket is a string; just bucket if
                bucket is a Bucket.

        """
        if isinstance(bucket, Bucket):
            return bucket
        return Bucket(bucket, client=client, session=session, config=config)

    def _get_n_threads(self):
        return self._n_threads

    def _set_n_threads(self, _):
        raise ValueError("n_threads cannot be modified after Bucket creation.")

    _n_threads: Optional[int] = None
    n_threads = property(_get_n_threads, _set_n_threads)
    exc: Optional[ThreadPoolExecutor] = None

    def put(
        self,
        obj: Union[str, Path, io.IOBase, bytes, None] = None,
        key: Optional[str] = None,
        literal_str: bool = False,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
    ) -> Optional[dict]:
        """
        Upload a file or buffer to an S3 bucket

        Args:
            obj: str, Path, or filelike / buffer object to upload; None for
                'touch' behavior
            key: S3 key (fully-qualified 'path' from bucket root). If not
                specified then str(file_or_buffer) is used. This will most
                likely look very nasty if it's a buffer.
            literal_str: if True, write passed string directly to object
                instead of interpreting it as a path
            config: boto3.s3.transfer.TransferConfig; bucket's default if None

        Returns:
            API response (if any)
        """
        config = self.config if config is None else config
        # If S3 key was not specified, use string rep of
        # passed object, up to 1024 characters
        key = str(obj)[:1024] if key is None else key
        # 'touch' - type behavior
        if obj is None:
            obj = io.BytesIO()
        # directly upload file from local storage

        if (
            (isinstance(obj, str) and literal_str is False)
            or (isinstance(obj, Path))
        ):
            return self.client.upload_file(
                Bucket=self.name, Filename=str(obj), Key=key, Config=config
            )
        # or: upload in-memory objects
        # encode string to bytes if we're writing it to S3 object instead
        # of interpreting it as a patha
        if isinstance(obj, str) and literal_str is True:
            obj = io.BytesIO(obj.encode("utf-8"))
        elif isinstance(obj, bytes):
            obj = io.BytesIO(obj)
        return self.client.upload_fileobj(
            Bucket=self.name, Fileobj=obj, Key=key, Config=config
        )

    def get(
        self,
        key: str,
        destination: Union[str, Path, io.IOBase, None] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
    ) -> tuple[Union[Path, str, io.IOBase], Optional[dict]]:
        """
        fetch an S3 object and write it into a file or filelike object.

        Args:
            key: object key (fully-qualified 'path' relative to bucket root)
            destination: where to write the retrieved object. May be a path or a
                filelike object. If not specified, constructs a new BytesIO
                buffer.
            config: optional transfer config

        Returns:
            outpath: the path, string, or buffer we wrote the object to
            response: the S3 API response, if Any
        """
        # TODO: add more useful error messages for streams opened in text mode
        if config is None:
            config = self.config
        if destination is None:
            destination = io.BytesIO()
        if isinstance(destination, io.IOBase):
            response = self.client.download_fileobj(
                self.name, key, destination, Config=config
            )
        else:
            response = self.client.download_file(
                self.name, key, destination, Config=config
            )
        if "seek" in dir(destination):
            destination.seek(0)
        return destination, response

    # TODO, maybe: rewrite this using lower-level methods. This may not be
    #  required, because flexibility with S3 -> S3 copies is less often useful
    #  than with uploads.
    def cp(
        self,
        source: str,
        destination: Optional[str] = None,
        destination_bucket: Optional[str] = None,
        extra_args: Optional[Mapping[str, str]] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
    ) -> tuple[str, Optional[dict]]:
        """
        Copy an S3 object to another location on S3.

        Args:
            source: key of object to copy (fully-qualified 'path' from root)
            destination: key to copy object to (by default, uses source key)
            destination_bucket: bucket to copy object to. if not
                specified, uses source bucket. this means that if destination
                and destination_bucket are both None, it overwrites the object
                inplace. this is useful for things like storage class changes.
            extra_args: ExtraArgs for boto3 bucket object
            config: optional transfer config

        Returns:
            uri: S3 URI of newly-created copy
            response: API response, if any
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
        response = destination_bucket_object.copy(
            copy_source, destination, ExtraArgs=extra_args, Config=config
        )
        return f"s3://{destination_bucket}:{destination}", response

    # TODO: verify types of returned dict values
    def head(self, key: str) -> dict[str, str]:
        """
        Get basic information about an S3 object in a nicely formatted dict.

        Args:
            self: Bucket or name of bucket
            key: object key (fully-qualified 'path' relative to bucket root)

        Returns:
            dict containing a curated selection of object headers.
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

    # TODO: scary, refactor
    def ls(
        self,
        prefix: Optional[str] = None,
        recursive: bool = False,
        formatting: Literal["simple", "contents", "df", "raw"] = "simple",
        cache: Union[str, Path, io.IOBase, None] = None,
        start_after: Optional[str] = None,
        cache_only: bool = False,
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

        Returns:
            Manifest of contents, format dependent on `formatting`; or None
                if `cache_only` is True.
        """
        kwargs = {'Bucket': self.name}
        if recursive is False:
            # try to treat prefixes like directories.
            # note that 'recursive' behavior is default -- the
            # ListObjects* methods don't treat prefixes as anything but parts
            # of an object key by default. _also_ note that this is different
            # from the default awscli s3 behavior, but not awscli s3api.
            kwargs['Delimiter'] = '/'
        if recursive is False and prefix is None:
            kwargs['Prefix'] = ''
        elif prefix is not None:
            kwargs['Prefix'] = prefix.lstrip('/')
        if start_after is not None:
            kwargs['StartAfter'] = start_after
        # pagination is typically slightly faster than iteratively passing
        # StartAfter based on the last key of a truncated response
        paginator = self.client.get_paginator('list_objects_v2')
        cache = Path(cache) if isinstance(cache, str) else cache
        self._maybe_prep_ls_cache(cache)
        pages = []
        for page in iter(paginator.paginate(**kwargs)):
            self._maybe_write_ls_cache(page, cache)
            if cache_only is False:
                pages.append(page)
        if cache_only is True:
            return
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
        return pd.DataFrame(objects)

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
        if (cache is None) or (objects := page.get("Contents") is None):
            return
        stream = cache if not isinstance(cache, Path) else cache.open('a+')
        try:
            for rec in objects:
                stream.write(",".join(map(_dtstr, rec.values())) + "\n")
        finally:
            if isinstance(cache, Path):
                stream.close()

    def rm(self, key: str) -> Optional[dict]:
        """
        Delete an S3 object.

        Args:
            key: key of object to delete (fully-qualified 'path' from root)

        Returns:
            API response, if any
        """
        return self.client.delete_object(Bucket=self.name, Key=key)

    def ls_multipart(self) -> dict:
        """
        List all multipart uploads associated with thuis bucket.

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

    def abort_multipart_upload(self,multipart: Mapping) -> dict:
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
        self, multipart: Mapping, parts: Mapping,
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
