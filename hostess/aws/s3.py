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
import inspect
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps
from io import BytesIO, IOBase, StringIO
from inspect import getmembers
from itertools import chain, cycle
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
    Union, Sequence, Callable,
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
from hostess.config.config import S3_DEFAULTS
from hostess.subutils import piped
from hostess.utilities import (
    curry, console_and_log, infer_stream_length, stamp
)

Puttable = Union[str, Path, IOBase, bytes]
"""type alias for Python objects Bucket will write to S3 """


@curry
def splitwrap(
    method: Callable[
    ["Bucket", ...], Any],
    seq_arity: Literal[1, 2]
) -> Callable[["Bucket", ...], Any]:
    """
    dispatch decorator for methods of Bucket that can accept either single
    source and/or destination arguments or sequences of them. automatically
    maps sequences into Bucket's thread pool, if it exists, and returns all
    results in a list. also fails gracefully, returning Exceptions from any
    call.
    """
    sig = inspect.signature(method)
    params = list(sig.parameters)
    defaults = {n: p.default for n, p in list(sig.parameters.items())}

    @wraps(method)
    def maybesplit(*args, **kwargs):
        bound = sig.bind(*args, **kwargs).arguments
        source = bound.get(params[1], defaults[params[1]])
        sseq = (
           hasattr(source, "index") and not isinstance(source, (str, bytes))
        )
        if seq_arity == 2:
            target = bound.get(params[2], defaults[params[2]])
            tseq = (
                hasattr(target, "index")
                and not isinstance(target, (str, bytes))
            )
            if sseq is False:
                if tseq is True:
                    raise TypeError("Mismatched sequence/nonsequence call")
                return method(*args, **kwargs)
            bound.pop(params[1])
            bound.pop(params[2], None)
            if (tseq is False) and (target is not None):
                raise TypeError("Mismatched sequence/nonsequence call")
            if target is None:
                target = cycle((None,))
            elif len(source) != len(target):
                raise ValueError("Mismatched source/target sequence lengths")
            seq_iter = zip(source, target)
        elif sseq is False:
            return method(*args, **kwargs)
        else:
            seq_iter = ((x,) for x in source)
            bound.pop(params[1])
        bucket = bound.pop(params[0])
        if bucket.n_threads is None:
            output = []
            for pre in seq_iter:
                try:
                    output.append(method(bucket, *pre, **bound))
                except KeyboardInterrupt:
                    raise
                except Exception as ex:
                    output.append(ex)
            return output
        exc = ThreadPoolExecutor(bucket.n_threads)
        futures = [
            exc.submit(method, bucket, *pre, **bound) for pre in seq_iter
        ]
        while not all(f.done() for f in futures):
            time.sleep(0.02)
        output = []
        for f in futures:
            try:
                output.append(f.result())
            except KeyboardInterrupt:
                raise
            except Exception as ex:
                output.append(ex)
        return output

    return maybesplit


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
        self.name = bucket_name
        self.contents = []
        if config is None:
            config = boto3.s3.transfer.TransferConfig(**S3_DEFAULTS['config'])
        self.config = config
        # populate s3 operations
        for name, thing in getmembers(sys.modules[__name__]):
            if name not in Bucket.__annotations__.keys():
                continue
            setattr(self, name, partial(thing, self, config=self.config))
        self.n_threads = n_threads

    def update_contents(
        self,
        prefix: Optional[str] = None,
        cache: Optional[Union[str, Path, IOBase]] = None,
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
        if upload_threads is None:
            exc = None
        else:
            exc =ThreadPoolExecutor(upload_threads)
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
        return self.cp(key, extra_args={"StorageClass": storage_class})

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
                options are "Expedited", "Standard", and "Bulk". Only
                "Standard" and "Bulk" are available for Deep Archive.
            days: number of days object(s) should remain restored before
                reverting to Glaciered state

        Returns:
            RestoreObject API response, or list of responses and/or Exceptions
        """
        restore_request = {
            "Days": days, "GlacierJobParameters": {"Tier": tier}
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
    ) -> Union[None, list[Optional[Exception]]]:
        """
        Upload files or buffers to an S3 bucket

        Args:
            obj: an individual str, Path, or filelike / buffer object to
                upload, or a sequence of such objects
            key: S3 key (fully-qualified 'path' from bucket root); or, if `obj`
                is a sequences, a sequence keys of the same length of `obj`.
                If `key` is not specified, key(s) are generated from the
                string representation of the uploaded object(s), truncated to
                1024 characters (maximum length of an S3 key).
            literal_str: if True, write passed strings directly to objects;
                otherwise, interpret them as paths to local files
            config: boto3.s3.transfer.TransferConfig; bucket's default if None

        Returns:
            None, or, for multi-upload, a list containing None for each
                successful put and an Exception for each failed one
        """
        config = self.config if config is None else config
        # If S3 key was not specified, use string rep of
        # passed object, up to 1024 characters
        key = str(obj)[:1024] if key is None else key
        # 'touch' - type behavior
        if obj is None:
            obj = BytesIO()
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
            obj = BytesIO(obj.encode("utf-8"))
        elif isinstance(obj, bytes):
            obj = BytesIO(obj)
        return self.client.upload_fileobj(
            Bucket=self.name, Fileobj=obj, Key=key, Config=config
        )

    @splitwrap(seq_arity=2)
    def get(
        self,
        key: Union[str, Sequence[str]],
        destination: Union[
            Union[str, Path, IOBase, None],
            Sequence[Union[str, Path, IOBase, None]]
        ] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
    ) -> Union[
            Union[Path, str, IOBase],
            list[Union[Path, str, IOBase, Exception]]
        ]:
        """
        write S3 object(s) into file(s) or filelike object(s).

        Args:
            key: object key(s) (fully-qualified 'path(s)' from root)
            destination: where to write the retrieved object(s). May be path(s)
                or filelike object(s). If not specified, constructs new BytesIO
                buffer(s).
            config: optional transfer config

        Returns:
            outpath: the path, string, or buffer we wrote the object to, or,
                for multi-get, a list containing one such outpath for each
                successful write and an Exception for each failed write
        """
        # TODO: add more useful error messages for streams opened in text mode
        if config is None:
            config = self.config
        if destination is None:
            destination = BytesIO()
        if isinstance(destination, IOBase):
            self.client.download_fileobj(
                self.name, key, destination, Config=config
            )
        else:
            Path(destination).parent.mkdir(parents=True, exist_ok=True)
            self.client.download_file(
                self.name, key, destination, Config=config
            )
        if "seek" in dir(destination):
            destination.seek(0)
        return destination

    def read(
        self,
        key: str,
        mode: Literal["r", "rb"] = "r",
        return_buffer: bool = False,
    ) -> Union[bytes, str, BytesIO, StringIO]:
        """
        Read an S3 object into memory.

        Args:
            key: fully-qualified 'path' to object
            mode: 'r' to read as text, 'rb' as bytes
            return_buffer: if True, return object as a StringIO/BytesIO; if
                False, return it as str/bytes

        Returns:
            Contents of object, in format specified by `mode` and
                `return_buffer`.
        """
        buf = self.get(key)
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
        extra_args: Optional[Mapping[str, str]] = None,
        config: Optional[boto3.s3.transfer.TransferConfig] = None,
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
            extra_args: ExtraArgs for boto3 bucket object
            config: optional transfer config

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

    def ls(
        self,
        prefix: Optional[str] = None,
        recursive: bool = False,
        formatting: Literal["simple", "contents", "df", "raw"] = "simple",
        cache: Union[str, Path, IOBase, None] = None,
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

    @splitwrap(seq_arity=1)
    def rm(self, key: str) -> Union[None, list[Optional[None]]]:
        """
        Delete an S3 object.

        Args:
            key: key of object to delete (fully-qualified 'path' from root)

        Returns:
            None, or, for multi-delete, a list containing None for successful
                deletes and an Exception for failed
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
