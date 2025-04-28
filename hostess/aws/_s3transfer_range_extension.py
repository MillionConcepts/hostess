import collections
from typing import Optional

from s3transfer.processpool import GetObjectSubmitter, ProcessPoolDownloader
from s3transfer.utils import calculate_num_parts, CallArgs



DownloadFileRequestWithRange = collections.namedtuple(
    'DownloadFileRequestWithRange',
    [
        'transfer_id',  # The unique id for the transfer
        'bucket',  # The bucket to download the object from
        'key',  # The key to download the object from
        'filename',  # The user-requested download location
        'extra_args',  # Extra arguments to provide to client calls
        'start_byte',
        'end_byte'
    ],
)


def calculate_range_parameter_with_bounds(
    part_size, 
    part_index, 
    num_parts, 
    start_byte, 
    end_byte
):
    start_range = part_index * part_size + start_byte
    if part_index == num_parts - 1:
        end_range = end_byte
    else:
        end_range = start_range + part_size - 1
    range_param = f'bytes={start_range}-{end_range}'
    return range_param


class GetObjectSubmitterWithRange(GetObjectSubmitter):

    def _get_size(self, download_file_request):
        
        size = (
            download_file_request.end_byte 
            - download_file_request.start_byte 
            + 1
        )
        return size

    def _submit_get_object_jobs(self, download_file_request):
        size = self._get_size(download_file_request)
        temp_filename = self._allocate_temp_file(download_file_request, size)
        self._submit_ranged_get_object_jobs(
            download_file_request, temp_filename, size
        )

    def _submit_ranged_get_object_jobs(
        self, download_file_request, temp_filename, size
    ):
        part_size = self._transfer_config.multipart_chunksize
        num_parts = calculate_num_parts(size, part_size)
        self._notify_jobs_to_complete(
            download_file_request.transfer_id, num_parts
        )
        for i in range(num_parts):
            offset = i * part_size
            range_parameter = calculate_range_parameter_with_bounds(
                part_size, 
                i, 
                num_parts, 
                download_file_request.start_byte, 
                download_file_request.end_byte
            )
            get_object_kwargs = {'Range': range_parameter}
            get_object_kwargs.update(download_file_request.extra_args)
            self._submit_get_object_job(
                transfer_id=download_file_request.transfer_id,
                bucket=download_file_request.bucket,
                key=download_file_request.key,
                temp_filename=temp_filename,
                offset=offset,
                extra_args=get_object_kwargs,
                filename=download_file_request.filename,
            )

    def _submit_single_get_object_job(
        self, download_file_request, temp_filename
    ):
        self._notify_jobs_to_complete(download_file_request.transfer_id, 1)
        self._submit_get_object_job(
            transfer_id=download_file_request.transfer_id,
            bucket=download_file_request.bucket,
            key=download_file_request.key,
            temp_filename=temp_filename,
            offset=download_file_request.start_byte,
            extra_args=download_file_request.extra_args,
            filename=download_file_request.filename,
        )


class ProcessPoolDownloaderWithRange(ProcessPoolDownloader):
    def download_file(
        self, 
        bucket, 
        key, 
        filename, 
        extra_args=None,
        start_byte: Optional[int] = None, 
        end_byte: Optional[int] = None
    ):
        self._start_if_needed()
        if extra_args is None:
            extra_args = {}
        self._validate_all_known_args(extra_args)
        transfer_id = self._transfer_monitor.notify_new_transfer()
        start_byte = 0 if start_byte is None else start_byte
        if end_byte is None or end_byte < 0:
            total_size = self._client_factory.create_client().head_object(
                Bucket=bucket,
                Key=key,
                **extra_args,
            )['ContentLength']
            seekback = 0 if end_byte is None else end_byte
            end_byte = total_size + seekback - 1
        download_file_request = DownloadFileRequestWithRange(
            transfer_id=transfer_id,
            bucket=bucket,
            key=key,
            filename=filename,
            extra_args=extra_args,
            start_byte=start_byte,
            end_byte=end_byte
        )

        self._download_request_queue.put(download_file_request)
        call_args = CallArgs(
            bucket=bucket,
            key=key,
            filename=filename,
            extra_args=extra_args,
            start_byte=start_byte,
            end_byte=end_byte
        )
        future = self._get_transfer_future(transfer_id, call_args)
        return future
    
    def _start_submitter(self):
        self._submitter = GetObjectSubmitterWithRange(
            transfer_config=self._transfer_config,
            client_factory=self._client_factory,
            transfer_monitor=self._transfer_monitor,
            osutil=self._osutil,
            download_request_queue=self._download_request_queue,
            worker_queue=self._worker_queue,
        )
        self._submitter.start()
    