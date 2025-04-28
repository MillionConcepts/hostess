from s3transfer.download import DownloadSubmissionTask, GetObjectTask
from s3transfer.manager import TransferManager
from s3transfer.utils import (
    CountCallbackInvoker, calculate_num_parts, get_callbacks, CallArgs
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


class DownloadSubmissionTaskWithRange(DownloadSubmissionTask):
    def _submit(
        self,
        client,
        config,
        osutil,
        request_executor,
        io_executor,
        transfer_future,
        bandwidth_limiter=None,
    ):
        download_output_manager = self._get_download_output_manager_cls(
            transfer_future, osutil
        )(osutil, self._transfer_coordinator, io_executor)
        self._submit_ranged_download_request(
            client,
            config,
            osutil,
            request_executor,
            io_executor,
            download_output_manager,
            transfer_future,
            bandwidth_limiter,
        )


    def _submit_ranged_download_request(
        self,
        client,
        config,
        osutil,
        request_executor,
        io_executor,
        download_output_manager,
        transfer_future,
        bandwidth_limiter,
    ):
        call_args = transfer_future.meta.call_args

        # Get the needed progress callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, 'progress')

        # Get a handle to the file that will be used for writing downloaded
        # contents
        fileobj = download_output_manager.get_fileobj_for_io_writes(
            transfer_future
        )
        call_args = transfer_future.meta.call_args
        # Determine the number of parts
        part_size = config.multipart_chunksize
        start_byte = call_args.start_byte
        end_byte = call_args.end_byte
        start_byte = 0 if start_byte is None else start_byte
        if end_byte is None or end_byte < 0:
            total_size = client.head_object(
                Bucket=call_args.bucket,
                Key=call_args.key,
                **call_args.extra_args,
            )['ContentLength']
            seekback = 0 if end_byte is None else end_byte
            end_byte = total_size + seekback - 1

        num_parts = calculate_num_parts(end_byte - start_byte, part_size)

        # Get any associated tags for the get object task.
        get_object_tag = download_output_manager.get_download_task_tag()

        # Callback invoker to submit the final io task once all downloads
        # are complete.
        finalize_download_invoker = CountCallbackInvoker(
            self._get_final_io_task_submission_callback(
                download_output_manager, io_executor
            )
        )
        for i in range(num_parts):
            # Calculate the range parameter
            range_parameter = calculate_range_parameter_with_bounds(
                part_size, i, num_parts, start_byte, end_byte
            )

            # Inject the Range parameter to the parameters to be passed in
            # as extra args
            extra_args = {'Range': range_parameter}
            extra_args.update(call_args.extra_args)
            finalize_download_invoker.increment()
            # Submit the ranged downloads
            self._transfer_coordinator.submit(
                request_executor,
                GetObjectTask(
                    transfer_coordinator=self._transfer_coordinator,
                    main_kwargs={
                        'client': client,
                        'bucket': call_args.bucket,
                        'key': call_args.key,
                        'fileobj': fileobj,
                        'extra_args': extra_args,
                        'callbacks': progress_callbacks,
                        'max_attempts': config.num_download_attempts,
                        'start_index': i * part_size,
                        'download_output_manager': download_output_manager,
                        'io_chunksize': config.io_chunksize,
                        'bandwidth_limiter': bandwidth_limiter,
                    },
                    done_callbacks=[finalize_download_invoker.decrement],
                ),
                tag=get_object_tag,
            )
        finalize_download_invoker.finalize()


class TransferManagerWithRange(TransferManager):
    def download(
        self,
        bucket,
        key,
        fileobj,
        extra_args=None,
        subscribers=None,
        start_byte=None,
        end_byte=None
    ):
        if extra_args is None:
            extra_args = {}
        if subscribers is None:
            subscribers = []
        self._validate_all_known_args(extra_args, self.ALLOWED_DOWNLOAD_ARGS)
        self._validate_if_bucket_supported(bucket)
        call_args = CallArgs(
            bucket=bucket,
            key=key,
            fileobj=fileobj,
            extra_args=extra_args,
            subscribers=subscribers,
            start_byte=start_byte,
            end_byte=end_byte
        )
        extra_main_kwargs = {'io_executor': self._io_executor}
        if self._bandwidth_limiter:
            extra_main_kwargs['bandwidth_limiter'] = self._bandwidth_limiter
        return self._submit_transfer(
            call_args, DownloadSubmissionTaskWithRange, extra_main_kwargs
        )