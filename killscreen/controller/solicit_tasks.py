import datetime as dt
import json
import logging

from pathlib import Path
from socket import gethostname
import time

import requests
import rich.console
import sh

from killscreen.controller.parsing import default_output_parser
from killscreen.utilities import console_and_log, LOG_DIR_PATH, stamp
from killscreen.subutils import console_stream_handlers


t_console = rich.console.Console()


def get_orders(command_url):
    orders = None
    while orders is None:
        try:
            order_response = requests.get(command_url)
            console_and_log(
                f"{stamp()} {order_response.status_code} "
                f"{order_response.content}"
            )
            orders = order_response.json()
        except (requests.exceptions.ConnectionError, ConnectionError) as e:
            t_console.print(f"[bold orange]{e}")
            time.sleep(2)
        except Exception as e:
            console_and_log(f"{type(e)}, {e}", level="error", style="bold red")
            raise
    return orders


def pipeline_stream_handler(out_list, err_list, verbose=False):
    out_targets = [
        getattr(logging, "info"),
        lambda msg: out_list.append(msg.replace("\r", "").strip()),
    ]
    if verbose:
        out_targets.append(t_console)
    err_targets = [
        getattr(logging, "error"),
        lambda msg: err_list.append(msg.replace("\r", "").strip()),
        lambda msg: t_console.print(msg, style="bold red"),
    ]
    return console_stream_handlers(out_targets, err_targets)


def execute_pipeline_script(
    task_id,
    pipeline_kwargs,
    pipeline_interpreter,
    pipeline_script,
    cleanup_function=None,
    cleanup_kwargs=None,
    output_parser_function=None,
    verbose_handling=False,
):
    start_time = dt.datetime.utcnow()
    out_list, err_list = [], []
    if output_parser_function is None:
        output_parser_function = default_output_parser
    process = None
    if pipeline_kwargs is None:
        pipeline_kwargs = {}
    try:
        command = sh.Command(pipeline_interpreter)
        if Path(pipeline_script).absolute().exists():
            script = str(Path(pipeline_script).absolute())
        else:
            raise FileNotFoundError(
                f"{str(Path(pipeline_script).absolute())} not found in system."
            )
        process = command(
            script,
            **pipeline_kwargs,
            _bg=True,
            _bg_exc=False,
            **pipeline_stream_handler(out_list, err_list, verbose_handling),
        )
        process.wait()
        exception = None
        status = "completed"
    except Exception as error:
        console_and_log(f"{type(error)}: {error}", "error")
        status = "execution failure"
        exception = error
    parsed_output = output_parser_function(
        process, out_list, err_list, exception
    )
    parsed_output["status"] = status
    if cleanup_function is not None:
        cleanup_dict = {
            "cleanup_status": "",
            "cleanup_exc_type": "",
            "cleanup_exc_msg": "",
        }
        if cleanup_kwargs is None:
            cleanup_kwargs = {}
        try:
            cleanup_function(out_list, err_list, **cleanup_kwargs)
            cleanup_dict["cleanup_status"] = "completed"
        except Exception as cleanup_exception:
            cleanup_dict["cleanup_status"] = "failure"
            cleanup_dict["cleanup_exc_type"] = str(type(cleanup_exception))
            cleanup_dict["cleanup_exc_msg"] = str(cleanup_exception)
            console_and_log(
                f"{cleanup_dict['cleanup_exc_type']}: "
                f"{cleanup_dict['cleanup_exc_msg']}",
                "error",
            )
        parsed_output |= cleanup_dict
    end_time = dt.datetime.utcnow()
    return {
        "task_id": task_id,
        "start_time": start_time.isoformat()[:-7],
        "end_time": end_time.isoformat()[:-7],
        "total_duration": (end_time - start_time).total_seconds(),
    } | parsed_output


def execute_pipeline_function(task_id, pipeline_kwargs, pipeline_function):
    start_time = dt.datetime.utcnow()
    console_and_log(stamp() + f"attempting {task_id}")
    parsed_output = {}
    if pipeline_kwargs is None:
        pipeline_kwargs = {}
    try:
        parsed_output |= pipeline_function(**pipeline_kwargs)
        status = "completed"
    except Exception as error:
        console_and_log(f"{type(error)}: {error}", "error")
        status = "execution failure"
        parsed_output |= {
            "pipeline_exc_type": str(type(error)),
            "pipeline_exc_msg": str(error),
        }
    parsed_output["status"] = status
    end_time = dt.datetime.utcnow()
    return {
        "task_id": task_id,
        "start_time": start_time.isoformat()[:-7],
        "end_time": end_time.isoformat()[:-7],
        "total_duration": (end_time - start_time).total_seconds(),
    } | parsed_output


def task_solicitation_server(
    base_url,
    execution_target=execute_pipeline_script,
    log_file=None,
    **execution_kwargs,
):
    if log_file is None:
        log_file = Path(LOG_DIR_PATH, "pipeline.log")
    logging.basicConfig(
        filename=log_file, encoding="utf-8", level=logging.INFO
    )
    orders = {"command": ""}
    console_and_log(f"{stamp()} initializing pipeline requests")
    while orders["command"] != "stop":
        console_and_log(f"{stamp()} requesting new orders")
        orders = get_orders(base_url + "/command")
        if orders["command"] == "wait":
            time.sleep(**orders["parameters"])
            continue
        if orders["command"] == "execute":
            bailout = False
            console_and_log(f"{stamp()} executing {orders['task_id']}")
            try:
                pipeline_results = execution_target(
                    orders["task_id"],
                    orders.get("parameters"),
                    **execution_kwargs,
                ) | {"host": gethostname()}
                console_and_log(
                    f"{stamp()} pipeline execution complete for "
                    f"{orders['task_id']}"
                )
            except Exception as e:
                console_and_log(
                    stamp() + f"{type(e)},{e}", level="error", style="bold red"
                )
                pipeline_results = {
                    "task_id": orders["task_id"],
                    "status": "handler failure",
                    "host": gethostname(),
                    "exc_type": str(type(e)),
                    "exc_msg": str(e),
                }
                console_and_log(
                    f"{stamp()} pipeline execution failed for "
                    f"{orders['task_id']}"
                )
                # TODO: not catching this correctly for some reason.
                if isinstance(e, KeyboardInterrupt):
                    bailout = True
            t_console.print(pipeline_results)
            requests.post(base_url + "/report", json.dumps(pipeline_results))
            console_and_log(f"{stamp()} report sent to {base_url}/report")
            if bailout is True:
                console_and_log(
                    f"{stamp()} received user-requested interrupt, quitting."
                )
    console_and_log(f"{stamp()} received stop command, quitting.")
