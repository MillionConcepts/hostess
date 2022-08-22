import json
from pathlib import Path
import re
import shutil
from typing import Mapping, Sequence, Optional, Callable

from cytoolz import first, valfilter
from flask import Flask
from flask import request
import rich.console

from killscreen.config import GENERAL_DEFAULTS
from killscreen.monitors import Bouncer, FakeBouncer
from killscreen.utilities import filestamp

console = rich.console.Console()

DEFAULT_LOG_COLUMNS = (
    "task_id",
    "status",
    "return_code",
    "start_time",
    "end_time",
    "total_duration",
    "pipeline_exc_type",
    "pipeline_exc_msg",
    "exc_type",
    "exc_msg",
    "host",
)


def task_server(
    tasks: Mapping,
    log_columns: Sequence[str] = DEFAULT_LOG_COLUMNS,
    default_app_state: Optional[Mapping] = None,
    task_filter: Optional[Callable] = None,
    server_name: str = "pipeline",
    ratelimit: int = 20,
    run=False,
    host="0.0.0.0",
    port=6000,
    logfile=None,
):
    """
    generic task server. suitable for some tasks all on its own, or can be
    used as a prototype for more complex behaviors.
    """
    if logfile is None:
        logfile = Path(
            GENERAL_DEFAULTS["log_path"],
            f"{server_name}_{filestamp()}_reports.csv",
        )
    logfile = Path(logfile)
    if not logfile.parent.exists():
        logfile.parent.mkdir(parents=True)
    app = Flask(__name__)
    app_state = {"running": True}
    if default_app_state is not None:
        app_state = app_state | default_app_state
    if task_filter is None:

        def task_filter(taskmap):
            return first(valfilter(lambda v: v["status"] == "ready", taskmap))

    if ratelimit > 0:
        bouncer = Bouncer(ratelimit=ratelimit)
    else:
        bouncer = FakeBouncer()

    @app.route("/report", methods=["POST"])
    def take_report():
        """
        receive a status report from a worker on a task. log this report and
        update in-memory representation of tasks.
        """
        try:
            # get report as json
            report = json.loads(request.get_data())
            task_id, status = report["task_id"], report["status"]
            # print status to console
            color = "green" if "successful" in status else "yellow"
            console.print(report, style=f"bold {color}")
            # update status in dictionary of tasks
            tasks[report["task_id"]]["status"] = status
            # log results
            if not logfile.exists():
                with logfile.open("w+") as log:
                    log.write(",".join(log_columns) + "\n")
            log_fields = [
                re.sub(r"[,\n]", "_", str(report.get(col)))
                for col in log_columns
            ]
            with logfile.open("a") as log:
                log.write(",".join(log_fields) + "\n")
            return "", 200
        except KeyboardInterrupt:
            raise
        except (KeyError, ValueError, TypeError, OSError) as e:
            # print error; log error
            console.print(f"{type(e)}: {e}", style="bold red")
            # TODO: add metadata about request
            log_fields = [
                "" if col != "error_msg" else str(e) for col in log_columns
            ]
            with logfile.open("a") as log:
                log.write(",".join(log_fields) + "\n")
            return "could not parse request", 400

    @app.route("/command", methods=["GET"])
    def issue_request():
        bouncer.click()
        if app_state["running"] is False:
            return json.dumps({"command": "stop", "parameters": {}})
        try:
            task_id = task_filter(tasks)
            task = tasks[task_id]
        except StopIteration:
            console.print("[bold cyan]list complete, sending stop command")
            dump_state()
            return json.dumps({"command": "stop", "parameters": {}})
        instruction = {"command": "execute", "task_id": task_id}
        parameters = task.get("parameters")
        if parameters is not None:
            instruction["parameters"] = parameters
        tasks[task_id]["status"] = "pending"
        return json.dumps(instruction)

    @app.route("/quit", methods=["GET"])
    def closeout():
        console.print(
            "entering shutdown mode, sending stop messages in response to all "
            "further requests"
        )
        dump_state()
        app_state["running"] = False
        return ""

    def dump_state():
        dumpfile = Path(
            logfile.parent, f"{server_name}_{filestamp()}_tasks.json"
        )
        with Path(dumpfile).open("w+") as dump_stream:
            json.dump(
                [
                    {"task_id": k, "status": v["status"]}
                    for k, v in tasks.items()
                ],
                dump_stream,
                indent=2,
            )
        shutil.copyfile(
            dumpfile, Path(logfile.parent, f"{server_name}_latest_tasks.json")
        )
        if Path(logfile).exists():
            shutil.copyfile(
                logfile,
                Path(logfile.parent, f"{server_name}_latest_reports.csv"),
            )

    app.debug = True

    if run is True:
        app.run(host, port, use_reloader=False)
        dump_state()

    return app
