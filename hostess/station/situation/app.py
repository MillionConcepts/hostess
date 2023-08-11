from typing import Optional

import fire
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import HorizontalScroll
from textual.widget import Widget
from textual.widgets import Pretty

from hostess.monitors import Stopwatch
from hostess.profilers import Profiler
from hostess.station.situation.interface_organizers import (
    get_situation,
    organize_delegates,
    organize_station,
)
from hostess.station.situation.widgets import DictColumn, ConnectionStatus
from hostess.station.station import get_port_from_shared_memory

# patterns defining sorting/retention of TreeNodes
STATION_PATTERNS = {
    "sort": (
        lambda l: (len(l) == 2) and (l[1] == "actors"),
        lambda l: (len(l) in (3, 4)) and l[1] == "tasks",
    ),
    "protect": (lambda l: (len(l) == 4) and l[1] == "tasks",),
}
DELEGATE_PATTERNS = {
    "sort": (
        lambda l: (len(l) == 3) and (l[2] == "actors"),
        lambda l: (len(l) in (3, 4)) and l[1] == "running",
    ),
    "protect": (lambda l: (len(l) == 4) and l[1] == "running",),
}

APP_PROFILER = Profiler({"time": Stopwatch()})


class SituationApp(App):
    def __init__(
        self,
        *args,
        host,
        fixed_port=None,
        station_name="station",
        update_interval=1.0,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.host, self.fixed_port = host, fixed_port
        self.station_name = station_name
        self.situation, self.update_interval = {}, update_interval

    def compose(self) -> ComposeResult:
        yield ConnectionStatus(id="statusbar")
        with HorizontalScroll():
            yield DictColumn(id="station-column", name="Station")
            yield DictColumn(id="delegate-column", name="Delegates")
        yield Pretty(APP_PROFILER, id="profiler-panel")

    def _handle_error(self, err: Exception, error_label: Widget):
        if isinstance(err, ConnectionError):
            try:
                self.port = get_port_from_shared_memory(self.station_name)
            except (FileNotFoundError, TypeError, ValueError) as ftve:
                err = ftve
        error_label.update(
            Text(f"status: error -- {err}", style="bold red")
        )

    def _populate_column_content(self):
        scol: DictColumn = self.query_one("#station-column")
        scol.mapping = organize_station(self.situation)
        scol.patterns = STATION_PATTERNS
        dcol: DictColumn = self.query_one("#delegate-column")
        dcol.mapping = organize_delegates(self.situation)
        dcol.patterns = DELEGATE_PATTERNS
        return dcol, scol

    def update_view(self):
        if self.fetching is True:
            return
        error_label = self.query_one("#statusbar")
        try:
            self.fetching = True
            if (self.port is None) and (self.fixed_port is None):
                self.port = get_port_from_shared_memory(self.station_name)
            APP_PROFILER.reset()
            with APP_PROFILER.context("fetching situation"):
                self.situation = get_situation(self.host, self.port)
            with APP_PROFILER.context("parsing content"):
                dcol, scol = self._populate_column_content()
            error_label.update(Text("status -- ok", style="bold green"))
            with APP_PROFILER.context("rendering new content"):
                scol.update()
                dcol.update()
            ppanel: Pretty = self.query_one("#profiler-panel")
            ppanel.update(APP_PROFILER)
        except (
            ValueError,
            AttributeError,
            TimeoutError,
            ConnectionError,
        ) as err:  # TODO: concatenate / specify those exceptions
            return self._handle_error(err, error_label)
        finally:
            self.fetching = False

    def on_mount(self):
        self.set_interval(1, self.update_view)

    port = None
    fetching = False


def run_situation_app(
    station_name: str = "station",
    host: str = "localhost",
    fixed_port: Optional[int] = None,
    update_interval: float = 1,
):
    app = SituationApp(
        host=host,
        fixed_port=fixed_port,
        station_name=station_name,
        update_interval=update_interval,
    )
    app.run()


if __name__ == "__main__":
    fire.Fire(run_situation_app)
