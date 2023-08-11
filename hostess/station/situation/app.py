from typing import Optional

import fire
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import HorizontalScroll
from textual.widget import Widget

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


class SituationApp(App):
    def __init__(
        self,
        *args,
        host=None,
        port=None,
        station_name="station",
        update_interval=1.0,
        **kwargs
    ):
        if (host is None) or (port is None):
            raise TypeError("host and port must be defined to initialize app")
        super().__init__(*args, **kwargs)
        self.host, self.port, self.station_name = host, port, station_name
        self.situation,  self.update_interval = {}, update_interval

    def compose(self) -> ComposeResult:
        yield ConnectionStatus(id="statusbar")
        with HorizontalScroll():
            yield DictColumn(id="station-column", name="Station")
            yield DictColumn(id="delegate-column", name="Delegates")
        # TODO: leaving this dead code in for now
        # yield Pretty(DEFAULT_PROFILER, id="profiler-panel")
        # yield Pretty(DEFAULT_TICKER, id="ticker-panel")

    def _handle_error(self, err: Exception, error_label: Widget):
        error_label.update(Text(f"status: error -- {err}", style="bold red"))
        if isinstance(err, ConnectionError):
            try:
                self.port = get_port_from_shared_memory(self.station_name)
            except (FileNotFoundError, TypeError, ValueError):
                return

    def update_view(self):
        if self.fetching is True:
            return
        error_label = self.query_one("#statusbar")
        try:
            self.fetching = True
            self.situation = get_situation(self.host, self.port)
            dcol, scol = self._populate_column_content()
            error_label.update(Text("status -- ok", style="bold green"))
            scol.update()
            dcol.update()
            # TODO: leaving this dead code in for now
            # ppanel: Pretty = self.query_one("#profiler-panel")
            # ppanel.update(DEFAULT_PROFILER)
            # tpanel: Pretty = self.query_one("#ticker-panel")
            # tpanel.update(DEFAULT_TICKER)
        except (
            TypeError,
            AttributeError,
            TimeoutError,
            ConnectionError,
        ) as err:  # TODO: concatenate / specify those exceptions
            return self._handle_error(err, error_label)
        finally:
            self.fetching = False

    def _populate_column_content(self):
        scol: DictColumn = self.query_one("#station-column")
        scol.mapping = organize_station(self.situation)
        scol.patterns = STATION_PATTERNS
        dcol: DictColumn = self.query_one("#delegate-column")
        dcol.mapping = organize_delegates(self.situation)
        dcol.patterns = DELEGATE_PATTERNS
        return dcol, scol

    def on_mount(self):
        self.set_interval(1, self.update_view)

    fetching = False


def run_situation_app(
    station_name: str = "station",
    host: str = "localhost",
    port: Optional[str] = None,
    update_interval: float = 1
):
    if port is None:
        port = get_port_from_shared_memory(station_name)
    app = SituationApp(
        host=host,
        port=port,
        station_name=station_name,
        update_interval=update_interval
    )
    app.run()


if __name__ == "__main__":
    fire.Fire(run_situation_app)
