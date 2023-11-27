from typing import Optional

import fire
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import HorizontalScroll, VerticalScroll, Container
from textual.css.query import NoMatches
from textual.widgets import Pretty, Collapsible, Label, \
    TabbedContent, TabPane

from hostess.monitors import Stopwatch
from hostess.profilers import Profiler
from hostess.station.situation.interface_organizers import (
    get_situation,
    organize_delegates,
    organize_station,
)
from hostess.station.situation.widgets import DictColumn, ConnectionStatus
from hostess.station.station import get_port_from_shared_memory

STATION_PATTERNS = {
    "sort": (
        lambda l: (len(l) == 2) and (l[1] == "actors"),
        lambda l: (len(l) in (3, 4)) and l[1] == "tasks",
    ),
    "protect": (lambda l: (len(l) == 4) and l[1] == "tasks",),
}
"""patterns defining sorting/retention of TreeNodes for Station pane"""
DELEGATE_PATTERNS = {
    "sort": (
        lambda l: (len(l) == 3) and (l[2] == "actors"),
        lambda l: (len(l) in (3, 4)) and l[1] == "running",
    ),
    "protect": (lambda l: (len(l) == 4) and l[1] == "running",),
}
"""patterns defining sorting/retention of TreeNodes for Delegate pane"""


class Logfiles:

    def __init__(self):
        self.logfiles = {}

    def update_from(self, situation):
        self.logfiles['station'] = situation['logfile']
        for d, v in situation['delegates'].items():
            self.logfiles[d] = v['logfile']


LOGFILES = Logfiles()
APP_PROFILER = Profiler({"time": Stopwatch()})


def make_logline(k, v):
    vert = VerticalScroll(Label("", id=f'log-tail-{k}'))
    vert.styles.max_height = "60vh"
    collapse = Collapsible(
        vert,
        title=f"{k}: {v}",
        id=f'logfile-{k}',
        collapsed=True
    )
    collapse.styles.max_height = "64vh"
    return collapse


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
        self.log_positions = {}

    def compose(self) -> ComposeResult:
        yield ConnectionStatus("status: initializing", id="statusbar")
        content = Container()
        with content:
            with TabbedContent(initial="situation"):
                with TabPane(title="situation", id='situation'):
                    container = HorizontalScroll(id='main-container')
                    with container:
                        yield DictColumn(id="station-column", name="Station")
                        yield DictColumn(id="delegate-column", name="Delegates")
                    with Collapsible(
                        title='profiler',
                            collapsed=True,
                            id='profiler-collapse'
                    ):
                        yield Pretty(APP_PROFILER, id="profiler")
                with TabPane(title="logs", id='logs', classes='log-pane'):
                    yield VerticalScroll(id="logs-container")

    def _handle_error(self, err: Exception):
        if isinstance(err, TimeoutError):
            self.query_one('#statusbar').update(
                Text(f"status -- delayed", style="light_salmon3")
            )
            return
        if isinstance(err, ConnectionError):
            try:
                self.port = get_port_from_shared_memory(self.station_name)
            except (FileNotFoundError, TypeError, ValueError) as ftve:
                err = ftve

        self.query_one('#statusbar').update(
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

    def update_logs(self):
        for k, v in LOGFILES.logfiles.items():
            try:
                logline = self.query_one(f"#logfile-{k}")
            except NoMatches:
                self.query_one('#logs-container').mount(
                    make_logline(k, v)
                )
                logline = self.query_one(f"#logfile-{k}")
            if not logline.collapsed:
                tail = self.query_one(f'#log-tail-{k}')
                with v.open() as stream:
                    if k in self.log_positions:
                        stream.seek(self.log_positions[k])
                    content = stream.read()
                    if len(content) > 0:
                        self.log_positions[k] = stream.tell()
                        tail.update(tail.renderable + content)

    def update_from_situation(self):
        with APP_PROFILER.context("parsing content"):
            dcol, scol = self._populate_column_content()
        LOGFILES.update_from(self.situation)
        with APP_PROFILER.context("rendering new content"):
            scol.update()
            dcol.update()

    def update_view(self):
        self.update_logs()
        if self.query_one(".log-pane").styles.display != 'none':
            self.update_logs()
            if self.has_content is True:
                self.deferred_situation_update = True
        if self.fetching is True:
            return
        try:
            if self.deferred_situation_update is True:
                self.update_from_situation()
            self.fetching = True
            if (self.port is None) and (self.fixed_port is None):
                self.port = get_port_from_shared_memory(self.station_name)
            APP_PROFILER.reset()
            with APP_PROFILER.context("fetching situation"):
                self.situation = get_situation(self.host, self.port)
            self.has_content = True
            self.update_from_situation()
            self.query_one('#statusbar').update(
                Text("status -- ok", style="bold green")
            )
            self.query_one("#profiler").update(APP_PROFILER)
        except (
            ValueError,
            AttributeError,
            TimeoutError,
            ConnectionError,
            FileNotFoundError
        ) as err:  # TODO: concatenate / specify those exceptions
            return self._handle_error(err)
        finally:
            self.fetching = False

    def on_mount(self):
        self.set_interval(1, self.update_view)

    port = None
    fetching = False
    has_content = False
    deferred_situation_update = False


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
