import datetime as dt
import random
import sys
import time
from collections import defaultdict
from copy import deepcopy
from typing import Optional

from cytoolz import keyfilter, keymap, groupby, valmap
from dustgoggles.codex.implements import Sticky
from dustgoggles.codex.memutilz import (
    deactivate_shared_memory_resource_tracker,
)
from dustgoggles.structures import NestingDict
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import HorizontalScroll, VerticalScroll
from textual.widgets import Label, Tree, Button
from textual.widgets._tree import TreeNode

from hostess.station.comm import read_comm
from hostess.station.messages import unpack_obj
from hostess.station.talkie import stsend

deactivate_shared_memory_resource_tracker()
PORT = Sticky("test_station_port").read()
print(PORT)


def get_view():
    response, _ = stsend(b"view", "localhost", PORT)
    comm = read_comm(response)
    return unpack_obj(comm["body"])


def mutate_mapping(mapping):
    for k, v in mapping.items():
        mapping[k] = random.randint(1, 10)


def add_config_to_actors(actors: dict, config: dict) -> dict:
    actors = deepcopy(actors)
    for name, props in actors.items():  # TODO: more elegant as groupby?
        inter = keyfilter(
            lambda k: k.split('_')[0] == name, config['interface']
        )
        if len(inter) > 0:
            props['interface'] = keymap(
                lambda k: "_".join(k.split("_")[1:]), inter
            )
        if (actor_config := config['cdict'].get(name)) is not None:
            props['cdict'] = actor_config
    return actors


def organize_tasks(tasks: dict) -> dict:
    out = NestingDict()
    for code, task in tasks.items():
        target = out[task['status']][task['name']][code]
        target['delegate'] = task['delegate']
        for k in ('description', 'duration'):
            if (v := task.get(k)) not in (None, {}):
                target[k] = v
        if target.get('duration') is None:
            target['duration'] = time.time() - task['init_time'].timestamp()
        target['times'] = valmap(
            lambda t: t if not isinstance(t, dt.datetime) else t.isoformat(),
            keyfilter(lambda f: f.endswith("_time"), task)
        )
        # NOTE: skipping action id as it is currently nowhere used.
    return out.todict()


def organize_station_view(view: dict) -> dict:
    view = keyfilter(lambda k: k != 'delegates', view)
    return {
        'id': f"{view['name']} -- {view['host']}:{view['port']}",
        'actors': add_config_to_actors(view['actors'], view['config']),
        'tasks': organize_tasks(view['tasks'])
    }


def add_children(node: TreeNode, obj: list | tuple | dict):
    if isinstance(obj, (list, tuple)):
        items = enumerate(obj)
    elif isinstance(obj, dict):
        items = obj.items()
    else:
        raise TypeError
    new_children = []
    for k, v in items:
        # TODO: is there a nicer way to do this? it's hard to really attach
        #  a fiducial at the station level.
        matches = [
            c for c in node.children if str(c.label) in (str(k), f"{k}: {v}")
        ]
        assert len(matches) < 2, f"too many matches on {k}"
        new_children += matches
        child = None if len(matches) == 0 else matches[0]
        if isinstance(v, (dict, list, tuple)):
            if child is None:
                new_children.append(child := node.add(str(k)))
            add_children(child, v)
        elif child is None:
            new_children.append(node.add_leaf(f"{k}: {v}"))
    for c in filter(lambda c: c not in new_children, node.children):
        c.remove()


def dict_to_tree(
    dictionary: dict,
    name: str = "",
    tree: Optional[Tree] = None,
    **tree_kwargs,
) -> Tree:
    tree = tree if tree is not None else Tree(name, **tree_kwargs)
    tree.root.expand()
    add_children(tree.root, dictionary)
    # tree.root.expand_all()
    return tree


class UpdateButton(Button):
    def __init__(self, *args, target, **kwargs):
        super().__init__(*args, **kwargs)
        self.target = target

    def press(self):
        self.target.update()


class DictColumn(VerticalScroll):
    DEFAULT_CSS = """
    Column {
        height: 1fr;
        width: 32;
        margin: 0 2;
    }
    """
    mapping = {}

    def on_mount(self):
        self.mount(UpdateButton("update", target=self, id=f"{self.id}-update"))

    def update(self) -> None:
        if len(self.children) == 1:
            self.mount(dict_to_tree(self.mapping, id=f"{self.id}-tree"))
            return
        tree = self.query_one(f"#{self.id}-tree")
        dict_to_tree(self.mapping, tree=tree)


class StationApp(App):
    def compose(self) -> ComposeResult:
        yield Label(id="error-label")
        # yield Footer(id="Footer")
        with HorizontalScroll():
            yield DictColumn(id="station-column")

    def update_view(self):
        error_label = self.query_one("#error-label")
        try:
            self.view = get_view()
            scol = self.query_one("#station-column")
            scol.mapping = organize_station_view(self.view)
            error_label.update(Text("ok", style="bold green"))
            scol.update()
        except (TypeError, AssertionError) as err:
            error_label.update(Text(f"error: {err}", style="bold red"))

    def on_mount(self):
        self.set_interval(1, self.update_view)

    view = {}


if __name__ == "__main__":
    app = StationApp()
    app.run()
