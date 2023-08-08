from copy import deepcopy
import datetime as dt
import random
import time
from typing import Optional, Sequence, Callable, Mapping

from cytoolz import keyfilter, keymap, valmap
from dustgoggles.codex.implements import Sticky
from dustgoggles.codex.memutilz import (
    deactivate_shared_memory_resource_tracker,
)
from dustgoggles.func import constant
from dustgoggles.structures import NestingDict
from rich.text import Text
from textual._loop import loop_last
from textual.app import App, ComposeResult
from textual.containers import HorizontalScroll, VerticalScroll
from textual.geometry import Size
from textual.widgets import Label, Tree, Button
from textual.widgets._tree import TreeNode, TreeDataType, _TreeLine

from hostess.station.comm import read_comm
from hostess.station.messages import unpack_obj
from hostess.station.talkie import stsend
from hostess.utilities import curry

deactivate_shared_memory_resource_tracker()
PORT = Sticky("test_station_port").read()
print(PORT)


STATION_PATTERNS = {
    'sort': (
        lambda l: (len(l) == 2) and (l[1] == 'actors'),
        lambda l: (len(l) in (3, 4)) and l[1] == 'tasks'
    ),
    'protect': (
        lambda l: (len(l) == 4) and l[1] == 'tasks',
    )
}

DELEGATE_PATTERNS = {
    # 'sort': (
    #     lambda l: (len(l) == 2) and (l[1] == 'actors'),
    #     lambda l: (len(l) in (3, 4)) and l[1] == 'tasks'
    # ),
    # 'protect': (
    #     lambda l: (len(l) == 4) and l[1] == 'tasks',
    # )
}

def get_parent_labels(node: TreeNode):
    active_node, labels = node, []
    while active_node is not None:
        labels.append(str(active_node.label))
        active_node = active_node.parent
    return tuple(reversed(labels))


class SortingTree(Tree):

    def __init__(
        self,
        *args,
        patterns: Mapping[str, Sequence[Callable[[tuple[str]], bool]]] = None,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.patterns = patterns if patterns is not None else {}

    def match(self, node, pattern_type):
        for p in self.patterns.get(pattern_type, ()):
            try:
                if p(get_parent_labels(node)) is True:
                    return True
            except (IndexError, ValueError):
                continue
        return False

    def _build(self) -> None:
        """Builds the tree by traversing nodes, and creating tree lines."""

        TreeLine = _TreeLine
        lines: list[_TreeLine] = []
        add_line = lines.append

        root = self.root

        def add_node(
            path: list[TreeNode[TreeDataType]],
            node: TreeNode[TreeDataType],
            last: bool
        ) -> None:
            child_path = [*path, node]
            node._line = len(lines)
            add_line(TreeLine(child_path, last))
            if node._expanded:
                if self.match(node, 'sort'):
                    children = sorted(
                        node._children, key=lambda c: str(c.label)
                    )
                else:
                    children = node._children
                for last, child in loop_last(children):
                    add_node(child_path, child, last)

        if self.show_root:
            add_node([], root, True)
        else:
            for node in self.root._children:
                add_node([], node, True)
        self._tree_lines_cached = lines

        guide_depth = self.guide_depth
        show_root = self.show_root
        get_label_width = self.get_label_width

        def get_line_width(line: _TreeLine) -> int:
            return get_label_width(line.node) + line._get_guide_width(
                guide_depth, show_root
            )

        if lines:
            width = max([get_line_width(line) for line in lines])
        else:
            width = self.size.width

        # noinspection PyTypeChecker
        self.virtual_size = Size(width, len(lines))
        if self.cursor_line != -1:
            if self.cursor_node is not None:
                self.cursor_line = self.cursor_node._line
            if self.cursor_line >= len(lines):
                self.cursor_line = -1
        self.refresh()
# noinspection PyPep8Naming,PyProtectedMember


def get_view():
    # TODO: handle recurring queries a little more gracefully
    response, _ = stsend(b"view", "localhost", PORT, timeout=0.5)
    if response == "timeout":
        raise TimeoutError('dropped packet')
    comm = read_comm(response)
    return unpack_obj(comm["body"])


def mutate_mapping(mapping):
    for k, v in mapping.items():
        mapping[k] = random.randint(1, 10)


def add_config_to_elements(actors: dict | list, config: dict) -> dict:
    actors = deepcopy(actors)
    if isinstance(actors, list):  # stub information for Delegates
        actors = {a: {} for a in actors}
    # TODO: more elegant with groupbys?
    for name, props in actors.items():
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
        times = valmap(
            lambda t: t if not isinstance(t, dt.datetime) else t.isoformat(),
            keyfilter(lambda f: f.endswith("_time"), task)
        )
        title = f"{times['init_time'][:21]} ({code})"
        target = out[task['status']][task['name']][title]
        target['times'] = times
        target['delegate'] = task['delegate']
        for k in ('description', 'duration'):
            if (v := task.get(k)) not in (None, {}):
                target[k] = v
        if target.get('duration') is None:
            target['duration'] = time.time() - task['init_time'].timestamp()
        # NOTE: skipping action id as it is currently nowhere used.
    for k in ('success', 'running', 'sent', 'pending', 'crash'):
        # noinspection PyStatementEffect
        out[k]
    return out.todict()


def organize_running_actions(reports: dict) -> dict:
    out = NestingDict()
    for r in reports:
        action = r['action']
        times = action['time']
        title = f"{times['start'][:21]} ({r['instruction_id']})"
        target = out[action['name']][title]
        target['times'] = times
        # TODO: maybe we should squeeze descriptions into the ActionReports?
        target['duration'] = (
             time.time()
             - dt.datetime.fromisoformat(times['start']).timestamp()
        )
    return out.todict()


def organize_station_view(view: dict) -> dict:
    view = keyfilter(lambda k: k != 'delegates', view)
    return {
        'id': f"{view['name']}@{view.get('host', '?')}:{view['port']}",
        'actors': add_config_to_elements(view['actors'], view['config']),
        'tasks': organize_tasks(view['tasks'])
    }


def delegate_dict(ddict: Mapping) -> dict:
    config = {'cdict': ddict['cdict'], 'interface': ddict['interface']}
    out = {
        'running': organize_running_actions(ddict['running']),
        'actors': add_config_to_elements(ddict['actors'], config),
        'sensors': add_config_to_elements(ddict['sensors'], config),
        'wait_time': ddict['wait_time']
    }
    return out


def organize_delegate_view(view: dict) -> dict:
    out = {}
    for ddict in view['delegates']:
        id_ = (
            f"{ddict['name']}@{ddict['host']} (PID {ddict['pid']}): "
            f"{ddict['inferred_status']}"
        )
        out[id_] = delegate_dict(ddict)
    return out


def dict_to_children(node: TreeNode, obj: list | tuple | dict):
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
            dict_to_children(child, v)
        elif child is None:
            new_children.append(node.add_leaf(f"{k}: {v}"))
    if "match" not in dir(node.tree):
        protectmatch = constant(False)
    else:
        protectmatch = curry(node.tree.match)(pattern_type="protect")
    for c in filter(lambda c: c not in new_children, node.children):
        if protectmatch(c):
            c.remove_children()
        else:
            c.remove()


def dict_to_tree(
    dictionary: dict,
    name: str = "",
    tree: Optional[Tree] = None,
    **tree_kwargs,
) -> Tree:
    tree = tree if tree is not None else SortingTree(name, **tree_kwargs)
    tree.root.expand()
    dict_to_children(tree.root, dictionary)
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
    patterns = {}

    def on_mount(self):
        pass
        self.mount(Label(self.name))
        # self.mount(UpdateButton("update", target=self, id=f"{self.id}-update"))

    def update(self) -> None:
        if len(self.children) == 1:
            tree = dict_to_tree(
                self.mapping, id=f"{self.id}-tree", patterns=self.patterns
            )
            self.mount(tree)
            return
        tree = self.query_one(f"#{self.id}-tree")
        dict_to_tree(self.mapping, tree=tree, patterns=self.patterns)


class StatusBar(Label):
    DEFAULT_CSS = """
    StatusBar {
        height: 2vh;
        margin-bottom: 1;
    }
    """


# noinspection PyUnresolvedReferences
class StationApp(App):
    def compose(self) -> ComposeResult:
        yield StatusBar(id="statusbar")
        # yield Footer(id="Footer")
        with HorizontalScroll():
            yield DictColumn(id="station-column", name="Station")
            yield DictColumn(id="delegate-column", name="Delegates")

    def update_view(self):
        error_label = self.query_one("#statusbar")
        try:
            self.view = get_view()
            scol: DictColumn = self.query_one("#station-column")
            scol.mapping = organize_station_view(self.view)
            scol.patterns = STATION_PATTERNS
            dcol = self.query_one("#delegate-column")
            dcol.mapping = organize_delegate_view(self.view)
            dcol.patterns = DELEGATE_PATTERNS
            error_label.update(Text("status -- ok", style="bold green"))
            scol.update()
            dcol.update()
        except (
            TypeError, AssertionError, AttributeError, TimeoutError
        ) as err:
            error_label.update(
                Text(f"status: error -- {err}", style="bold red")
            )

    def on_mount(self):
        self.set_interval(1, self.update_view)

    view = {}


if __name__ == "__main__":
    app = StationApp()
    app.run()
