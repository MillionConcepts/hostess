import datetime as dt
import random
import time
from collections import defaultdict
from functools import wraps, partial
from typing import Optional, Sequence, Callable, Mapping

import fire
from cytoolz import keyfilter, keymap, valmap, partition
from dustgoggles.codex.implements import Sticky
from dustgoggles.codex.memutilz import (
    deactivate_shared_memory_resource_tracker,
)
from dustgoggles.func import constant
from dustgoggles.structures import NestingDict
from rich.text import Text

from textual_callback_signature_monkeypatch import do_patch
do_patch()

from textual._loop import loop_last
from textual.app import App, ComposeResult
from textual.containers import HorizontalScroll, VerticalScroll
from textual.geometry import Size
from textual.widgets import Button, Label, Pretty, Tree
from textual.widgets._tree import TreeNode, TreeDataType, _TreeLine

from hostess.profilers import DEFAULT_PROFILER
from hostess.station.comm import read_comm
from hostess.station.messages import unpack_obj
from hostess.station.talkie import stsend
from hostess.utilities import curry

deactivate_shared_memory_resource_tracker()

PRINTCACHE = []


# TODO: maybe redundant?
STATION_PATTERNS = {
    'sort': (
        lambda l: (len(l) == 2) and (l[1] == 'actors'),
        lambda l: (len(l) in (3, 4)) and l[1] == 'tasks'
    ),
    'protect': (lambda l: (len(l) == 4) and l[1] == 'tasks',)
}

DELEGATE_PATTERNS = {
    'sort': (
        lambda l: (len(l) == 3) and (l[2] == 'actors'),
        lambda l: (len(l) in (3, 4)) and l[1] == 'running'
    ),
    'protect': (lambda l: (len(l) == 4) and l[1] == 'running',),
    'protect': (lambda l: (len(l) == 4) and l[1] == 'running',)
}


def get_parent_labels(node: TreeNode):
    active_node, labels = node, []
    while active_node is not None:
        labels.append(str(active_node.label))
        active_node = active_node.parent
    return tuple(reversed(labels))


def _is_dropnode(node):
    """is a node designed to hide stuff?"""
    return isinstance(node.data, dict) and node.data['type'] == 'dropnode'


# noinspection PyPep8Naming,PyProtectedMember,PyShadowingNames
class SortingTree(Tree):

    """
    tree that can alphabetize nodes and protect them from removal;
    also allows nodes under root level to have long labels.
    """

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
                    drop = filter(lambda c: _is_dropnode(c), node._children)
                    reg = filter(lambda c: not _is_dropnode(c), node._children)
                    children = sorted(reg, key=lambda c: str(c.label))
                    # currently we don't actually have a case where we have
                    # dropdowns and not-dropdowns, but this is the correct
                    # behavior
                    children += sorted(drop, key=lambda c: c.data['start'])
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


def get_view(host, port):
    if (host is None) or (port is None):
        raise ConnectionError("valid station address not available")
    # TODO: handle recurring queries a little more gracefully
    response, _ = stsend(b"view", host, port, timeout=0.5)
    # TODO: timeout tracker
    if response == "timeout":
        raise TimeoutError('dropped packet')
    try:
        comm = read_comm(response)
    except TypeError:
        raise ConnectionError(f'bad response: {response[:128]}')
    return unpack_obj(comm["body"])


def mutate_mapping(mapping):
    for k, v in mapping.items():
        mapping[k] = random.randint(1, 10)


def add_config_to_elements(elements: dict, config: dict) -> dict[str]:
    """join config information to matching Actors/Sensors"""
    out = {}
    for name, classname in elements.items():
        out[name] = {'class': classname}
        element_interface = keyfilter(
            lambda k: k.split('_')[0] == name, config['interface']
        )
        if len(element_interface) > 0:
            out[name]['interface'] = keymap(
                lambda k: "_".join(k.split("_")[1:]), element_interface
            )
        if (element_cdict := config.get('cdict', {}).get(name)) is not None:
            out[name]['cdict'] = element_cdict
    return out


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
    return out
    # return out.todict()


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


def delegate_dict(ddict: Mapping) -> dict:
    config = {
        'cdict': ddict.get('cdict', {}),
        'interface': ddict.get('interface', {})
    }
    out = {'status': ddict['inferred_status'], 'wait_time': ddict['wait_time']}
    if ddict.get('busy') is True:
        ddict['wait_time'] = str(out['wait_time']) + ' [busy]'
    for element_type in ('actors', 'sensors'):
        if len(element_dict := ddict.get(element_type, {})) > 0:
            out[element_type] = add_config_to_elements(element_dict, config)
    if len(ddict.get('running', [])) > 0:
        out['running'] = organize_running_actions(ddict['running'])
    else:
        out['running'] = []
    return out


def organize_delegate_view(view: dict) -> dict:
    out = {}
    for ddict in view['delegates']:
        title = (
            f"{ddict['name']}@{ddict.get('host', '?')}: "
            f"(PID {ddict.get('pid', '?')})"
        )
        out[title] = delegate_dict(ddict)
    return out


def organize_station_view(view: dict) -> dict:
    view = keyfilter(lambda k: k != 'delegates', view)
    return {
        'id': f"{view['name']}@{view.get('host', '?')}:{view['port']}",
        'actors': add_config_to_elements(view['actors'], view['config']),
        'tasks': organize_tasks(view['tasks']),
        'threads': view['threads']
    }


def populate_dropnodes(node, items, max_items):
    dropnodes = {}
    for c in tuple(node.children):
        if _is_dropnode(c):
            if c.data['start'] > len(items):
                c.remove()
            else:
                dropnodes[c.data['start']] = c
        else:
            # note that we lose expanded status when we 'move' TreeNodes into
            # dropdowns this way
            c.remove()
    # TODO: need to be able to do incomplete partitions
    partitions = partition(max_items, items)
    for i, p in enumerate(partitions):
        start, stop = i * max_items + 1, (i + 1) * max_items
        if start not in dropnodes.keys():
            drop = node.add(
                f'{start}-{stop}', data={'type': 'dropnode', 'start': start}
            )
        else:
            drop = dropnodes[start]
        with DEFAULT_PROFILER.context('dropnode_population'):
            # TODO, maybe: we _could_ just skip populating non-expanded nodes,
            #  which would be wildly more efficient for many applications.
            #  it has the disadvantage, as the application is currently
            #  written, that expanded dropdowns will not populate until the
            #  next update cycle.
            ticked(populate_children_from_dict, 'dropnode_dig')(drop, {k: v for k, v in p}, max_items)


def populate_children_from_dict(
    node: TreeNode, obj: list | tuple | dict, max_items: Optional[int]
) -> None:
    """warning: expects a SortingTree as parent of the TreeNode."""
    if isinstance(obj, (list, tuple)):
        items = tuple(enumerate(obj))
    elif isinstance(obj, dict):
        items = obj.items()
    else:
        raise TypeError
    if (max_items is not None) and (len(items) > max_items):
        return populate_dropnodes(node, items, max_items)
    items, ok_children = tuple(enumerate(items)), []
    prefix_map = {str(c.label).split(': ')[0]: c for c in node.children}
    for i, kv in items:
        k, v = kv
        # TODO: is there a nicer way to do this? it's hard to really attach
        #  a fiducial at the station level. The colon-splitting thing is also
        #  a little hazardous.
        before = str(k).split(': ')[0]
        child = prefix_map.get(before)
        if child is not None:
            ok_children.append(child)
        if isinstance(v, (dict, list, tuple)):
            if child is None:
                ok_children.append(child := node.add(str(k)))
            elif str(child.label) != str(k):
                child.set_label(str(k))
            populate_children_from_dict(child, v, max_items)
        elif child is None:
            ok_children.append(node.add_leaf(f"{k}: {v}"))
        elif str(child.label) != f"{k}: {v}":
            child.set_label(f"{k}: {v}")
        if _is_dropnode(node):
            PRINTCACHE.append(child)
            PRINTCACHE.append((node, len(node.children)))
    # prevent removal of 'emptied' nodes we'd like to optionally remain
    # expanded (action names, etc.)
    for c in filter(
        lambda c: c not in ok_children and not _is_dropnode(c), node.children
    ):
        if node.tree.match(c, "protect"):
            c.remove_children()
        else:
            c.remove()


def dict_to_tree(
    dictionary: dict,
    name: str = "",
    tree: Optional[Tree] = None,
    max_items: Optional[int] = None,
    **tree_kwargs,
) -> Tree:
    tree = tree if tree is not None else SortingTree(name, **tree_kwargs)
    tree.root.expand()
    populate_children_from_dict(tree.root, dictionary, max_items)
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
    max_items = 20

    def on_mount(self):
        pass
        self.mount(Label(self.name))

    def update(self) -> None:
        tkwargs = {
            'dictionary': self.mapping,
            'patterns': self.patterns,
            'max_items': self.max_items
        }
        if len(self.children) == 1:
            tree = dict_to_tree(id=f"{self.id}-tree", **tkwargs)
            self.mount(tree)
            return
        tree = self.query_one(f"#{self.id}-tree")
        dict_to_tree(tree=tree, **tkwargs)


class StatusBar(Label):
    DEFAULT_CSS = """
    StatusBar {
        height: 2vh;
        margin-bottom: 1;
    }
    """


class StationApp(App):

    def __init__(
        self, *args, host=None, port=None, memory_address=None, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.host, self.port, self.memory_address = host, port, memory_address
        self.view = {}

    def compose(self) -> ComposeResult:
        yield StatusBar(id="statusbar")
        # yield Footer(id="Footer")
        with HorizontalScroll():
            yield DictColumn(id="station-column", name="Station")
            yield DictColumn(id="delegate-column", name="Delegates")
        yield Pretty(DEFAULT_PROFILER, id='profiler-panel')
        yield Pretty(TICKER, id='ticker-panel')

    # noinspection PyTypeChecker
    def update_view(self):
        if self.fetching is True:
            return
        error_label = self.query_one("#statusbar")
        try:
            self.fetching = True
            self.view = get_view(self.host, self.port)
            scol: DictColumn = self.query_one("#station-column")
            scol.mapping = organize_station_view(self.view)
            scol.patterns = STATION_PATTERNS
            dcol: DictColumn = self.query_one("#delegate-column")
            dcol.mapping = organize_delegate_view(self.view)
            dcol.patterns = DELEGATE_PATTERNS
            error_label.update(Text("status -- ok", style="bold green"))
            # with DEFAULT_PROFILER.context('update-station-column'):
            scol.update()
            # with DEFAULT_PROFILER.context('update-delegate-column'):
            dcol.update()
            ppanel: Pretty = self.query_one("#profiler-panel")
            ppanel.update(DEFAULT_PROFILER)
            tpanel: Pretty = self.query_one('#ticker-panel')
            tpanel.update(TICKER)
        except (
            TypeError, AttributeError, TimeoutError, ConnectionError
        ) as err:
            error_label.update(
                Text(f"status: error -- {err}", style="bold red")
            )
            if isinstance(err, ConnectionError):
                try:
                    self.port = Sticky(self.memory_address).read()
                except (FileNotFoundError, TypeError, ValueError):
                    return
        finally:
            self.fetching = False

    def on_mount(self):
        self.set_interval(1, self.update_view)

    fetching = False


def run_station_viewer(
    memory_address: str = 'station-port-report',
    host: str = 'localhost',
    port: int = None,
):
    if isinstance(port, int):
        port = port
    else:
        port = Sticky(memory_address).read()
    app = StationApp(host=host, port=port, memory_address=memory_address)
    app.run()


if __name__ == "__main__":
    fire.Fire(run_station_viewer)
