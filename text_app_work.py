import random
from typing import Optional

from cytoolz import keyfilter
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import HorizontalScroll, VerticalScroll
from textual.widgets import Label, Tree, Button
from textual.widgets._tree import TreeNode

from hostess.station.comm import read_comm
from hostess.station.messages import unpack_obj
from hostess.station.talkie import stsend

PORT = 14152


def get_view():
    response, _ = stsend(b'view', 'localhost', PORT)
    comm = read_comm(response)
    return unpack_obj(comm['body'])


def mutate_mapping(mapping):
    for k, v in mapping.items():
        mapping[k] = random.randint(1, 10)


def dict_to_tree(
    dictionary: dict,
    name: str = '',
    tree: Optional[Tree] = None,
    **tree_kwargs
) -> Tree:
    tree = tree if tree is not None else Tree(name, **tree_kwargs)
    tree.root.expand()
    add_children(tree.root, dictionary)
    return tree


def add_children(node: TreeNode, obj: list | tuple | dict):
    if isinstance(obj, (list, tuple)):
        items = enumerate(obj)
    elif isinstance(obj, dict):
        items = obj.items()
    else:
        raise TypeError
    for k, v in items:
        if isinstance(v, (dict, list, tuple)):
            try:
                child = next(c for c in node.children if str(c.label) == str(k))
            except StopIteration:
                child = node.add(str(k))
            add_children(child, v)
        elif any(str(c.label) == f"{k}: {v}" for c in node.children):
            continue
        else:
            node.add_leaf(f"{k}: {v}")


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

    # TODO: remember expansion
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

    def update_station_view(self):
        error_label = self.query_one("#error-label")
        try:
            self.view = get_view()
            scol = self.query_one("#station-column")
            scol.mapping = keyfilter(lambda k: k != 'delegates', self.view)
            error_label.update(Text("ok", style="bold green"))
        except TypeError:
            error_label.update(Text("error", style="bold red"))

    def on_mount(self):
        self.set_interval(1, self.update_station_view)

    view = {}


if __name__ == "__main__":
    app = StationApp()
    app.run()
