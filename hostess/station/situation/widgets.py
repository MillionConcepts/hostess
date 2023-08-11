"""widgets and other `textual` interaction for the situation frontend."""
from typing import Mapping, Sequence, Callable, Optional

from cytoolz import partition
from textual._loop import loop_last
from textual.containers import VerticalScroll
from textual.geometry import Size
from textual.widgets import Tree, Label
from textual.widgets._tree import TreeNode, _TreeLine, TreeDataType

from hostess.monitors import ticked, DEFAULT_TICKER
from hostess.profilers import DEFAULT_PROFILER


def get_parent_labels(node: TreeNode):
    active_node, labels = node, []
    while active_node is not None:
        labels.append(str(active_node.label))
        active_node = active_node.parent
    return tuple(reversed(labels))


def _is_dropnode(node):
    """is a node designed to hide stuff?"""
    return isinstance(node.data, dict) and node.data['type'] == 'dropnode'


# noinspection PyPep8Naming,PyShadowingNames
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
            ticked(
                populate_children_from_dict, 'dropnode_dig', DEFAULT_TICKER
            )(drop, {k: v for k, v in p}, max_items)


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
    # prevent removal of 'emptied' nodes we'd like to optionally remain
    # expanded (action names, etc.)
    for c in filter(
        lambda c: c not in ok_children and not _is_dropnode(c), node.children
    ):
        # noinspection PyUnresolvedReferences
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
        # noinspection PyTypeChecker
        dict_to_tree(tree=tree, **tkwargs)


# noinspection PyPep8Naming,PyShadowingNames
class SortingTree(Tree):
    """tree that can alphabetize nodes and protect them from removal"""

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


class ConnectionStatus(Label):
    DEFAULT_CSS = """
    StatusBar {
        height: 2vh;
        margin-bottom: 1;
    }
    """
