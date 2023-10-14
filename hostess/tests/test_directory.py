import pandas as pd
from hostess.directory import (
    index_breadth_first,
    make_treeframe,
    make_level_table,
)


def test_directory():
    """simple test of stability for hostess.directory"""
    manifest = pd.DataFrame(index_breadth_first("../"))
    treeframe = make_treeframe(manifest)
    level_table = make_level_table(treeframe)
    assert set(manifest.columns) == {
        "path",
        "size",
        "excluded",
        "directory",
        "ATIME",
        "CTIME",
        "MTIME",
        "suffix",
    }
    assert set(level_table.columns) == {"size", "count", "level"}
    # note: changing hostess directory structure could break this
    for i in treeframe.columns:
        assert isinstance(i, int) or i in {"filename", "size", "suffix"}
    assert {"filename", "size", "suffix"}.issubset(treeframe.columns)
