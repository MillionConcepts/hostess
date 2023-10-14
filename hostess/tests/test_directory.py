import pandas as pd
from hostess.directory import (
    index_breadth_first, make_treeframe, make_level_table
)


def test_directory():
    """simple test of stability for hostess.directory"""
    manifest = pd.DataFrame(index_breadth_first("../"))
    treeframe = make_treeframe(manifest)
    level_table = make_level_table(treeframe)
    assert manifest.columns.to_list() == [
        'path', 'size', 'excluded', 'directory', 'ATIME', 'CTIME', 'MTIME'
    ]
    assert level_table.columns.to_list() == ['size', 'count', 'level']
    # note: changing hostess directory structure could break this
    assert treeframe.columns.to_list() == [
        0, 1, 2, 3, 'filename', 'size', 'suffix'
    ]
