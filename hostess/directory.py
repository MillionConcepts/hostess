"""utilities for indexing and summarizing filesystems"""

from collections import deque
import datetime as dt
from functools import partial
from itertools import chain
import os
from pathlib import Path
import re
import time
from typing import Union, Collection, Any, Callable

from dustgoggles.func import zero
from hostess.subutils import Viewer
from hostess.utilities import mb, unix2dt
from more_itertools import chunked
import pandas as pd


def mtimes(stat: os.stat_result) -> dict[str, dt.datetime]:
    """
    Formats filesystem timestamps into a dictionary of datetimes.

    Args:
        stat: os.stat_result object (typically from `Path.stat` or `os.stat`).

    Returns:
        dictionary whose values are datetimes and whose keys are 'atime',
            'mtime', and 'ctime'.
    """
    return {
        f"{letter.upper()}TIME": unix2dt(getattr(stat, f"st_{letter}time"))
        for letter in ("a", "c", "m")
    }


LSRecord = dict[str, Union[str, float, bool, dt.datetime]]
"""
a record containing identifying information about a file. produced by 
`lsdashl` and used by other functions in this module. has keys:  

* "path": str (string version of relative path)
* "size": float (file size in MB, rounded to 3 places)
* "excluded": bool (placeholder for exclusions, always False)
* "directory": bool (is it a directory?)
* "suffix": str (last filename suffix)
* "atime", "mtime", "ctime": datetime (UNIX file times)
"""


LSFrame = pd.DataFrame
"""
DataFrame suitable for use by several functions in this module. typically 
produced by calling the DataFrame constructor on a list of LSRecords.
"""

TreeFrame = pd.DataFrame
"""
DataFrame containing hierarchical 'tree' information. Produced by calling 
`make_treeframe` on an LSFrame.
"""


def lsdashl(
    directory: Union[str, Path], include_directories: bool = True
) -> list[LSRecord]:
    """
    a kind of `ls -l`. returns a list of records containing identifying
    information about the contents of `directory`.

    Args:
        directory: directory to list
        include_directories: include or omit subdirectories
    """
    listings = []
    for path in Path(directory).iterdir():
        if (include_directories is False) and (path.is_dir()):
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        listings.append(
            {
                "path": str(path),
                "size": mb(stat.st_size, 3),
                "excluded": False,
                "directory": path.is_dir(),
                "suffix": path.suffix
            } | mtimes(stat)
        )
    return listings


def check_inclusion(
    record: LSRecord, skip_directories: Collection[Union[str, re.Pattern]] = ()
) -> LSRecord:
    """
    simple prefiltering function. sets an LSRecord's "excluded" value to True
    if the record represents a directory and the directory name matches any
    of the regex patterns in `skip_directories`.

    Args:
        record: record to prefilter
        skip_directories: list of regex expressions that define exclusions
    """
    matcher = partial(re.match, string=record["path"])
    if record["directory"] is True and any(map(matcher, skip_directories)):
        record["excluded"] = True
    return record


def index_breadth_first(root: Union[str, Path]) -> list[LSRecord]:
    """
    Recursively index all directories under `root`.

    Args:
        root: top directory of index

    Returns:
        list of LSRecords describing contents of all directories under and
            including `root`.
    """
    discoveries = []
    search_targets = deque([root])
    while len(search_targets) > 0:
        target = search_targets.pop()
        try:
            contents = tuple(map(check_inclusion, lsdashl(target)))
        except PermissionError:
            continue
        discoveries += contents
        for record in contents:
            if (record["directory"] is True) and (record["excluded"] is False):
                search_targets.append(record["path"])
    return discoveries


def _parse_fileinfo(magic_viewer: Viewer) -> list[dict[str, str]]:
    """parses stdout from the POSIX `file` utility."""
    fileinfo = []
    for line in ''.join(magic_viewer.out).split('\n'):
        if line == '':
            continue
        fn, result = re.split(":", line, maxsplit=1)
        fileinfo.append(
            {"path": fn.strip(), "info": result.strip().replace(",", ";")}
        )
    return fileinfo


def do_magic(manifest: LSFrame, log: Callable[[str], Any] = zero) -> LSFrame:
    """
    Adds 'magic' info from the POSIX `file` utility to a DataFrame of
    file / directory listings.

    Args:
        manifest: dataframe of file information, probably produced via
            `pd.DataFrame(index_breadth_first(something))`
        log: logger function (by default just throws log info away)

    Returns:
        `manifest` with an 'info' column added in-place.
    """
    files = manifest.loc[manifest["directory"] == False]["path"].to_list()
    log(f"performing magic on {len(files)} files")
    viewers = [
        Viewer.from_command("file", *chunk) for chunk in chunked(files, 100)
    ]
    while any(not v.done for v in viewers):
        time.sleep(0.05)
    infoframe = pd.DataFrame(
        list(chain.from_iterable(map(_parse_fileinfo, viewers)))
    )
    infoframe.index = infoframe["path"]
    manifest["info"] = None
    available = manifest["path"].loc[manifest["path"].isin(infoframe.index)]
    manifest.loc[available.index, "info"] = (
        infoframe["info"].loc[available].to_numpy()
    )
    return manifest


def _squishlevels(join: pd.DataFrame, levels: dict) -> pd.DataFrame:
    """helper function for _make_levelframe()"""
    for ix in range(0, len(join.columns) - 2):
        join.iloc[:, ix] += "/"
    for ix in range(1, len(join.columns) - 1):
        levels[ix] = join.iloc[:, :ix].sum(axis=1)
    levelframe = pd.DataFrame(levels)
    levelframe["filename"] = levelframe.iloc[:, -2]
    return levelframe


def _make_levelframe(group: pd.DataFrame, squish: bool) -> pd.DataFrame:
    """helper function for make_treeframe()"""
    levels = {}
    join = group.dropna(axis=1).copy()
    if squish is True:
        levelframe = _squishlevels(join, levels)
    else:
        levelframe = join.rename(columns={join.columns[-2]: 'filename'})
    try:
        # TODO: probably superfluous
        levelframe["suffix"] = (
            levelframe["filename"].str.split(".", expand=True, n=1)[1]
        )
    except KeyError:
        levelframe['suffix'] = ''
    levelframe["size"] = group["size"]
    return levelframe


def make_treeframe(manifest: LSFrame, squish: bool = False) -> TreeFrame:
    """
    Takes a DataFrame of file and directory listings and produces a new
    DataFrame with additional columns representing hierarchical components
    of the contents' paths.

    Args:
        manifest: file/directory DataFrame
        squish: squish levels together in output?
    """
    stripped = manifest.loc[~manifest['directory'], 'path'].copy()
    parts = stripped.str.split("/", expand=True)
    parts["size"] = manifest["size"]
    n_parts, levelframes = parts.isna().sum(axis=1), []
    for _, group in parts.groupby(n_parts):
        levelframes.append(_make_levelframe(group, squish))
    treeframe = pd.concat(levelframes)
    return treeframe


def make_level_table(treeframe: TreeFrame) -> pd.DataFrame:
    """
    Make a DataFrame of summary information about directory sizes, file count,
    etc. from a TreeFrame.

    Args:
        treeframe: dataframe containing path information created using `make_treeframe`
    """
    level_tables = []
    levels = [
        c for c in treeframe.columns if c not in ("filename", "size", "suffix")
    ]
    for level in levels:
        table = treeframe.pivot_table(
            values="size", index=level, aggfunc=["sum", len]
        )
        table["level"] = level
        table.columns = ["size", "count", "level"]
        table = table.sort_values(by="count", ascending=False)
        level_tables.append(table)
    level_table = pd.concat(level_tables)
    level_table["size"] = level_table["size"].round(2)
    return level_table
