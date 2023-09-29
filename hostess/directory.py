"""utilities for indexing and summarizing filesystems"""

from collections import deque
from functools import partial
from itertools import chain
from pathlib import Path
import re

from dustgoggles.func import zero
from hostess.subutils import Viewer
from hostess.utilities import mb, unix2dt
from more_itertools import chunked
import pandas as pd


def mtimes(stat):
    return {
        f"{letter.upper()}TIME": unix2dt(getattr(stat, f"st_{letter}time"))
        for letter in ("a", "c", "m")
    }


def lsdashl(directory, include_directories=True):
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
            }
            | mtimes(stat)
        )
    return listings


def check_inclusion(record, skip_directories=()):
    matcher = partial(re.match, string=record["path"])
    if record["directory"] is True and any(map(matcher, skip_directories)):
        record["excluded"] = True
    return record


def index_breadth_first(root):
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


def do_magic(manifest, log=zero):
    files = manifest.loc[manifest["directory"] == False]["path"].to_list()
    log(f"performing magic on {len(files)} files")
    viewers = [
        Viewer.from_command("file", chunk) for chunk in chunked(files, 1000)
    ]
    infoframe = pd.DataFrame(
        list(chain.from_iterable(map(parse_fileinfo, viewers)))
    )
    infoframe.index = infoframe["path"]
    manifest["info"] = None
    available = manifest["path"].loc[manifest["path"].isin(infoframe.index)]
    manifest.loc[available.index, "info"] = (
        infoframe["info"].loc[available].to_numpy()
    )
    return manifest


def parse_fileinfo(magic_viewer):
    fileinfo = []
    for line in magic_viewer.out:
        fn, result = re.split(":", line, maxsplit=1)
        fileinfo.append(
            {"path": fn.strip(), "info": result.strip().replace(",", ";")}
        )
    return fileinfo


def _squishlevels(join, levels):
    for ix in range(0, len(join.columns) - 2):
        join.iloc[:, ix] += "/"
    for ix in range(1, len(join.columns) - 1):
        levels[ix] = join.iloc[:, :ix].sum(axis=1)
    levelframe = pd.DataFrame(levels)
    levelframe["filename"] = levelframe.iloc[:, -2]
    return levelframe


def _make_levelframe(group, squish):
    levels = {}
    join = group.dropna(axis=1).copy()
    if squish is True:
        levelframe = _squishlevels(join, levels)
    else:
        levelframe = join.rename(columns={join.columns[-2]: 'filename'})
    try:
        levelframe["suffix"] = (
            levelframe["filename"].str.split(".", expand=True, n=1)[1]
        )
    except KeyError:
        levelframe['suffix'] = ''
    levelframe["size"] = group["size"]
    return levelframe


def make_treeframe(manifest, squish=False):
    stripped = manifest.loc[~manifest['directory'], 'path'].copy()
    parts = stripped.str.split("/", expand=True)
    parts["size"] = manifest["size"]
    n_parts, levelframes = parts.isna().sum(axis=1), []
    for _, group in parts.groupby(n_parts):
        levelframes.append(_make_levelframe(group, squish))
    treeframe = pd.concat(levelframes)
    return treeframe


def make_level_table(treeframe):
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
