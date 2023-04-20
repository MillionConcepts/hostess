from pathlib import Path

from PIL import Image


def append_to(file, content):
    with open(file, "a") as stream:
        stream.write(content)


def thumbnail(path: Path, thumbnail_size: tuple[int, int] = (240, 240)):
    image = Image.open(path)
    image.thumbnail(thumbnail_size)
    image.save(Path(path.parent, path.name.replace("full", "thumbnail")))
