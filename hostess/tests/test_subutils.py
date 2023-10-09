import time

from hostess.subutils import RunCommand, Viewer


def test_viewer_1():
    viewer = Viewer.from_command("echo hi")
    time.sleep(0.01)
    assert str(viewer).startswith("Viewer for finished process echo hi")
    assert viewer.out[0] == 'hi\n'


def test_viewer_2():
    trash = [i for i in range(1000)]

    def empty_trash():
        for _ in range(len(trash)):
            trash.pop()

    assert len(trash) == 1000
    viewer = Viewer.from_command('echo hi', _done=empty_trash)
    time.sleep(0.1)
    assert len(trash) == 0
    assert viewer.stderr == []


# test_viewer_1()
# test_viewer_2()
from pathlib import Path

ra=1
dec = 1
xylist_path="xylist.fits"
image_width=100
image_height=100
output_path = "./here"
crpix_x = 50
crpix_y = 50
radius = 5
solve_process = Viewer.from_command(
    "solve-field",
    xylist_path,
    overwrite=True,
    no_plots=True,
    dir_=output_path,
    width=image_width,
    height=image_height,
    scale_units="arcsecperpix",
    scale_low=1.0,
    scale_high=1.6,
    N="none",
    U="none",
    axy=f"a{Path(xylist_path).name}",
    B="none",
    M="none",
    R="none",
    temp_axy=True,
    _3=ra,
    _4=dec,
    crpix_x=crpix_x,
    crpix_y=crpix_y,
    radius=5,
)
a = 1