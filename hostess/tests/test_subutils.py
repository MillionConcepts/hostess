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


test_viewer_1()
test_viewer_2()

