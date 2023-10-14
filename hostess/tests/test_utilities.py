import array
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from io import BytesIO, StringIO
from pathlib import Path

from hostess.utilities import (
    infer_stream_length,
    notary,
    Aliased,
    timeout_factory,
    signal_factory,
)


def test_infer_stream_length_offline():
    """
    test that infer_stream_length produces expected results
    for all of the object types it's supposed to work with
    (that don't require mocking HTTP)
    """
    # make a 256-byte array
    arr = array.array("B", [i for i in range(256)])
    bufio = BytesIO(arr.tobytes())
    # does it work on a binary IO object?
    assert infer_stream_length(bufio) == 256
    bufio.seek(0)
    temp = Path("arr")
    temp.write_bytes(bufio.read())
    # does it work on a Path to a file?
    assert infer_stream_length(temp) == 256
    # does it work on a string path to a file?
    assert infer_stream_length("arr") == 256
    # does it work on a buffered reader from an open file?
    with temp.open("rb") as instream:
        assert infer_stream_length(instream) == 256
    temp.unlink()


def test_notary():
    """
    test that notary's output can place objects into its implicitly-defined
    note cache, eject the cache when asked, and print them reliably.
    """
    yeller = notary(resolution=3, be_loud=True)
    integers = [i for i in range(256)]
    buf = StringIO()
    with redirect_stdout(buf):  # catch the print() calls
        for i in integers:
            yeller(i)
            time.sleep(0.001)
    # noinspection PyArgumentList
    assert list(yeller(eject=True).values()) == integers
    buf.seek(0)
    assert list(map(int, buf.read().splitlines())) == integers


def test_aliased():
    """test basic aliasing functionality"""

    class Appendable:
        """i'm a library class that only appends"""

        def __init__(self):
            self.cache = []

        def append(self, obj):
            self.cache.append(obj)

    def writer(obj, target):
        """i'm a library function that only writes"""
        target.write(obj)

    writeable_appender = Aliased(Appendable(), ("write",), "append")
    writer(1, writeable_appender)
    assert writeable_appender.cache.pop() == 1


def test_timeout_factory():
    """test for timeout_factory()"""
    waiting, unwait = timeout_factory(timeout=0.05)
    # start the timer
    waiting()
    time.sleep(0.02)
    # did we get the right delay?
    assert round(waiting(), 3) == 0.02
    unwait()
    waiting()
    time.sleep(0.04)
    # did unwait() correctly reset the time cache in enclosing scope?
    waiting()
    # will we correctly time out now?
    time.sleep(0.01)
    try:
        print(waiting())
    except TimeoutError:
        return
    raise ValueError("Should have timed out.")


def test_signal_factory():
    """
    test that signal_factory appropriately modifies a dictionary for
    inter-thread communication
    """

    def loop_forever(threadname, signal_cache):
        """hang out until signaled"""
        while True:
            if signal_cache.get(threadname) is not None:
                return threadname
            time.sleep(0.1)

    signals = {i: None for i in range(4)}
    signaler = signal_factory(signals)
    executor = ThreadPoolExecutor(4)
    futures = [executor.submit(loop_forever, i, signals) for i in range(4)]
    for i in range(4):
        # everything should be running now
        assert futures[i].running()
    for i in range(4):
        signaler(i)
        # give it time to quit
        time.sleep(0.1)
        # now it should have quit
        assert futures[i].running() is False
        assert futures[i].result() == i
