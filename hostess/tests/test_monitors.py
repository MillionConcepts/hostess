import re
from itertools import cycle
import time

from hostess.monitors import (
    Bouncer,
    Stopwatch,
    make_stat_printer,
    make_monitors,
)


def test_monitor_init():
    """test proper monitor initialization"""
    monitors = make_monitors(digits=1)
    printstats = make_stat_printer(monitors)
    text = printstats()
    decimal = r"\d\.\d+"
    assert re.match(
        rf"{decimal} %;user {decimal};system {decimal};idle {decimal};iowait "
        rf"{decimal};{decimal} MB;total {decimal} MB;"
        rf"used {decimal} MB;free {decimal} MB;read {decimal} MB;write "
        rf"{decimal} MB;read count {decimal} MB;"
        rf"write count {decimal} MB;sent {decimal} MB;recv {decimal} MB;"
        rf"sent count {decimal} MB;recv count {decimal} MB;{decimal} s",
        text
    )


def test_stopwatch():
    """test basic stopwatch functionality"""
    sw = Stopwatch(digits=1)
    sw.click()
    time.sleep(0.2)
    # check basic interval behavior
    assert sw.peek(which="interval") == "0.2 s"
    # check pause / unpause behavior
    sw.pause()
    time.sleep(0.2)
    assert sw.peek(which="interval") == "0.2 s"
    sw.start()
    sw.click()
    # check interval v. total time tracking
    time.sleep(0.2)
    assert sw.peek(which="interval") == "0.2 s"
    assert sw.peek(which="total") == "0.6 s"


# TODO: difficult to test specific functionality of other monitors due to
#  environment-specificity. perhaps consider some sort of environment mock?


def test_bouncer():
    """test bouncer event queue and rate-limiting"""
    sw = Stopwatch(digits=2)
    count, lap = cycle((3, 0)), cycle(("0.01 s", "0.21 s"))
    bouncer = Bouncer(ratelimit=5, window=0.2)
    sw.click()
    for _ in range(20):
        time.sleep(0.01)
        bouncer.click()
        bouncer.click()
        bouncer.click()
        assert len(bouncer.events) == next(count)
        assert sw.clickpeek() == next(lap)
    assert sw.peek(which="total") == "2.22 s"
