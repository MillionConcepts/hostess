from cytoolz import first
from cytoolz.curried import get

from hostess.utilities import mb

PROC_NET_DEV_FIELDS = (
    "bytes",
    "packets",
    "errs",
    "drop",
    "fifo",
    "frame",
    "compressed",
    "multicast",
)


class FakeNetstat:
    """fake simple network monitor."""

    def __init__(self, rejects=("lo",), round_to=2):
        self.rejects = rejects
        self.round_to = round_to
        self.full = ()
        self.absolute, self.last, self.interval, self.total = {}, {}, {}, {}

    def update(self):
        return

    def display(self, which="interval", interface=None):
        return ""

    fake = True


class Netstat(FakeNetstat):
    """simple network monitor. works only on *nix at present."""

    # TODO: monitor TX as well as RX, etc.
    def __init__(self, rejects=("lo",), round_to=2):
        super().__init__(rejects, round_to)
        self.update()
        try:
            heaviest_traffic = max(map(get("bytes"), self.full))
            self.default_interface = first(
                filter(lambda v: v["bytes"] == heaviest_traffic, self.full)
            )['interface']
        except (KeyError, StopIteration):
            self.default_interface = None

    def update(self):
        self.full = parseprocnetdev(catprocnetdev(), self.rejects)
        for line in self.full:
            interface, bytes_ = line["interface"], line["bytes"]
            self.absolute[interface] = bytes_
            if interface not in self.interval.keys():
                self.total[interface] = 0
                self.interval[interface] = 0
                self.last[interface] = bytes_
            else:
                self.interval[interface] = bytes_ - self.last[interface]
                self.total[interface] += self.interval[interface]
                self.last[interface] = bytes_

    def display(self, which="interval", interface=None):
        if which not in ("absolute", "total", "interval", "last"):
            raise ValueError(
                "'which' argument to Netstat.display() must be 'absolute', "
                "'total', 'interface', or 'last'."
            )
        interface = self.default_interface if interface is None else interface
        infix = f"{which} " if which != "interval" else ""
        if interface is None:
            value = first(getattr(self, which).values())
        else:
            value = getattr(self, which)[interface]
        return f"{mb(value, self.round_to)} {infix}MB"

    def __repr__(self):
        return str(self.absolute)

    fake = False


def catprocnetdev():
    with open("/proc/net/dev") as stream:
        return stream.read()


def parseprocnetdev(procnetdev, rejects=("lo",)):
    interface_lines = filter(
        lambda l: ":" in l[:12], map(str.strip, procnetdev.split("\n"))
    )
    entries = []
    for interface, values in map(lambda l: l.split(":"), interface_lines):
        if interface in rejects:
            continue
        records = {
            field: int(number)
            for field, number in zip(
                PROC_NET_DEV_FIELDS, filter(None, values.split(" "))
            )
        }
        entries.append({"interface": interface} | records)
    return entries
