from hostess.station.bases import Actor
from hostess.tests.utilz import segfault


class Traitor(Actor):
    def match(self, event, **_):
        return True

    def execute(self, _node, _event, **_):
        segfault()
