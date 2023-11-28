from hostess.station.bases import Actor
from hostess.tests.utilz import segfault


class NormalActor(Actor):
    def match(self, event, **_):
        return True

    def execute(self, _node, _event, **_):
        segfault()

    name = "normal_actor_that_does_useful_things"
    actortype = "action"


class TrivialActor(Actor):
    def match(self, event, **_):
        return False

    def execute(self, node, event, **_):
        return

    actortype = "trivial"
    name = "trivial"
