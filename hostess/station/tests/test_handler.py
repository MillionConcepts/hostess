import numpy as np

import hostess.station.messages as me
from hostess.station.actors import FuncCaller


class FakeNode:
    def __init__(self):
        self.actions = {}


def test_function_call():
    action = me.make_function_call_action(
        func="array", module="numpy", kwargs={"object": 1}, name="makearray"
    )
    instruction = me.make_instruction("do", action=action)
    node = FakeNode()
    FuncCaller().match(instruction)
    FuncCaller().execute(node, instruction, key=1)
    result = node.actions[1]["result"]
    assert result == np.array([1])
