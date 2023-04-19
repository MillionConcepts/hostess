import numpy as np

import hostess.station.messages as me
from hostess.station.actors import FunctionCall


class FakeNode():
    def __init__(self):
        self.actions = {}


def test_function_call():
    action = me.make_function_call_action(
        func='array',
        module='numpy',
        arguments={'object': 1},
        name='makearray'
    )
    instruction = me.make_instruction("do", action=action)
    node = FakeNode()
    FunctionCall().match(instruction)
    FunctionCall().execute(node, instruction, key=1)
    result = node.actions[1]['result'][0]
    assert result == np.array([1])

test_function_call()