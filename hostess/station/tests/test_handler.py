import numpy as np

import hostess.station.messages as me
from hostess.station import rules


class FakeNode:
    def __init__(self):
        self.running_actions = []


def test_function_call():
    action = me.make_function_call_action(
        func='array',
        module='numpy',
        arguments={'object': 1},
        name='makearray'
    )
    instruction = me.make_instruction("do", action=action)
    node = FakeNode()
    rules.FunctionCall().match(instruction)
    rules.FunctionCall().execute(node, instruction)
    result = node.running_actions[0]['result'][0]
    assert result == np.array([1])

test_function_call()