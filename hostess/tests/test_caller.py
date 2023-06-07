import json
import pickle

from dustgoggles.dynamic import Dynamic
from dustgoggles.test_utils import random_nested_dict

from hostess.caller import to_heredoc, format_deserializer, format_importer
from hostess.tests.test_utilz import defwrap


def test_to_heredoc():
    """white-box test for to_heredoc"""
    expected_heredoc = """__BOUNDARYTAG__ 
    def add(a, b):
        return a + b
    __BOUNDARYTAG__
    """
    source = """def add(a, b):
        return a + b"""
    assert to_heredoc(source) == expected_heredoc


def test_format_deserializer():
    """
    gently-fuzzed test for performance of serializers dynamically generated
    from format_deserializer's output
    """
    for sformat in ("json", "pickle"):
        serializer = json.dumps if sformat == 'json' else pickle.dumps
        desource = format_deserializer(sformat)
        deserializer = Dynamic(
            defwrap("def deserialize(payload)", desource + "\nreturn payload")
        )
        # int isn't json-compliant and won't survive roundtrip
        rdict = random_nested_dict(20, types=[str])
        assert deserializer(serializer(rdict)) == rdict


def test_format_importer():
    """whitebox test of importer dynamically generated from format_importer"""
    imsource = format_importer("statistics", "mean")
    # format_importer is meant for script injection, so we need to chop off
    # the guard clause for this test
    imsource = imsource.replace('if __name__ == "__main__":\n', '')
    imsource = imsource.replace('    ', '')
    usemean = "return target((1, 2, 3))"
    domean = Dynamic(
        defwrap("def domean()", imsource + usemean)
    )
    assert domean() == 2
