from hostess.caller import to_heredoc


def test_to_heredoc():
    implied_heredoc = """__BOUNDARYTAG__ 
    def add(a, b):
        return a + b
    __BOUNDARYTAG__
    """
    source = """def add(a, b):
        return a + b"""
    assert to_heredoc(source) == implied_heredoc


