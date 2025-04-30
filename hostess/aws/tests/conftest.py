def pytest_addoption(parser):
    parser.addoption(
        "--run-aws",
        action="store_true",
        default=False,
        help="run live AWS tests",
    )