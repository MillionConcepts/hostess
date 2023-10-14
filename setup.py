from setuptools import setup, find_packages

setup(
    name="hostess",
    version="0.1.1a",
    description="intuitive admin library",
    author="Million Concepts",
    author_email="mstclair@millionconcepts.com",
    packages=find_packages(),
    python_requiers=">=3.9",
    install_requires=[
        "boto3",
        "cytoolz",
        "dill",
        "dustgoggles",
        "invoke",
        "fabric",
        "more-itertools",
        "pandas",
        "pympler",
        "python-magic",
        "requests",
        "rich",
    ]
)
