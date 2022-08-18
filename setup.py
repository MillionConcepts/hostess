from setuptools import setup, find_packages

setup(
    name="killscreen",
    version="0.1.0a",
    description="an intuitive network admin library",
    author="Million Concepts",
    author_email="mstclair@millionconcepts.com",
    packages=find_packages(),
    python_requiers=">=3.9",
    install_requires=[
        "boto3",
        "cytoolz",
        "dustgoggles",
        "fire",
        "flask",
        "pandas",
        "pip",
        "python-magic",
        "requests",
        "rich",
        "sh"
    ]
)
