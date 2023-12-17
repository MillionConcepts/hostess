from setuptools import find_packages, setup

setup(
    name="hostess",
    version="0.9.1",
    description="intuitive admin library",
    author="Million Concepts",
    author_email="mstclair@millionconcepts.com",
    packages=find_packages(),
    package_data={
        "": ["station/tests/test_data/*.*", "station/proto/*.*"]
    },
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Unix Shell"
    ],
    install_requires=[
        "cytoolz",
        "dustgoggles",
        "invoke",
        "more-itertools",
        "pandas",
        "psutil",
        "pyyaml",
        "requests",
        "rich",
    ],
    extras_require={
        "aws": ["boto3", "fabric"],
        "directory": ["python-magic"],
        "profilers": ["pympler"],
        "ssh": ["fabric"],
        "station": ["dill", "fire", "protobuf", "textual"],
        "tests": ["pillow", "pytest"]
    }
)
