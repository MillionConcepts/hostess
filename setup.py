from setuptools import setup, find_packages

setup(
    name="hostess",
    version="0.6.0b",
    description="intuitive admin library",
    author="Million Concepts",
    author_email="mstclair@millionconcepts.com",
    packages=find_packages(),
    python_requiers=">=3.9",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research" 
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: BSD License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Unix Shell"
    ],
    install_rzquires=[
        "cytoolz",
        "dustgoggles",
        "invoke",
        "more-itertools",
        "pandas",
        "psutil",
        "pyyaml"
        "requests",
        "rich",
    ],
    extras_require={
        "aws": ["boto3", "fabric"],
        "directory": ["python-magic"],
        "profilers": ["pympler"],
        "ssh": ["fabric"],
        "station": ["dill", "protobuf", "textual"],
        "tests": ["pillow", "pytest"]
    }
)
