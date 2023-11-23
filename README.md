# hostess

## overview
`hostess` is a collection of utilities that provide lightweight, Pythonic
interfaces to distributed resources, along with a framework (`station`) for 
workflow coordination built on these dependencies. It reduces syntactic 
headaches and boilerplate while still allowing low-level manipulation and, 
more broadly, still feeling like code. `hostess` has a special emphasis on 
fitting intuitively and idiomatically into common data science and scientific 
analysis workflows, but it is fundamentally a general-purpose utility and 
administration library.

[You can find extended documentation and an API reference on readthedocs.](https://hostess.readthedocs.io)
[![Documentation Status](https://readthedocs.org/projects/hostess/badge/?version=latest)](https://hostess.readthedocs.io/en/latest/?badge=latest)

## cautions
`hostess` is a reliable and fairly feature-complete beta in active use on 
multiple projects. However, we do not guarantee interface stability or 
backwards compatibility, and it lacks comprehensive documentation. Also, as
with any system administration software, we recommend using it very carefully.

## installation
`hostess` is not currently available via any package manager. 
For now, we recommend downloading the source, using a Conda-compatible tool 
and the provided [environment.yml](environment.yml) file to set up an 
appropriate conda env, then installing `hostess` into that environment using 
`pip`. For instance, if you have `mamba` and `git` installed:
```
git clone https://github.com/MillionConcepts/hostess.git
cd hostess
mamba env create -f environment.yml
conda activate hostess
pip install -e .
```
The environment file includes dependencies for all extras. If you need 
fine-grained control over dependencies, please examine the extra dependencies 
defined in the `setup.py` file.

## compatibility
1. `hostess`'s core features are closely linked to the shell, so it is 
only fully compatible with Unix-scented operating systems. Mac OS counts, 
as does Windows Subsystem for Linux (WSL). Windows *outside* of WSL doesn't.
2. Many parts of `hostess` is written with the assumption that you've got 
remote resources you'd like to interact with, but dialing out is not strictly 
required. It can be used for LAN administration, and many of its utilties 
could be used purely locally.
3. `hostess` requires Python >= 3.9.
4. `hostess` is very lightweight. If a machine has enough resources to run 
5. a Python interpreter, it can probably run `hostess`.  

## tests
`hostess` includes a simple test suite compatible with `pytest`. More 
comprehensive tests are planned. In particular, non-local networking features 
currently lack test coverage. You can run the tests by executing `pytest -s` 
in the root directory. The `-s` flag is mandatory because `pytest`'s default 
output capturing breaks some `hostess` features covered by the tests.

## licensing
You can do almost anything with this software that you like, subject only to 
the extremely permissive terms of the [BSD 3-Clause License](LICENSE).
