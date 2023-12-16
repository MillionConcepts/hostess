# hostess

## overview

`hostess` provides lightweight, Pythonic interfaces to distributed resources, 
system processes, and Python internals. It includes high-level modules that 
provide special functionality for working with EC2 instances and S3 buckets,
and a framework (`station`) for workflow coordination -- along with all their 
lower-level building blocks.

`hostess` reduces syntactic headaches and boilerplate while still 
allowing low-level manipulation and, more broadly, still feeling like code. 
`hostess` has a special emphasis on fitting intuitively and idiomatically into 
common data science and scientific analysis workflows, but it is fundamentally 
a general-purpose utility and administration library.

## installation

`hostess` is available on PyPI and conda-forge. We recommend installing it
into a Conda environment using `mamba`:
`mamba install -n my_environment -c conda-forge hostess`. 

The conda-forge package installs all optional dependencies other than those
for tests and Notebooks. If you require more granular control over 
dependencies, please install from source.

## documentation

In addition to the API reference on this RTD site, the 
[`hostess` GitHub repository](https://github.com/MillionConcepts/hostess)
includes Jupyter Notebooks that offer detailed tutorials for using `hostess` 
to interact with AWS EC2 instances and S3 buckets: examples/ec2.ipynb and 
examples/s3.ipynb. These Notebooks have two additional dependencies: `jupyter` 
and `pillow`. Additional tutorials are forthcoming.

## compatibility

1. `hostess`'s core features are closely linked to the shell, so it is 
only fully compatible with Unix-scented operating systems. Linux and MacOS 
count, as does Windows Subsystem for Linux (WSL). Windows *outside* of WSL 
doesn't.
2. Some `hostess` modules require network access, specifically `ssh` and the
`aws` submodules. The `aws` submodules also require an AWS account. See the 
example Notebooks for more details on this.
3. `hostess` requires Python >= 3.9.
4. `hostess` is very lightweight. If a machine has enough resources to run 
a Python interpreter, it can probably run `hostess`.
5. `hostess.station` is not fully compatible with MacOS. MacOS compatibility
is planned. All other parts of `hostess` are compatible with MacOS.

## cautions

`hostess` is a reliable and fairly feature-complete beta in active use on
multiple projects. However, we do not yet guarantee interface stability or
backwards compatibility. 

Also, as with any system administration software, we recommend using it very 
carefully.

## tests

`hostess` includes a simple test suite compatible with `pytest`. More 
comprehensive tests are planned. (In particular, non-local networking features 
currently lack test coverage.) You can run the tests by executing `pytest -s` 
in the root directory. The `-s` flag is mandatory because `pytest` captures
stdout by default, which breaks some `hostess` features covered by the tests.

Tests require two additional dependencies: `pytest` and `pillow`.

## licensing

You can do almost anything with this software that you like, subject only to 
the extremely permissive terms of the [BSD 3-Clause License](LICENSE).
