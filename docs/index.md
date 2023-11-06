# Welcome to hostess

## overview

`hostess` is a collection of utilities that provide lightweight, Pythonic
interfaces to cloud and networked resources. It reduces syntactic headaches
and boilerplate while still allowing low-level manipulation and, more broadly,
still feeling like code. `hostess` has a special emphasis on fitting intuitively
and idiomatically into common data science and scientific analysis workflows, but
is fundamentally a general-purpose utility and administration library.

You can access the source code for `hostess` on github at: https://github.com/MillionConcepts/hostess

## cautions

`hostess` is in alpha and lacks comprehensive documentation or tests. It is 
powerful system administration software, so we recommend using it very 
carefully.

## installation

`hostess` is not currently available via any package manager. 
For now, we recommend downloading the source, using a `conda`-compatible tool 
and the provided [environment.yml](https://github.com/MillionConcepts/hostess/blob/main/environment.yml) file to set up an 
appropriate conda env, then installing `hostess` into that environment using 
`pip`. For instance, if you have `mamba` and `git` installed, this will 
probably work (no promises):
```
git clone https://github.com/MillionConcepts/hostess.git
cd hostess
mamba env create -f environment.yml
conda activate hostess
pip install -e .
```

## compatibility
1. `hostess`'s core features are closely linked to the shell, so it is 
only fully compatible with Unix-scented operating systems. Mac OS counts, 
as does Windows Subsystem for Linux (WSL). Windows *outside* of WSL doesn't.
2. Although `hostess` is written with the assumption that you've got remote 
resources you'd like to interact with, dial-out privileges are not strictly 
required. It could be used for LAN administration, and some of its utilties 
could be used purely locally.
3. `hostess` requires Python >= 3.9.
4. `hostess` is very lightweight. If a machine has enough 
resources to run a Python interpreter, it can probably run `hostess`.  
