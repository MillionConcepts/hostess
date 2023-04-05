# hostess

## overview

`hostess` is a collection of utilities that provide lightweight, Pythonic
interfaces to cloud and networked resources. They are intended to reduce syntactic headaches
and boilerplate while still allowing low-level manipulation and, more broadly,
still feeling like code. `hostess` has a special emphasis on fitting intuitively
and idiomatically into common data science and scientific analysis workflows, but
is fundamentally a general-purpose utility and administration library.

## cautions

`hostess` is in alpha and lacks comprehensive documentation or tests. Because it is 
powerful system administration software, we recommend approaching it with more than 
the usual caution you might extend to any alpha-stage software.

## installation

`hostess` is not currently available via any package manager. 
For now, we recommend downloading the source, using a `conda`-compatible tool and the provided
[environment.yml](environment.yml) file to set up an appropriate conda env,
then installing `hostess` into that environment using `pip`. For instance, if you 
have `mamba` and `git` installed, this will probably work (no promises):
```
git clone https://github.com/MillionConcepts/hostess.git
cd hostess
mamba env create -f environment.yml
conda activate hostess
pip install -e .
```

## compatibility
`hostess`'s core features are intimately linked to the shell, so it is only
compatible with Unix-scented operating systems. Windows Subsystem for Linux counts.

Although `hostess` is written with the assumption that you've got remote resources you'd 
like to interact with, dial-out privileges are not strictly required. It could
be used for LAN administration, and some of its utilties could be used purely locally.

`hostess` requires Python >= 3.9.

## licensing

You can do almost anything with this software that you like, subject only to the extremely 
permissive terms of the [BSD 3-Clause License](LICENSE).
