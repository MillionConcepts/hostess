# killscreen

## overview

`killscreen` is a collection of utilities that provide lightweight, Pythonic
interfaces to cloud and networked resources. They are intended to reduce syntactic headaches
and boilerplate while still allowing low-level manipulation and, more broadly,
still feeling like code. `killscreen` has a special emphasis on fitting intuitively
and idiomatically into common data science and scientific analysis workflows, but
is fundamentally a general-purpose utility and administration library.

## cautions

`killscreen` is in early alpha and lacks comprehensive documentation or tests. 
Because it imagines user stories like "I would like to quickly compose a one-liner that 
terminates all my EC2 instances," we recommend approaching it with more than the 
usual caution you might extend to any alpha-stage software.

## installation

`killscreen` is not currently available via any package manager. 
For now, we recommend downloading the source, using a `conda`-compatible tool and the provided
[environment.yml](environment.yml) file to set up an appropriate conda env,
then installing `killscreen` into that environment using `pip`. For instance, if you 
have `mamba` and `git` installed, this will probably work (no promises):
```
git clone https://github.com/MillionConcepts/killscreen.git
cd killscreen
mamba env create -f environment.yml
conda activate killscreen
pip install -e.
```

## compatibility
`killscreen`'s core features are intimately linked to the shell, so it is only
compatible with Unix-scented operating systems. Windows Subsystem for Linux counts.

Although `killscreen` is written with the assumption that you've got remote resources you'd 
like to interact with, dial-out privileges are not strictly required. It could
be used for LAN administration, and some of its utilties could be used purely locally.

`killscreen` requires Python >= 3.9.

## licensing

You can do almost anything with this software that you like, subject only to the extremely 
permissive terms of the [BSD 3-Clause License](LICENSE).
