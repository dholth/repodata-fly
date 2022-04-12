#!/usr/bin/env python
import glob
import pathlib
import patchme
import os

# must expand before os.chdir()
paths = [
    os.path.abspath(path) for path in glob.glob("**/repodata.json", recursive=True)
]

for path in paths:
    os.chdir(os.path.dirname(path))
    print(os.getcwd())
    patchme.patchme()
