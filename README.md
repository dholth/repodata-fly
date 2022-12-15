A fly.io VM to cache repodata.json

Create a volume "data" and make sure the VM has extra RAM to handle the
large responses.

pyz files
=========

Includes all dependencies, executable with python <file.pyz> or ./file.pyz to
use /usr/bin/python3.

`pip install zipapps`, `./buildapp.sh` to rebuild.
