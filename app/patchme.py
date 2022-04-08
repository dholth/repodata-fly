#!/usr/bin/env python
# create all patches

import glob
import subprocess
import tempfile
import json
import os.path
import pathlib
import logging

log = logging.getLogger(__name__)

cmd = "/jpatchset"

def patchme():
    """
    chdir to desired directory, then call.
    """
    current_repodata = list(filter(lambda x: not x.endswith('-patch.json'), sorted(glob.glob('current_repodata*.json'))))
    repodata = list(filter(lambda x: not x.endswith('-patch.json'), sorted(glob.glob('repodata*.json'))))

    with tempfile.TemporaryDirectory() as td:
        for patchset, series in ("current_repodata-patch.json", current_repodata), ("repodata-patch.json", repodata):
            if not os.path.exists(patchset):
                pathlib.Path(patchset).write_text(json.dumps({'url':'', 'latest':'', 'patches':[]}))
            latest = ""
            latest_path = pathlib.Path(patchset).with_suffix('.last')
            if latest_path.exists():
                latest = latest_path.read_text()
            for left, right in zip(series, series[1:]):
                if left <= latest:
                    log.debug(f"skip {left}")
                    continue
                print(left, right)
                try:
                    subprocess.run(f"gunzip -c < {left} > {td}/{left}", shell=True, check=True)
                    subprocess.run(f"gunzip -c < {right} > {td}/{right}", shell=True, check=True)
                    subprocess.run(f"{cmd} -l {td}/{left} -r {td}/{right} -p {patchset} -i -o", shell=True, check=True)
                except subprocess.CalledProcessError as e:
                    print(os.getcwd(), e)
                    continue
                os.unlink(f"{td}/{left}")
                os.unlink(f"{td}/{right}")
                latest_path.write_text(left)
