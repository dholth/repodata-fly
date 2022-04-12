#!/usr/bin/env python
# create all patches

import glob
import subprocess
import json
import os.path
import pathlib
import logging
import re

import truncateable

# if numbers get very large,
# import natsort
# note repodata.json may come before all numbered ones

log = logging.getLogger(__name__)

cmd = "/jpatchset"


def is_series(filename):
    return re.match("\w*(-\d+)?\.json", filename)


def patchme():
    """
    chdir to desired directory, then call.
    """
    current_repodata = list(
        filter(is_series, sorted(glob.glob("current_repodata*.json")),)
    )
    repodata = list(filter(is_series, sorted(glob.glob("repodata*.json"))))

    for patchset, series in (
        ("current_repodata-patch.json", current_repodata),
        ("repodata-patch.json", repodata),
    ):
        if not os.path.exists(patchset):
            pathlib.Path(patchset).write_text(
                json.dumps({"url": "", "latest": "", "patches": []})
            )
        latest = ""
        latest_path = pathlib.Path(patchset).with_suffix(".last")
        if latest_path.exists():
            latest = latest_path.read_text()
        for left, right in zip(series, series[1:]):
            if left <= latest:
                log.debug(f"skip {left}")
                continue
            print(left, right)
            try:
                # newer jpatchset transparently supports compressed left, right
                # but is slower than pypy
                subprocess.run(
                    f"{cmd} -l {left} -r {right} -p {patchset} -i -o",
                    shell=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(os.getcwd(), e)
                continue
            latest_path.write_text(left)

        data = json.load(open(patchset, "r"))
        with open(pathlib.Path(patchset).with_suffix(".jlap"), "wb+") as out:
            writer = truncateable.JlapWriter(out)
            for obj in reversed(data["patches"]):
                writer.write(obj)
            data.pop("patches")
            headers = pathlib.Path(series[-1]).with_stem(
                pathlib.Path(series[-1]).stem + "-headers"
            )
            if headers.exists():
                data["headers"] = json.loads(headers.read_text())
            writer.write(data)
            writer.finish()
