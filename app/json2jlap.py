"""
Simple json to jlap "*/repodata.json" -> "*/repodata.jlap tool.

Copy */repodata.json to */.cache/repodata.json.last

Read */repodata.jlap

Diff */repodata.json with */.cache/repodata.json

Write */repodata.jlap

Same for current_repodata.jlap

If output jlap is larger than a set size, remove older diffs.
"""

from __future__ import annotations

import itertools
import json
import logging
import shutil
from hashlib import blake2b
from io import IOBase
from pathlib import Path

import click
import jsonpatch
from truncateable import JlapReader, JlapWriter
from jlaptrim import trim_if_larger

log = logging.getLogger("__name__")

# attempt to control individual patch size (will fall back to re-downloading
# repodata.json) without serializing to bytes just to measure
PATCH_STEPS_LIMIT = 8192


DIGEST_SIZE = 32


def hfunc(data: bytes):
    return blake2b(data, digest_size=DIGEST_SIZE)


class HashReader:
    """
    Hash a file while it is being read.
    """

    def __init__(self, fp: IOBase):
        self.fp = fp
        self.hash = blake2b(digest_size=DIGEST_SIZE)

    def read(self, bytes=None):
        data = self.fp.read(bytes)
        self.hash.update(data)
        return data


def hash_and_load(path):
    with path.open("rb") as fp:
        h = HashReader(fp)
        obj = json.load(h)
    return obj, h.hash.digest()


def json2jlap_one(cache: Path, repodata: Path):
    previous_repodata = cache / (repodata.name + ".last")

    patches = []

    jlapfile = (repodata.parent / repodata.name).with_suffix(".jlap")
    if jlapfile.exists():
        with jlapfile.open("rb") as jlap:
            patchfile = JlapReader(jlap)
            *patches, metadata = list(patch for patch, _ in patchfile.readobjs())

    if (
        previous_repodata.exists()
        and repodata.stat().st_mtime > previous_repodata.stat().st_mtime
    ):
        current, current_digest = hash_and_load(repodata)
        previous, previous_digest = hash_and_load(previous_repodata)

        jpatch = jsonpatch.make_patch(previous, current)

        # inconvenient to add bytes size limit here; limit number of steps?
        if previous_digest == current_digest:
            log.warn("Skip identical %s", repodata)
        elif len(jpatch.patch) > PATCH_STEPS_LIMIT:
            log.warn("Skip large %s-step patch", len(jpatch.patch))
        else:
            patches.append(
                {
                    "to": current_digest.hex(),
                    "from": previous_digest.hex(),
                    "patch": jpatch.patch,
                }
            )

        # metadata
        patches.append({"url": repodata.name, "latest": current_digest.hex()})

        with jlapfile.open("wb+") as jlap:
            patchfile = JlapWriter(jlap)
            for patch in patches:
                patchfile.write(patch)
            patchfile.finish()

    if (
        not previous_repodata.exists()
        or repodata.stat().st_mtime > previous_repodata.stat().st_mtime
    ):
        shutil.copyfile(repodata, previous_repodata)


@click.command()
@click.option("--cache", required=True, help="Cache directory.")
@click.option("--repodata", required=True, help="Repodata directory.")
@click.option(
    "--trim-low",
    required=False,
    default=2**20 * 5,
    show_default=True,
    help="Maximum size after trim.",
)
@click.option(
    "--trim-high",
    required=False,
    default=0,
    show_default=True,
    help="Trim if larger than size; 0 to disable.",
)
def json2jlap(cache, repodata, trim_low, trim_high):
    cache = Path(cache).expanduser()
    repodata = Path(repodata).expanduser()
    repodatas = itertools.chain(
        repodata.glob("*/repodata.json"), repodata.glob("*/current_repodata.json")
    )
    for repodata in repodatas:
        # require conda-index's .cache folder
        cachedir = Path(cache, repodata.parent.name, ".cache")
        if not cachedir.is_dir():
            continue
        json2jlap_one(cachedir, repodata)
        if trim_high > trim_low:
            trim_if_larger(trim_high, trim_low, repodata)


def go():
    logging.basicConfig(
        format="%(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=logging.INFO,
    )
    json2jlap()


if __name__ == "__main__":
    go()
