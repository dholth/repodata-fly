#!/usr/bin/env python3
"""
Local server for (repo.anaconda.com|conda.anaconda.org) that caches
repodata.json, and updates repodata.json using patches from repodata.fly.dev.
Other requests (for packages e.g.) are redirects to the upstream server.

Usage:

$ python repodata_proxy.py &

$ conda install -c http://localhost:8080/conda.anaconda.org/conda-forge <package>
"""

import bottle
from bottle import route, request, response, run, HTTP_CODES

import appdirs
import logging
import requests
import tempfile
import os.path
import gzip
import json

import sync_jlap
import update_conda_cache
import truncateable

from update_conda_cache import hash_func

log = logging.getLogger(__name__)

from pathlib import Path

CACHE_DIR = Path(appdirs.user_cache_dir("repodata-proxy"))

MIRROR_URL = "https://repodata.fly.dev"

CHUNK_SIZE = 1 << 14

session = sync_jlap.make_session((CACHE_DIR / "jlap_cache.db"))

sync = sync_jlap.SyncJlap(session, CACHE_DIR)


@route("/")
def welcome():
    return """
    <h1>repodata.json proxy</h1>
    <p>Efficient repodata.json updates via repodata.fly.dev patch sets</p>
    <p>Use as a replacement for conda-forge or defaults channel:</p>
    <p><code>conda install --override-channels -c http://localhost:8080/conda.anaconda.org/conda-forge [package]</p>
    <p>After repodata.json is cached, updates will consume a tiny amount of bandwidth.</p>
    """


def fetch_repodata_json(server, path, cache_path):
    """
    Fetch new repodata.json; cache to a gzip'd file.

    Return (path, digest)
    """
    upstream = f"https://{server}/{path}"

    with tempfile.NamedTemporaryFile(dir=CACHE_DIR, delete=False) as outfile:
        compressed = gzip.open(outfile, "w")
        response = requests.get(upstream, stream=True)
        response.raise_for_status()
        hash = hash_func()
        for chunk in response.iter_content(CHUNK_SIZE):
            hash.update(chunk)
            compressed.write(chunk)

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        compressed.close()
        os.replace(outfile.name, cache_path)

    return cache_path, hash.digest()


class DigestReader:
    """
    Read and hash at the same time.
    """

    def __init__(self, fp):
        self.fp = fp
        self.hash = hash_func()

    def read(self, bytes=None):
        buf = self.fp.read(bytes)
        self.hash.update(buf)
        return buf


def apply_patches(cache_path, jlap_path):
    """
    Return patched version of cache_path, as an object
    """
    patches = []
    with jlap_path.open("rb") as fp:
        jlap = truncateable.JlapReader(fp)
        patches = list(obj for obj, _ in jlap.readobjs())
        assert "latest" in patches[-1]

    meta = patches[-1]
    patches = patches[:-1]
    digest_reader = DigestReader(gzip.open(cache_path))
    original = json.load(digest_reader)
    original_hash = digest_reader.hash.digest()

    patched = update_conda_cache.apply_patches(
        original, patches, original_hash.hex(), meta["latest"]
    )
    return patched


@route(r"/<server:re:(repo\.anaconda\.com|conda\.anaconda\.org)>/<path:path>")
def mirror(server, path):

    upstream = f"https://{server}/{path}"

    # find packages on original server
    if not path.endswith("repodata.json"):
        response = bottle.response
        response.add_header("Location", upstream)
        response.status = 302
        return response

    # return cached repodata.json with latest patches applied
    assert path.endswith("repodata.json")

    jlap_url = f"{MIRROR_URL}/{server}/{path[:-len('.json')]}.jlap"
    jlap_path = sync.update_url(jlap_url)

    log.info("jlap in %s", jlap_url)

    cache_path = Path(CACHE_DIR / server / path).with_suffix(".json.gz")
    if not cache_path.exists():
        cache_path, digest = fetch_repodata_json(server, path, cache_path)
        assert digest  # check exists in patch file...

    log.debug("serve %s", cache_path)

    new_data = apply_patches(cache_path, jlap_path)

    # response = bottle.static_file(
    #     str(cache_path.relative_to(CACHE_DIR)), root=CACHE_DIR
    # )

    # TODO generate last-modified from .jlap file, serve data from

    return new_data


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    log.setLevel(logging.DEBUG)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Cache in %s", CACHE_DIR)

    run()
