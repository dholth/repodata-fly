#!/usr/bin/env python3
"""
Local server for (repo.anaconda.com|conda.anaconda.org) that caches
repodata.json, and updates repodata.json using patches from repodata.fly.dev.
Other requests (for packages e.g.) are redirects to the upstream server.

Usage:

$ python repodata_proxy.py &

$ conda install -c http://localhost:8080/conda.anaconda.org/conda-forge <package>
"""

import gzip
import json
import logging
import mimetypes
import os.path
import tempfile
import time

import appdirs
import bottle
import requests
import sync_jlap
import truncateable
import update_conda_cache
from bottle import HTTPError, HTTPResponse, parse_date, request, route, run
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

    log.info("patches: %s", jlap_url)

    cache_path = Path(CACHE_DIR / server / path).with_suffix(".json.gz")
    if not cache_path.exists():
        cache_path, digest = fetch_repodata_json(server, path, cache_path)
        assert digest  # check exists in patch file...

    # headers based on last modified of patch file
    response = static_file_headers(
        str(jlap_path.relative_to(CACHE_DIR)), root=CACHE_DIR
    )
    del response.headers["Content-Length"]

    if response.status_code != 200:
        return response

    log.debug("serve %s", cache_path)

    new_data = apply_patches(cache_path, jlap_path)

    log.info("Begin serialize")
    buf = json.dumps(new_data)
    log.info("End serialize")
    patched_path = cache_path.with_suffix(".new.json.gz")
    with gzip.open(patched_path, "wt", compresslevel=3) as out:
        out.write(buf)

    response = HTTPResponse(
        body=open(patched_path, "rb"),
        status=200,
        headers={
            "Content-Length": patched_path.stat().st_size,
            "Content-Encoding": "gzip",
        },
        **response.headers,
    )

    return response


def static_file_headers(
    filename, root, mimetype="auto", download=False, charset="UTF-8"
) -> bottle.BaseResponse:
    """
    bottle.static_file_headers but without opening file
    """

    root = os.path.abspath(root) + os.sep
    filename = os.path.abspath(os.path.join(root, filename.strip("/\\")))
    headers = dict()

    if not filename.startswith(root):
        return HTTPError(403, "Access denied.")
    if not os.path.exists(filename) or not os.path.isfile(filename):
        return HTTPError(404, "File does not exist.")
    if not os.access(filename, os.R_OK):
        return HTTPError(403, "You do not have permission to access this file.")

    if mimetype == "auto":
        mimetype, encoding = mimetypes.guess_type(filename)
        if encoding:
            headers["Content-Encoding"] = encoding

    if mimetype:
        if mimetype[:5] == "text/" and charset and "charset" not in mimetype:
            mimetype += "; charset=%s" % charset
        headers["Content-Type"] = mimetype

    if download:
        download = os.path.basename(filename if download == True else download)
        headers["Content-Disposition"] = 'attachment; filename="%s"' % download

    stats = os.stat(filename)
    headers["Content-Length"] = clen = stats.st_size
    lm = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(stats.st_mtime))
    headers["Last-Modified"] = lm

    ims = request.environ.get("HTTP_IF_MODIFIED_SINCE")
    if ims:
        ims = parse_date(ims.split(";")[0].strip())
    if ims is not None and ims >= int(stats.st_mtime):
        headers["Date"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        return HTTPResponse(status=304, **headers)

    body = "" if request.method == "HEAD" else open(filename, "rb")

    # headers["Accept-Ranges"] = "bytes"
    # ranges = request.environ.get("HTTP_RANGE")
    # if "HTTP_RANGE" in request.environ:
    #     ranges = list(parse_range_header(request.environ["HTTP_RANGE"], clen))
    #     if not ranges:
    #         return HTTPError(416, "Requested Range Not Satisfiable")
    #     offset, end = ranges[0]
    #     headers["Content-Range"] = "bytes %d-%d/%d" % (offset, end - 1, clen)
    #     headers["Content-Length"] = str(end - offset)
    #     if body:
    #         body = _file_iter_range(body, offset, end - offset)
    #     return HTTPResponse(body, status=206, **headers)

    return HTTPResponse(body, **headers)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    log.setLevel(logging.DEBUG)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Cache in %s", CACHE_DIR)

    run()
