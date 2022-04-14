"""
Synchronize local patch files with repodata.fly.dev
"""

import os
import sys

from pathlib import Path
from no_cache import discard_serializer
from requests_cache import CachedSession

import truncateable

import logging

session = CachedSession(
    "http_cache_jlap",
    allowable_codes=[200, 206],
    match_headers=["Accept", "Range"],
    serializer=discard_serializer,
    cache_control=True,
    expire_after=30,  # otherwise cache only expires if response header says so
)


session.headers["User-Agent"] = "update-conda-cache/0.0.1"

REPOS = [
    "repo.anaconda.com/pkgs/main",
    "conda.anaconda.org/conda-forge",
]

SUBDIRS = [
    "linux-64",
    "noarch",
    "osx-64",
    "osx-arm64",
    "win-64",
]

MIRROR = "repodata.fly.dev"

BASEDIR = Path("patches")


def line_offsets(path: Path):
    """
    Return byte offset to next-to-last line in path.
    """
    sum = 0
    offsets = []
    with path.open("rb") as data:
        for line in data:
            offsets.append(sum)
            sum += len(line)
    try:
        return offsets[-2]
    except IndexError:
        return 0


def update():
    for repo in REPOS:
        for subdir in SUBDIRS:
            for url in [
                f"https://{MIRROR}/{repo}/{subdir}/repodata-patch.jlap",
                f"https://{MIRROR}/{repo}/{subdir}/current_repodata-patch.jlap",
            ]:

                output = BASEDIR / Path(url.lstrip("https://"))
                headers = {}
                if not output.exists():
                    output.parent.mkdir(parents=True, exist_ok=True)
                    # XXX bypass cache if not output.exists()
                else:
                    offset = line_offsets(output)
                    headers = {"Range": "bytes=%d-" % offset}

                response = session.get(url, headers=headers)
                if response.from_cache:
                    print(f"from_cache {url} {response.expires}")
                    continue

                print(
                    response.status_code, len(response.content), url, response.headers
                )
                if response.status_code == 200:
                    print("Overwrite")
                    output.write_bytes(response.content)
                elif response.status_code == 206:
                    size_before = os.stat(output).st_size
                    os.truncate(output, offset)
                    with output.open("ba") as out:
                        tell = out.tell()
                        print(
                            "Append %d-%d (%d lines)"
                            % (
                                tell,
                                tell + len(response.content),
                                len(response.content.splitlines()),
                            )
                        )
                        out.write(response.content)
                    size_after = os.stat(output).st_size
                    print(
                        "Was %d, now %d bytes, delta %d"
                        % (size_before, size_after, (size_after - size_before))
                    )
                else:
                    print("Unexpected status %d" % response.status_code)

                # verify checksum
                # can cache checksum of next-to-last line instead of recalculating all
                # (remove consumed lines from local file and store full length)
                with output.open("rb") as fp:
                    jlap = truncateable.JlapReader(fp)
                    for _obj in jlap.readobjs():
                        pass

                print()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "-v":
        logging.basicConfig(level=logging.DEBUG)
    update()
