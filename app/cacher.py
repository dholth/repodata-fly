"""
Cache several conda repodata plus history.

Uses Mercurial to track older revisions - more efficient than git for this use case.
"""

from pathlib import Path
from no_cache import discard_serializer
from requests_cache import CachedSession

import pathlib
import json

import subprocess

TIME_LIMIT = 600


def update_cache():
    session = CachedSession(
        "http_cache_repodata",
        allowable_codes=[200, 206],
        match_headers=["Accept", "Range"],
        serializer=discard_serializer,
        cache_control=True,
        expire_after=600,  # otherwise cache only expires if response header says so
    )

    with session.cache.responses.connection() as conn:
        conn.execute("PRAGMA journal_mode=wal")

    session.headers["User-Agent"] = "repodata.fly.dev/0.0.1"

    REPOS = [
        "repo.anaconda.com/pkgs/main",
        "repo.anaconda.com/pkgs/msys2",
        "repo.anaconda.com/pkgs/r",
        "conda.anaconda.org/conda-forge",
    ]

    # All mentioned in channeldata.json, some active
    SUBDIRS = [
        # "linux-32",
        "linux-64",
        # "linux-aarch64",
        # "linux-armv6l",
        # "linux-armv7l",
        # "linux-ppc64le",
        # "linux-s390x",
        "noarch",
        "osx-64",
        "osx-arm64",
        # "win-32",
        "win-64",
        # "zos-z",
    ]

    FILENAMES = [
        "repodata.json",
        "repodata-headers.json",
        "repodata-patch.json",
        "repodata-patch.jlap",
        "current_repodata.json",
        "current_repodata-headers.json",
        "current_repodata-patch.json",
        "current_repodata-patch.jlap",
    ]

    def commit(cwd):
        if not Path(cwd, ".hg").exists():
            subprocess.run(["hg", "init"], cwd=cwd, check=True)
        # on first commit(), later 'hg status' check will be clean with no files added
        subprocess.run(
            ["hg", "add"] + [fn for fn in FILENAMES if Path(cwd, fn).exists()],
            cwd=cwd,
            check=True,
        )
        status = subprocess.run(
            ["hg", "status", "-am"], stdout=subprocess.PIPE, cwd=cwd, check=True
        )
        if status.stdout.decode("utf-8") == "":
            print("repository is clean")
            return
        subprocess.run(
            ["hg", "commit", "-u", "repodata", "-m", "checkpoint"], cwd=cwd, check=True,
        )

    SHOW_HEADERS = set(
        ("date", "content-type", "last-modified", "age", "expires", "cache-control",)
    )

    for repo in REPOS:
        for subdir in SUBDIRS:
            for url in [
                f"https://{repo}/{subdir}/repodata.json",
                f"https://{repo}/{subdir}/current_repodata.json",
            ]:
                response = session.get(url)
                print(response.from_cache, url)
                print(response.cache_key)

                print(
                    {
                        k: v
                        for k, v in response.headers.lower_items()
                        if k in SHOW_HEADERS
                    }
                )
                output = pathlib.Path(url.lstrip("https://"))
                headers = output.with_stem(f"{output.stem}-headers")
                if not output.exists() or not response.from_cache:
                    try:
                        json.loads(response.content)
                    except json.decoder.JSONDecodeError:
                        print("NOT JSON")
                        continue

                    output.parent.mkdir(parents=True, exist_ok=True)

                    if output.is_symlink():
                        output.unlink()  # if symlink was broken?

                    output.write_bytes(response.content)
                    headers.write_text(json.dumps(dict(response.headers.lower_items())))

                commit(output.parent)


if __name__ == "__main__":
    import signal

    signal.alarm(TIME_LIMIT)
    update_cache()
