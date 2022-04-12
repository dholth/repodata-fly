"""
Cache several conda repodata plus history.
"""

import requests_cache
import pathlib
import contentstore
import os.path
import json

backend = contentstore.ContentStoreCache(
    db_path="content_cache", content_path="content"
)
session = requests_cache.CachedSession(backend=backend, cache_control=True)

REPOS = [
    "repo.anaconda.com/pkgs/main",
    "conda.anaconda.org/conda-forge",
]

SUBDIRS = [
    "linux-32",
    "linux-64",
    "linux-aarch64",
    "linux-armv6l",
    "linux-armv7l",
    "linux-ppc64le",
    "linux-s390x",
    "noarch",
    "osx-64",
    "osx-arm64",
    "win-32",
    "win-64",
    "zos-z",
]

for repo in REPOS:
    for subdir in SUBDIRS:
        for url in [
            f"https://{repo}/{subdir}/repodata.json",
            f"https://{repo}/{subdir}/current_repodata.json",
        ]:
            response = session.get(url)
            print(response.from_cache, url)
            print(response.cache_key)
            print(response.headers)
            output = pathlib.Path(url.lstrip("https://"))
            headers = output.with_stem(f"{output.stem}-headers")
            stem = output.stem
            if not output.exists() or not response.from_cache:
                try:
                    json.loads(response.content)
                except json.decoder.JSONDecodeError:
                    print("NOT JSON")
                    continue
                if output.exists():
                    i = 0
                    while output.with_stem(f"{stem}-{i:03d}").exists():
                        i += 1
                    output.rename(output.with_stem(f"{stem}-{i:03d}"))
                    if headers.exists():
                        headers.rename(headers.with_stem(f"{headers.stem}-{i:03d}"))
                output.parent.mkdir(parents=True, exist_ok=True)
                content_path: pathlib.Path = backend.responses.digest_path(
                    response.content
                )
                relative = os.path.relpath(content_path, output.parent)
                if output.is_symlink():
                    output.unlink()  # if symlink was broken?
                output.symlink_to(relative)
                headers.write_text(json.dumps(dict(response.headers.lower_items())))
