"""
Update local conda cache based on patchsets from repodata.fly.dev

(Doesn't actually write to cache; just prints results)

Requirements: `pip install jsonpatch requests-cache`
"""
import json
import requests_cache
import hashlib
import glob
import os
import re
import subprocess
import jsonpatch
import sys


def make_session():
    session = requests_cache.CachedSession(
        cache_control=True, allowable_codes=(200, 206), expire_after=300
    )
    session.headers["User-Agent"] = "update-conda-cache/0.0.1"
    return session


# mirrored on patch server
supported = re.compile(
    r"https://((conda\.anaconda\.org/conda-forge|repo.anaconda.com/pkgs/main)/.*)"
)


def hash_func(data: bytes = b""):
    return hashlib.blake2b(data, digest_size=32)


def conda_normalize_hash(data):
    """
    Normalize raw_data in the same way as conda-build index.py, return hash.
    """
    # serialization options used by conda-build's index.py
    # (conda should cache the unmodified response)
    data_buffer = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False).encode(
        "utf-8"
    )

    data_hash = hash_func(data_buffer)
    data_hash.update(b"\n")  # conda_build/index.py _write_repodata adds newline
    data_hash = data_hash.hexdigest()

    return data_hash


def apply_patches(data, patches, have, want):
    apply = []
    for patch in reversed(patches):
        if have == want:
            break
        if patch["to"] == want:
            apply.append(patch)
            want = patch["from"]

    if have != want:
        print(f"No patch from local revision {have}")
        apply.clear()

    print(f"Apply {len(apply)} patches...")

    while apply:
        patch = apply.pop()
        print(f"{patch['from']} -> {patch['to']}, {len(patch['patch'])} steps")
        data = jsonpatch.JsonPatch(patch["patch"]).apply(data, in_place=True)

    return data


def update_cache():
    """
    Look inside conda's cache, try to update the .json
    """
    session = make_session()

    conda_info = json.loads(
        subprocess.run(
            ["conda", "info", "--json"], stdout=subprocess.PIPE, check=True
        ).stdout
    )

    pkgs_dirs = sys.argv[1:] + conda_info["pkgs_dirs"]

    first_pkg_dir = next(path for path in pkgs_dirs if os.path.exists(path))

    cachedir = os.path.join(first_pkg_dir, "cache")

    print(f"Update caches in {cachedir} (first existing of {pkgs_dirs})")

    for file in glob.glob(os.path.join(cachedir, "*.json")):
        print(f"Parse, normalize {file}: ", end="")
        raw_data = json.load(open(os.path.join(cachedir, file)))
        print(raw_data["_url"])
        match = supported.match(raw_data["_url"])
        if match:
            # remove in-band cache headers
            data = {k: v for (k, v) in raw_data.items() if not k.startswith("_")}
            data_hash = conda_normalize_hash(data)
            part = match[1]
            resp = session.get(f"https://repodata.fly.dev/{part}/repodata-patch.jlap")
            resp.raise_for_status()
            print(
                f"{file} {data_hash} {resp.status_code} in {resp.url}?",
                data_hash in resp.text,
            )
            lines = resp.text.splitlines()
            metadata = json.loads(lines[-2])
            patches = [json.loads(line) for line in lines[1:-2]]
            print(f"{len(patches)} patches available")
            new_data = apply_patches(data, patches, data_hash, metadata["latest"])
            new_data_hash = conda_normalize_hash(new_data)
            if new_data_hash == metadata["latest"]:
                print("SUCCESS ", end="")
            else:
                print("   FAIL ", end="")
            print(f"New hash is {new_data_hash} (wanted {metadata['latest']}")
            patch_size = len(resp.text)
            data_size = os.stat(file).st_size
            print(
                f"Patch file is {len(resp.text)} bytes; data {os.stat(file).st_size} bytes; {data_size/patch_size:0.02f}x smaller"
            )
        else:
            print(f"{raw_data['_url']} not in mirror")
        print()


if __name__ == "__main__":
    update_cache()
