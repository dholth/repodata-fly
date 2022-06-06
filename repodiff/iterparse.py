import contextlib
import gzip
import hashlib
import json
import pathlib
import time

import ijson  # also yajl, please
import tqdm

jgz = pathlib.Path(
    "~/Library/Caches/repodata-proxy/conda.anaconda.org/conda-forge/linux-64/repodata.json.gz"
).expanduser()


@contextlib.contextmanager
def timeme(message=""):
    begin = time.time()
    yield
    end = time.time()
    print(f"{message}{end-begin:0.02f}s")


with timeme():
    repodata = json.load(gzip.open(jgz))
    packages = repodata["packages"]
    items = len(repodata["packages"])

if False:
    items = []
    with timeme():
        repodata = gzip.open(jgz)

        # builtin json.parse is much, much faster compared to yajl2_cffi
        # however pypi wheels provide yajl_c which is comparable to json.load
        # conda-forge just has packages but anaconda also has packages.conda
        # way to get everything except a particular prefix?
        for item in tqdm.tqdm(ijson.kvitems(repodata, "packages"), total=items):
            item

hash = "a" * 64

print(f"{items}# of packages")
print(f"all packages serialized {len(json.dumps(packages)):,}")
print(f"all keys {len(json.dumps(list(packages.keys()))):,}")
print(f"all values {len(json.dumps(list(packages.values()))):,}")
with_fake_hashes = {k: hash for k in packages.keys()}
print(f"with hashes {len(json.dumps(with_fake_hashes).encode('utf-8')):,}")
# too compressible with fake hashes
print(
    f"with hashes.gz {len(gzip.compress(json.dumps(with_fake_hashes).encode('utf-8'))):,}"
)

packages_items = list(packages.items())

for x in range(16):
    slice = 2**x
    with timeme(f"hash items 1/{slice} {len(packages_items)/slice:,} "):
        hashes = [
            hashlib.blake2b(
                (":".join((k, json.dumps(v))).encode("utf-8")), digest_size=32
            ).hexdigest()
            for k, v in packages_items[::slice]
        ]

    print(f"just hashes, {len(json.dumps(hashes)):0.2f}\n")
