import ijson  # also yajl, please
import gzip
import json
import tqdm
import time
import contextlib


@contextlib.contextmanager
def timeme():
    begin = time.time()
    yield
    end = time.time()
    print(f"{end-begin:0.02f}s")


with timeme():
    repodata = gzip.open("repodata.json.gz")
    items = len(json.load(repodata)["packages"])

with timeme():
    repodata = gzip.open("repodata.json.gz")

    # builtin json.parse is much, much faster compared to yajl2_cffi
    # however pypi wheels provide yajl_c which is comparable to json.load
    # conda-forge just has packages but anaconda also has packages.conda
    # way to get everything except a particular prefix?
    for item in tqdm.tqdm(ijson.kvitems(repodata, "packages"), total=items):
        item
