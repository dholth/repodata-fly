"""
Truncatable log format. Save summary hash of any number of earlier lines then
append new data.

<initial> line line line <summary hash>

The hash of each line is hash(hash(line-1) + line) The hash of the second line
is hash(line-1 + line)
"""

from hashlib import blake2b
from io import RawIOBase, BytesIO
import pprint
import json
from typing import Tuple

DIGEST_SIZE = 32  # 160 bits a minimum 'for security' length?
MAX_LINEID_BYTES = 64


def hfunc(data, key):
    # blake2b(digest_size=32).hexdigest() is the maximum blake2b key length
    return blake2b(
        data.encode("utf-8"), key=key.encode("utf-8"), digest_size=DIGEST_SIZE
    )


def bhfunc(data, key):
    return blake2b(data, key=key, digest_size=DIGEST_SIZE)


def testlines():
    lines = "\n".join(str(x) for x in range(10))

    print(lines.splitlines())

    splits = lines.splitlines()

    iv = "0" * DIGEST_SIZE * 2

    lines = [iv] + splits

    def line_numbers(lines):
        """
        Generate hashed line numbers as a summary of all previous lines.
        """
        key = None
        for line in lines:
            if not key:
                key = line
                print(key)
                continue
            key = hfunc(line, key).hexdigest()
            # print(key, line)
            yield (key, line)

    l0 = list(line_numbers(lines))
    pprint.pprint(l0)

    print()
    print("\n".join(lines))

    while len(l0):
        print()
        f1 = [l0[0][0]] + [l[1] for l in l0[1:]]
        print("\n".join(f1))
        print()

        l1 = list(line_numbers(f1))
        pprint.pprint(l1)

        l0 = l1


class JlapReader:
    def __init__(self, fp: RawIOBase):
        self.fp = fp
        self.lineid = fp.readline().rstrip(b"\n")
        assert len(self.lineid) <= MAX_LINEID_BYTES

    def read(self) -> Tuple[bytes, dict]:
        """
        Read one json line from file. Yield (line id, obj)
        """
        line = self.fp.readline()
        if not line.endswith(b"\n"):  # last line
            assert self.lineid == line, ("summary hash mismatch", self.lineid, line)
            return
        # without newline
        self.lineid = bhfunc(line[:-1], self.lineid).hexdigest().encode("utf-8")
        return (json.loads(line), self.lineid)

    def readobjs(self):
        obj = True
        while obj:
            obj = self.read()
            yield obj


class JlapWriter:
    def __init__(
        self, fp: RawIOBase, lineid: bytes = ("0" * DIGEST_SIZE * 2).encode("utf-8")
    ):
        self.fp = fp
        self.fp.write(lineid + b"\n")
        self.lineid = lineid

    def write(self, obj):
        """
        Write one json line to file.
        """
        line = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.lineid = bhfunc(line, self.lineid).hexdigest().encode("utf-8")
        self.fp.write(line)
        self.fp.write(b"\n")

    def finish(self):
        self.fp.write(self.lineid)


def test():
    bio = BytesIO()
    writer = JlapWriter(bio, ("0" * DIGEST_SIZE * 2).encode("utf-8"))
    for i in range(10):
        writer.write(i)
    writer.finish()

    print()

    print(bio.getvalue().decode("utf-8"))

    bio.seek(0)

    print("\nreading")

    reader = JlapReader(bio)
    for obj in reader.readobjs():
        print(obj)


if __name__ == "__main__":
    test()
