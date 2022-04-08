"""
Truncatable log format. Save summary hash of any number of earlier lines then
append new data.

<initial> line line line <summary hash>

The hash of each line is hash(hash(line-1) + line) The hash of the second line
is hash(line-1 + line)
"""

from hashlib import blake2b


def hfunc(data, key):
    # blake2b(digest_size=32).hexdigest() is the maximum blake2b key length
    return blake2b(data.encode("utf-8"), key=key.encode("utf-8"), digest_size=32)


lines = "\n".join(str(x) for x in range(10))

print(lines.splitlines())

splits = lines.splitlines()

a = hfunc(splits[1], splits[0]).hexdigest()
print(a)
b = hfunc(splits[2], a).hexdigest()
print(b)
