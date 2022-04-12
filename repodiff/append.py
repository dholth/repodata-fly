"""
Fetch latest and append, replacing penultimate line of jlap
"""

data = open("repodata.jlap", "rb").read()

with open("repodata.jlap", "rb+") as jlap:
    offsets = [0]
    for line in jlap:
        print(line[-10:])
        offsets.append(len(line) + offsets[-1])

print(offsets[-10:])
print(data[offsets[-3] :])  # where we want to overwrite
