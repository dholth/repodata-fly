import requests
import truncateable

if __name__ == "__main__":
    data = requests.get(
        "https://repodata.fly.dev/conda.anaconda.org/conda-forge/linux-64/repodata-patch.json"
    ).json()

    with open("repodata.jlap", "wb+") as out:
        writer = truncateable.JlapWriter(out)
        for obj in reversed(data["patches"]):
            writer.write(obj)
        data.pop("patches")
        writer.write(data)
        writer.finish()
