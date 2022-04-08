"""
requests-cache backend that stores metadata in sqlite, and response content in
files based on their hash.
"""

import requests_cache
from requests_cache import SQLitePickleDict
from requests_cache.backends.sqlite import SQLiteCache

import hashlib
import attrs
import tempfile
import gzip
import os
from pathlib import Path


def hash_func(data):
    return hashlib.blake2b(data, digest_size=32)


class ContentStoreCache(SQLiteCache):
    def __init__(self, db_path="content_cache", content_path="", **kwargs):
        """
        content_path: root of content store
        """
        super().__init__(db_path, **kwargs)
        self.content_path = Path(content_path)
        # do we want to make it by default?
        self.content_path.mkdir(parents=True, exist_ok=True)
        self.responses = ContentStorePickleDict(
            db_path, content_path, table_name="responses", **kwargs
        )


class ContentStorePickleDict(SQLitePickleDict):
    """
    Store metadata in sqlite, but _content in the filesystem based on its hash.

    Lightly compress content to save space.
    """

    def __init__(self, db_path, content_path, **kwargs):
        super().__init__(db_path, **kwargs)
        self.content_path = Path(content_path)

    def __setitem__(self, key, value):

        if hasattr(value, "_content"):
            with tempfile.NamedTemporaryFile(dir=self.content_path) as f:
                gzip.GzipFile(fileobj=f, compresslevel=3, mode="w").write(
                    value._content
                )
                digest_path = self.digest_path(value._content)
                if not digest_path.exists():
                    os.link(f.name, digest_path)

            value = attrs.evolve(value)
            value._content = digest_path.stem.encode(
                "utf-8"
            )  # attrs.evolve doesn't expect _content
            value._content_path = str(digest_path)

        super().__setitem__(key, value)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        with gzip.GzipFile(
            (self.content_path / value._content.decode("utf-8")).with_suffix(".gz"), "r"
        ) as f:
            value._content = f.read()
        return value

    def digest_path(self, bytes):
        """
        hash bytes and return path
        """
        digest = hash_func(bytes).hexdigest()
        digest_path = (self.content_path / digest).with_suffix(".gz")
        return digest_path


def test_contentstore():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        backend = ContentStoreCache(db_path=td / "content_cache", content_path=td)
        session = requests_cache.CachedSession(backend=backend)

        response = session.get("https://repodata.fly.dev")
        content = response.content
        content_hash = hash_func(content).hexdigest()
        assert (td / content_hash).with_suffix(".gz").exists()
        assert not response.from_cache
        print(response.content)
        # assert hasattr(response, "_content_path")

        response = session.get("https://repodata.fly.dev")
        content = response.content
        content_hash = hash_func(content).hexdigest()
        assert (td / content_hash).with_suffix(".gz").exists()
        assert response.from_cache
        print(response.content)
        # assert hasattr(response, "_content_path")


if __name__ == "__main__":
    test_contentstore()
