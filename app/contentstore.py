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
            db_path, table_name="responses", **kwargs
        )


class ContentStorePickleDict(SQLitePickleDict):
    """
    Store metadata in sqlite, but _content in the filesystem based on its hash.

    Lightly compress content to save space.
    """

    def __init__(self, content_path, **kwargs):
        super().__init__(**kwargs)
        self.content_path = Path(content_path)

    def __setitem__(self, key, value):

        if hasattr(value, "_content"):
            with tempfile.NamedTemporaryFile(dir=self.content_path) as f:
                gzip.GzipFile(fileobj=f, compresslevel=3).write(value._content)
                digest = hash_func(value._content).hexdigest()
                os.link(f.name, digest)

            value = attrs.evolve(value, _content=digest)

        super().__setitem__(key, value)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        with gzip.GzipFile(self.content_path / value._content, "r") as f:
            return attrs.evolve(value, _content=f.read())


def test_contentstore():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        backend = ContentStoreCache(db_path=td / "content_cache", content_path=td)
        session = requests_cache.CachedSession(backend=backend)
        response = session.get("https://repodata.fly.dev")
        content = response.body
        content_hash = hash_func(content).hexdigest()
        assert (td / content_hash).exists()
