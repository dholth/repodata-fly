#!/opt/pypy39/bin/pypy3
"""
Generate patches from Mercurial revisions.
"""

import jsonpatch
import subprocess
import json
import hashlib
import sqlite3
import itertools
import truncateable
import logging

from pathlib import Path

log = logging.getLogger(__name__)


def hash_func(data=b""):
    return hashlib.blake2b(data, digest_size=32)


def make_patches(file, cwd=None, from_revision=0):

    # log from oldest to newest
    revisions = json.loads(
        subprocess.run(
            ["hg", "log", "-v", "-Tjson", f"-r{from_revision}:"],
            cwd=cwd,
            stdout=subprocess.PIPE,
            check=True,
        ).stdout
    )

    previous = None
    for rev_log in revisions:
        if file in rev_log["files"]:
            rev_bytes = subprocess.run(
                ["hg", "cat", "-r", rev_log["node"], file],
                cwd=cwd,
                stdout=subprocess.PIPE,
                check=True,
            ).stdout
            current = {
                "digest": hash_func(rev_bytes).hexdigest(),
                "obj": json.loads(rev_bytes),
            }
            rev_bytes = b""

            if previous:
                patch = jsonpatch.make_patch(previous["obj"], current["obj"])
                patchobj = {
                    "to": current["digest"],
                    "from": previous["digest"],
                    "patch": patch.patch,
                }
                yield (rev_log, file, patchobj)

            previous = current


def store_patches(conn):
    """
    Store patches from per-subdir mercurial repositories into sqlite.

    Run with cwd = (base of mirror)
    """
    conn.execute("PRAGMA journal_mode=wal")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS patches 
            (id INTEGER PRIMARY KEY, 
            url TEXT NOT NULL, 
            hg_rev_to INTEGER, 
            patch TEXT NOT NULL, 
            timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL)
        """
    )

    for repodata in itertools.chain(
        Path().rglob("**/repodata.json"), Path().rglob("**/current_repodata.json")
    ):
        base_path = repodata.parent
        base_url = str(base_path)

        # might need to be per-file
        # exclude negative revisions, which count from the end in mercurial
        newest_rev = conn.execute(
            "SELECT max(max(hg_rev_to), 0) FROM patches WHERE url = ?", (f"{repodata}",)
        ).fetchone()[0]

        if newest_rev is None:
            newest_rev = 0

        for rev, file, patch in make_patches(
            cwd=base_path.absolute(), from_revision=newest_rev, file=repodata.name
        ):
            log.info(f"new patch for {base_url}/{file}")
            with conn:
                conn.execute(
                    "INSERT INTO patches (url, hg_rev_to, patch) VALUES (?, ?, ?)",
                    (f"{base_url}/{file}", rev["rev"], json.dumps(patch)),
                )

        headers = None
        headers_file = repodata.with_stem(f"{repodata.stem}-headers")
        if headers_file.exists():
            try:
                headers = json.loads(headers_file.read_text())
            except json.JSONDecodeError:
                log.warn("%s was not JSON", headers_file)

        # regenerate patch file right away
        write_jlap(conn, base_url, repodata.name, headers=headers)


def write_jlap(conn, base_url, file, headers):
    outfile = Path(base_url, file).with_suffix(".jlap")
    outfile_temp = Path(base_url, file).with_suffix(".jlap.tmp")
    assert not str(outfile).endswith(".json")
    with outfile_temp.open("wb+") as out:
        writer = truncateable.JlapWriter(out)
        latest_line = {}
        for row in conn.execute(
            "SELECT patch FROM patches WHERE url = ? ORDER BY hg_rev_to",
            (f"{base_url}/{file}",),
        ):
            line = row[0]
            # TODO add non-reparsing writer
            latest_line = json.loads(line)
            writer.write(latest_line)

        latest = latest_line.get("to")
        if not latest_line.get("to"):
            # we like big buffers
            latest = hash_func(Path(base_url, file).read_bytes()).digest().hex()

        writer.write(
            {"url": f"https://{base_url}/{file}", "latest": latest, "headers": headers,}
        )
        writer.finish()

    if not outfile.exists() or outfile_temp.read_bytes() != outfile.read_bytes():
        log.info("Overwrite changed %s", outfile)
        outfile_temp.replace(outfile)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(message)s", datefmt="%Y-%m-%dT%H:%M:%S", level=logging.INFO,
    )
    log.info("Update .jlap patchsets")
    db_path = "/data/cacher/patches.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        store_patches(conn)
    finally:
        conn.close()
    log.info("Finish update .jlap patchsets")
