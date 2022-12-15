"""
Rewrite .jlap if too big.
"""

from __future__ import annotations

import logging
from pathlib import Path

import click
from jlapcore import jlap_buffer, write_jlap_buffer

log = logging.getLogger("__name__")


def trim(jlap: Path, target_size: int, target_path: Path | None = None):
    if not target_path:
        target_path = jlap

    def lines():
        for line in jlap.open("rb"):
            # strip trailing \n if present
            if line.endswith(b"\n"):
                yield line[:-1]
            else:
                yield line  # ugly

    buffer = jlap_buffer(lines(), iv=b"", pos=0)

    end_position = buffer[-1][0]

    if end_position <= target_size:
        return False  # write to target_path?

    limit_position = end_position - target_size

    buffer = [element for element in buffer if element[0] >= limit_position]
    # replace first line with iv for second line.
    # breaks if buffer is empty...
    buffer[0] = (0, buffer[0][2], buffer[0][2])

    # don't write degenerate .jlap
    if len(buffer) < 2:
        return False

    write_jlap_buffer(target_path, buffer)

    return True


def trim_if_larger(high: int, low: int, jlap: Path):
    """
    Check the size of Path jlap.

    If it is larger than high bytes, rewrite to be smaller than low bytes.

    Return True if modified.
    """
    if jlap.stat().st_size > high:
        print(f"trim {jlap.stat().st_size} \N{RIGHTWARDS ARROW} {low} {jlap}")
        return trim(jlap, low, jlap)
    return False


@click.command()
@click.option(
    "--high",
    required=False,
    help="Trim if larger than size.",
    default=2**20 * 10,
    show_default=True,
)
@click.option(
    "--low",
    required=False,
    help="Maximum size after trim.",
    default=2**20 * 3,
    show_default=True,
)
@click.argument("jlap")
def jlaptrim(high: int, low: int, jlap):
    jlap = Path(jlap).expanduser()
    trim_if_larger(high, low, jlap)


def go():
    logging.basicConfig(
        format="%(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=logging.INFO,
    )
    jlaptrim()  # type: ignore


if __name__ == "__main__":
    go()
