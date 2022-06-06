#!/usr/bin/env python3
import glob
import os
import pathlib

os.chdir("/data/http")

pathlib.Path("index.html").write_text(
    "\n".join(
        (
            """
<html>
<head>
<title>repodata.json experiment</title>
<style>
td:first-child { text-align: end }
</style>
</head>
<body>
<p>repodata.json differential experiment</p>
<p>Source code at <a href="https://github.com/dholth/repodata-fly/">github.com/dholth/repodata-fly/</a>
<table>""",
            "\n".join(
                f'<tr><td>{os.stat(patch).st_size:,}</td><td><a href="{patch}">{patch}</a></td></tr>'
                for patch in glob.glob("**/*repodata.jlap", recursive=True)
            ),
            """</table></body></html>""",
        )
    )
)
