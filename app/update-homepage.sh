#!/bin/sh
cd /data/http
python -c "import glob; print('<p>repodata.json differential experiment</p>', '<br>\n'.join(f'<a href=\"{patch}\">{patch}</a>' for patch in glob.glob('**/*-patch.json', recursive=True)))" > index.html
