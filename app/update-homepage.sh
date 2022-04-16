#!/bin/sh
cd /data/http
python -c "import glob, os; print('<p>repodata.json differential experiment</p><table>', '\n'.join(f'<tr><td>{os.stat(patch).st_size}</td><td><a href=\"{patch}\">{patch}</a></td></tr>' for patch in glob.glob('**/*repodata.jlap', recursive=True))), '</table>'" > index.html
