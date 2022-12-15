#!/bin/sh
cd app
# pypy package 'zipapps' makes self-contained file
python -m zipapps -p /usr/bin/python3 -c -m repodata_proxy:go -a repodata_proxy.py,no_cache.py,sync_jlap.py,truncateable.py,update_conda_cache.py -r ../requirements.txt -o ../repodata.pyz
chmod +x ../repodata.pyz

# standalone json-to-jlap
python -m zipapps -p /usr/bin/python3 -c -m json2jlap:go -a json2jlap.py,truncateable.py,jlapcore.py,jlaptrim.py -r ../requirements-json2jlap.txt -o ../json2jlap.pyz
chmod +x ../json2jlap.pyz