#!/bin/sh

# init
# /data doesn't exist when the Dockerfile runs
if [ ! -e /data/http/index.html ]; then
	mkdir -p /data/http
	ln -sf /data/http /srv/http # didn't work in Dockerfile?
	echo "<p>repodata.json differential experiment</p>" > /data/http/index.html
fi
mkdir -p /data/cacher

# periodic job
cd /data/cacher
while true; do
	python /app/cacher.py
	sleep 300
done
