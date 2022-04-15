#!/bin/sh

# init
# /data doesn't exist when the Dockerfile runs
if [ ! -e /data/http/index.html ]; then
	mkdir -p /data/http
	ln -sf /data/http /srv/http # didn't work in Dockerfile?
	/app/update-homepage.sh
fi

mkdir -p /data/cacher

# domain names tend to have 3 dots
ln -s /data/cacher/*.*.* /data/http

# periodic job
cd /data/cacher
while true; do
	# todo time limit if it hangs
	python /app/cacher.py
	/opt/py39/bin/pypy3 /app/patchfromhg.py
	/app/update-homepage.sh
	sleep 300
done
