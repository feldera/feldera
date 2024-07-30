#!/bin/bash

cd "$(dirname "$0")"

maxmem=0
while true
do
	procs=$(ps -e -o comm,rss | grep java)
	nums=${procs//java/}
	mem=$(echo "$nums" | awk '{sum += $1} END {print sum}')
	mem=$((mem + 0))
	if [ "$mem" -gt "$maxmem" ]; then
		maxmem=$mem
	fi
	echo $maxmem > maxmem
	sleep 5s
done