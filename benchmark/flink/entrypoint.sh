#! /bin/sh

# Tolerate bug where flink docker container expects config.yaml in the
# root (see https://issues.apache.org/jira/browse/FLINK-34725).
test -e /config.yaml || ln -s /opt/flink/conf/config.yaml /config.yaml

case "$1" in
    taskmanager)
	echo "Starting Flink and waiting for it to set up"
	/docker-entrypoint.sh "$@" & pid=$!
	sleep 5
	echo "Generating input"
	/opt/nexmark/bin/side_input_gen.sh
	echo "Starting metric client"
	/opt/nexmark/bin/metric_client.sh start
	echo "Starting memory monitoring"
	while sleep 1; do
	    # We have two Java processes running: Flink and the
	    # metrics client.  We should ignore memory from the
	    # metrics client because that's just for instrumentation.
	    # So, get the RSS for all java processes and then drop all
	    # but the maximum, since Flinnk uses more memory by far.
	    mem=$(ps -C java -o rss= | sort -rn | head -1)
	    echo "mem: $mem"
	done & mempid=$!
	wait $pid
	kill $mempid
	wait
	;;
    *)
	exec /docker-entrypoint.sh "$@"
	;;
esac

