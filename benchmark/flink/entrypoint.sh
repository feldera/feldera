#! /bin/sh

# Tolerate bug where flink docker container expects config.yaml in the
# root (see https://issues.apache.org/jira/browse/FLINK-34725).
test -e /config.yaml || ln -s /opt/flink/conf/config.yaml /config.yaml

case "$1" in
    taskmanager)
	echo "Starting Flink and waiting for it to set up"
	/docker-entrypoint.sh "$@" &
	sleep 5
	echo "Generating input"
	/opt/nexmark/bin/side_input_gen.sh
	echo "Starting metric client"
	/opt/nexmark/bin/metric_client.sh start
	wait
	;;
    *)
	exec /docker-entrypoint.sh "$@"
	;;
esac

