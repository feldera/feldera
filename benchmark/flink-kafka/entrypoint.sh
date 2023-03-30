#! /bin/sh
case "$1" in
    taskmanager)
	echo "Starting Flink $1 and waiting for it to set up"
	/docker-entrypoint.sh "$@" &
	sleep 5
	echo "Generating input"
	/opt/nexmark/bin/side_input_gen.sh
	echo "Starting metric client"
	/opt/nexmark/bin/metric_client.sh start
	wait
	;;
    jobmanager)
	echo "Starting Flink $1 and waiting for it to set up"
	/docker-entrypoint.sh "$@" &

	# The following works for queries, but Kafka just used more
	# and more storage until it filled up my 1.1 TB of free space
	# after a few minutes.
	/opt/kafka/bin/kafka-topics.sh --create --topic nexmark --bootstrap-server broker:29092 --partitions 8

	# The following makes Kafka purge old data quickly, but it
	# just deleted everything after a few minutes for me, which
	# made all the tests fail.
	#/opt/kafka/bin/kafka-topics.sh --create --topic nexmark --bootstrap-server broker:29092 --partitions 8 --config retention.ms=60000 --config delete.retention.ms=0 --config file.delete.delay.ms=0 --config retention.bytes=100000000 --config segment.bytes=10000000

	while sleep 1; do :; done
	;;
    *)
	exec /docker-entrypoint.sh "$@"
	;;
esac

