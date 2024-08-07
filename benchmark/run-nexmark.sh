#! /bin/bash

set -o pipefail

runner=feldera
mode=stream
events=100k
language=default
output=nexmark.csv
query=all
if ! cores=$(nproc 2>/dev/null) || test $cores -gt 16; then
    cores=16
fi
run=:
storage=false
parse=false
skip_counting_events=false

# Feldera SQL options.
kafka_broker=localhost:9092
kafka_from_feldera=
api_url=http://localhost:8080
partitions=1

# Dataflow options.
project=
bucket=
region=us-west1
core_quota=24

nextarg=
for arg
do
    if test -n "$nextarg"; then
	eval $nextarg'=$arg'
	nextarg=
	continue
    fi
    case $arg in
	--runner=*)
	    runner=${arg#--runner=}
	    ;;
	--runner|-r)
	    nextarg=runner
	    ;;
	--stream|--streaming|-s)
	    mode=stream
	    ;;
	--batch|-b)
	    mode=batch
	    ;;
	--mode=*)
	    mode=${arg#--mode=}
	    ;;
	--mode|-e)
	    nextarg=mode
	    ;;
	--events=*)
	    events=${arg#--events=}
	    ;;
	--events|-e)
	    nextarg=events
	    ;;
	--language=*)
	    language=${arg#--language=}
	    ;;
	--language|-L)
	    nextarg=language
	    ;;
	--query=*)
	    query=${arg#--query=}
	    ;;
	--query|-q)
	    nextarg=query
	    ;;
	--cores|-c)
	    nextarg=cores
	    ;;
	--cores=*)
	    cores=${arg#--cores=}
	    ;;
	--output|-o)
	    nextarg=output
	    ;;
	--output=*)
	    output=${arg#--output=}
	    ;;
	--dry-run|-n)
	    run=false
	    ;;
	--parse)
	    parse=:
	    run=false
	    ;;
	--kafka-broker=*)
	    kafka_broker=${arg#--kafka-broker=}
	    ;;
	--kafka-broker)
	    nextarg=kafka_broker
	    ;;
	--kafka-from-feldera=*)
	    kafka_from_feldera=${arg#--kafka-from-feldera=}
	    ;;
	--kafka-from-feldera)
	    nextarg=kafka_from_feldera
	    ;;
	--skip-counting-events)
	    skip_counting_events=:
	    ;;
	--api-url=*)
	    api_url=${arg#--api-url=}
	    ;;
	--api-url)
	    nextarg=api_url
	    ;;
	--partitions=*)
	    partitions=${arg#--partitions=}
	    ;;
	--partitions)
	    nextarg=partitions
	    ;;
	--storage)
	    storage=:
	    ;;
	--project=*)
	    project=${arg#--project=}
	    ;;
	--project)
	    nextarg=project
	    ;;
	--bucket=*)
	    bucket=${arg#--bucket=}
	    ;;
	--bucket)
	    nextarg=bucket
	    ;;
	--region=*)
	    region=${arg#--region=}
	    ;;
	--region)
	    nextarg=region
	    ;;
	--core-quota=*)
	    core_quota=${arg#--core-quota=}
	    ;;
	--core-quota)
	    nextarg=core_quota
	    ;;
	--help)
	    cat <<EOF
run-nexmark, for running the Nexmark benchmark with various backends
Usage: $0 [OPTIONS]

The following options are supported:
  -r, --runner=RUNNER   Use back end RUNNER, one of: feldera flink beam.direct
                        beam.flink beam.spark beam.dataflow
  -s, --stream          Run stream analytics (default)
  -b, --batch           Run batch analytics
  -e, --events=EVENTS   Run EVENTS events (default: 100k)
  -L, --language=LANG   Use given query LANG: default sql zetasql
  -c, --cores=CORES     Use CORES cores for computation (default: min(16,nproc))
  -q, --query=QUERY     Queries to run (default: all)
  -o, --output=OUTPUT   Append CSV-formatted output to OUTPUT (default: nexmark.csv).

By default, run-nexmark runs the tests and appends parsed results to
nexmark.txt.  These options select other modes of operation:
  -n, --dry-run         Don't run anything, just print what would have run
  --parse < LOG         Parse log output from stdin, print results to stdout.

The feldera backend with --language=sql takes the following additional options:
  --kafka-broker=BROKER  Kafka broker (default: $kafka_broker)
  --kafka-from-feldera=BROKER  Kafka broker as accessed from Feldera (defaults
                               to the same as --kafka-broker)
  --api-url=URL         URL to the Feldera API (default: $api_url)
  --partitions=N        number of Kafka partitions (default: 1)

The beam.dataflow backend takes more configuration.  These settings are
required:
  --project=PROJECT     Project name
  --bucket=BUCKET       Bucket name
These Dataflow settings are optional:
  --region=REGION       GCP region to use (default: us-west1)
  --core-quota=CORES    GCP core quota for running tests in parallel (default: 24)

  --help                Print this help message and exit
EOF
	    exit 0
	    ;;
	*)
	    echo >&2 "$0: unknown option '$arg' (use --help for help)"
	    exit 1
	    ;;
    esac
done
if test -n "$nextarg"; then
    echo >&2 "$0: missing argument to '$nextarg'"
    exit 1
fi

case $runner in
    dbsp) runner=feldera ;;
    feldera | flink | beam.direct | beam.flink | beam.spark) ;;
    beam.dataflow)
	if ! $parse; then
	    if test -z "$project"; then
		echo >&2 "$0: beam.dataflow runner requires --project"
		exit 1
	    fi
	    case $bucket in
		*[!-a-z0-9-_.]*)
		    echo >&2 "$0: invalid bucket name '$bucket' (don't include gs:// prefix)"
		    exit 1
		    ;;
		'')
		    echo >&2 "$0: beam.dataflow runner requires --bucket"
		    exit 1
	    esac
	fi
	;;
    *) echo >&2 "$0: unknown runner '$runner'"; exit 1 ;;
esac
case $runner:$language in
    *:default | feldera:sql | beam.*:sql | beam.*:zetasql) ;;
    *:sql) echo >&2 "$0: only beam.* and feldera support $language" ;;
    *:zetasql) echo >&2 "$0: only beam.* support $language" ;;
    *) echo >&2 "$0: unknown query language '$language'"; exit 1 ;;
esac
case $events in
    [1-9]*k) events=${events%k}000 ;;
    [1-9]*M) events=${events%M}000000 ;;
    [1-9]*G) events=${events%G}000000000 ;;
    *[!0-9]*)
	echo >&2 "$0: --events must be a number with optional k, M, or G suffix"
	exit 1
	;;
esac
query=${query#q}
case $query in
    all | [0-9] | [0-9][0-9]) ;;
    *) echo >&2 "$0: --query must be 'all' or a number (with an optional 'q' prefix)"; exit 1 ;;
esac
case $mode in
    stream | streaming) mode=stream streaming=true ;;
    batch) streaming=false ;;
    *) echo >&2 "$0: --mode must be 'stream' or 'batch'"; exit 1 ;;
esac

find_program() {
    for program
    do
	if ($program --version >/dev/null 2>&1); then
	    echo "$program"
	    return
	fi
    done
    case $# in
	1) echo >&2 "$0: '$1' is not in \$PATH" ;;
	*) echo >&2 "$0: none of '$*' is in \$PATH" ;;
    esac
    exit 1
}
run() {
    # Print the command in a cut-and-pastable form
    space=
    for word
    do
	case $word in
	    *[!-a-zA-Z0-9=:/.@_]*) printf "$space'%s'" "$(echo "$word" | sed "s,','\\\\'',")" ;;
	    *) printf "$space%s" "$word" ;;
	esac
	space=' '
    done
    echo

    if $run; then
	"$@"
    else
	return 0
    fi
}

run_log() {
    if $run; then
	run "$@" 2>&1 | tee -a log.txt
    else
	run "$@"
    fi
}

run_beam_nexmark() {
    local runner=$1; shift

    if test "$language" != beam; then
	set -- --queryLanguage=$language "$@"
    fi
    if test "$query" != all; then
	set -- --query=$query "$@"
    fi

    set -- \
	--streaming=$streaming \
	--suite=SMOKE \
	--numEvents=$events \
	--manageResources=false \
	--monitorJobs=true \
	--enforceEncodability=true \
	--enforceImmutability=true \
	"$@"

    run_log beam/beam/gradlew -p beam/beam :sdks:java:testing:nexmark:run \
	    -Pnexmark.runner=":runners:$runner" -Pnexmark.args="$*"
}

beam2csv() {
    while read number desc eps results; do
	case $number in
	    00[0-9][0-9]) ;;
	    *) continue ;;
	esac

	query=
	case ${desc%;} in
	    query:PASSTHROUGH) query=q0 ;;
	    query:CURRENCY_CONVERSION) query=q1 ;;
	    query:SELECTION) query=q2 ;;
	    query:LOCAL_ITEM_SUGGESTION) query=q3 ;;
	    query:AVERAGE_PRICE_FOR_CATEGORY) query=q4 ;;
	    query:HOT_ITEMS) query=q5 ;;
	    query:AVERAGE_SELLING_PRICE_BY_SELLER) query=q6 ;;
	    query:HIGHEST_BID) query=q7 ;;
	    query:MONITOR_NEW_USERS) query=q8 ;;
	    query:WINNING_BIDS) query=q9 ;;
	    query:LOG_TO_SHARDED_FILES) query=q10 ;;
	    query:USER_SESSIONS) query=q11 ;;
	    query:PROCESSING_TIME_WINDOWS) query=q12 ;;
	    query:PORTABILITY_BATCH) query=q15 ;;
	    query:RESHUFFLE) query=q16 ;;
	    query:BOUNDED_SIDE_INPUT_JOIN) query=q13 ;;
	    query:SESSION_SIDE_INPUT_JOIN) query=q14 ;;
	    query:*) echo >&2 "unknown query: $desc"; continue ;;
	    '*'*) continue ;;
	esac
	if test -n "$query"; then
	    eval conf$number=$query
	    continue
	fi
	eval query='$'conf$number
	echo "$csv_common,$query,$cores,$events,$desc"
    done | sort | uniq
}

feldera2csv() {
    sed 's/[ 	]//g' | tr Q q | while read line; do
	case $line in
	    *,feldera,stream,sql,*)
		echo "$line"
		;;
	    *'│'*)
		save_IFS=$IFS IFS=│; set $line; IFS=$save_IFS
		shift
		case $1:$2 in
		    [qQ]*:*,*) ;;
		    *) continue ;;
		esac
		parse_time() {
		    case $1 in
			*ms) echo "${1%ms}/1000" | bc -l ;;
			*s) echo "${1%s}" ;;
		    esac
		}
		parse_mem() {
		    case $1 in
			*EiB) expr="${1%EiB} * 1024 * 1024 * 1024 * 1024 * 1024 * 1024" ;;
			*PiB) expr="${1%PiB} * 1024 * 1024 * 1024 * 1024 * 1024" ;;
			*TiB) expr="${1%TiB} * 1024 * 1024 * 1024 * 1024" ;;
			*GiB) expr="${1%GiB} * 1024 * 1024 * 1024" ;;
			*MiB) expr="${1%MiB} * 1024 * 1024" ;;
			*KiB) expr="${1%KiB} * 1024" ;;
			*B) expr="${1%B}" ;;
			*) return 1 ;;
		    esac
		    echo "$expr" | bc -l | sed 's/\..*//'
		}
		query=$1 events=$(echo "$2" | sed 's/,//g') cores=$3 elapsed=$(parse_time "$4")
		user_time=$(parse_time $7) system_time=$(parse_time $8)
		cpu_time=$(echo "$user_time + $system_time" | bc -l)
		mem=$(parse_mem $9)
		echo "$csv_common,$query,$cores,$events,$elapsed,$mem,$cpu_time"
		;;
	    q[0-9]*,[0-9]*,[0-9]*,[0-9]*)
		save_IFS=$IFS IFS=,; set $line; IFS=$save_IFS
		query=$1 cores=$2 events=$3 elapsed=$4
		echo "$csv_common,$query,$cores,$events,$elapsed"
		;;
	esac
    done
}

# Parses this format:
#
# +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
# | Nexmark Query     | Events Num        | Cores             | Time(s)           | Cores * Time(s)   | Throughput/Cores  |
# +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
# |q0                 |100,000,000        |2.14               |144.910            |309.639            |322.96 K/s         |
# |q1                 |100,000,000        |1.41               |202.768            |286.167            |349.45 K/s         |
# ...
# |Total              |2,100,000,000      |140.145            |4675.789           |36773.486          |2.96 M/s           |
# +-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
flink2csv() {
    elapsed=
    memory=unknown
    print_if_saved() {
	if test -n "$elapsed"; then
	    cpu_msec=$(echo "$cpu_sec * 1000" | bc -l | sed 's/\..*//')
	    echo "$csv_common,$query,$cores,$events,$elapsed,$memory,$cpu_msec"
	    elapsed=
	    memory=unknown
	fi
    }
    sed 's/[ 	]//g' | while read line; do
	case $line in
	    *'|'*)
		save_IFS=$IFS IFS='|'; set $line; IFS=$save_IFS
		shift
		case $1:$2 in
		    q*:*,*) ;;
		    *) continue ;;
		esac
		print_if_saved
		query=$1 events=$(echo "$2" | sed 's/,//g') elapsed=$4 cpu_sec=$5
		;;
	    q[0-9]*' '[0-9]*' '[0-9]*' '[0-9]*)
		print_if_saved
		set $line
		query=$1 events=$2 elapsed=$4 cpu_sec=$5
		;;
	    'totalmemory:'*)
		kib=$(echo "$line" | sed 's/totalmemory:\([0-9]*\)KiB$/\1/')
		memory=$(echo "$kib * 1024" | bc)
		print_if_saved
		;;
	    *)
		continue
		;;
	esac
    done
    print_if_saved
}

if $parse; then
    reference='-r /dev/stdin'
else
    reference=
fi
when=$(LC_ALL=C date -u '+%+4Y-%m-%d %H:%M:%S' $reference)
csv_heading=when,runner,mode,language,name,num_cores,num_events,elapsed,peak_memory_bytes,cpu_seconds
parse() {
    csv_common=$when,$runner,$mode,$language
    case $runner in
	feldera) feldera2csv ;;
	flink) flink2csv ;;
	beam.*) beam2csv ;;
	*) echo >&2 "unknown runner $runner"; exit 1 ;;
    esac
}
if $parse; then
    parse
    exit $?
fi

rm -f log.txt

cat <<EOF
Running Nexmark suite with configuration:
  runner: $runner
  mode: $mode
  language: $language
  events: $events
  query: $query
  cores: $cores
EOF
case $runner:$language in
    feldera:default)
	if stat -f ${TMPDIR:-/tmp} 2>&1 | grep -q 'Type: tmpfs'; then
	    echo >&2 "$0: warning: temporary files will be stored on tmpfs in ${TMPDIR:-/tmp}, suggest setting \$TMPDIR to point to a physical file system"
	fi
	CARGO=$(find_program cargo)
	case $query in
	    all) queries="q0 q1 q2 q3 q4 q5 q6 q7 q8 q9 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22" ;;
	    *) queries=q$query ;;
	esac
	for query in $queries; do
	    run_log $CARGO bench --bench nexmark -- \
		--first-event-rate=10000000 \
		--max-events=$events \
		--cpu-cores $cores \
		--num-event-generators $cores \
		--source-buffer-size 10000 \
		--input-batch-size 40000 \
		--query $query
	done
	;;

    feldera:sql)
	RPK=$(find_program rpk)
	CARGO=$(find_program cargo)
	topics="auction-$partitions-$events bid-$partitions-$events person-$partitions-$events"
	echo >&2 "$0: checking for already generated events in '$topics'..."
	count_events() {
	    for topic in $topics; do
		for p in $(seq 0 $(expr $partitions - 1)); do
		    $RPK -X brokers="$kafka_broker" topic consume -p $p -f '%v' -o :end $topic
		done
	    done | wc -l
	}
	count_partitions() {
	    for topic in $topics; do
		$RPK -X brokers="$kafka_broker" topic describe $topic 2>/dev/null | awk '{if ($1 == "PARTITIONS") {printf("%d,", $2);}}'
	    done
	}
	check_topics() {
	    n_partitions=$(count_partitions | sed 's/,$//')
	    expect_partitions="$partitions,$partitions,$partitions"
	    if test "$n_partitions" != "$expect_partitions"; then
		msg="topics '$topics' contained '$n_partitions' partitions instead of '$expect_partitions'"
		return 1
	    fi

	    if $skip_counting_events; then
		echo >&2 "$0: assuming '$topics' contained $events events (because of --skip-counting-events)"
		return 0
	    fi
	    n_events=$(count_events)
	    if test "$n_events" != "$events"; then
		msg="topics '$topics' contained $n_events events instead of $events"
		return 1
	    fi

	    return 0
	}
	if ! check_topics; then
	    echo >&2 "$0: $msg.  (Re)creating..."
	    run_log $RPK topic -X brokers=$kafka_broker delete $topics
	    run_log $RPK topic -X brokers=$kafka_broker create --partitions $partitions $topics
	    run_log $CARGO run -p dbsp_nexmark --example generate --features with-kafka -- --max-events $events --topic-suffix=-$partitions-$events -O bootstrap.servers=$kafka_broker || exit 1

	    if ! check_topics; then
		echo >&2 "$0: generation failed ($msg)."
		exit 1
	    fi
	fi
	rm -f results.csv
	CARGO=$(find_program cargo)
	run_log feldera-sql/run.py \
	    --api-url="$api_url" \
	    -O bootstrap.servers="${kafka_from_feldera:-${kafka_broker}}" \
	    --cores $cores \
	    --poller-threads 10 \
	    --input-topic-suffix="-$partitions-$events" \
	    --csv results.csv \
	    $(if $storage; then printf "%s" --storage; fi) \
	    --query $(if test $query = all; then echo all; else echo q$query; fi)
	;;

    flink:*)
	# XXX --stream
	# Each Flink replica has two cores.
	replicas=$(expr \( $cores + 1 \) / 2)
	yml=flink/docker-compose-${cores}core.yml
	sed "# Generated automatically -- do not modify!    -*- buffer-read-only: t -*-
s/replicas: .*/replicas: $replicas/" < flink/docker-compose.yml > $yml

	if test "$events" -lt 10000000; then
	    echo >&2 "warning: This benchmark will probably fail to run properly because the Flink container does not handle metrics properly for tests that run less than 10 seconds.  Consider increasing the number of events from $events to at least 10M."
	fi

	case $query in
	    all) queries="q0 q1 q2 q3 q4 q5 q7 q8 q9 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22" ;;
	    *) queries=q$query ;;
	esac
	for query in $queries; do
	    DOCKER=$(find_program docker podman)
	    run $DOCKER compose -p nexmark -f $yml down -t 0
	    run $DOCKER compose -p nexmark -f $yml up -d --build --force-recreate --renew-anon-volumes || exit 1
	    run_log $DOCKER exec nexmark-jobmanager-1 run.sh --queries "$query" --events $events || exit 1
	    mem=0
	    for replica in $(seq $replicas); do
		# Get max memory
		container=nexmark-taskmanager-$replica
		container_mem=$(docker logs $container 2>&1 | sed -n 's/^mem: \([0-9]*\)/\1/p' | sort -rn | head -1)
		echo "$container memory: $container_mem KiB" >> log.txt
		mem=$(expr $mem + $container_mem)
	    done
	    echo "total memory: $mem KiB" >> log.txt
	    run $DOCKER compose -p nexmark -f $yml down -t 0 || exit 1
	done
	;;

    beam.direct:*)
	run_beam_nexmark direct-java \
		    --runner=DirectRunner \
		    --targetParallelism=$cores
	;;

    beam.flink:*)
	# Flink tends to peak at about 2*parallelism cores according to 'top'.
	parallelism=$(expr \( $cores + 1 \) / 2)
	run_beam_nexmark flink:1.13 \
		    --runner=FlinkRunner \
		    --flinkMaster='[local]' \
		    --parallelism=$parallelism \
		    --maxParallelism=$parallelism
	;;

    beam.spark:*)
	if test $streaming = true; then
	    echo >&2 "$0: warning: $runner hangs in streaming mode"
	fi
	run_beam_nexmark spark:3 \
		    --runner=SparkRunner \
		    --sparkMaster="local[$cores]"
	;;

    beam.dataflow:*)
	region=us-west1
	cores_per_worker=4	# For the default Dataflow worker machine type
	n_workers=$(expr $cores / $cores_per_worker)
	test "$n_workers" = 0 && n_workers=1
	cores_per_test=$(expr $n_workers '*' $cores_per_worker)
	parallel=$(expr $core_quota / $cores_per_test)
	if test "$parallel" = 0; then
	    echo >&2 "$0: can't run tests with $core_quota cores because $cores_per_worker cores per worker and $n_workers workers requires at least $cores_per_test cores; reduce number of cores or request a quota increase from Google"
	    exit 1
	fi
	run_beam_nexmark google-cloud-dataflow-java \
	    --runner=DataflowRunner \
	    --project=$project \
	    --resourceNameMode=QUERY_RUNNER_AND_MODE \
	    --tempLocation=gs://$bucket/nexmark \
	    --exportSummaryToInfluxDB=false \
	    --exportSummaryToBigQuery=false \
	    --region=$region \
	    --numWorkers=$n_workers \
	    --maxNumWorkers=$n_workers \
	    --autoscalingAlgorithm=NONE \
	    --nexmarkParallel=$parallel
	;;

    *)
	echo >&2 "unknown runner $runner"
	exit 1
	;;
esac

if $run; then
    if test ! -e $output; then
	echo "$csv_heading" >> $output
    fi
    parse < log.txt >> $output
else
    exit 0
fi
