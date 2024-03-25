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
parse=false

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
    *:default | beam.*:sql | beam.*:zetasql) ;;
    *:sql | *:zetasql) echo >&2 "$0: only beam.* support $language" ;;
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
case $query in
    all | [0-9] | [0-9][0-9]) ;;
    *) echo >&2 "$0: --query must be 'all' or a number"; exit 1 ;;
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
	run "$@" 2>&1 | tee log.txt
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
	    *'│'*)
		save_IFS=$IFS IFS=│; set $line; IFS=$save_IFS
		shift
		case $1:$2 in
		    [qQ]*:*,*) ;;
		    *) continue ;;
		esac
		query=$1 events=$(echo "$2" | sed 's/,//g') cores=$3
		case $4 in
		    *ms) elapsed=$(echo "${4%ms}/1000" | bc -l) ;;
		    *s) elapsed=${4%s} ;;
		    *) continue ;;
		esac
		echo "$csv_common,$query,$cores,$events,$elapsed"
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
    sed 's/[ 	]//g' | while read line; do
	case $line in
	    *'|'*)
		save_IFS=$IFS IFS='|'; set $line; IFS=$save_IFS
		shift
		case $1:$2 in
		    q*:*,*) ;;
		    *) continue ;;
		esac
		query=$1 events=$(echo "$2" | sed 's/,//g') cores=$3 elapsed=$4
		;;
	    q[0-9]*' '[0-9]*' '[0-9]*' '[0-9]*)
		set $line
		query=$1 events=$2 cores=$3 elapsed=$4
		;;
	    *)
		continue
		;;
	esac
	echo "$csv_common,$query,$cores,$events,$elapsed"
    done
}

if $parse; then
    reference='-r /dev/stdin'
else
    reference=
fi
when=$(LC_ALL=C date -u '+%+4Y-%m-%d %H:%M:%S' $reference)
csv_heading=when,runner,mode,language,name,num_cores,num_events,elapsed
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

cat <<EOF
Running Nexmark suite with configuration:
  runner: $runner
  mode: $mode
  language: $language
  events: $events
  query: $query
  cores: $cores
EOF
case $runner in
    feldera)
	rm -f results.csv
	set -- \
	    --first-event-rate=10000000 \
	    --max-events=$events \
	    --cpu-cores $cores \
	    --num-event-generators $cores \
	    --source-buffer-size 10000 \
	    --input-batch-size 40000 \
	    --csv "$PWD/results.csv"
	if test "$query" != all; then
	    set -- "$@" --query=q$query
	fi
	CARGO=$(find_program cargo)
	run_log $CARGO bench --bench nexmark -- "$@"
	;;

    flink)
	# XXX --stream
	# Each Flink replica has two cores.
	replicas=$(expr \( $cores + 1 \) / 2)
	yml=flink/docker-compose-${cores}core.yml
	sed "# Generated automatically -- do not modify!    -*- buffer-read-only: t -*-
s/replicas: .*/replicas: $replicas/" < flink/docker-compose.yml > $yml

	# Query names need 'q' prefix
	if test "$query" != all; then
	    query=q$query
	fi

	DOCKER=$(find_program docker podman)
	run $DOCKER compose -p nexmark -f $yml down -t 0
	run $DOCKER compose -p nexmark -f $yml up -d --build --force-recreate --renew-anon-volumes || exit 1
	run_log $DOCKER exec nexmark-jobmanager-1 run.sh --queries "$query" --events $events || exit 1
	run $DOCKER compose -p nexmark -f $yml down -t 0 || exit 1
	;;

    beam.direct)
	run_beam_nexmark direct-java \
		    --runner=DirectRunner \
		    --targetParallelism=$cores
	;;

    beam.flink)
	# Flink tends to peak at about 2*parallelism cores according to 'top'.
	parallelism=$(expr \( $cores + 1 \) / 2)
	run_beam_nexmark flink:1.13 \
		    --runner=FlinkRunner \
		    --flinkMaster='[local]' \
		    --parallelism=$parallelism \
		    --maxParallelism=$parallelism
	;;

    beam.spark)
	if test $streaming = true; then
	    echo >&2 "$0: warning: $runner hangs in streaming mode"
	fi
	run_beam_nexmark spark:3 \
		    --runner=SparkRunner \
		    --sparkMaster="local[$cores]"
	;;

    beam.dataflow)
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
