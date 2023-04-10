#! /bin/sh

runner=dbsp
streaming=true
events=100k
language=default
query=all
if ! cores=$(nproc 2>/dev/null) || test $cores -gt 16; then
    cores=16
fi
run=:

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
	    streaming=true
	    ;;
	--batch|-b)
	    streaming=false
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
	--dry-run|-n)
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
  -r, --runner=RUNNER   Use back end RUNNER, one of: dbsp flink beam/direct
                        beam/flink beam/spark beam/dataflow
  -s, --stream          Run stream analytics (default)
  -b, --batch           Run batch analytics
  -e, --events=EVENTS   Run EVENTS events (default: 100k)
  -L, --language=LANG   Use given query LANG: default sql zetasql
  -c, --cores=CORES     Use CORES cores for computation (default: min(16,nproc))
  -q, --query=QUERY     Queries to run (default: all)
  -n, --dry-run         Don't run anything, just print what would have run

The beam/dataflow backend takes more configuration.  These settings are
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
    dbsp | flink | beam/direct | beam/flink | beam/spark) ;;
    beam/dataflow)
	if test -z "$project"; then
	    echo >&2 "$0: beam/dataflow runner requires --project"
	    exit 1
	fi
	case $bucket in
	    *[!-a-z0-9-_.]*)
		echo >&2 "$0: invalid bucket name '$bucket' (don't include gs:// prefix)"
		exit 1
		;;
	    '')
		echo >&2 "$0: beam/dataflow runner requires --bucket"
		exit 1
	esac
	;;
    *) echo >&2 "$0: unknown runner '$runner'"; exit 1 ;;
esac
case $runner:$language in
    *:default | beam/*:sql | beam/*:zetasql) ;;
    *:sql | *:zetasql) echo >&2 "$0: only beam/* support $language" ;;
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

cat <<EOF
Running Nexmark suite with configuration:
  runner: $runner
  streaming: $streaming
  events: $events
  query: $query
  cores: $cores
EOF
case $runner in
    beam/*) echo "  query language: ${language:-(beam)}" ;;
esac

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

    run beam/beam/gradlew -p beam/beam :sdks:java:testing:nexmark:run \
	-Pnexmark.runner=":runners:$runner" -Pnexmark.args="$*"
}
case $runner in
    dbsp)
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
	find_program cargo
	run cargo bench --bench nexmark -- "$@"
	;;

    flink)
	# XXX --stream
	# Each Flink replica has two cores.
	replicas=$(expr \( $cores + 1 \) / 2)
	yml=flink/docker-compose-${cores}core.yml
	sed "s/replicas: .*/replicas: $replicas/" < flink/docker-compose.yml > $yml

	# Query names need 'q' prefix
	if test "$query" != all; then
	    query=q$query
	fi

	DOCKER=$(find_program podman docker)
	DOCKER_COMPOSE=$(find_program podman-compose docker-compose)
	run $DOCKER_COMPOSE -p nexmark -f $yml down -t 0
	run $DOCKER_COMPOSE -p nexmark -f $yml up -d || exit 1
	run $DOCKER exec nexmark_jobmanager_1 run-nexmark.sh --queries "$query" --events $events || exit 1
	run $DOCKER_COMPOSE -p nexmark -f $yml down -t 0 || exit 1
	;;

    beam/direct)
	run_beam_nexmark direct-java \
		    --runner=DirectRunner \
		    --targetParallelism=$cores
	;;

    beam/flink)
	# Flink tends to peak at about 2*parallelism cores according to 'top'.
	parallelism=$(expr \( $cores + 1 \) / 2)
	run_beam_nexmark flink:1.13 \
		    --runner=FlinkRunner \
		    --flinkMaster='[local]' \
		    --parallelism=$parallelism \
		    --maxParallelism=$parallelism
	;;

    beam/spark)
	if test $streaming = true; then
	    echo >&2 "$0: warning: $runner hangs in streaming mode"
	fi
	run_beam_nexmark spark:3 \
		    --runner=SparkRunner \
		    --sparkMaster="local[$cores]"
	;;

    beam/dataflow)
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
