#! /bin/sh

runner=direct
streaming=true
suite=SMOKE
language=beam
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
	--suite=*)
	    suite=${arg#--suite=}
	    ;;
	--suite|-S)
	    nextarg=suite
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
  -r, --runner=RUNNER   Use back end RUNNER, one of: direct flink spark dataflow
  -s, --stream          Run stream analytics (default)
  -b, --batch           Run batch analytics
  -s, --suite=SUITE     Run given SUITE: SMOKE (100k events) or STRESS (10M)
  -L, --language=LANG   Use given query LANG: beam (default) sql zetasql
  -c, --cores=CORES     Use CORES cores for computation (default: min(16,nproc))
  -q, --query=QUERY     Queries to run (default: all)
  -n, --dry-run         Don't run anything, just print what would have run

The Dataflow backend takes more configuration.  These settings are
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
    direct | flink | spark) ;;
    dataflow)
	if test -z "$project"; then
	    echo >&2 "$0: Dataflow runner requires --project"
	    exit 1
	fi
	case $bucket in
	    *[!-a-z0-9-_.]*)
		echo >&2 "$0: invalid bucket name '$bucket' (don't include gs:// prefix)"
		exit 1
		;;
	    '')
		echo >&2 "$0: Dataflow runner requires --bucket"
		exit 1
	esac
	;;
    *) echo >&2 "$0: unknown runner '$runner'"; exit 1 ;;
esac
case $suite in
    SMOKE) num_events=100000 ;;
    STRESS) num_events=10000000 ;;
    *) echo >&2 "$0: unknown suite '$suite'"; exit 1 ;;
esac
case $language in
    beam | sql | zetasql) ;;
    *) echo >&2 "$0: unknown query language '$language'"; exit 1 ;;
esac

cat <<EOF
Running Beam Nexmark suite with configuration:
  runner: $runner
  streaming: $streaming
  suite: $suite ($num_events events)
  query language: ${language:-(beam)}
  query: $query
  cores: $cores
EOF

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

run_nexmark() {
    runner=$1; shift
    run beam/gradlew -p beam :sdks:java:testing:nexmark:run \
	-Pnexmark.runner=":runners:$runner" -Pnexmark.args="$*"
}
set -- \
    --streaming=$streaming \
    --suite=$suite \
    --manageResources=false \
    --monitorJobs=true \
    --enforceEncodability=true \
    --enforceImmutability=true

# By default, the SMOKE and STRESS suites for Nexmark, which are the
# suites we care about, run one-tenth as many events for tests 4, 6,
# and 9 as in the other tests, e.g. for SMOKE they run 10,000 events
# instead of 100,000.  That's confusing, so let's override the number
# of events so that they all run the same number.
#
# There's a more subtle implication here.  With --query, to run only
# one specific query, but without --numEvents, the query will run with
# (for SMOKE) 100,000 events and then again with 10,000 events.
# That's because Nexmark (for SMOKE) always generates a test
# configuration for every test, some of those with 10,000 events and
# some with 100,000, and then --query changes all of them to be for a
# particular query, and then Nexmark internally does a "distinct"
# operation and ends up with two different configurations.  Thus,
# overriding the number of events fixes the problem.
set -- "$@" --numEvents=$num_events

if test "$language" != beam; then
    set -- "$@" --queryLanguage=$language
fi
if test "$query" != all; then
    set -- "$@" --query=$query
fi
case $runner in
    direct)
	run_nexmark direct-java "$@" \
		    --runner=DirectRunner \
		    --targetParallelism=$cores
	;;

    flink)
	# Flink tends to peak at about 2*parallelism cores according to 'top'.
	parallelism=$(expr $cores / 2)
	case $parallelism in
	    [1-9] | [1-9][0-9] | [1-9][0-9][0-9]) ;;
	    *) parallelism=1 ;;
	esac
	run_nexmark flink:1.13 "$@" \
		    --runner=FlinkRunner \
		    --flinkMaster='[local]' \
		    --parallelism=$parallelism \
		    --maxParallelism=$parallelism
	;;

    spark)
	run_nexmark spark:3 "$@" \
		    --runner=SparkRunner \
		    --sparkMaster="local[$cores]"
	;;

    dataflow)
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
	run_nexmark google-cloud-dataflow-java "$@" \
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
