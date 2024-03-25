#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

FLINK_HOME=/opt/flink
export FLINK_HOME

NEXMARK_HOME=/opt/nexmark
NEXMARK_LIB_DIR=$NEXMARK_HOME/lib
NEXMARK_QUERY_DIR=$NEXMARK_HOME/queries
NEXMARK_LOG_DIR=$NEXMARK_HOME/log
NEXMARK_CONF_DIR=$NEXMARK_HOME/conf
NEXMARK_BIN_DIR=$NEXMARK_HOME/bin

### Exported environment variables ###
export NEXMARK_HOME
export NEXMARK_LIB_DIR
export NEXMARK_QUERY_DIR
export NEXMARK_LOG_DIR
export NEXMARK_CONF_DIR
export NEXMARK_BIN_DIR

events=100000000
queries=all

nextarg=
for arg
do
    if test -n "$nextarg"; then
	eval $nextarg'=$arg'
	nextarg=
	continue
    fi
    case $arg in
	--events=*)
	    events=${arg#--events=}
	    ;;
	--events|-e)
	    nextarg=events
	    ;;
	--queries=*)
	    queries=${arg#--queries=}
	    ;;
	--queries|-q)
	    nextarg=queries
	    ;;
	--help)
	    cat <<EOF
run-nexmark, for running the Nexmark benchmark with Flink.
Usage: $0 [OPTIONS]

The following options are supported:
  -e, --events=EVENTS   Run EVENTS events (default: 100000000)
  -q, --queries=QUERIES Queries to run, e.g. "q1,q3" (default: all)
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

cat > /opt/nexmark/conf/nexmark.yaml <<EOF
nexmark.metric.reporter.host: nexmark-jobmanager-1
nexmark.metric.reporter.port: 9098

nexmark.workload.suite.100m.events.num: $events
nexmark.workload.suite.100m.tps: 10000000
nexmark.workload.suite.100m.queries: "q0,q1,q2,q3,q4,q5,q7,q8,q9,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22"
nexmark.workload.suite.100m.warmup.duration: 120s
nexmark.workload.suite.100m.warmup.events.num: 100000000
nexmark.workload.suite.100m.warmup.tps: 10000000

flink.rest.address: localhost
flink.rest.port: 8081
EOF

exec java -cp "/opt/flink/lib/*:/opt/nexmark/lib/*" com.github.nexmark.flink.Benchmark --location /opt/nexmark --category oa --queries "$queries"
