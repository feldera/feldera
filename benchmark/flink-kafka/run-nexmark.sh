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

cat > /opt/nexmark/conf/nexmark.yaml <<EOF
nexmark.metric.monitor.delay: 0sec
nexmark.workload.suite.10m.warmup.events.num: 0
nexmark.workload.suite.10m.warmup.duration: 0sec
nexmark.workload.suite.10m.tps: 10000000
nexmark.workload.suite.10m.events.num: 10000000
nexmark.workload.suite.10m.queries: "q0,q1,q2,q3,q4,q5,q7,q8,q9,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22"
nexmark.workload.suite.datagen.tps: 10000000
nexmark.workload.suite.datagen.warmup.events.num: 0
nexmark.workload.suite.datagen.warmup.duration: 0sec
nexmark.workload.suite.datagen.events.num: 10000000
nexmark.workload.suite.datagen.queries: "insert_kafka"
nexmark.workload.suite.datagen.queries.cep: "insert_kafka"
flink.rest.address: localhost
flink.rest.port: 8081
kafka.bootstrap.servers: broker:29092
EOF

exec java -cp "/opt/flink/lib/*:/opt/nexmark/lib/*" com.github.nexmark.flink.Benchmark --location /opt/nexmark --queries "${1:-all}" --categories oa
