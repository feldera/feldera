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

exec java -cp "/opt/flink/lib/*:/opt/nexmark/lib/*" com.github.nexmark.flink.Benchmark --location /opt/nexmark --queries "${1:-all}" --categories oa
