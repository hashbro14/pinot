#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Local 4-container Pinot cluster from the locally built distribution. Remote segments are read from
# localtest/deepstore (file://, mounted at /deepstore in the server) with per-GET latency injected by
# server.conf, or from the REAL S3 deep store when the table config points there and AWS creds exist.
# Usage: [SERVER_CONF=server-baseline.conf] run-local-cluster.sh up|down|status
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
DIST=$(ls -d "$DIR"/../pinot-distribution/target/apache-pinot-*-bin/apache-pinot-*-bin 2>/dev/null | head -1)
# Docker Desktop needs a canonical host path for bind mounts (no "..")
[ -n "$DIST" ] && DIST=$(cd "$DIST" && pwd -P)
NET=pinot-local
IMG=eclipse-temurin:21-jre

case "${1:-up}" in
  down)
    docker rm -f lp-zk lp-controller lp-broker lp-server 2>/dev/null || true
    docker network rm $NET 2>/dev/null || true
    echo "local cluster removed"; exit 0;;
  status)
    docker ps --filter name=lp- --format '{{.Names}} {{.Status}}'; exit 0;;
esac

[ -d "$DIST" ] || { echo "distribution not found — build with -Pbin-dist first"; exit 1; }
echo "using dist: $DIST"

# AWS creds are optional: the file:// deep store under localtest/deepstore needs none
aws configure export-credentials --format env-no-export > "$DIR/aws-env.list" 2>/dev/null || : > "$DIR/aws-env.list"

docker network create $NET 2>/dev/null || true
docker rm -f lp-zk lp-controller lp-broker lp-server 2>/dev/null || true

docker run -d --name lp-zk --network $NET --platform linux/arm64 \
  zookeeper:3.9.3 > /dev/null

sleep 5

docker run -d --name lp-controller --network $NET --platform linux/arm64 \
  --env-file "$DIR/aws-env.list" -e AWS_REGION=eu-west-1 \
  -e JAVA_OPTS="-Xms256M -Xmx1G" \
  -v "$DIST:/opt/pinot" -v "$DIR:/conf" -p 9001:9000 $IMG \
  /opt/pinot/bin/pinot-admin.sh StartController -zkAddress lp-zk:2181 -clusterName pinot-local \
    -configFileName /conf/controller.conf > /dev/null

docker run -d --name lp-broker --network $NET --platform linux/arm64 \
  -e JAVA_OPTS="-Xms256M -Xmx1G" \
  -v "$DIST:/opt/pinot" -p 8098:8099 $IMG \
  /opt/pinot/bin/pinot-admin.sh StartBroker -zkAddress lp-zk:2181 -clusterName pinot-local > /dev/null

docker run -d --name lp-server --network $NET --platform linux/arm64 \
  --env-file "$DIR/aws-env.list" -e AWS_REGION=eu-west-1 \
  -e JAVA_OPTS="${SERVER_JAVA_OPTS:--Xms512M -Xmx2G} -Dlog4j2.configurationFile=/conf/log4j2-local.xml" \
  -v "$DIST:/opt/pinot" -v "$DIR:/conf" -v "$DIR/deepstore:/deepstore" $IMG \
  /opt/pinot/bin/pinot-admin.sh StartServer -zkAddress lp-zk:2181 -clusterName pinot-local \
    -configFileName /conf/${SERVER_CONF:-server.conf} > /dev/null

echo "cluster starting — controller: http://localhost:9001  broker: http://localhost:8098"
