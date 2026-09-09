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

# Runs the benchmark query shapes against the local broker and pairs each with the server's remote fetch stats.
# Usage: bench.sh [label]
set -uo pipefail
B=http://localhost:8098
T=artist_dashboard_daily_streams_aggregated
LABEL=${1:-run}
q() {
  local sql="$1"
  local res; res=$(curl -s -X POST $B/query/sql -H 'Content-Type: application/json' \
    -d "$(jq -cn --arg s "$sql" '{sql:$s}')")
  echo "--- $sql"
  echo "$res" | jq -c '{timeUsedMs, numDocsScanned, numSegmentsQueried, numSegmentsProcessed, numEntriesScannedPostFilter, rows: (.resultTable.rows|length), exceptions: (.exceptions|map(.message)|.[0]|.[0:120])}'
  sleep 1
  local after; after=$(docker exec lp-server grep -c "Remote fetch stats" /logs/pinot-all.log)
  if [ "$after" -gt "$STATS_SEEN" ]; then
    docker exec lp-server grep "Remote fetch stats" /logs/pinot-all.log | tail -n +$((STATS_SEEN + 1)) | sed 's/.*Remote fetch stats for table: [^,]*,/    fetch stats:/'
  else
    echo "    fetch stats: no remote GETs"
  fi
  STATS_SEEN=$after
}
STATS_SEEN=$(docker exec lp-server grep -c "Remote fetch stats" /logs/pinot-all.log)
echo "=== $LABEL"
q "SELECT COUNT(*) FROM $T"
q "SELECT * FROM $T LIMIT 10"
q "SELECT artist_id, SUM(all_streams) FROM $T WHERE day_dt = '2026-04-09' GROUP BY artist_id ORDER BY SUM(all_streams) DESC LIMIT 10"
q "SELECT SUM(all_streams), SUM(full_streams) FROM $T WHERE artist_id = 126"
q "SELECT country_code, SUM(all_streams) FROM $T WHERE artist_id = 126 GROUP BY country_code ORDER BY SUM(all_streams) DESC LIMIT 20"
q "SELECT song_name, SUM(all_streams) FROM $T WHERE artist_id = 126 GROUP BY song_name ORDER BY SUM(all_streams) DESC LIMIT 20"
