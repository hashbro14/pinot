#!/usr/bin/env bash
# Full local cycle: (re)start the cluster, wait for the remote server, register table + segments, run bench.
# Usage: cycle.sh "<label>"
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
LABEL=${1:-run}
"$DIR/run-local-cluster.sh" up | tail -1
for i in $(seq 1 80); do
  curl -sf localhost:9001/health >/dev/null 2>&1 && docker exec lp-server grep -q "Remote querying enabled" /logs/pinot-all.log 2>/dev/null && break
  if [ "$(docker inspect -f '{{.State.Running}}' lp-server 2>/dev/null)" != "true" ]; then
    echo "SERVER DIED:"; docker logs lp-server 2>&1 | grep -E "Exception|Error" | head -5; exit 1
  fi
  sleep 3
done
docker exec lp-server grep "Remote querying enabled" /logs/pinot-all.log | tail -1 | sed 's/.*Remote querying enabled/server: remote querying enabled/' | cut -c1-200
"$DIR/setup-table.sh" >/dev/null 2>&1
for i in $(seq 1 80); do
  [ "$(curl -s localhost:9001/tables/artist_dashboard_daily_streams_aggregated/externalview | grep -o ONLINE | wc -l | tr -d ' ')" = "2" ] && break
  sleep 3
done
echo "segments online: $(curl -s localhost:9001/tables/artist_dashboard_daily_streams_aggregated/externalview | grep -o ONLINE | wc -l | tr -d ' ')"
sleep 3
"$DIR/bench.sh" "$LABEL"
