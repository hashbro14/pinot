#!/usr/bin/env bash
# Registers schema + remote-query table on the local controller and uploads the two local segment tars.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
C=http://localhost:9001
T=artist_dashboard_daily_streams_aggregated
until curl -sf $C/health >/dev/null; do echo "waiting for controller"; sleep 3; done
# /health goes green before Helix is ready to take schema writes: retry the first write until it lands
for i in $(seq 1 30); do
  curl -sf -X POST $C/schemas -H 'Content-Type: application/json' -d @"$DIR/schema.json" && echo && break
  echo "controller not ready for writes yet ($i)"; sleep 3
done
curl -sf -X POST $C/tables -H 'Content-Type: application/json' -d @"$DIR/table.json" && echo
for d in 2026-04-09 2026-06-22; do
  tar=$(ls ~/pinot-ingest/work/$d/segments/*.tar.gz)
  echo "uploading $(basename $tar)"
  curl -sf -F segment=@"$tar" "$C/v2/segments?tableName=$T&tableType=OFFLINE" && echo
done
sleep 5
curl -s "$C/segments/$T/servers" | head -c 600; echo
