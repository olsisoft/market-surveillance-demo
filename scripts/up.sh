#!/usr/bin/env bash
# Build + boot the Market Surveillance demo, then print where to watch it.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "▶ Building and starting the Market Surveillance demo ..."
docker compose up --build -d

echo
echo "▶ Following the provisioner until it finishes setup ..."
docker compose logs -f provisioner &
LOGS_PID=$!
# Wait for the provisioner container to exit (setup complete).
docker compose wait provisioner >/dev/null 2>&1 || true
kill "$LOGS_PID" >/dev/null 2>&1 || true

echo
echo "✓ Up. Open these:"
echo "    Desk console (record this):  http://localhost:8088"
echo "    Pulse UI / pipeline canvas:  http://localhost:9090"
echo
echo "  Follow the live market data:   docker compose logs -f marketdata-gen"
echo "  Tear everything down:          ./scripts/down.sh"
