#!/usr/bin/env bash
# Stop the demo and remove its volumes (fresh start next time).
set -euo pipefail
cd "$(dirname "$0")/.."
echo "▶ Stopping the Market Surveillance demo and removing volumes ..."
docker compose down -v
echo "✓ Down."
