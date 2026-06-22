#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# One-shot provisioner for the Market Surveillance demo. Idempotent: safe to
# re-run against an existing Pulse data volume.
#
#   1. wait for Pulse /health
#   2. register the admin (ignore "already exists") + log in for a JWT
#   3. install the catalog artefacts: fix-decode (WASM) + market-anomaly-scorer (ONNX)
#   4. (optional) set the LLM provider when an API key is supplied
#   5. point the notification webhook (= the webhook sink) at the desk-intake console
#   6. deploy the market-surveillance template (reuse if already deployed)
#   7. resolve the webhook ingress URL and publish it to the shared volume so the
#      market-data generator knows where to POST
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

PULSE_URL="${PULSE_URL:-http://pulse:9090}"
ADMIN_USER="${ADMIN_USER:-desk}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-Surveil1234!}"
ORG_NAME="${ORG_NAME:-Surveillance Desk}"
TEMPLATE_ID="${TEMPLATE_ID:-market-surveillance}"
DESK_WEBHOOK_URL="${DESK_WEBHOOK_URL:-http://desk-intake:8088/ingest}"
INGRESS_FILE="${INGRESS_FILE:-/shared/ingress.url}"
LLM_PROVIDER="${LLM_PROVIDER:-}"
LLM_MODEL="${LLM_MODEL:-}"
LLM_API_KEY="${LLM_API_KEY:-}"

say() { echo "[provisioner] $*"; }
die() { echo "[provisioner][FATAL] $*" >&2; exit 1; }

# ── 1. wait for Pulse ────────────────────────────────────────────────────────
say "waiting for Pulse at ${PULSE_URL}/health ..."
for i in $(seq 1 60); do
  if curl -fsS "${PULSE_URL}/health" >/dev/null 2>&1; then
    say "Pulse is up."
    break
  fi
  sleep 3
  [ "$i" = "60" ] && die "Pulse did not become healthy in time."
done

# ── 2. register + login ──────────────────────────────────────────────────────
say "registering admin '${ADMIN_USER}' (ignored if it already exists) ..."
curl -fsS -X POST "${PULSE_URL}/api/auth/register" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASSWORD}\",\"organizationName\":\"${ORG_NAME}\"}" \
  >/dev/null 2>&1 || say "register skipped (user likely already exists)."

say "logging in ..."
TOKEN=""
for i in $(seq 1 10); do
  TOKEN=$(curl -fsS -X POST "${PULSE_URL}/api/auth/login" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"${ADMIN_USER}\",\"password\":\"${ADMIN_PASSWORD}\"}" \
    | jq -r '.accessToken // empty')
  [ -n "$TOKEN" ] && break
  sleep 2
done
[ -n "$TOKEN" ] || die "could not obtain an access token."
AUTH=(-H "Authorization: Bearer ${TOKEN}")
say "authenticated."

# ── 3. install catalog artefacts ─────────────────────────────────────────────
for ART in fix-decode market-anomaly-scorer; do
  say "installing catalog artefact: ${ART}"
  curl -fsS -X POST "${PULSE_URL}/api/pulse/catalog/${ART}/install" \
    "${AUTH[@]}" -H 'Content-Type: application/json' >/dev/null \
    && say "  ✓ ${ART} installed" \
    || say "  ! ${ART} install returned non-2xx (may already be installed)"
done

# ── 4. LLM provider (optional) ───────────────────────────────────────────────
if [ -n "$LLM_API_KEY" ] && [ -n "$LLM_PROVIDER" ]; then
  say "configuring LLM provider: ${LLM_PROVIDER} (${LLM_MODEL:-default model})"
  curl -fsS -X PUT "${PULSE_URL}/api/pulse/settings/llm" \
    "${AUTH[@]}" -H 'Content-Type: application/json' \
    -d "{\"provider\":\"${LLM_PROVIDER}\",\"model\":\"${LLM_MODEL}\",\"apiKey\":\"${LLM_API_KEY}\"}" \
    >/dev/null && say "  ✓ LLM provider set" || say "  ! LLM provider update failed"
else
  say "no LLM key supplied — Pulse will auto-detect / fall back (pipeline still runs)."
fi

# ── 5. wire the webhook sink to the desk-intake console ──────────────────────
say "pointing the notification webhook (sink) at ${DESK_WEBHOOK_URL}"
curl -fsS -X PUT "${PULSE_URL}/api/pulse/settings/notifications" \
  "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d "{\"enabled\":true,\"webhookUrl\":\"${DESK_WEBHOOK_URL}\"}" \
  >/dev/null && say "  ✓ sink webhook wired" || say "  ! notifications update failed"

# ── 6. deploy the template (reuse if already deployed) ───────────────────────
say "looking for an existing '${TEMPLATE_ID}' deployment ..."
PIPELINE_ID=$(curl -fsS "${PULSE_URL}/api/pulse/pipelines" "${AUTH[@]}" \
  | jq -r --arg n "Market Surveillance" \
    '(if type=="array" then . else .pipelines end) // [] | map(select(.name==$n)) | .[0].id // empty')

if [ -z "$PIPELINE_ID" ]; then
  say "deploying template '${TEMPLATE_ID}' ..."
  DEPLOY=$(curl -fsS -X POST "${PULSE_URL}/api/pulse/templates/${TEMPLATE_ID}/deploy" \
    "${AUTH[@]}" -H 'Content-Type: application/json' \
    -d '{"name":"Market Surveillance"}')
  PIPELINE_ID=$(echo "$DEPLOY" | jq -r '.pipelineId // empty')
  [ -n "$PIPELINE_ID" ] || die "deploy did not return a pipelineId. Response: $DEPLOY"
  say "  ✓ deployed pipeline ${PIPELINE_ID}"
else
  say "  ✓ reusing existing pipeline ${PIPELINE_ID}"
fi

# ── 7. resolve + publish the webhook ingress URL ─────────────────────────────
say "resolving the webhook ingress URL ..."
INGRESS_PATH=""
for i in $(seq 1 10); do
  INGRESS_PATH=$(curl -fsS "${PULSE_URL}/api/pulse/pipelines/${PIPELINE_ID}/manifest" "${AUTH[@]}" \
    | jq -r '.ingress[0].url // empty')
  [ -n "$INGRESS_PATH" ] && break
  sleep 2
done
[ -n "$INGRESS_PATH" ] || die "could not resolve a webhook ingress URL from the manifest."

mkdir -p "$(dirname "$INGRESS_FILE")"
echo "${PULSE_URL}${INGRESS_PATH}" > "$INGRESS_FILE"
say "  ✓ ingress published: ${PULSE_URL}${INGRESS_PATH}"
say "done. The market-data generator will start sending FIX ticks."
say "Open the desk console:  http://localhost:8088"
say "Open Pulse:             http://localhost:9090"
exit 0
