# Market Surveillance — a one-command Pulse + StreamFlow demo

One pipeline. Six runtimes. Live FIX market data in, surveillance alerts out — in
real time, on your laptop, with a single `docker compose up`.

```
FIX ticks ─▶ WASM decode ─▶ ONNX anomaly ─▶ rule risk-limits ─▶ streaming OHLC
                                                     │
                                                     ▼
                          LLM compliance memo ─▶ MCP desk alert ─▶ sink connector
```

This is a capabilities showcase: a **single Pulse pipeline** that runs *every*
compute primitive the platform offers on one capital-markets surveillance flow.

| Stage | Engine | What it does |
|---|---|---|
| **FIX Decode** | streaming + **WASM** | a sandboxed WebAssembly module (Chicory, fuel-bounded) decodes raw FIX `tag=value` into clean JSON ticks |
| **Anomaly Scorer** | streaming + **ONNX** | an embedded ONNX model scores each tick for spoofing / spikes — inference *inside* the stream, no model server |
| **Risk Limits** | **rule-based** | deterministic, auditable risk-limit checks (imbalance, volume z-score, spread, price band) |
| **OHLC Candles** | **streaming** | per-symbol OHLC bars via a tumbling window |
| **Compliance Memo** | **LLM** | writes a regulator-grade surveillance memo for each flagged event |
| **Desk Alert** | **MCP** | routes the alert to Slack / PagerDuty / Jira |
| **Delivery** | **sink connector** | webhook → the desk console; dashboard → the OHLC screen |

The event engine is **StreamFlow**, running *embedded inside Pulse* (free tier, no
license, no external cluster). Pulse is pulled from the public registry — **you do
not need any source code, just this folder.**

---

## Run it

Requirements: Docker + Docker Compose. ~2 GB RAM free.

```bash
cd market-surveillance-demo
cp .env.example .env        # optional — defaults work out of the box

# Pin to a Pulse release that ships the market-surveillance template + the
# fix-decode (WASM) and market-anomaly-scorer (ONNX) catalog artefacts:
export STREAMFLOW_TAG=vX.Y.Z

./scripts/up.sh             # pulls Pulse + builds the local sidecars, boots, waits
```

Then open:

- **http://localhost:8088** — the **Desk Intake** console. Surveillance alerts
  stream in live, each with a severity chip, the symbol, the model's reasoning,
  and (with an LLM key) a full compliance memo.
- **http://localhost:9090** — **Pulse**. Log in with the demo admin
  (`desk` / `Surveil1234!`) and open the *Market Surveillance* pipeline to see the
  node graph — WASM · ONNX · rules · streaming · LLM · MCP · sink — running.

Tear down (removes volumes for a clean slate):

```bash
./scripts/down.sh
```

> **Pin the tag.** `STREAMFLOW_TAG` must point at a Pulse release whose image
> already contains the `market-surveillance` template + the `fix-decode` /
> `market-anomaly-scorer` catalog artefacts. With an older tag the provisioner's
> catalog-install / template-deploy steps will not find them.

---

## What boots

| Service | Port | Role |
|---|---|---|
| `pulse` | 9090 / 9091 | the product + the embedded StreamFlow engine; hosts the pipeline (pulled image) |
| `desk-intake` | 8088 | the webhook-sink target + the live surveillance console |
| `provisioner` | — | one-shot: registers the admin, installs the WASM + ONNX catalog artefacts, wires the sink, deploys the template, publishes the ingress URL, then exits |
| `marketdata-gen` | — | replays FIX market data with a spoofing burst every ~20s |

The provisioner is **idempotent** — re-running `up.sh` reuses the deployed pipeline.

---

## Make the LLM memo real (optional)

Out of the box the LLM stage falls back gracefully (the pipeline still runs end to
end; the memo is a placeholder). For a genuine compliance memo, set a provider +
key in `.env` before `up.sh`:

```env
LLM_PROVIDER=anthropic
LLM_MODEL=claude-haiku-4-5
LLM_API_KEY=sk-ant-...
```

(OpenAI works too: `LLM_PROVIDER=openai`, `LLM_MODEL=gpt-4o-mini`.)

## Make the desk alert real (optional)

The **Desk Alert (MCP)** node pages Slack / PagerDuty / Jira. To enable real
dispatch, after first boot open Pulse → **Settings → MCP / Integrations**, install
those plugins and paste your tokens. Without them, every alert still lands on the
desk-intake console via the **webhook sink** — which is the path the demo records.

---

## Tuning the feed

Set these in `.env` (or leave the defaults):

```env
RATE_PER_SEC=5            # ticks per second
SPOOF_EVERY_SEC=20        # how often to inject a spoofing burst
SYMBOLS=ACME,GLOBEX,INITECH,HOOLI,VEHEMENT
```
