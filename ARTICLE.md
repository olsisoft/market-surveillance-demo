# Six runtimes. One pipeline. Real-time market surveillance you can `docker compose up`.

*A capabilities demo of Pulse + StreamFlow — built to be run, not just watched.*

---

## The problem with surveillance stacks

If you've ever built real-time trade surveillance, you know the shape of the
pain. It's never one system — it's a *zoo*:

- a **FIX/market-data decoder** in C++ or Java,
- a **feature pipeline** feeding a **Python model service** for anomaly scoring,
- a **rules engine** (often a pile of stored procedures) for the hard risk limits
  a regulator will actually ask about,
- a **stream processor** (Flink/Kafka Streams) building OHLC bars and windows,
- a **case-management / alerting** layer bolted on the side,
- and glue — so much glue — moving data between all of it.

Six teams, six deploy cadences, six places for an event to silently go missing.

**What if it were one pipeline?**

## The demo: one pipeline, every primitive

This is a single **Pulse** pipeline. Live FIX market data goes in; surveillance
alerts come out. Every box below is one node you wire on a canvas:

```
FIX ticks ─▶ WASM decode ─▶ ONNX anomaly ─▶ rule risk-limits ─▶ streaming OHLC
                                                     │
                                                     ▼
                          LLM compliance memo ─▶ MCP desk alert ─▶ sink connector
```

| Stage | Runtime | What it actually does |
|---|---|---|
| **FIX Decode** | **WASM** | a sandboxed WebAssembly module (fuel- and memory-bounded) decodes raw FIX `tag=value` into clean JSON ticks. Your code, your language — Rust, TinyGo, anything that compiles to wasm32. |
| **Anomaly Scorer** | **ONNX** | an embedded ONNX model scores each tick for spoofing/spike patterns. Inference runs *inside the stream* — there is no Python service, no model server, no network hop. |
| **Risk Limits** | **rule-based** | the deterministic, auditable checks a regulator reads: order imbalance, volume z-score, spread blow-out, ±price band. |
| **OHLC Candles** | **streaming** | per-symbol candles via a tumbling window — classic stream processing. |
| **Compliance Memo** | **LLM** | turns a flagged event into a regulator-grade narrative: what tripped, the context, the suspected pattern. |
| **Desk Alert** | **MCP** | pages the desk via Slack / PagerDuty / Jira through the Model Context Protocol. |
| **Delivery** | **sink connector** | webhook → the live desk console; dashboard → the OHLC screen. |

Six runtimes that are normally six systems. Here they're six **nodes**, wired by
topics, deployed together, observable together.

## How a node is built

This is the part I most want you to see, because it's where the disruption lives.

A Pulse node isn't a microservice you write, containerise, and operate. It's a
small declarative spec on a graph. The WASM decode stage is literally:

```json
{ "type": "wasm", "module": "fix-decode" }
```

The ONNX scorer:

```json
{ "type": "mlPredict", "model": "market-anomaly-scorer",
  "inputFields": ["price_change_pct","volume_zscore","spread_bps","order_imbalance"],
  "outputField": "anomaly" }
```

The risk limits are plain conditions; the OHLC stage is a window with
aggregations; the memo is a prompt; the desk alert names its MCP tools. You wire
inputs to outputs by topic, hit deploy, and the engine runs it. The WASM module
and the ONNX model are **catalog artefacts you install with one click** — no
build server, no model-serving infra.

That's the shift: the *capabilities* (sandboxed compute, embedded ML, stream
windows, LLM reasoning, tool-calling) are first-class node types. You compose;
you don't re-platform.

## And underneath: StreamFlow, not Kafka

A detail that matters. Pulse doesn't ride on Kafka. The event mesh is
**StreamFlow** — its own engine, with a native shard-per-core, io_uring write
path. In this demo it runs **embedded inside Pulse** (free, in-JVM, nothing to
stand up); the identical pipeline runs unchanged against a remote StreamFlow
cluster when you need the throughput. Kafka is just *one connector* it can speak
— not the substrate.

So the "fast path" here isn't a broker hop. It's the engine.

## The part that makes it real: run it yourself

This isn't a slide. It's a repo you can boot in one command:

```bash
# grab the demo folder (link below) — no monorepo, no source build
cd demo-market-surveillance
docker compose -f docker-compose.public.yml up   # pulls the public Pulse image
# open http://localhost:8088  ← the live surveillance desk
# open http://localhost:9090  ← the pipeline canvas, all six nodes running
```

A market-data generator streams FIX ticks and injects a **spoofing burst every
~20 seconds**. Watch the desk console light up: the burst is decoded, scored,
caught by the risk limits, written up as a memo, and dispatched — end to end, in
real time, on your laptop.

No license. No external cluster. No model server. No Kafka.

---

*Built with Pulse + StreamFlow. The full demo — docker-compose, the FIX-decode
WASM module, the ONNX anomaly model, the generator, and the desk console — is in
the repo. Clone it, run it, point it at your own feed.*
