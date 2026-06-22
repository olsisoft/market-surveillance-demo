#!/usr/bin/env python3
"""
desk-intake
===========

Two jobs in one tiny stdlib-only web app:

  1. **Webhook sink target.** The Market-Surveillance pipeline delivers alert
     notifications via an HTTP webhook sink. We accept those POSTs at `/ingest`,
     parse the JSON (tolerantly -- the shape is *not* guaranteed), stamp a
     server `receivedAt`, and keep the most recent 200 in an in-memory ring
     buffer.

  2. **Live surveillance dashboard.** `GET /` serves a self-contained dark
     "trading-desk" console (all CSS/JS inline -- no CDNs) that polls
     `/api/alerts` every 2s and renders each alert as a card with a severity
     chip, symbol, summary, timestamp, and a collapsible raw-JSON view.

Endpoints
---------
    POST /ingest        accept JSON webhook delivery -> 200 {"ok":true}
    GET  /api/alerts    -> {"count": N, "alerts": [most-recent-first]}
    GET  /  /index.html -> the dashboard HTML
    GET  /health        -> 200 {"status":"ok"}

Threaded server so dashboard polls never block webhook deliveries (and vice
versa). All access to the ring buffer is guarded by a lock.
"""

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

PORT = int(os.environ.get("PORT", "8088"))
RING_CAPACITY = 200

# --------------------------------------------------------------------------- #
# Thread-safe ring buffer of received alerts                                  #
# --------------------------------------------------------------------------- #

_lock = threading.Lock()
_alerts = []          # newest appended at the end
_seq = 0              # monotonic id for stable client-side keys


def store_alert(obj):
    """Append an alert object (already augmented), trimming to RING_CAPACITY."""
    global _seq
    with _lock:
        _seq += 1
        obj["_seq"] = _seq
        _alerts.append(obj)
        # Trim from the front so we keep only the most recent RING_CAPACITY.
        if len(_alerts) > RING_CAPACITY:
            del _alerts[: len(_alerts) - RING_CAPACITY]


def snapshot_newest_first():
    """Return a shallow copy of the buffer, most-recent first."""
    with _lock:
        return list(reversed(_alerts))


# --------------------------------------------------------------------------- #
# Dashboard HTML (fully self-contained: inline CSS + JS, no external assets)  #
# --------------------------------------------------------------------------- #

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Market Surveillance — Desk Intake</title>
<style>
  :root {
    --bg:        #0a0e14;
    --bg-2:      #0f1620;
    --panel:     #121a26;
    --panel-2:   #161f2e;
    --border:    #1f2b3d;
    --text:      #d7e0ec;
    --muted:     #6f8197;
    --muted-2:   #4b5b70;
    --accent:    #38bdf8;
    --mono: "SF Mono", "JetBrains Mono", "Fira Code", "Cascadia Code",
            ui-monospace, Menlo, Consolas, monospace;
    --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica,
            Arial, sans-serif;
    --green:  #2ecc71;
    --amber:  #f1c40f;
    --orange: #e67e22;
    --red:    #e74c3c;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; }
  body {
    background:
      radial-gradient(1200px 600px at 80% -10%, #12203210, transparent),
      linear-gradient(180deg, var(--bg) 0%, var(--bg-2) 100%);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    -webkit-font-smoothing: antialiased;
  }

  /* ---- header ---- */
  header {
    position: sticky; top: 0; z-index: 10;
    display: flex; align-items: center; gap: 18px;
    padding: 16px 24px;
    background: rgba(10,14,20,0.86);
    backdrop-filter: blur(8px);
    border-bottom: 1px solid var(--border);
  }
  .brand { display: flex; flex-direction: column; gap: 2px; }
  .brand h1 {
    margin: 0; font-size: 16px; font-weight: 650; letter-spacing: 0.3px;
  }
  .brand .sub {
    font-size: 11px; color: var(--muted); letter-spacing: 1.4px;
    text-transform: uppercase;
  }
  .spacer { flex: 1; }
  .live {
    display: inline-flex; align-items: center; gap: 7px;
    font-size: 12px; color: var(--muted); font-family: var(--mono);
  }
  .live .dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--green); box-shadow: 0 0 0 0 rgba(46,204,113,0.7);
    animation: pulse 1.8s infinite;
  }
  @keyframes pulse {
    0%   { box-shadow: 0 0 0 0 rgba(46,204,113,0.55); }
    70%  { box-shadow: 0 0 0 7px rgba(46,204,113,0); }
    100% { box-shadow: 0 0 0 0 rgba(46,204,113,0); }
  }
  .counter {
    font-family: var(--mono); font-size: 13px;
    background: var(--panel); border: 1px solid var(--border);
    padding: 6px 12px; border-radius: 8px; color: var(--text);
  }
  .counter b { color: var(--accent); }

  /* ---- layout ---- */
  main { padding: 22px 24px 60px; max-width: 1180px; margin: 0 auto; }

  .empty {
    margin-top: 12vh; text-align: center; color: var(--muted);
    font-family: var(--mono); font-size: 14px;
  }
  .empty .glyph { font-size: 34px; opacity: 0.4; display: block; margin-bottom: 14px; }

  .grid { display: grid; grid-template-columns: 1fr; gap: 12px; }

  /* ---- alert card ---- */
  .card {
    background: linear-gradient(180deg, var(--panel) 0%, var(--panel-2) 100%);
    border: 1px solid var(--border);
    border-left: 3px solid var(--muted-2);
    border-radius: 10px;
    padding: 14px 16px;
    animation: fadein 0.35s ease;
  }
  @keyframes fadein { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: none; } }
  .card.sev-LOW      { border-left-color: var(--green); }
  .card.sev-MEDIUM   { border-left-color: var(--amber); }
  .card.sev-HIGH     { border-left-color: var(--orange); }
  .card.sev-CRITICAL { border-left-color: var(--red); }

  .card-top { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .chip {
    font-family: var(--mono); font-size: 10.5px; font-weight: 700;
    letter-spacing: 1px; text-transform: uppercase;
    padding: 4px 9px; border-radius: 999px; color: #06121a;
    background: var(--muted-2);
  }
  .chip.sev-LOW      { background: var(--green); }
  .chip.sev-MEDIUM   { background: var(--amber); }
  .chip.sev-HIGH     { background: var(--orange); color: #1a0f04; }
  .chip.sev-CRITICAL { background: var(--red); color: #fff; }

  .symbol {
    font-family: var(--mono); font-weight: 700; font-size: 15px;
    color: var(--text); letter-spacing: 0.5px;
  }
  .title { font-size: 14px; font-weight: 600; color: var(--text); }
  .ts {
    margin-left: auto; font-family: var(--mono); font-size: 11.5px;
    color: var(--muted);
  }
  .summary {
    margin: 9px 0 4px; font-size: 13px; line-height: 1.5; color: #b6c4d6;
  }
  .metrics {
    display: flex; gap: 16px; flex-wrap: wrap; margin-top: 8px;
    font-family: var(--mono); font-size: 12px;
  }
  .metrics .m { color: var(--muted); }
  .metrics .m b { color: var(--text); font-weight: 600; }

  details {
    margin-top: 10px; border-top: 1px dashed var(--border); padding-top: 8px;
  }
  details > summary {
    cursor: pointer; font-family: var(--mono); font-size: 11px;
    color: var(--muted); list-style: none; user-select: none;
  }
  details > summary::-webkit-details-marker { display: none; }
  details > summary:hover { color: var(--accent); }
  details > summary::before { content: "▸ "; }
  details[open] > summary::before { content: "▾ "; }
  pre.raw {
    margin: 8px 0 0; padding: 12px;
    background: #070b11; border: 1px solid var(--border); border-radius: 8px;
    font-family: var(--mono); font-size: 11.5px; line-height: 1.45;
    color: #9fd0e6; overflow-x: auto; white-space: pre;
  }
  footer {
    text-align: center; color: var(--muted-2); font-size: 11px;
    font-family: var(--mono); padding: 22px 0;
  }
</style>
</head>
<body>
  <header>
    <div class="brand">
      <h1>Market Surveillance — Desk Intake</h1>
      <span class="sub">Webhook Sink · Live Alert Console</span>
    </div>
    <div class="spacer"></div>
    <span class="live"><span class="dot"></span> live</span>
    <span class="counter"><b id="count">0</b> alerts</span>
  </header>

  <main>
    <div id="empty" class="empty">
      <span class="glyph">◴</span>
      Waiting for alerts from the pipeline…
    </div>
    <div id="grid" class="grid"></div>
  </main>

  <footer>desk-intake · polling /api/alerts every 2s</footer>

<script>
(function () {
  "use strict";

  // ----- tolerant field extraction --------------------------------------- //
  // The webhook payload shape is not guaranteed. We dig through the object
  // (and common nested containers) to surface the fields we want to show.

  var NESTED_KEYS = ["payload", "message", "data", "alert", "body", "event"];

  function candidateObjects(obj) {
    // Returns the alert object plus any nested containers worth searching.
    var out = [obj];
    NESTED_KEYS.forEach(function (k) {
      if (obj && typeof obj[k] === "object" && obj[k] !== null) {
        out.push(obj[k]);
        // one more level for things like payload.data
        NESTED_KEYS.forEach(function (k2) {
          if (obj[k] && typeof obj[k][k2] === "object" && obj[k][k2] !== null) {
            out.push(obj[k][k2]);
          }
        });
      }
    });
    return out;
  }

  function pick(obj, keys) {
    // First non-empty value found across the object + nested containers.
    var objs = candidateObjects(obj);
    for (var i = 0; i < objs.length; i++) {
      var o = objs[i];
      if (!o || typeof o !== "object") continue;
      for (var j = 0; j < keys.length; j++) {
        var key = keys[j];
        // case-insensitive match against the object's own keys
        var found = Object.keys(o).find(function (kk) {
          return kk.toLowerCase() === key.toLowerCase();
        });
        if (found !== undefined) {
          var v = o[found];
          if (v !== null && v !== undefined && v !== "") return v;
        }
      }
    }
    return undefined;
  }

  function normalizeSeverity(raw) {
    if (raw === undefined || raw === null) return null;
    var s = String(raw).toUpperCase().trim();
    if (s.indexOf("CRIT") === 0 || s === "SEV1" || s === "P1") return "CRITICAL";
    if (s.indexOf("HIGH") === 0 || s === "SEV2" || s === "P2") return "HIGH";
    if (s.indexOf("MED")  === 0 || s === "SEV3" || s === "P3") return "MEDIUM";
    if (s.indexOf("LOW")  === 0 || s === "INFO" || s === "P4") return "LOW";
    if (["LOW","MEDIUM","HIGH","CRITICAL"].indexOf(s) >= 0) return s;
    return null;
  }

  function fmtTimestamp(v) {
    if (v === undefined || v === null) return "";
    var d;
    if (typeof v === "number") {
      // epoch seconds vs millis heuristic
      d = new Date(v < 1e12 ? v * 1000 : v);
    } else {
      var n = Number(v);
      if (!isNaN(n) && String(v).trim() !== "") {
        d = new Date(n < 1e12 ? n * 1000 : n);
      } else {
        d = new Date(v);
      }
    }
    if (isNaN(d.getTime())) return String(v);
    return d.toLocaleTimeString([], { hour12: false }) +
           "." + String(d.getMilliseconds()).padStart(3, "0");
  }

  function esc(s) {
    return String(s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  // ----- card rendering -------------------------------------------------- //

  function renderCard(alert) {
    var symbol   = pick(alert, ["symbol", "ticker", "instrument", "sym"]);
    var severity = normalizeSeverity(
                     pick(alert, ["severity", "level", "priority", "sev"]));
    var title    = pick(alert, ["title", "rule", "ruleName", "alertType",
                                "type", "name", "category"]);
    var summary  = pick(alert, ["summary", "memo", "message", "description",
                                "detail", "details", "text", "reason"]);
    var ts       = pick(alert, ["timestamp", "time", "ts", "5005",
                                "eventTime", "detectedAt"]);

    // surveillance metrics, if the pipeline forwarded them
    var imb   = pick(alert, ["order_imbalance", "orderImbalance", "5004"]);
    var zsc   = pick(alert, ["volume_zscore", "volumeZscore", "5002"]);
    var spr   = pick(alert, ["spread_bps", "spreadBps", "5003"]);

    var sev = severity || "MEDIUM"; // default chip if none provided

    var card = document.createElement("div");
    card.className = "card sev-" + sev;

    var html = '<div class="card-top">';
    html += '<span class="chip sev-' + sev + '">' + esc(sev) + '</span>';
    if (symbol !== undefined) html += '<span class="symbol">' + esc(symbol) + '</span>';
    if (title  !== undefined) html += '<span class="title">' + esc(title)  + '</span>';
    var tsShow = ts !== undefined ? fmtTimestamp(ts)
                                  : fmtTimestamp(alert.receivedAt);
    html += '<span class="ts">' + esc(tsShow) + '</span>';
    html += '</div>';

    if (summary !== undefined) {
      html += '<div class="summary">' + esc(summary) + '</div>';
    }

    var metricBits = [];
    if (imb !== undefined) metricBits.push('<span class="m">imbalance <b>' + esc(imb) + '</b></span>');
    if (zsc !== undefined) metricBits.push('<span class="m">vol z <b>' + esc(zsc) + '</b></span>');
    if (spr !== undefined) metricBits.push('<span class="m">spread <b>' + esc(spr) + ' bps</b></span>');
    if (metricBits.length) {
      html += '<div class="metrics">' + metricBits.join("") + '</div>';
    }

    // Always show the full raw JSON so nothing is hidden.
    html += '<details><summary>raw payload</summary>' +
            '<pre class="raw">' + esc(JSON.stringify(alert, null, 2)) +
            '</pre></details>';

    card.innerHTML = html;
    return card;
  }

  // ----- polling --------------------------------------------------------- //

  var grid  = document.getElementById("grid");
  var empty = document.getElementById("empty");
  var count = document.getElementById("count");

  function refresh() {
    fetch("/api/alerts", { cache: "no-store" })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var alerts = (data && data.alerts) || [];
        count.textContent = (data && data.count) || alerts.length || 0;

        if (!alerts.length) {
          empty.style.display = "";
          grid.innerHTML = "";
          return;
        }
        empty.style.display = "none";

        // Re-render; small N (<=200) so a full rebuild is fine and avoids
        // tricky diffing. Each card keyed by _seq implicitly via order.
        var frag = document.createDocumentFragment();
        alerts.forEach(function (a) { frag.appendChild(renderCard(a)); });
        grid.innerHTML = "";
        grid.appendChild(frag);
      })
      .catch(function () { /* transient -- next tick will retry */ });
  }

  refresh();
  setInterval(refresh, 2000);
})();
</script>
</body>
</html>
"""


# --------------------------------------------------------------------------- #
# HTTP handler                                                                #
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    # Keep the default access log quiet-ish but useful.
    def log_message(self, fmt, *args):
        print(f"[desk-intake] {self.address_string()} {fmt % args}", flush=True)

    # ---- small response helpers ---- #

    def _send_json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, code, html):
        body = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    # ---- routing ---- #

    def do_GET(self):
        path = self.path.split("?", 1)[0]

        if path in ("/", "/index.html"):
            self._send_html(200, DASHBOARD_HTML)
            return

        if path == "/api/alerts":
            alerts = snapshot_newest_first()
            self._send_json(200, {"count": len(alerts), "alerts": alerts})
            return

        if path == "/health":
            self._send_json(200, {"status": "ok"})
            return

        self._send_json(404, {"error": "not found", "path": path})

    def do_POST(self):
        path = self.path.split("?", 1)[0]

        if path != "/ingest":
            self._send_json(404, {"error": "not found", "path": path})
            return

        # Read the raw body (Content-Length may be absent on some clients).
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length > 0 else b""
        text = raw.decode("utf-8", errors="replace")

        # Tolerant parse: JSON if we can, otherwise wrap the raw text.
        try:
            parsed = json.loads(text) if text.strip() else {}
            if not isinstance(parsed, dict):
                # JSON array / scalar -> wrap so the buffer holds objects.
                parsed = {"payload": parsed}
        except (json.JSONDecodeError, ValueError):
            parsed = {"raw": text}

        # Stamp server receive time (ISO-ish, plus epoch millis for the UI).
        parsed["receivedAt"] = int(time.time() * 1000)
        store_alert(parsed)

        # Always acknowledge so the pipeline's sink considers delivery a success.
        self._send_json(200, {"ok": True})


# --------------------------------------------------------------------------- #
# Entrypoint                                                                  #
# --------------------------------------------------------------------------- #

def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[desk-intake] listening on 0.0.0.0:{PORT} "
          f"(ring capacity {RING_CAPACITY})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[desk-intake] interrupted; shutting down", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
