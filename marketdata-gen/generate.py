#!/usr/bin/env python3
"""
marketdata-gen
==============

Continuously synthesises FIX (4.2) market-data messages and POSTs them to the
Market-Surveillance pipeline's webhook ingress.

The ingress URL is *not* known at build time -- the provisioner discovers (or
mints) the webhook and writes the full URL into a shared file. We poll that file
until it appears, then start streaming.

A FIX message here is a `|`-delimited string of `tag=value` pairs. Tags used:

    8    = BeginString          (always "FIX.4.2")
    35   = MsgType              (always "D" / NewOrderSingle)
    55   = Symbol
    54   = Side                 (1 = BUY, 2 = SELL)
    44   = Price
    38   = OrderQty
    5001 = price_change_pct     (custom surveillance tag)
    5002 = volume_zscore        (custom)
    5003 = spread_bps           (custom)
    5004 = order_imbalance      (custom)
    5005 = ts                   (epoch millis, custom)

Most ticks are "normal". Every SPOOF_EVERY_SEC seconds we inject a short burst of
clearly-anomalous ticks on a single rotating symbol (high imbalance, high volume
z-score, large size, all BUY) to simulate layering / spoofing -- this is what the
downstream surveillance rules are meant to flag.

Stdlib only (urllib.request). Resilient by design: a single failed POST never
crashes the loop.
"""

import os
import random
import sys
import time
import urllib.error
import urllib.request

# --------------------------------------------------------------------------- #
# Configuration (all overridable via environment)                             #
# --------------------------------------------------------------------------- #

INGRESS_FILE = os.environ.get("INGRESS_FILE", "/shared/ingress.url")
RATE_PER_SEC = float(os.environ.get("RATE_PER_SEC", "5"))
SPOOF_EVERY_SEC = float(os.environ.get("SPOOF_EVERY_SEC", "20"))
SYMBOLS = [
    s.strip()
    for s in os.environ.get("SYMBOLS", "ACME,GLOBEX,INITECH,HOOLI,VEHEMENT").split(",")
    if s.strip()
]

# Deterministic-ish seed so demos look the same-ish each run but still "live".
random.seed(1337)

# Per-symbol running price for the gaussian random walk. Seeded with plausible
# starting prices spread across the symbol set.
_BASE_PRICES = [42.50, 187.55, 13.20, 91.10, 6.75]
prices = {
    sym: _BASE_PRICES[i % len(_BASE_PRICES)] + random.uniform(-2.0, 2.0)
    for i, sym in enumerate(SYMBOLS)
}


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #

def log(msg):
    """Timestamped stdout line (flushed by PYTHONUNBUFFERED)."""
    print(f"[marketdata-gen] {msg}", flush=True)


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def wait_for_ingress():
    """
    Block until INGRESS_FILE exists and is non-empty, then return its contents
    (the full ingress URL). Polls every 2s.
    """
    log(f"waiting for ingress URL at {INGRESS_FILE} ...")
    while True:
        try:
            if os.path.isfile(INGRESS_FILE):
                with open(INGRESS_FILE, "r", encoding="utf-8") as fh:
                    url = fh.read().strip()
                if url:
                    log(f"ingress URL acquired: {url}")
                    return url
        except OSError as exc:
            # File may be mid-write or briefly unreadable -- just retry.
            log(f"could not read ingress file yet ({exc}); retrying")
        time.sleep(2)


def build_fix(symbol, side, price, qty,
              price_change_pct, volume_zscore, spread_bps,
              order_imbalance, ts_millis):
    """
    Assemble the `|`-delimited FIX string with tags in the required order:
    8,35,55,54,44,38,5001,5002,5003,5004,5005.
    """
    fields = [
        ("8", "FIX.4.2"),
        ("35", "D"),
        ("55", symbol),
        ("54", str(side)),
        ("44", f"{price:.2f}"),
        ("38", str(qty)),
        ("5001", f"{price_change_pct:.3f}"),
        ("5002", f"{volume_zscore:.2f}"),
        ("5003", f"{spread_bps:.1f}"),
        ("5004", f"{order_imbalance:.3f}"),
        ("5005", str(ts_millis)),
    ]
    return "|".join(f"{tag}={val}" for tag, val in fields)


def post_fix(url, fix_message):
    """
    POST a single FIX message as text/plain. Returns True on a 2xx response.
    Never raises -- all connection / HTTP errors are caught and logged so the
    main loop keeps streaming even while the pipeline is still warming up.
    """
    data = fix_message.encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "text/plain"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            status = resp.getcode()
            if 200 <= status < 300:
                return True
            log(f"non-2xx from ingress: HTTP {status}")
            return False
    except urllib.error.HTTPError as exc:
        log(f"HTTP error posting tick: {exc.code} {exc.reason}")
        return False
    except (urllib.error.URLError, OSError) as exc:
        # Pipeline may not be up yet -- this is expected early on.
        log(f"connection error posting tick: {exc}")
        return False


def now_millis():
    return int(time.time() * 1000)


# --------------------------------------------------------------------------- #
# Tick generators                                                             #
# --------------------------------------------------------------------------- #

def make_normal_tick(symbol):
    """
    Produce one benign tick for `symbol`, advancing its running price via a
    small gaussian random walk.
    """
    # Random walk the price a little; keep it strictly positive.
    drift = random.gauss(0.0, 0.15)
    prices[symbol] = max(0.5, prices[symbol] + drift)
    price = prices[symbol]

    price_change_pct = random.uniform(-0.5, 0.5)
    volume_zscore = clamp(random.gauss(0.0, 1.0), -3.0, 3.0)
    spread_bps = random.uniform(1.0, 10.0)
    order_imbalance = random.uniform(-0.2, 0.2)
    qty = random.randint(100, 3000)
    side = random.choice([1, 2])  # 1 = BUY, 2 = SELL

    return build_fix(
        symbol=symbol,
        side=side,
        price=price,
        qty=qty,
        price_change_pct=price_change_pct,
        volume_zscore=volume_zscore,
        spread_bps=spread_bps,
        order_imbalance=order_imbalance,
        ts_millis=now_millis(),
    )


def make_spoof_tick(symbol):
    """
    Produce one clearly-anomalous tick for `symbol`: heavy one-sided imbalance,
    high volume z-score, oversized quantity, BUY side. Several of these in quick
    succession simulate layering / spoofing.
    """
    drift = random.gauss(0.05, 0.20)  # slight upward push during the burst
    prices[symbol] = max(0.5, prices[symbol] + drift)
    price = prices[symbol]

    price_change_pct = random.uniform(0.8, 1.8)          # noticeable move
    volume_zscore = random.uniform(3.0, 4.5)             # anomalous volume
    spread_bps = random.uniform(8.0, 20.0)               # widening spread
    order_imbalance = random.uniform(0.55, 0.70)         # heavily one-sided
    qty = random.randint(10000, 20000)                   # oversized
    side = 1                                             # all BUY

    return build_fix(
        symbol=symbol,
        side=side,
        price=price,
        qty=qty,
        price_change_pct=price_change_pct,
        volume_zscore=volume_zscore,
        spread_bps=spread_bps,
        order_imbalance=order_imbalance,
        ts_millis=now_millis(),
    )


# --------------------------------------------------------------------------- #
# Main loop                                                                    #
# --------------------------------------------------------------------------- #

def main():
    if not SYMBOLS:
        log("no symbols configured; nothing to do")
        sys.exit(1)

    url = wait_for_ingress()

    interval = 1.0 / RATE_PER_SEC if RATE_PER_SEC > 0 else 0.2
    log(
        f"streaming: rate={RATE_PER_SEC}/s spoof_every={SPOOF_EVERY_SEC}s "
        f"symbols={SYMBOLS}"
    )

    last_spoof = time.monotonic()
    spoof_rotation = 0

    while True:
        loop_start = time.monotonic()

        # Time to inject a spoof burst?
        if (loop_start - last_spoof) >= SPOOF_EVERY_SEC:
            last_spoof = loop_start
            symbol = SYMBOLS[spoof_rotation % len(SYMBOLS)]
            spoof_rotation += 1
            burst = random.randint(4, 6)
            log(f"[spoof] symbol={symbol} burst={burst} (layering/spoofing)")
            for _ in range(burst):
                post_fix(url, make_spoof_tick(symbol))
                # Tight burst -- ticks come fast, but still throttled a touch.
                time.sleep(min(interval, 0.1))
            # Don't fall through to a normal tick this iteration.
            continue

        # Normal tick on a random symbol.
        symbol = random.choice(SYMBOLS)
        post_fix(url, make_normal_tick(symbol))

        # Pace to RATE_PER_SEC, accounting for time already spent this loop.
        elapsed = time.monotonic() - loop_start
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("interrupted; exiting")
