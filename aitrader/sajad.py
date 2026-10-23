"""
sajad.py: wave-2 paper-trading engine following @SajadTrades' memecoin method.

Pipeline (one background thread, every API call goes through one rate limiter):
  1. SCREEN    each minute: one `market trending` call with server-side age/mcap filters,
               then client-side safety gates (total fees, bundlers, top-10, rug, honeypot, dev).
  2. WATCHLIST coins that pass are watched for hours on 5-minute candles (like Sajad's tabs).
  3. SIGNAL    wave-2 setup: wave 1 done, >=40% pullback, support holding, downtrend line from
               the ATH with >=3 touches broken on rising volume, still room below the ATH.
  4. PAPER     every signal becomes a paper trade, re-priced every minute on 1-minute candles:
               hard stop, 50% at ~2x (or just under the ATH), ladder just under ATH / round
               market caps, trailing stop, "it's over" exit, time stops, 20% moon bag.
  5. RESULTS   win rate, profit, monthly run-rate vs the GMGN Plus plan.

Nothing here can place a real trade. It only reads market data and writes local files.
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import pathlib
import random
import re
import shutil
import subprocess
import threading
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

HERE = pathlib.Path(__file__).resolve().parent
OUT_DIR = HERE / "outputs"
STATE_PATH = OUT_DIR / "sajad_state.json"
EVENTS_PATH = OUT_DIR / "sajad_events.jsonl"
CONFIG_OVERRIDE_PATH = HERE / "sajad_config.json"   # optional, edit to tune without touching code
ENV_PATH = pathlib.Path.home() / ".config" / "gmgn" / ".env"
SOL_MINT = "So11111111111111111111111111111111111111112"
RULES_VERSION = 2   # v2: confirmed breakouts, structural stop, 5m-close stops, one loss per coin

# ──────────────────────────────────────────────────────────────────────────
# Config. Every number here comes from Sajad's videos; see the comments.
# Override any key by putting it in aitrader/sajad_config.json.
# ──────────────────────────────────────────────────────────────────────────
DEFAULT_CFG: dict[str, Any] = {
    # Rate limiting. Free plan bucket = 5 weight. We refill slowly to stay far from bans.
    "rate_capacity": 5.0,
    "rate_refill_per_s": 1.0,          # raise to ~4 on the Plus plan
    "weights": {"trending": 3, "kline": 2, "other": 2},

    # Screen ("yesterday's runners" + "same-day wave 2"): age 1h-2d, mcap >= 100k
    "screen_every_s": 60,
    "screen_min_age": "1h",
    "screen_max_age": "48h",
    "screen_min_mcap": 100_000,
    "min_total_fees_sol": 10.0,        # Sajad: 10-15 SOL total fees = legit trading (gas_fee field)
    "max_bundler_rate": 0.30,
    "max_top10_rate": 0.35,
    "max_rug_ratio": 0.30,
    "max_dev_hold_rate": 0.10,

    # Watchlist
    "watch_max": 15,
    "watch_eval_every_s": 180,         # re-read 5m candles per coin every 3 min
    "watch_max_age_h": 96,             # meme life cycle is 3-4 days
    "watch_drop_below_mcap": 40_000,   # dead
    "max_losses_per_coin": 1,          # "if it didn't work, walk away from that coin"
    "min_dev_score": 0.15,             # Token Screen's dev reputation cut-off (serial ruggers, reskins)
    "escape_top10_jump": 0.15,         # escape if top-10 holders rise 15 points after entry

    # Wave-2 detector (5m candles)
    "min_candles": 24,                 # 2h of history
    "wave1_done_candles": 6,           # ATH at least 30 min old
    "min_pullback": 0.40,              # Sajad's examples: 50-86% pullbacks are normal
    "max_pullback": 0.92,              # deeper = dead
    "low_age_candles": 3,              # pullback low at least 15 min old (not still falling)
    "base_zone": 0.08,                 # lows within 8% of the low count as support tests
    "min_base_tests": 2,
    "pivot_window": 2,
    "line_tolerance": 0.05,            # a pivot within 5% (log) of the line counts as a touch
    "min_touches": 3,                  # "tested at least three times = a line you can trust"
    "breakout_margin": 0.01,           # close >= 1% above the line
    "max_extension": 0.25,             # don't chase more than 25% above the line
    "volume_mult": 1.5,                # breakout candle USD volume vs prior 12-candle average
    "max_entry_vs_ath": 0.80,          # need room to run: entry <= 80% of ATH
    "rsi_oversold": 35,

    # Paper trading
    "stake_sol": 0.15,                 # Sajad, small account: 20-40% per trade, >= 0.2 SOL; 0.15 of 0.5
    "max_open_trades": 8,
    "entry_slippage": 0.03,
    "exit_slippage": 0.03,
    "stop_slippage": 0.05,
    "fee_pct": 0.01,                   # per side
    "min_risk": 0.10, "max_risk": 0.20,  # stop 10-20% below entry; skip the trade if it would need more
    "emergency_stop": 0.30,            # mid-candle crash exit; normal stops wait for a 5m close
    "support_buffer": 0.03,
    "tp1_mult": 2.0, "tp1_min_mult": 1.5, "tp1_frac": 0.50,
    "ladder_frac": 0.10, "ladder_count": 3,
    "below_level": 0.94,               # sell just under levels (4.7M under 5M, 18.8M under 20M)
    "ath_target_mult": 0.93,           # "just under the previous all-time high, in the body"
    "round_levels": [250e3, 500e3, 1e6, 2e6, 3e6, 5e6, 10e6, 20e6, 30e6, 50e6, 100e6, 200e6, 500e6, 1e9],
    "trail_pct": 0.30,                 # after TP1, trail the rest 30% under the peak
    "moonbag_trail_pct": 0.40,         # once only the moon bag is left
    "time_stop_h": 6, "time_stop_min_gain": 0.30,
    "max_hold_h": 72,
    "update_every_s": 60,
    "over_lower_high": 0.10,           # "it's over": lower high >=10% under the peak...
    "over_volume_ratio": 0.40,         # ...and volume < 40% of what it was at the peak

    # Results
    "plan_usd_month": 29.0,
    "sol_usd_fallback": 150.0,
}


def load_cfg() -> dict:
    cfg = json.loads(json.dumps(DEFAULT_CFG))
    if CONFIG_OVERRIDE_PATH.exists():
        try:
            cfg.update(json.loads(CONFIG_OVERRIDE_PATH.read_text()))
        except Exception as e:  # bad JSON must not kill the bot
            log_event("config_error", error=str(e))
    return cfg


def _f(v, default=0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _b(v) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes")
    return bool(v)


def log_event(kind: str, **kw):
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with EVENTS_PATH.open("a") as fh:
            fh.write(json.dumps({"ts": time.time(), "kind": kind, **kw}, default=str) + "\n")
    except Exception:
        pass


def fmt_mcap(v: float) -> str:
    if v >= 1e9:
        return f"${v/1e9:.2f}B"
    if v >= 1e6:
        return f"${v/1e6:.2f}M"
    if v >= 1e3:
        return f"${v/1e3:.0f}K"
    return f"${v:.0f}"


# ──────────────────────────────────────────────────────────────────────────
# Rate limiter: one leaky bucket for every GMGN call, plus a hard pause on 429.
# ──────────────────────────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, capacity: float, refill_per_s: float):
        self.capacity = capacity
        self.refill = refill_per_s
        self.tokens = capacity
        self.last = time.monotonic()
        self.paused_until = 0.0          # wall-clock seconds
        self.lock = threading.Lock()
        self.calls = 0
        self.bans = 0

    def _top_up(self):
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.refill)
        self.last = now

    def acquire(self, weight: float):
        with self.lock:
            while True:
                if time.time() < self.paused_until:
                    raise RateLimited(self.paused_until)
                self._top_up()
                if self.tokens >= weight:
                    self.tokens -= weight
                    self.calls += 1
                    return
                time.sleep(max(0.05, (weight - self.tokens) / self.refill))

    def pause(self, seconds: float):
        self.paused_until = max(self.paused_until, time.time() + seconds)
        self.tokens = 0.0
        self.bans += 1


class RateLimited(Exception):
    def __init__(self, until: float):
        super().__init__(f"rate limited until {until}")
        self.until = until


def call_weight(args: list[str], cfg: dict) -> float:
    """Rate-limit weight for a gmgn-cli call (published: trending 3, kline 2; others assumed 2)."""
    head = [a for a in args if not a.startswith("-")][:2]
    if head == ["market", "trending"]:
        return cfg["weights"]["trending"]
    if head == ["market", "kline"]:
        return cfg["weights"]["kline"]
    return cfg["weights"].get("other", 2)


def gate(args: list[str]):
    """Called by app.py before every gmgn-cli call so Token Screen and Wave-2 share one budget."""
    try:
        ENGINE.limiter.acquire(call_weight(args, ENGINE.cfg))
    except RateLimited as e:
        raise RuntimeError("GMGN rate limit: paused until "
                           + time.strftime("%H:%M:%S", time.localtime(e.until))) from None


def note_result(text: str):
    """Called by app.py with gmgn-cli stderr; a 429 pauses every caller until the ban ends."""
    if text and "429" in text:
        m = re.search(r"~(\d+)s remaining", text)
        wait = (int(m.group(1)) if m else 60) + 5
        ENGINE.limiter.pause(wait)
        log_event("rate_limited", wait_s=wait, where="token_screen", msg=text[:300])


# ──────────────────────────────────────────────────────────────────────────
# Data sources
# ──────────────────────────────────────────────────────────────────────────
def _read_env() -> dict:
    out = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                v = v.strip()
                if len(v) >= 2 and v[0] in "\"'" and v[-1] == v[0]:
                    v = v[1:-1]
                out[k.strip()] = v.replace("\\n", "\n")
    return out


def _candles_from(resp) -> list[dict]:
    data = resp.get("data", resp) if isinstance(resp, dict) else resp
    rows = data.get("list") if isinstance(data, dict) else data
    out = []
    for r in rows or []:
        t = _f(r.get("time"))
        if t > 1e12:                       # some endpoints return ms
            t /= 1000.0
        out.append(dict(t=int(t), o=_f(r.get("open")), h=_f(r.get("high")), l=_f(r.get("low")),
                        c=_f(r.get("close")), v=_f(r.get("amount"))))
    out.sort(key=lambda c: c["t"])
    return [c for c in out if c["h"] > 0 and c["l"] > 0]


class LiveSource:
    """Calls the gmgn-cli installed on the VPS. Read-only commands only."""
    name = "live"

    def __init__(self, limiter: RateLimiter, cfg: dict):
        self.limiter = limiter
        self.cfg = cfg
        self.env = {**os.environ, **_read_env()}

    def _run(self, args: list[str], weight: float) -> dict:
        self.limiter.acquire(weight)
        cmd = ["gmgn-cli", *args, "--raw"]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30, env=self.env)
        err = (out.stderr or "") + (out.stdout if out.returncode != 0 else "")
        if out.returncode != 0 or "HTTP 429" in err:
            if "429" in err:
                m = re.search(r"~(\d+)s remaining", err)
                wait = (int(m.group(1)) if m else 60) + 5
                self.limiter.pause(wait)
                log_event("rate_limited", wait_s=wait, msg=err[:300])
                raise RateLimited(time.time() + wait)
            raise RuntimeError(err.strip()[:400] or f"gmgn-cli exit {out.returncode}")
        resp = json.loads(out.stdout)
        if isinstance(resp, dict) and resp.get("code") not in (0, None):
            raise RuntimeError(f"gmgn code={resp.get('code')} {resp.get('message') or resp.get('error')}")
        return resp

    def trending(self) -> list[dict]:
        c = self.cfg
        resp = self._run(["market", "trending", "--chain", "sol", "--interval", "1h",
                          "--order-by", "volume", "--direction", "desc", "--limit", "100",
                          "--filter", "renounced", "--filter", "frozen", "--filter", "not_wash_trading",
                          "--min-created", c["screen_min_age"], "--max-created", c["screen_max_age"],
                          "--min-marketcap", str(int(c["screen_min_mcap"]))],
                         c["weights"]["trending"])
        data = resp.get("data", resp) if isinstance(resp, dict) else {}
        return (data.get("rank") or data.get("tokens") or []) if isinstance(data, dict) else []

    def kline(self, address: str, resolution: str, t_from: int, t_to: int) -> list[dict]:
        resp = self._run(["market", "kline", "--chain", "sol", "--address", address,
                          "--resolution", resolution, "--from", str(int(t_from)), "--to", str(int(t_to))],
                         self.cfg["weights"]["kline"])
        return _candles_from(resp)


class MockSource:
    """Synthetic coins so the dashboard can be tested without a key. Never used once a key exists."""
    name = "mock"

    def __init__(self, limiter: RateLimiter, cfg: dict):
        self.limiter = limiter
        self.cfg = cfg
        self.rng = random.Random(42)
        self.coins: dict[str, dict] = {}
        now = int(time.time()) // 60 * 60
        names = ["BREAKR", "BASED", "STILLDUMP", "WAVE1", "FLATLINE", "LADDER", "DOGWIF2", "NOBASE"]
        for i, name in enumerate(names):
            addr = f"Mock{name}{'x' * (36 - len(name))}pump"[:44]
            supply = 1e9
            path = self._history(i, now)
            self.coins[addr] = dict(symbol=name, supply=supply, path=path, created=now - len(path) * 60,
                                    fees=8 + i * 3, bundler=0.05 + 0.03 * i, top10=0.18 + 0.02 * i)

    def _history(self, i: int, now: int) -> list[list]:
        """1-minute path: pump to ATH, pull back, base, lower highs, and for some coins a breakout."""
        rng = random.Random(100 + i)
        n = 60 * 18
        start, ath = 2e-5, 2e-4 * (1 + i * 0.3)
        low = ath * (0.35 if i != 2 else 0.2)
        out = []
        p = start
        for k in range(n):
            frac = k / n
            if frac < 0.15:
                target = start + (ath - start) * (frac / 0.15) ** 2
            elif frac < 0.35:
                target = ath - (ath - low) * ((frac - 0.15) / 0.20)
            else:
                # basing with lower highs: swings that decay toward the low
                phase = (frac - 0.35) / 0.65
                swing = (ath * 0.55 - low) * (1 - phase) * max(0.0, math.sin(phase * 3.0 * 2 * math.pi))
                target = low * 1.05 + swing
                if i in (0, 5) and frac > 0.97:          # breakout at the end
                    target = low * (1.6 + (frac - 0.97) * 20)
                if i == 2:                                # keeps dumping
                    target = low * (1 - phase * 0.6)
            p = max(target * (1 + rng.uniform(-0.03, 0.03)), 1e-9)
            o = out[-1][4] if out else p
            vol = 2000 + 40000 * (1 if frac < 0.2 else 0.15) * rng.random()
            if i in (0, 5) and frac > 0.97:
                vol *= 6
            out.append([now - (n - k) * 60, o, max(o, p) * 1.01, min(o, p) * 0.99, p, vol])
        return out

    def _advance(self):
        now = int(time.time()) // 60 * 60
        for coin in self.coins.values():
            path = coin["path"]
            while path[-1][0] + 60 <= now:
                o = path[-1][4]
                c = max(o * (1 + self.rng.uniform(-0.04, 0.05)), 1e-9)
                path.append([path[-1][0] + 60, o, max(o, c) * 1.01, min(o, c) * 0.99, c,
                             2000 + 8000 * self.rng.random()])

    def trending(self) -> list[dict]:
        self.limiter.acquire(self.cfg["weights"]["trending"])
        self._advance()
        rows = []
        for addr, coin in self.coins.items():
            last = coin["path"][-1][4]
            ath = max(r[2] for r in coin["path"])
            rows.append(dict(address=addr, symbol=coin["symbol"], price=last,
                             market_cap=last * coin["supply"], history_highest_market_cap=ath * coin["supply"],
                             gas_fee=coin["fees"], creation_timestamp=coin["created"],
                             bundler_rate=coin["bundler"], top_10_holder_rate=coin["top10"],
                             rug_ratio=0.05, is_honeypot=False, dev_team_hold_rate=0.01,
                             volume=sum(r[5] for r in coin["path"][-60:])))
        return [r for r in rows if r["market_cap"] >= 50_000]

    def kline(self, address: str, resolution: str, t_from: int, t_to: int) -> list[dict]:
        self.limiter.acquire(self.cfg["weights"]["kline"])
        self._advance()
        coin = self.coins.get(address)
        if not coin:
            return []
        step = {"1m": 60, "5m": 300}.get(resolution, 300)
        buckets: dict[int, dict] = {}
        for t, o, h, l, c, v in coin["path"]:
            if t < t_from or t > t_to:
                continue
            b = t // step * step
            cur = buckets.get(b)
            if cur is None:
                buckets[b] = dict(t=b, o=o, h=h, l=l, c=c, v=v)
            else:
                cur["h"] = max(cur["h"], h); cur["l"] = min(cur["l"], l); cur["c"] = c; cur["v"] += v
        return [buckets[k] for k in sorted(buckets)]


def live_available() -> bool:
    return bool(_read_env().get("GMGN_API_KEY")) and shutil.which("gmgn-cli") is not None


# ──────────────────────────────────────────────────────────────────────────
# Screen: safety gates on one trending row (no extra API calls)
# ──────────────────────────────────────────────────────────────────────────
def screen_row(row: dict, cfg: dict) -> tuple[bool, str]:
    if _b(row.get("is_honeypot")):
        return False, "honeypot"
    fees = _f(row.get("gas_fee"))
    if fees < cfg["min_total_fees_sol"]:
        return False, f"total fees {fees:.1f} SOL < {cfg['min_total_fees_sol']:.0f}"
    b = _f(row.get("bundler_rate"))
    if b > cfg["max_bundler_rate"]:
        return False, f"bundlers {b:.0%}"
    t10 = _f(row.get("top_10_holder_rate"))
    if t10 > cfg["max_top10_rate"]:
        return False, f"top-10 hold {t10:.0%}"
    rug = _f(row.get("rug_ratio"))
    if rug > cfg["max_rug_ratio"]:
        return False, f"rug score {rug:.2f}"
    dev = _f(row.get("dev_team_hold_rate"))
    if dev > cfg["max_dev_hold_rate"]:
        return False, f"dev holds {dev:.0%}"
    if _f(row.get("price")) <= 0 or _f(row.get("market_cap")) <= 0:
        return False, "no price"
    return True, "ok"


# ──────────────────────────────────────────────────────────────────────────
# Wave-2 detector (pure function, unit-tested)
# ──────────────────────────────────────────────────────────────────────────
def rsi(closes: list[float], n: int = 14) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= n:
        return out
    gains = [max(0.0, closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    losses = [max(0.0, closes[i - 1] - closes[i]) for i in range(1, len(closes))]
    ag = sum(gains[:n]) / n
    al = sum(losses[:n]) / n
    for i in range(n, len(closes)):
        if i > n:
            ag = (ag * (n - 1) + gains[i - 1]) / n
            al = (al * (n - 1) + losses[i - 1]) / n
        out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return out


def detect_wave2(candles: list[dict], cfg: dict) -> dict:
    """Return {stage, detail, signal, metrics}. `candles` = closed 5m candles, oldest first."""
    n = len(candles)
    res = dict(stage="too_new", detail=f"{n} candles, need {cfg['min_candles']}", signal=False, metrics={})
    if n < cfg["min_candles"]:
        return res
    highs = [c["h"] for c in candles]
    lows = [c["l"] for c in candles]
    closes = [c["c"] for c in candles]
    vols = [c["v"] for c in candles]
    L = n - 1

    a = max(range(n), key=lambda i: highs[i])
    ath = highs[a]
    m = res["metrics"]
    m["ath"] = ath
    m["last"] = closes[L]
    if L - a < cfg["wave1_done_candles"]:
        res.update(stage="wave1", detail="wave 1 still running (ATH is recent)")
        return res

    b = min(range(a + 1, n), key=lambda i: lows[i])
    low = lows[b]
    depth = 1 - low / ath
    m["support"] = low
    m["pullback"] = depth
    if depth < cfg["min_pullback"]:
        res.update(stage="shallow", detail=f"pullback {depth:.0%}, waiting for {cfg['min_pullback']:.0%}+")
        return res
    if depth > cfg["max_pullback"]:
        res.update(stage="dead", detail=f"down {depth:.0%} from ATH")
        return res
    if L - b < cfg["low_age_candles"]:
        res.update(stage="falling", detail=f"still making new lows ({depth:.0%} off ATH)")
        return res

    # support tests: separate local lows within base_zone of the low
    zone = low * (1 + cfg["base_zone"])
    tests, last_test = 0, -10
    for i in range(a + 1, n):
        is_local = lows[i] <= lows[i - 1] and (i == L or lows[i] <= lows[i + 1])
        if lows[i] <= zone and is_local and i - last_test >= 3:
            tests += 1
            last_test = i
    m["support_tests"] = tests
    if tests < cfg["min_base_tests"]:
        res.update(stage="basing", detail=f"support tested {tests}/{cfg['min_base_tests']}")
        return res

    # downtrend line from the ATH through later pivot highs (log space, upper hull)
    w = cfg["pivot_window"]
    y = [math.log(h) for h in highs]
    pivots = [i for i in range(a + 1, L - 1)
              if i - w >= 0 and i + w < n and highs[i] >= max(highs[i - w:i + w + 1])]
    slopes = [(y[p] - y[a]) / (p - a) for p in pivots]
    if not slopes or max(slopes) >= 0:
        res.update(stage="no_line", detail="no downtrend line yet")
        return res
    s = max(slopes)

    def line(i: int) -> float:
        return y[a] + s * (i - a)

    tol = cfg["line_tolerance"]
    touches = 1 + sum(1 for p in pivots if abs(y[p] - line(p)) <= tol)
    m["touches"] = touches
    m["line_now"] = math.exp(line(L))
    if touches < cfg["min_touches"]:
        res.update(stage="line_weak", detail=f"downtrend line touched {touches}/{cfg['min_touches']}")
        return res

    margin = math.log(1 + cfg["breakout_margin"])
    above = [math.log(closes[i]) - line(i) for i in range(n)]
    m["vs_line"] = math.exp(above[L]) - 1
    # Sajad: the first poke above the line is often a fake-out. Enter only when the candle after
    # the breakout also closes above the line (the break held / retested and pushed off).
    if above[L] < margin:
        if above[L - 1] >= margin:
            res.update(stage="fakeout", detail="broke the line, then closed back under it (fake-out)")
        else:
            res.update(stage="below_line",
                       detail=(f"{-m['vs_line']:.0%} below the downtrend line" if m["vs_line"] < -0.005
                               else "touching the downtrend line, needs a clean close above it"))
        return res
    if above[L - 1] < margin:
        res.update(stage="breaking_out", detail="broke the line, waiting for one more 5-min close above it")
        return res
    if above[L - 2] >= margin:
        res.update(stage="already_broke", detail="broke out earlier, missed the entry")
        return res
    if m["vs_line"] > cfg["max_extension"]:
        res.update(stage="extended", detail=f"{m['vs_line']:.0%} above the line, too late to chase")
        return res

    B = L - 1                                         # the breakout candle
    prior = vols[max(0, B - 12):B]
    avg = sum(prior) / len(prior) if prior else 0
    m["volume_ratio"] = max(vols[B], vols[L]) / avg if avg > 0 else 0
    if m["volume_ratio"] < cfg["volume_mult"]:
        res.update(stage="weak_volume", detail=f"broke the line on weak volume ({m['volume_ratio']:.1f}x)")
        return res
    if closes[L] > ath * cfg["max_entry_vs_ath"]:
        res.update(stage="no_room", detail="too close to the ATH")
        return res

    # Stop "closely underneath": below the breakout candles and the line. If that is more than
    # max_risk away, the structure is wrong for this method, so skip rather than widen the stop.
    stop_level = min(lows[B], lows[L], math.exp(line(L))) * (1 - cfg["support_buffer"])
    entry_fill = closes[L] * (1 + cfg["entry_slippage"])
    risk = 1 - stop_level / entry_fill
    m["stop_level"] = stop_level
    m["risk"] = risk
    if risk > cfg["max_risk"]:
        res.update(stage="stop_too_far", detail=f"confirmed breakout, but the stop would be {risk:.0%} away (max {cfg['max_risk']:.0%})")
        return res

    r = rsi(closes)
    pull_rsi = [v for v in r[a:L] if v is not None]
    m["rsi_min"] = min(pull_rsi) if pull_rsi else None
    m["oversold"] = bool(pull_rsi) and min(pull_rsi) <= cfg["rsi_oversold"]
    res.update(stage="signal", signal=True,
               detail=(f"confirmed break of a {touches}-touch downtrend line on {m['volume_ratio']:.1f}x volume, "
                       f"{depth:.0%} pullback, support tested {tests}x, stop {risk:.0%} below"
                       + (", RSI was oversold" if m["oversold"] else "")))
    return res


# ──────────────────────────────────────────────────────────────────────────
# Paper trades
# ──────────────────────────────────────────────────────────────────────────
def build_targets(entry: float, supply: float, ath: float, cfg: dict) -> list[dict]:
    """TP1 (50%) at 2x or just under the ATH if that comes first; then 10% chunks under levels."""
    tp1 = max(min(entry * cfg["tp1_mult"], ath * cfg["ath_target_mult"]), entry * cfg["tp1_min_mult"])
    targets = [dict(price=tp1, frac=cfg["tp1_frac"], label="TP1 (half)", hit=False)]
    levels = {ath * cfg["ath_target_mult"]: "under ATH"}
    for lv in cfg["round_levels"]:
        levels[lv * cfg["below_level"] / supply] = f"under {fmt_mcap(lv)}"
    for price in sorted(levels):
        if len(targets) > cfg["ladder_count"]:
            break
        if price > tp1 * 1.10 and price > targets[-1]["price"] * 1.05:   # skip near-duplicates
            targets.append(dict(price=price, frac=cfg["ladder_frac"], label=levels[price], hit=False))
    return targets


def open_trade(coin: dict, det: dict, cfg: dict, now: float) -> dict:
    m = det["metrics"]
    ref = m["last"]
    fill = ref * (1 + cfg["entry_slippage"])
    stop = m.get("stop_level") or m["support"] * (1 - cfg["support_buffer"])
    risk = 1 - stop / fill
    if risk > cfg["max_risk"]:
        stop = fill * (1 - cfg["max_risk"])
    elif risk < cfg["min_risk"]:
        stop = fill * (1 - cfg["min_risk"])
    supply = coin["supply"]
    tokens = cfg["stake_sol"] * (1 - cfg["fee_pct"]) / fill   # "SOL-denominated tokens"
    return dict(
        id=uuid.uuid4().hex[:10], address=coin["address"], symbol=coin["symbol"], supply=supply,
        opened_at=now, entry_ref=ref, entry_fill=fill, entry_mcap=ref * supply,
        stake_sol=cfg["stake_sol"], tokens=tokens, stop=stop, initial_stop=stop,
        targets=build_targets(fill, supply, m["ath"], cfg), remaining=1.0, tp1_done=False,
        peak=fill, last_price=ref, last_check=now - 60, fills=[], realized_sol=0.0,
        status="open", closed_at=None, exit_reason=None, signal=det["detail"], metrics=m,
        recent_1m=[], manual=None,
    )


def _sell(tr: dict, frac: float, price: float, ts: float, reason: str, cfg: dict):
    frac = min(frac, tr["remaining"])
    if frac <= 1e-9:
        return
    proceeds = tr["tokens"] * frac * price * (1 - cfg["fee_pct"])
    tr["remaining"] -= frac
    tr["realized_sol"] += proceeds
    tr["fills"].append(dict(ts=ts, price=price, mcap=price * tr["supply"], frac=frac, reason=reason,
                            sol=proceeds))
    if tr["remaining"] <= 1e-9:
        tr["remaining"] = 0.0
        close_trade(tr, ts, reason)


def close_trade(tr: dict, ts: float, reason: str):
    tr["status"] = "closed"
    tr["closed_at"] = ts
    tr["exit_reason"] = reason


def process_candle(tr: dict, c: dict, cfg: dict):
    """Apply one 1m candle. Pessimistic: the stop is checked before the high."""
    if tr["status"] != "open" or c["t"] + 60 <= tr["last_check"]:
        return
    moon = 1 - sum(t["frac"] for t in tr["targets"])
    if tr["tp1_done"]:
        # in profit: trailing/breakeven stop behaves like a real stop order (fires on the wick)
        if c["o"] <= tr["stop"]:
            _sell(tr, tr["remaining"], c["o"] * (1 - cfg["stop_slippage"]), c["t"], "trailing stop (gap)", cfg)
            return
        if c["l"] <= tr["stop"]:
            _sell(tr, tr["remaining"], tr["stop"] * (1 - cfg["stop_slippage"]), c["t"], "trailing stop", cfg)
            return
    else:
        # Sajad reads 5-minute charts: a brief wick under the stop doesn't count, a 5m close does.
        # Only a real crash (emergency level) exits mid-candle.
        emergency = tr["entry_fill"] * (1 - cfg["emergency_stop"])
        if c["o"] <= emergency:
            _sell(tr, tr["remaining"], c["o"] * (1 - cfg["stop_slippage"]), c["t"], "emergency stop (gap)", cfg)
            return
        if c["l"] <= emergency:
            _sell(tr, tr["remaining"], emergency * (1 - cfg["stop_slippage"]), c["t"], "emergency stop", cfg)
            return
        if c["t"] % 300 == 240 and c["c"] <= tr["stop"]:      # last minute of a 5m candle = its close
            _sell(tr, tr["remaining"], c["c"] * (1 - cfg["stop_slippage"]), c["t"], "stop loss (5-min close)", cfg)
            return
    for t in tr["targets"]:
        if not t["hit"] and c["h"] >= t["price"] and tr["remaining"] - moon > 1e-9:
            t["hit"] = True
            _sell(tr, min(t["frac"], tr["remaining"] - moon), t["price"] * (1 - cfg["exit_slippage"]),
                  c["t"], t["label"], cfg)
            if not tr["tp1_done"]:
                tr["tp1_done"] = True
                tr["stop"] = max(tr["stop"], tr["entry_fill"])           # breakeven
    tr["peak"] = max(tr["peak"], c["h"])
    if tr["tp1_done"]:
        only_moon = all(t["hit"] for t in tr["targets"])
        trail = cfg["moonbag_trail_pct"] if only_moon else cfg["trail_pct"]
        tr["stop"] = max(tr["stop"], tr["peak"] * (1 - trail))
    tr["last_price"] = c["c"]


def escape_reason(snap: dict, row: dict, cfg: dict) -> str | None:
    """Rug signs on a coin we hold (same checks as Token Screen's escape monitor)."""
    if _b(row.get("is_honeypot")):
        return "honeypot detected"
    if row.get("renounced_mint") is not None and not _b(row.get("renounced_mint")) and snap.get("renounced_mint", True):
        return "mint authority no longer renounced"
    t10 = _f(row.get("top_10_holder_rate"), None)
    if t10 is not None and snap.get("top10") is not None and t10 - snap["top10"] >= cfg["escape_top10_jump"]:
        return f"top-10 holders jumped {snap['top10']:.0%} → {t10:.0%}"
    return None


def to_5m(c1: list[dict]) -> list[dict]:
    out: dict[int, dict] = {}
    for c in c1:
        b = c["t"] // 300 * 300
        cur = out.get(b)
        if cur is None:
            out[b] = dict(c, t=b)
        else:
            cur["h"] = max(cur["h"], c["h"]); cur["l"] = min(cur["l"], c["l"]); cur["c"] = c["c"]; cur["v"] += c["v"]
    return [out[k] for k in sorted(out)]


def its_over(c5: list[dict], cfg: dict) -> bool:
    """Lower high after the peak + volume dying (Sajad's exit signs 1 and 3)."""
    if len(c5) < 12:
        return False
    p = max(range(len(c5)), key=lambda i: c5[i]["h"])
    after = c5[p + 1:]
    if len(after) < 4:
        return False
    lower_high = max(c["h"] for c in after[1:]) <= c5[p]["h"] * (1 - cfg["over_lower_high"])
    around = [c["v"] for c in c5[max(0, p - 3):p + 3]]
    recent = [c["v"] for c in c5[-6:]]
    vol_dying = (sum(recent) / len(recent)) < cfg["over_volume_ratio"] * (sum(around) / len(around))
    return lower_high and vol_dying


def update_trade(tr: dict, candles_1m: list[dict], now: float, cfg: dict):
    for c in candles_1m:
        if c["t"] + 60 > now:          # still forming
            continue
        if c["t"] < tr["last_check"]:
            continue
        process_candle(tr, c, cfg)
        tr["recent_1m"].append(c)
        tr["last_check"] = c["t"] + 60
        if tr["status"] != "open":
            return
    tr["recent_1m"] = tr["recent_1m"][-360:]
    if tr["status"] != "open":
        return
    last = tr["last_price"]
    moon = 1 - sum(t["frac"] for t in tr["targets"])
    gain_peak = tr["peak"] / tr["entry_fill"] - 1
    held_h = (now - tr["opened_at"]) / 3600
    if (tr["tp1_done"] or gain_peak >= 0.5) and tr["remaining"] - moon > 1e-9 and its_over(to_5m(tr["recent_1m"]), cfg):
        _sell(tr, tr["remaining"] - moon, last * (1 - cfg["exit_slippage"]), now, "it's over (lower high, volume dying)", cfg)
    if tr["status"] == "open" and held_h >= cfg["time_stop_h"] and gain_peak < cfg["time_stop_min_gain"]:
        _sell(tr, tr["remaining"], last * (1 - cfg["exit_slippage"]), now, f"time stop ({cfg['time_stop_h']}h, no move)", cfg)
    if tr["status"] == "open" and held_h >= cfg["max_hold_h"]:
        _sell(tr, tr["remaining"], last * (1 - cfg["exit_slippage"]), now, "life-cycle exit (3 days)", cfg)


def trade_view(tr: dict, cfg: dict) -> dict:
    unreal = tr["tokens"] * tr["remaining"] * tr["last_price"] * (1 - cfg["fee_pct"]) * (1 - cfg["exit_slippage"])
    value = tr["realized_sol"] + (unreal if tr["status"] == "open" else 0)
    pnl = value - tr["stake_sol"]
    v = {k: tr[k] for k in ("id", "address", "symbol", "opened_at", "closed_at", "status", "exit_reason",
                             "entry_ref", "entry_fill", "entry_mcap", "stake_sol", "stop", "initial_stop",
                             "remaining", "tp1_done", "realized_sol", "signal", "manual")}
    v.update(
        pnl_sol=pnl, pnl_pct=pnl / tr["stake_sol"], last_price=tr["last_price"],
        last_mcap=tr["last_price"] * tr["supply"], stop_mcap=tr["stop"] * tr["supply"],
        peak_gain=tr["peak"] / tr["entry_fill"] - 1, now_gain=tr["last_price"] / tr["entry_fill"] - 1,
        targets=[dict(t, mcap=t["price"] * tr["supply"]) for t in tr["targets"]],
        fills=tr["fills"], hold_h=((tr["closed_at"] or time.time()) - tr["opened_at"]) / 3600,
    )
    if tr.get("manual") and tr["manual"].get("fill_price"):
        mf = tr["manual"]
        you_tokens = mf["size_sol"] * (1 - cfg["fee_pct"]) / mf["fill_price"]
        you_val = sum(you_tokens * f["frac"] * f["price"] for f in tr["fills"]) * (1 - cfg["fee_pct"])
        if tr["status"] == "open":
            you_val += you_tokens * tr["remaining"] * tr["last_price"] * (1 - cfg["fee_pct"])
        v["manual_pnl_sol"] = you_val - mf["size_sol"]
        v["manual_pnl_pct"] = v["manual_pnl_sol"] / mf["size_sol"]
    return v


def compute_stats(trades: list[dict], cfg: dict, started_at: float, sol_usd: float) -> dict:
    views = [trade_view(t, cfg) for t in trades]
    closed = [v for v in views if v["status"] == "closed"]
    opened = [v for v in views if v["status"] == "open"]
    wins = [v for v in closed if v["pnl_sol"] > 0]
    losses = [v for v in closed if v["pnl_sol"] <= 0]
    gross_win = sum(v["pnl_sol"] for v in wins)
    gross_loss = -sum(v["pnl_sol"] for v in losses)
    net = sum(v["pnl_sol"] for v in closed)
    unreal = sum(v["pnl_sol"] for v in opened)
    days = max((time.time() - started_at) / 86400, 1 / 24)
    # a monthly projection from a few hours is meaningless; wait for 2 days of data
    monthly_sol = (net + unreal) / days * 30 if days >= 2 else None
    return dict(
        closed=len(closed), open=len(opened), wins=len(wins), losses=len(losses),
        win_rate=(len(wins) / len(closed)) if closed else None,
        avg_win_pct=(sum(v["pnl_pct"] for v in wins) / len(wins)) if wins else None,
        avg_loss_pct=(sum(v["pnl_pct"] for v in losses) / len(losses)) if losses else None,
        profit_factor=(gross_win / gross_loss) if gross_loss > 0 else None,
        net_sol=net, open_pnl_sol=unreal, best_pct=max((v["pnl_pct"] for v in closed), default=None),
        worst_pct=min((v["pnl_pct"] for v in closed), default=None),
        days_running=days, monthly_sol=monthly_sol,
        monthly_usd=monthly_sol * sol_usd if monthly_sol is not None else None,
        plan_usd=cfg["plan_usd_month"], sol_usd=sol_usd,
        covers_plan=monthly_sol is not None and (monthly_sol * sol_usd) >= cfg["plan_usd_month"] and len(closed) >= 20,
        stake_sol=cfg["stake_sol"],
    )


# ──────────────────────────────────────────────────────────────────────────
# Engine
# ──────────────────────────────────────────────────────────────────────────
class Engine:
    def __init__(self):
        self.cfg = load_cfg()
        self.limiter = RateLimiter(self.cfg["rate_capacity"], self.cfg["rate_refill_per_s"])
        self.lock = threading.RLock()
        self.source = None
        self.state = self._load()
        self.status = dict(last_screen=None, last_error=None, screen=dict(rows=0, passed=0, rejects={}))
        self.next_screen = 0.0
        self.next_sol_price = 0.0
        self._thread: threading.Thread | None = None
        # set by app.py: addr -> (score 0..1 | None, note). Reuses Token Screen's dev reputation code.
        self.dev_checker = None

    # persistence
    def _load(self) -> dict:
        base = dict(started_at=time.time(), watch={}, trades=[], alerts=[], coin_losses={}, sol_usd=None)
        if STATE_PATH.exists():
            try:
                base.update(json.loads(STATE_PATH.read_text()))
            except Exception as e:
                log_event("state_load_error", error=str(e))
        if base.get("rules_version") != RULES_VERSION:
            if base["trades"] or base["alerts"]:
                OUT_DIR.mkdir(parents=True, exist_ok=True)
                archive = OUT_DIR / f"sajad_state_rules_v{base.get('rules_version', 1)}.json"
                archive.write_text(json.dumps(base, default=str))
                log_event("results_archived", file=str(archive), trades=len(base["trades"]))
            base.update(started_at=time.time(), trades=[], alerts=[], coin_losses={})
            for w in base["watch"].values():
                w["last_eval"] = 0
            base["rules_version"] = RULES_VERSION
        return base

    def save(self):
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, default=str))
        tmp.replace(STATE_PATH)

    def pick_source(self):
        want = LiveSource if live_available() else MockSource
        if not isinstance(self.source, want):
            self.source = want(self.limiter, self.cfg)
            log_event("source", name=self.source.name)

    # steps (network calls happen outside the lock so the dashboard never hangs)
    def screen(self, now: float):
        rows = self.source.trending()
        with self.lock:
            self._apply_screen(rows, now, origin="scan")

    def feed(self, rows: list[dict], extra: dict | None = None):
        """Token Screen hands over the coins that passed its own safety gates (no extra API calls).
        `extra` maps address -> {dev_eval, dev_note} when Token Screen already checked the developer."""
        if not rows:
            return
        with self.lock:
            self._apply_screen(rows, time.time(), origin="token_screen", extra=extra or {})

    def _apply_screen(self, rows: list[dict], now: float, origin: str = "scan", extra: dict | None = None):
        passed, rejects = 0, {}
        extra = extra or {}
        by_addr = {}
        for r in rows:
            addr = r.get("address")
            if not addr:
                continue
            by_addr[addr] = r
            ok, why = screen_row(r, self.cfg)
            key = why.split(" ")[0] if not ok else "ok"
            if not ok:
                rejects[key] = rejects.get(key, 0) + 1
                continue
            passed += 1
            if self.state["coin_losses"].get(addr, 0) >= self.cfg["max_losses_per_coin"]:
                continue
            price, mcap = _f(r.get("price")), _f(r.get("market_cap"))
            w = self.state["watch"].get(addr)
            info = dict(address=addr, symbol=str(r.get("symbol") or "?")[:20], supply=mcap / price,
                        mcap=mcap, ath_mcap=_f(r.get("history_highest_market_cap")),
                        fees_sol=_f(r.get("gas_fee")), bundler=_f(r.get("bundler_rate")),
                        top10=_f(r.get("top_10_holder_rate")), volume_1h=_f(r.get("volume")),
                        created=_f(r.get("creation_timestamp")), seen=now,
                        smart=int(_f(r.get("smart_degen_count"))), kol=int(_f(r.get("renowned_count"))),
                        renounced_mint=_b(r.get("renounced_mint")) if r.get("renounced_mint") is not None else True,
                        honeypot=_b(r.get("is_honeypot")), rug=_f(r.get("rug_ratio")))
            if addr in extra:
                info.update(dev_eval=extra[addr].get("dev_eval"), dev_note=extra[addr].get("dev_note"),
                            dev_checked=now)
            if w:
                w.update(info)
                if origin == "token_screen":
                    w["from_token_screen"] = True
            else:
                info.update(added=now, last_eval=0, stage="new", detail="waiting for first chart read",
                            from_token_screen=(origin == "token_screen"))
                self.state["watch"][addr] = info
        self._escape_check(by_addr, now)
        # prune and cap the watchlist: keep the highest-volume coins, never drop one with an open trade
        open_addrs = {t["address"] for t in self.state["trades"] if t["status"] == "open"}
        watch = self.state["watch"]
        for addr in list(watch):
            w = watch[addr]
            age_h = (now - w["created"]) / 3600 if w.get("created") else 0
            if addr in open_addrs:
                continue
            if age_h > self.cfg["watch_max_age_h"] or w["mcap"] < self.cfg["watch_drop_below_mcap"] \
                    or w.get("stage") in ("dead", "dev_reject") or now - w["seen"] > 12 * 3600:
                del watch[addr]
        if len(watch) > self.cfg["watch_max"]:
            ranked = sorted(watch.values(), key=lambda w: (w["address"] in open_addrs, w["volume_1h"]), reverse=True)
            for w in ranked[self.cfg["watch_max"]:]:
                del watch[w["address"]]
        if origin == "scan":
            self.status["screen"] = dict(rows=len(rows), passed=passed, rejects=rejects)
            self.status["last_screen"] = now
        else:
            self.status["last_feed"] = now
            self.status["feed_passed"] = passed

    def _escape_check(self, rows_by_addr: dict, now: float):
        """Escape monitor for paper trades: exit on the rug signs Token Screen watches for."""
        for tr in self.state["trades"]:
            if tr["status"] != "open" or tr["address"] not in rows_by_addr:
                continue
            reason = escape_reason(tr.get("entry_snap") or {}, rows_by_addr[tr["address"]], self.cfg)
            if reason:
                price = _f(rows_by_addr[tr["address"]].get("price")) or tr["last_price"]
                tr["last_price"] = price
                _sell(tr, tr["remaining"], price * (1 - self.cfg["stop_slippage"]), now, "escape: " + reason, self.cfg)
                self._after_close(tr)

    def evaluate(self, addr: str, now: float):
        with self.lock:
            w = self.state["watch"].get(addr)
            created = (w or {}).get("created") or 0
        if not w:
            return
        t_from = int(max(created, now - 48 * 3600))
        candles = self.source.kline(addr, "5m", t_from, int(now))
        with self.lock:
            w = self.state["watch"].get(addr)
            det = self._apply_eval(w, candles, now) if w else None
            need_dev = bool(det) and self.dev_checker is not None and \
                (w.get("dev_eval") is None or now - w.get("dev_checked", 0) > 3600)
        if not det:
            return
        if need_dev:                                   # creator history, only when a signal fires
            try:
                score, note = self.dev_checker(addr)
            except Exception as e:
                score, note = None, f"dev check failed: {str(e)[:80]}"
            with self.lock:
                if addr in self.state["watch"]:
                    w = self.state["watch"][addr]
                    w.update(dev_eval=score, dev_note=note, dev_checked=time.time())
        with self.lock:
            w = self.state["watch"].get(addr)
            if not w:
                return
            score = w.get("dev_eval")
            if score is not None and score < self.cfg["min_dev_score"]:
                w["stage"], w["detail"] = "dev_reject", f"setup found, but creator looks bad: {w.get('dev_note') or 'low dev score'}"
                log_event("signal_dev_reject", symbol=w["symbol"], dev_eval=score, note=w.get("dev_note"))
                return
            self._open_from_signal(w, det, candles, now)

    def _apply_eval(self, w: dict, candles: list[dict], now: float) -> dict | None:
        """Update the coin's stage; return the detection if it is a tradable signal."""
        closed = [c for c in candles if c["t"] + 300 <= now]
        det = detect_wave2(closed, self.cfg)
        w["last_eval"] = now
        w["stage"], w["detail"] = det["stage"], det["detail"]
        if closed:
            w["mcap"] = closed[-1]["c"] * w["supply"]
        if not det["signal"]:
            return None
        if any(t["address"] == w["address"] and t["status"] == "open" for t in self.state["trades"]):
            return None
        if sum(1 for t in self.state["trades"] if t["status"] == "open") >= self.cfg["max_open_trades"]:
            log_event("signal_skipped_max_open", symbol=w["symbol"])
            return None
        det["last_candle_t"] = closed[-1]["t"]
        return det

    def _open_from_signal(self, w: dict, det: dict, candles: list[dict], now: float):
        tr = open_trade(w, det, self.cfg, now)
        tr["last_check"] = det["last_candle_t"] + 300
        tr["entry_snap"] = dict(top10=w.get("top10", 0.0), renounced_mint=w.get("renounced_mint", True))
        self.state["trades"].append(tr)
        alert = dict(id=tr["id"], ts=now, symbol=w["symbol"], address=w["address"], seen=False,
                     detail=det["detail"], entry_price=tr["entry_ref"], entry_mcap=tr["entry_mcap"],
                     stop_mcap=tr["stop"] * w["supply"], stop_pct=1 - tr["stop"] / tr["entry_fill"],
                     targets=[dict(label=t["label"], mcap=t["price"] * w["supply"], frac=t["frac"]) for t in tr["targets"]],
                     stake_sol=tr["stake_sol"], smart=w.get("smart", 0), kol=w.get("kol", 0),
                     dev_eval=w.get("dev_eval"), dev_note=w.get("dev_note"),
                     from_token_screen=w.get("from_token_screen", False))
        self.state["alerts"].insert(0, alert)
        self.state["alerts"] = self.state["alerts"][:100]
        log_event("signal", **{k: alert[k] for k in ("symbol", "address", "entry_mcap", "stop_mcap", "detail")})

    def reprice(self, trade_id: str, now: float):
        with self.lock:
            tr = next((t for t in self.state["trades"] if t["id"] == trade_id), None)
            if not tr or tr["status"] != "open":
                return
            addr, since = tr["address"], int(tr["last_check"]) - 60
        candles = self.source.kline(addr, "1m", since, int(now))
        with self.lock:
            self._apply_reprice(tr, candles, now)

    def _apply_reprice(self, tr: dict, candles: list[dict], now: float):
        before = tr["status"]
        update_trade(tr, candles, now, self.cfg)
        if before == "open" and tr["status"] == "closed":
            self._after_close(tr)

    def _after_close(self, tr: dict):
        pnl = trade_view(tr, self.cfg)["pnl_sol"]
        if pnl <= 0:
            self.state["coin_losses"][tr["address"]] = self.state["coin_losses"].get(tr["address"], 0) + 1
        log_event("closed", symbol=tr["symbol"], reason=tr["exit_reason"], pnl_sol=pnl)

    def refresh_sol_price(self, now: float):
        try:
            c = self.source.kline(SOL_MINT, "1h", int(now) - 3 * 3600, int(now)) if self.source.name == "live" else []
            if c and 10 < c[-1]["c"] < 10_000:          # sanity check before trusting it
                with self.lock:
                    self.state["sol_usd"] = c[-1]["c"]
        except RateLimited:
            raise
        except Exception as e:
            log_event("sol_price_error", error=str(e)[:200])
        self.next_sol_price = now + 3600

    def tick(self):
        now = time.time()
        self.pick_source()
        cfg = self.cfg
        try:
            # open trades first: they matter most
            with self.lock:
                due_trades = [t["id"] for t in self.state["trades"]
                              if t["status"] == "open" and now - t["last_check"] >= cfg["update_every_s"]]
            for tid in due_trades:
                self.reprice(tid, time.time())
            if now >= self.next_screen:
                self.next_screen = now + cfg["screen_every_s"]
                self.screen(time.time())
            with self.lock:
                due = [w for w in self.state["watch"].values()
                       if now - w.get("last_eval", 0) >= cfg["watch_eval_every_s"]]
                pick = min(due, key=lambda w: w.get("last_eval", 0))["address"] if due else None
            if pick:
                self.evaluate(pick, time.time())
            if now >= self.next_sol_price:
                self.refresh_sol_price(now)
        finally:
            with self.lock:
                self.save()

    def run(self):
        while True:
            try:
                self.tick()
                self.status["last_error"] = None
            except RateLimited as e:
                self.status["last_error"] = f"GMGN rate limit: pausing until {time.strftime('%H:%M:%S', time.localtime(e.until))}"
            except Exception as e:
                self.status["last_error"] = str(e)[:300]
                log_event("error", error=str(e)[:500])
            time.sleep(5)

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(target=self.run, daemon=True, name="sajad-engine")
            self._thread.start()

    # views
    def snapshot(self) -> dict:
        with self.lock:
            cfg = self.cfg
            sol_usd = self.state.get("sol_usd") or cfg["sol_usd_fallback"]
            trades = self.state["trades"]
            order = ["signal", "breaking_out", "below_line", "weak_volume", "line_weak", "basing", "no_line",
                     "falling", "shallow", "wave1", "fakeout", "stop_too_far", "extended", "already_broke",
                     "no_room", "too_new", "new", "dev_reject", "dead"]
            watch = sorted(self.state["watch"].values(),
                           key=lambda w: (order.index(w["stage"]) if w.get("stage") in order else 99, -w["volume_1h"]))
            return dict(
                source=self.source.name if self.source else None,
                status=dict(self.status, paused_until=self.limiter.paused_until, api_calls=self.limiter.calls,
                            rate_bans=self.limiter.bans, now=time.time()),
                alerts=self.state["alerts"][:30],
                unseen=sum(1 for a in self.state["alerts"] if not a["seen"]),
                watch=watch,
                open=[trade_view(t, cfg) for t in trades if t["status"] == "open"],
                closed=[trade_view(t, cfg) for t in reversed(trades) if t["status"] == "closed"][:200],
                stats=compute_stats(trades, cfg, self.state["started_at"], sol_usd),
                config={k: cfg[k] for k in ("stake_sol", "min_total_fees_sol", "max_bundler_rate", "max_top10_rate",
                                            "min_pullback", "min_touches", "volume_mult", "tp1_mult", "trail_pct")},
            )


ENGINE = Engine()

# ──────────────────────────────────────────────────────────────────────────
# API
# ──────────────────────────────────────────────────────────────────────────
router = APIRouter(prefix="/api/sajad")


class SeenIn(BaseModel):
    id: str | None = None          # None = mark all seen


class ManualIn(BaseModel):
    trade_id: str
    fill_price: float | None = None   # your real entry price; empty = paper entry
    size_sol: float = 0.1
    clear: bool = False


@router.get("/state")
def api_state():
    return ENGINE.snapshot()


@router.post("/seen")
def api_seen(body: SeenIn):
    with ENGINE.lock:
        for a in ENGINE.state["alerts"]:
            if body.id is None or a["id"] == body.id:
                a["seen"] = True
        ENGINE.save()
    return dict(ok=True)


@router.post("/manual")
def api_manual(body: ManualIn):
    with ENGINE.lock:
        tr = next((t for t in ENGINE.state["trades"] if t["id"] == body.trade_id), None)
        if not tr:
            raise HTTPException(404, "trade not found")
        if body.clear:
            tr["manual"] = None
        else:
            if body.size_sol <= 0:
                raise HTTPException(400, "size must be > 0")
            tr["manual"] = dict(ts=time.time(), size_sol=body.size_sol,
                                fill_price=body.fill_price or tr["entry_fill"])
        ENGINE.save()
    return dict(ok=True)


@router.get("/export.csv")
def api_export():
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["opened_utc", "closed_utc", "symbol", "address", "status", "entry_mcap", "exit_reason",
                "pnl_sol", "pnl_pct", "peak_gain_pct", "hold_h", "you_took_it", "your_pnl_sol", "signal"])
    with ENGINE.lock:
        for t in ENGINE.state["trades"]:
            v = trade_view(t, ENGINE.cfg)
            iso = lambda ts: time.strftime("%Y-%m-%d %H:%M", time.gmtime(ts)) if ts else ""
            w.writerow([iso(v["opened_at"]), iso(v["closed_at"]), v["symbol"], v["address"], v["status"],
                        round(v["entry_mcap"]), v["exit_reason"] or "", round(v["pnl_sol"], 5),
                        round(v["pnl_pct"] * 100, 1), round(v["peak_gain"] * 100, 1), round(v["hold_h"], 2),
                        "yes" if v["manual"] else "", round(v.get("manual_pnl_sol", 0), 5) if v["manual"] else "",
                        v["signal"]])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=memetrader_paper_trades.csv"})
