"""Tests for the wave-2 detector and the paper-trade engine. Run: python -m pytest aitrader/tests -q"""
import math
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import sajad  # noqa: E402

CFG = sajad.DEFAULT_CFG


def wave2_chart(breakout=True, volume_mult=5.0, last_close_mult=1.05, confirm=True):
    """60 x 5m candles: pump to ATH 10, pull back to a base at 2.0 (tested twice),
    lower highs on a downtrend line from the ATH, then (optionally) a breakout on volume."""
    a, s = 10, (math.log(3.0) - math.log(10.0)) / 40
    line = lambda i: math.exp(math.log(10.0) + s * (i - a))
    cs = []
    for i in range(56):
        if i < a:
            p = 1 + 9 * (i / a) ** 2
            c = dict(o=p * 0.95, h=p * 1.02, l=p * 0.93, c=p, v=5000)
        elif i == a:
            c = dict(o=9.5, h=10.0, l=9.0, c=9.6, v=8000)
        else:
            top = line(i) * 0.80                      # normal candles stay well under the line
            c = dict(o=top * 0.95, h=top, l=max(2.3, top * 0.85), c=top * 0.93, v=1000)
            if i in (22, 36):                         # the two later touches of the line
                c.update(h=line(i) * 0.99)
            if i in (25, 40):                         # support tests
                c.update(l=2.0)
            if i < 25:                                # falling leg into the first low
                c.update(l=max(2.05, c["l"]))
        c["t"] = 1_000_000 + i * 300
        cs.append(c)
    L = len(cs) - 1
    if breakout:
        B = L - 1 if confirm else L                   # breakout candle, then (optionally) a confirming close
        close = line(B) * last_close_mult
        cs[B].update(o=line(B) * 0.95, h=close * 1.01, l=line(B) * 0.93, c=close, v=1000 * volume_mult)
        if confirm:
            c2 = line(L) * last_close_mult * 1.02
            cs[L].update(o=close, h=c2 * 1.01, l=line(L) * 0.97, c=c2, v=1000 * volume_mult * 0.8)
    return cs


def test_signal_on_clean_wave2():
    det = sajad.detect_wave2(wave2_chart(), CFG)
    assert det["signal"], det
    assert 0.0 < det["metrics"]["risk"] <= CFG["max_risk"]
    assert det["metrics"]["touches"] >= 3
    assert det["metrics"]["support_tests"] >= 2
    assert 0.75 < det["metrics"]["pullback"] < 0.85


def test_no_signal_without_breakout():
    det = sajad.detect_wave2(wave2_chart(breakout=False), CFG)
    assert not det["signal"]
    assert det["stage"] == "below_line"


def test_no_signal_on_weak_volume():
    det = sajad.detect_wave2(wave2_chart(volume_mult=1.0), CFG)
    assert det["stage"] == "weak_volume"


def test_no_chasing():
    det = sajad.detect_wave2(wave2_chart(last_close_mult=1.6), CFG)
    assert det["stage"] == "extended"


def test_wave1_running():
    cs = wave2_chart()[:13]
    cs += [dict(t=cs[-1]["t"] + 300 * (k + 1), o=9, h=11 + k, l=8.5, c=10 + k, v=9000) for k in range(12)]
    assert sajad.detect_wave2(cs, CFG)["stage"] == "wave1"


def test_still_falling():
    cs = wave2_chart(breakout=False)
    cs[-1].update(l=1.5, c=1.6)                       # fresh new low right now
    assert sajad.detect_wave2(cs, CFG)["stage"] == "falling"


def _trade(entry=1.0, support=0.85, ath=4.0):
    det = dict(metrics=dict(last=entry, support=support, ath=ath), detail="test")
    coin = dict(address="X", symbol="X", supply=1_000_000)
    tr = sajad.open_trade(coin, det, CFG, now=0)
    tr["last_check"] = 0
    return tr


def candle(t, o, h, l, c, v=1000):
    return dict(t=t, o=o, h=h, l=l, c=c, v=v)


def test_stop_loss_limits_loss():
    tr = _trade()
    assert 0.10 <= 1 - tr["stop"] / tr["entry_fill"] <= 0.20
    sajad.update_trade(tr, [candle(0, 1, 1.02, 0.6, 0.65)], now=600, cfg=CFG)
    v = sajad.trade_view(tr, CFG)
    assert tr["status"] == "closed" and tr["exit_reason"].startswith("emergency stop")
    assert -0.40 < v["pnl_pct"] < -0.25


def test_take_profit_moonbag_and_trailing():
    tr = _trade()
    tp1 = tr["targets"][0]["price"]
    seq = [candle(60, 1.0, tp1 * 1.01, 0.98, tp1)]                  # TP1: half sold, stop -> breakeven
    seq += [candle(120, tp1, 3.9, tp1, 3.8)]                        # runs through the ATH target
    seq += [candle(180, 3.8, 3.85, 1.5, 1.6)]                       # collapse: trailing stop
    sajad.update_trade(tr, seq, now=1000, cfg=CFG)
    v = sajad.trade_view(tr, CFG)
    assert tr["targets"][0]["hit"] and tr["tp1_done"]
    assert tr["status"] == "closed" and "trailing" in tr["exit_reason"]
    assert v["pnl_pct"] > 0.5, v["pnl_pct"]


def test_breakeven_after_tp1_never_a_loss():
    tr = _trade()
    tp1 = tr["targets"][0]["price"]
    seq = [candle(60, 1.0, tp1 * 1.01, 0.98, tp1), candle(120, tp1, tp1, 0.5, 0.55)]
    sajad.update_trade(tr, seq, now=1000, cfg=CFG)
    assert sajad.trade_view(tr, CFG)["pnl_sol"] > 0


def test_targets_are_under_levels_and_ordered():
    tr = _trade(entry=0.2, support=0.17, ath=1.0)                    # supply 1M -> entry mcap 200k
    prices = [t["price"] for t in tr["targets"]]
    assert prices == sorted(prices)
    assert abs(sum(t["frac"] for t in tr["targets"]) - 0.8) < 1e-9  # 20% moon bag
    mcaps = [t["price"] * 1_000_000 for t in tr["targets"]]
    assert all(m < 1e6 or abs(m - 940e3) > 1 for m in mcaps)


def test_time_stop():
    tr = _trade()
    flat = [candle(60 * k, 1.0, 1.05, 0.97, 1.0) for k in range(1, 5)]
    sajad.update_trade(tr, flat, now=7 * 3600, cfg=CFG)
    assert tr["status"] == "closed" and tr["exit_reason"].startswith("time stop")


def test_screen_row_gates():
    good = dict(price=0.001, market_cap=300000, gas_fee=14, bundler_rate=0.1, top_10_holder_rate=0.2,
                rug_ratio=0.05, is_honeypot=False, dev_team_hold_rate=0.0)
    assert sajad.screen_row(good, CFG)[0]
    assert not sajad.screen_row(dict(good, gas_fee=3), CFG)[0]
    assert not sajad.screen_row(dict(good, bundler_rate=0.45), CFG)[0]
    assert not sajad.screen_row(dict(good, top_10_holder_rate=0.6), CFG)[0]
    assert not sajad.screen_row(dict(good, is_honeypot=True), CFG)[0]


def test_stats():
    a, b = _trade(), _trade()
    sajad.update_trade(a, [candle(0, 1, 1.02, 0.7, 0.75)], now=600, cfg=CFG)
    tp1 = b["targets"][0]["price"]
    sajad.update_trade(b, [candle(60, 1, tp1 * 1.01, 0.98, tp1), candle(120, tp1, tp1, 0.5, 0.55)], now=600, cfg=CFG)
    st = sajad.compute_stats([a, b], CFG, started_at=0, sol_usd=150)
    assert st["closed"] == 2 and st["wins"] == 1 and st["win_rate"] == 0.5


def test_escape_reasons():
    snap = dict(top10=0.20, renounced_mint=True)
    assert sajad.escape_reason(snap, dict(is_honeypot=1), CFG) == "honeypot detected"
    assert "top-10" in sajad.escape_reason(snap, dict(top_10_holder_rate=0.40), CFG)
    assert sajad.escape_reason(snap, dict(top_10_holder_rate=0.25, renounced_mint=1), CFG) is None
    assert "mint" in sajad.escape_reason(snap, dict(renounced_mint=0), CFG)


def _fresh_engine():
    e = sajad.Engine()
    e.state = dict(started_at=0, watch={}, trades=[], alerts=[], coin_losses={}, sol_usd=None)
    return e


ROW = dict(address="A1", symbol="AAA", price=0.001, market_cap=300000, history_highest_market_cap=900000,
           gas_fee=14, bundler_rate=0.1, top_10_holder_rate=0.2, rug_ratio=0.05, is_honeypot=0,
           renounced_mint=1, creation_timestamp=0, volume=50000, smart_degen_count=5, renowned_count=1)


def test_token_screen_feed_adds_to_watchlist():
    e = _fresh_engine()
    e._apply_screen([ROW], now=1000, origin="token_screen", extra={"A1": dict(dev_eval=0.7, dev_note="ok")})
    w = e.state["watch"]["A1"]
    assert w["from_token_screen"] and w["smart"] == 5 and w["dev_eval"] == 0.7
    assert "last_screen" not in e.status or e.status["last_screen"] is None


def test_escape_closes_open_paper_trade():
    e = _fresh_engine()
    e._apply_screen([ROW], now=1000)
    det = dict(metrics=dict(last=0.001, support=0.00085, ath=0.003), detail="t", last_candle_t=900)
    e._open_from_signal(e.state["watch"]["A1"], det, [], 1000)
    e._apply_screen([dict(ROW, top_10_holder_rate=0.45)], now=1100)
    tr = e.state["trades"][0]
    assert tr["status"] == "closed" and tr["exit_reason"].startswith("escape")


def test_dev_gate_blocks_signal(monkeypatch):
    e = _fresh_engine()
    e._apply_screen([ROW], now=1000)
    e.dev_checker = lambda addr: (0.05, "serial rugger")
    det = dict(signal=True, stage="signal", detail="t", metrics=dict(last=0.001, support=0.00085, ath=0.003),
               last_candle_t=900)
    monkeypatch.setattr(e, "_apply_eval", lambda w, c, now: det)
    class Src:
        name = "mock"
        def kline(self, *a): return []
    e.source = Src()
    e.evaluate("A1", 2000)
    assert not e.state["trades"]
    assert e.state["watch"]["A1"]["stage"] == "dev_reject"


def test_unconfirmed_breakout_waits():
    assert sajad.detect_wave2(wave2_chart(confirm=False), CFG)["stage"] == "breaking_out"


def test_fakeout_is_rejected():
    cs = wave2_chart()
    a, sl = 10, (math.log(3.0) - math.log(10.0)) / 40
    L = len(cs) - 1
    line_L = math.exp(math.log(10.0) + sl * (L - a))
    cs[L].update(c=line_L * 0.9, l=line_L * 0.85)    # second candle falls back under the line
    assert sajad.detect_wave2(cs, CFG)["stage"] == "fakeout"


def test_stop_too_far_is_skipped():
    cs = wave2_chart()
    cs[-2]["l"] = 2.2                                # deep wick (still above the 2.0 base): stop would be >20% away
    assert sajad.detect_wave2(cs, CFG)["stage"] == "stop_too_far"


def test_wick_below_stop_does_not_stop_out():
    tr = _trade()
    stop = tr["stop"]
    # minute 1 of a 5m candle wicks under the stop but closes back above; 5m close (t%300==240) is fine
    seq = [candle(300, 1.0, 1.0, stop * 0.95, 0.99), candle(540, 0.99, 1.0, 0.98, 0.99)]
    sajad.update_trade(tr, seq, now=900, cfg=CFG)
    assert tr["status"] == "open"


def test_five_minute_close_below_stop_exits():
    tr = _trade()
    stop = tr["stop"]
    sajad.update_trade(tr, [candle(540, stop * 1.01, stop * 1.02, stop * 0.97, stop * 0.98)], now=900, cfg=CFG)
    assert tr["status"] == "closed" and tr["exit_reason"] == "stop loss (5-min close)"
