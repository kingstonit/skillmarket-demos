# memetrader

Wil's Solana memecoin paper trader, following @SajadTrades' wave-2 method. Forked from GMGN's `skillmarket-demos/aitrader`.

- `sajad.py`: the strategy engine (screen → watchlist → wave-2 signal → paper trade → results) and its `/api/sajad/*` routes. All Sajad rules and numbers are in `DEFAULT_CFG`; override them in `sajad_config.json` (not tracked by git).
- `static/index.html`: GMGN's dashboard. Token Screen and Wallet Eval are unchanged apart from a slower poll (60s); a third **Wave-2** tab (look for the `memetrader` comments and `w2` prefix) reads `/api/sajad/state`.
- Token Screen feeds its safe coins into the Wave-2 watchlist (`sajad.ENGINE.feed` in `screen_once`), and Wave-2 asks Token Screen's dev reputation code at signal time (`_wave2_dev_check`).
- `app.py`: GMGN's original backend. Kept, with trading locked (`LIVE_TRADING_DISABLED = True`) and its polling slowed for the free plan.
- `tests/test_sajad.py`: detector and paper-engine tests. Run `python -m pytest tests -q`.
- `deploy/memetrader.service`: systemd unit (port 8010, localhost only).

Rules of the road:
- Paper only. Nothing in `sajad.py` places trades. Wil executes by hand in GMGN.
- Every GMGN call, from both tabs, goes through one `RateLimiter` (`sajad.gate` in `LiveGMGN`). The free plan bucket is 5 weight; a 429 pauses all calls until the reset time. Never retry during a ban.
- Data comes from `gmgn-cli` 1.5.2 (`market trending`, `market kline`). Check the CLI source before changing any command or field name.
- Runtime state lives in `outputs/` (`sajad_state.json`, `sajad_events.jsonl`), not in git.
