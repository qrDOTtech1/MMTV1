"""GHOST V3 — interface REELLE dans le NAVIGATEUR (Flask) + temps reel (SSE).

 - MultiTrader : moteur multi-marche (BTC reel / ETH paper, extensible), fill
   verifie on-chain, resolution via settlement Polymarket, plancher 5$, stop 5
   pertes consecutives, sizing dynamique.
 - real_control.api.Api : reutilise UNIQUEMENT pour les courbes/backfill Binance/
   horloge (lecture seule ; son moteur interne n'est jamais demarre ici).

Le trading REEL ne demarre que sur POST /api/start (clic bouton).
"""

import csv
import hashlib
import hmac
import io
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, Response, jsonify, request, send_from_directory  # noqa: E402

from real_web.trader import MultiTrader, SYMBOLS  # noqa: E402
from real_control.api import Api as ReadApi  # noqa: E402 (courbes/horloge/log en lecture seule)

ROOT = Path(__file__).parent
app = Flask(__name__, static_folder=str(ROOT), static_url_path="")
trader = MultiTrader()
reader = ReadApi()  # read-only : courbes, backfill, horloge, log

# AUTH TOKEN (Steven 04/08, deploiement Railway public) : cette API n'avait
# aucune authentification -> acceptable sur 127.0.0.1 local, mais une fois
# exposee publiquement sur Railway, n'importe qui avec l'URL pourrait lire
# tout l'historique de trades OU declencher /api/start /api/stop sur de
# l'argent reel. Verification par token partage (header Authorization:
# Bearer <MMTRADE_API_TOKEN>), comparaison a temps constant (pas de timing
# attack). Si MMTRADE_API_TOKEN n'est pas configure au deploiement, l'API
# refuse TOUT (fail-closed) plutot que de tourner grande ouverte par defaut.
_API_TOKEN = os.environ.get("MMTRADE_API_TOKEN", "")


@app.before_request
def _check_auth():
    if request.path in ("/", "/api/precheck") or request.path.startswith("/static"):
        return None  # healthcheck Railway + page statique : pas de secret expose
    if not _API_TOKEN:
        return jsonify({"error": "MMTRADE_API_TOKEN non configure -> API verrouillee"}), 503
    got = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
    if not hmac.compare_digest(got, _API_TOKEN):
        return jsonify({"error": "unauthorized"}), 401
    return None


@app.route("/")
def index():
    return send_from_directory(str(ROOT), "index.html")


# ── controle (SEULS points qui tradent, sur POST) ──
@app.route("/api/start", methods=["POST"])
def start():
    return jsonify(trader.start())


@app.route("/api/stop", methods=["POST"])
def stop():
    return jsonify(trader.stop())


@app.route("/api/mode", methods=["POST"])
def mode():
    d = request.get_json(force=True)
    return jsonify(trader.set_mode(d.get("symbol"), d.get("mode")))


@app.route("/api/floor", methods=["POST"])
def floor():
    d = request.get_json(force=True)
    return jsonify(trader.set_floor(d.get("floor")))


@app.route("/api/opportunity", methods=["POST"])
def opportunity():
    d = request.get_json(force=True)
    return jsonify(trader.set_opportunity(d.get("symbol"), d.get("enabled")))


@app.route("/api/risk-free", methods=["POST"])
def risk_free():
    d = request.get_json(force=True)
    return jsonify(trader.set_risk_free(d.get("symbol"), d.get("enabled")))


@app.route("/api/ultrapoly", methods=["POST"])
def ultrapoly():
    d = request.get_json(force=True)
    return jsonify(trader.set_ultrapoly(d.get("enabled")))


@app.route("/api/ultrapoly-real", methods=["POST"])
def ultrapoly_real():
    d = request.get_json(force=True)
    return jsonify(trader.set_ultrapoly_real(d.get("enabled")))


@app.route("/api/deltaneutral", methods=["POST"])
def deltaneutral():
    d = request.get_json(force=True)
    return jsonify(trader.set_deltaneutral(d.get("enabled")))


@app.route("/api/raz", methods=["POST"])
def raz():
    return jsonify(trader.raz())


@app.route("/api/arb-budget", methods=["GET", "POST"])
def arb_budget():
    if request.method == "GET":
        return jsonify({"arb_budget": trader.arb_budget()})
    d = request.get_json(force=True)
    return jsonify(trader.set_arb_budget(d.get("arb_budget")))


@app.route("/api/marketmaker", methods=["POST"])
def marketmaker():
    d = request.get_json(force=True)
    return jsonify(trader.set_marketmaker(d.get("enabled")))


@app.route("/api/marketmaker/reset-kill", methods=["POST"])
def marketmaker_reset_kill():
    return jsonify(trader.reset_mm_kill())


@app.route("/api/real-history")
def real_history():
    return jsonify(trader.fetch_real_history())


# ── lecture seule ──
@app.route("/api/precheck")
def precheck():
    return jsonify(trader.precheck())


@app.route("/api/snapshot")
def snapshot():
    return jsonify(trader.snapshot())


@app.route("/api/risk-config")
def risk_config():
    """Expose les seuils SL/TP/sizing (Steven 04/08, 'AUCUN SL TP ????') : ces
    reglages tournent deja depuis longtemps cote bot mais n'etaient visibles
    dans AUCUN dashboard, local ou web -- juste des constantes Python. Lecture
    seule (pas de POST) : ce sont des constantes de code, pas un etat modifiable
    a chaud comme le floor ou le kill-switch."""
    import real_web.trader as _t
    from real_web.trader import MultiTrader as _MT

    return jsonify({
        "tp": {
            "fractions_par_palier": list(_t.PNL_TP_FRACTIONS),
            "cibles_pnl_pct": list(_t.PNL_TP_TARGETS),
            "trailing_activation_pct": _t.PNL_TRAIL_ACTIVATION,
            "trailing_giveback_pct": _t.PNL_TRAIL_GIVEBACK,
        },
        "sl": {
            "seuil_pct": _t.PNL_SL_PCT,
            "secs_left_min": _t.PNL_SL_MIN_SECS_LEFT,
            "poll_intervalle_s": _t.FAST_EXIT_POLL_S,
            "multiplicateur_contextuel_par_symbole": getattr(_MT, "_CTX_SL_MULTIPLIER", {}),
        },
        "orphan": {
            "tp_price": _t.ORPHAN_TP_PRICE,
            "tp_min_profit": _t.ORPHAN_TP_MIN_PROFIT,
            "tp_sell_fraction": _t.ORPHAN_TP_SELL_FRACTION,
        },
        "arb_sl": {
            "secs_left_activation": _t.ARB_SL_SECS_LEFT,
            "bid_threshold": _t.ARB_SL_BID_THRESHOLD,
        },
        "both_side_sl": {
            "prix_seuil": _t.BOTH_SIDE_SL_PRICE,
            "secs_left_min": _t.BOTH_SIDE_SL_MIN_SECS_LEFT,
        },
        "sizing_hedge": {
            "underdog_bet_usd": _t.UNDERDOG_BET_USD,
            "underdog_coverage_mult": _t.DOG_COVERAGE_MULT,
            "favorite_bet_max_usd": _t.FAVORITE_BET_MAX_USD,
            "favorite_target_net_usd": _t.FAV_TARGET_NET_USD,
            "favorite_max_price": _t.FAV_MAX_PRICE,
            "min_calibrated_prob": _t.MIN_CALIBRATED_PROB,
        },
        "kelly": {
            "fraction": _t.KELLY_FRACTION,
            "assumed_edge_fallback": _t.KELLY_ASSUMED_EDGE,
        },
        "binance_ws_sizing": {
            "momentum_boost_mult": _t.BINANCE_MOMENTUM_BOOST,
            "danger_reduce_mult": _t.BINANCE_DANGER_REDUCE,
        },
        "rl_exit": {
            "enabled": _t.RL_EXIT_ENABLED,
            "shadow_mode": _t.RL_EXIT_SHADOW,
            "interval_s": _t.RL_EXIT_INTERVAL_S,
            "min_secs_left": _t.RL_EXIT_MIN_SECS_LEFT,
        },
    })


@app.route("/api/enginebtb3")
def enginebtb3_status():
    """Statut du squelette ENGINEBTB3 (Steven 04/08) : import LOCAL et isole,
    jamais au niveau module -- si ce package casse un jour, ca ne doit jamais
    empecher le bot reel de demarrer. Rien de reel derriere pour l'instant,
    voir enginebtb3/__init__.py et ENGINEBTB3_SPEC.txt."""
    try:
        import enginebtb3
        from enginebtb3 import config as _btb3_config

        return jsonify({
            "status": enginebtb3.STATUS,
            "active": enginebtb3.ACTIVE,
            "markets": _btb3_config.MARKETS,
            "weather_markets": _btb3_config.WEATHER_MARKETS,
        })
    except Exception as e:
        return jsonify({"status": "error", "active": False, "error": str(e)[:200]})


@app.route("/api/execution-quality")
def execution_quality():
    """Fill ratio / EV net de fees / fraicheur des donnees (Steven 04/08,
    '5 metriques prioritaires'). Agrege l'historique capture par
    _record_execution_quality() -- pure lecture, rien ici n'influence le
    trading en cours."""
    hist = trader.state.get("execution_quality_history", [])
    total = len(hist)
    filled = sum(1 for h in hist if h.get("filled"))
    ev_vals = [h.get("ev_net_fees_pct") for h in hist if h.get("ev_net_fees_pct") is not None]
    ev_slip_vals = [h.get("ev_net_slippage_pct") for h in hist if h.get("ev_net_slippage_pct") is not None]
    age_vals = [h.get("feed_age_ms") for h in hist if h.get("feed_age_ms") is not None]
    fillpct_vals = [h.get("fill_pct") for h in hist if h.get("fill_pct") is not None]
    partial = sum(1 for v in fillpct_vals if 0 < v < 100)

    ae_vals, fe_vals = [], []
    for sym in SYMBOLS:
        for t in trader.state["markets"][sym].get("trades", []):
            entry = t.get("entry_price")
            pts = t.get("price_log") or []
            if not entry or not pts:
                continue
            prices = [p.get("price") for p in pts if p.get("price") is not None]
            if not prices:
                continue
            ae_vals.append(round((min(prices) - entry) / entry * 100, 2))
            fe_vals.append(round((max(prices) - entry) / entry * 100, 2))

    return jsonify({
        "history": hist[-300:],
        "stats": {
            "attempted": total,
            "filled": filled,
            "fill_ratio_pct": round(filled / total * 100, 1) if total else None,
            "partial_fill_count": partial,
            "partial_fill_rate_pct": round(partial / len(fillpct_vals) * 100, 1) if fillpct_vals else None,
            "avg_ev_net_fees_pct": round(sum(ev_vals) / len(ev_vals), 2) if ev_vals else None,
            "avg_ev_net_slippage_pct": round(sum(ev_slip_vals) / len(ev_slip_vals), 2) if ev_slip_vals else None,
            "avg_feed_age_ms": round(sum(age_vals) / len(age_vals), 1) if age_vals else None,
            "max_feed_age_ms": max(age_vals) if age_vals else None,
            "avg_adverse_excursion_pct": round(sum(ae_vals) / len(ae_vals), 2) if ae_vals else None,
            "avg_favorable_excursion_pct": round(sum(fe_vals) / len(fe_vals), 2) if fe_vals else None,
        },
    })


@app.route("/api/latency")
def latency():
    """Historique structure des mesures CHRONO + percentiles (Steven 04/08,
    'onglet dedie calcul latence historique'). p50/p95/p99 sur chaque etape
    (pas juste la mediane -- les pics rares sont ceux qui coutent le plus cher
    sur un marche qui bouge en quelques secondes)."""
    hist = trader.state.get("latency_history", [])

    def _pctl(vals, p):
        vals = sorted(v for v in vals if v is not None)
        if not vals:
            return None
        k = (len(vals) - 1) * (p / 100)
        f, c = int(k), min(int(k) + 1, len(vals) - 1)
        if f == c:
            return vals[f]
        return round(vals[f] + (vals[c] - vals[f]) * (k - f), 1)

    def _stats(key):
        vals = [h.get(key) for h in hist]
        return {
            "p50": _pctl(vals, 50),
            "p95": _pctl(vals, 95),
            "p99": _pctl(vals, 99),
            "min": min((v for v in vals if v is not None), default=None),
            "max": max((v for v in vals if v is not None), default=None),
            "count": sum(1 for v in vals if v is not None),
        }

    _rust_flags = [h.get("rust_used") for h in hist if h.get("rust_used") is not None]
    return jsonify({
        "history": hist[-300:],
        "stats": {
            "total_ms": _stats("total_ms"),
            "avant_post_ms": _stats("avant_post_ms"),
            "baseline_ms": _stats("baseline_ms"),
            "signature_ms": _stats("signature_ms"),
            "rust_resign_ms": _stats("rust_resign_ms"),
            "post_orders_ms": _stats("post_orders_ms"),
        },
        "rust_usage_pct": round(sum(1 for f in _rust_flags if f) / len(_rust_flags) * 100, 1) if _rust_flags else None,
    })


@app.route("/api/killswitch", methods=["GET", "POST"])
def killswitch():
    """Seuils du kill-switch global, reglables a chaud (Steven 04/08) : pas
    besoin de redeployer pour changer un seuil. GET = etat actuel + config.
    POST = met a jour un ou plusieurs seuils (garde les autres inchanges)."""
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        ks = trader.state.setdefault("killswitch", {})
        changed = []
        for k in ("enabled", "cash_floor_usd", "max_session_loss_usd", "max_global_consec_losses"):
            if k in body:
                ks[k] = body[k]
                changed.append(f"{k}={body[k]}")
        trader._save()
        if changed:
            try:
                trader._pool.submit(trader._db_save_config_state)
                trader._pool.submit(trader._db_log_config_event, "killswitch_config", ", ".join(changed))
            except Exception:
                pass
    return jsonify({
        "config": trader.state.get("killswitch"),
        "triggered": trader.state.get("killswitch_triggered"),
        "global_consec_losses": trader._global_consec_losses,
        "session_start_cash": trader._session_start_cash,
    })


@app.route("/api/killswitch/reset", methods=["POST"])
def killswitch_reset():
    """Efface le declenchement (Steven 04/08) : ne remet JAMAIS les modes a
    'real' automatiquement -- ca reste une decision humaine explicite, le
    kill-switch remet juste le compteur a zero pour permettre de re-activer
    manuellement les symboles voulus."""
    trader.state["killswitch_triggered"] = None
    trader._global_consec_losses = 0
    trader._save()
    return jsonify({"ok": True})




@app.route("/api/curve")
def curve():
    return jsonify(reader.get_price_curve(int(request.args.get("range", 300))))


@app.route("/api/clock")
def clock():
    return jsonify(reader.get_cycle_clock())


@app.route("/api/log")
def log():
    # n jusqu'a 5000 lignes + filtre texte optionnel q (Steven 22/07 : acces au
    # journal complet depuis le dash, le fichier ne s'efface jamais)
    n = min(int(request.args.get("n", 60)), 5000)
    q = (request.args.get("q") or "").strip().lower()
    lines = reader.get_log_tail(n if not q else 5000)
    if q:
        lines = [ln for ln in lines if q in ln.lower()][-n:]
    return jsonify(lines)


@app.route("/api/logfile")
def logfile():
    """Telecharge le journal COMPLET (fichier brut)."""
    from real_web.trader import LOG_FILE as _LF
    from flask import send_file

    return send_file(
        str(_LF),
        mimetype="text/plain",
        as_attachment=True,
        download_name="ghost_v3_real.log",
    )


# ── TRADES API : pagination, recherche, filtrage, tri, export ──


def _collect_all_trades():
    """Rassemble tous les trades de tous les marches + POLY en une seule liste."""
    all_trades = []
    state = trader.state
    for sym, mk in state.get("markets", {}).items():
        for t in mk.get("trades", []):
            t["_market_key"] = sym
            all_trades.append(t)
    return all_trades


def _parse_ts(val):
    """Parse un timestamp en float (epoch seconds). Accepte epoch ou ISO string."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return None


def _filter_sort_trades(trades, args):
    """Filtre, trie et pagine une liste de trades selon les parametres query."""
    # ── Filtrage ──
    q = (args.get("q") or "").strip().lower()
    symbol = (args.get("symbol") or "").strip().upper()
    mode = (args.get("mode") or "").strip().lower()
    side = (args.get("side") or "").strip().lower()
    strat = (args.get("strat") or "").strip().lower()
    win = args.get("win")
    from_date = _parse_ts(args.get("from_date"))
    to_date = _parse_ts(args.get("to_date"))
    min_pnl = args.get("min_pnl", type=float)
    max_pnl = args.get("max_pnl", type=float)

    filtered = []
    for t in trades:
        if symbol and t.get("symbol", t.get("_market_key", "")).upper() != symbol:
            continue
        if mode and t.get("mode", "").lower() != mode:
            continue
        if side and t.get("side", "").lower() != side:
            continue
        if strat and t.get("strat", "").lower() != strat:
            continue
        if win is not None:
            win_bool = win.lower() in ("1", "true", "yes")
            if bool(t.get("win")) != win_bool:
                continue
        ts = t.get("opened_ts") or t.get("start_ts") or 0
        if from_date and ts < from_date:
            continue
        if to_date and ts > to_date:
            continue
        pnl_val = t.get("pnl")
        if min_pnl is not None and (pnl_val is None or pnl_val < min_pnl):
            continue
        if max_pnl is not None and (pnl_val is None or pnl_val > max_pnl):
            continue
        if q:
            searchable = " ".join(
                str(v)
                for v in [
                    t.get("symbol", ""),
                    t.get("slug", ""),
                    t.get("side", ""),
                    t.get("mode", ""),
                    t.get("strat", ""),
                    t.get("resolved_by", ""),
                    t.get("loss_tag", ""),
                ]
            ).lower()
            if q not in searchable:
                continue
        filtered.append(t)

    # ── Tri ──
    sort_by = (args.get("sort_by") or "opened_ts").strip()
    sort_dir = (args.get("sort_dir") or "desc").strip().lower()
    reverse = sort_dir != "asc"
    valid_sort = {
        "opened_ts",
        "start_ts",
        "pnl",
        "cost",
        "filled_shares",
        "entry_price",
        "symbol",
        "mode",
        "side",
        "win",
        "strat",
    }
    if sort_by not in valid_sort:
        sort_by = "opened_ts"
    filtered.sort(
        key=lambda t: (t.get(sort_by) is None, t.get(sort_by) or 0), reverse=reverse
    )

    total = len(filtered)

    # ── Pagination ──
    page = max(1, args.get("page", 1, type=int))
    per_page = min(200, max(1, args.get("per_page", 25, type=int)))
    start = (page - 1) * per_page
    paged = filtered[start : start + per_page]

    # ── Stats ──
    pnls = [t.get("pnl", 0) for t in filtered]
    wins = [t for t in filtered if t.get("win")]
    stats = {
        "total": total,
        "wins": len(wins),
        "losses": total - len(wins),
        "win_rate": round(len(wins) / total * 100, 1) if total else 0,
        "total_pnl": round(sum(pnls), 3),
        "avg_pnl": round(sum(pnls) / total, 3) if total else 0,
        "best_pnl": round(max(pnls), 3) if pnls else 0,
        "worst_pnl": round(min(pnls), 3) if pnls else 0,
    }

    return paged, total, page, per_page, stats


@app.route("/api/trades")
def api_trades():
    """API paginee des trades avec filtrage, tri et stats."""
    trades = _collect_all_trades()
    paged, total, page, per_page, stats = _filter_sort_trades(trades, request.args)
    return jsonify(
        {
            "ok": True,
            "trades": paged,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": max(1, -(-total // per_page)),
            "stats": stats,
        }
    )


def _fetch_updown5m_events(wallet, max_pages, headers):
    """Pagine data-api.polymarket.com/activity pour un wallet, ne garde que
    les evenements sur les marches Up/Down 5min. Retourne (events, error)."""
    import requests

    events = []
    seen = set()
    for off in range(0, max_pages * 500, 500):
        try:
            r = requests.get(
                "https://data-api.polymarket.com/activity",
                params={"user": wallet, "limit": 500, "offset": off},
                headers=headers,
                timeout=20,
            )
            batch = r.json()
        except Exception as e:
            return None, f"appel Polymarket echoue: {e}"
        if not isinstance(batch, list) or not batch:
            break
        new = 0
        for a in batch:
            if "updown-5m" not in (a.get("slug") or ""):
                continue
            k = (a.get("transactionHash"), a.get("slug"), a.get("outcome"), a.get("timestamp"), a.get("size"), a.get("type"))
            if k in seen:
                continue
            seen.add(k)
            events.append(a)
            new += 1
        if new == 0 and len(batch) < 500:
            break
    return events, None


def _band_of(px):
    for lo, hi, name in (
        (0.0, 0.30, "0.00-0.30"),
        (0.30, 0.50, "0.30-0.50"),
        (0.50, 0.70, "0.50-0.70"),
        (0.70, 0.85, "0.70-0.85"),
        (0.85, 0.90, "0.85-0.90"),
        (0.90, 0.95, "0.90-0.95"),
        (0.95, 0.98, "0.95-0.98"),
        (0.98, 1.01, "0.98-1.00"),
    ):
        if lo <= px < hi:
            return name
    return None


def _analyze_updown5m(updown, min_band_n=3):
    """Coeur d'analyse partage entre /api/copy-analysis et /api/copy-discover :
    prend une liste d'evenements DEJA filtres sur updown-5m et sort les
    metriques (bandes de prix, ROI, usage arb). Compte TOUS les achats, y
    compris ceux sans redeem -- c'est le point qui evite le biais de survie
    trouve deux fois sur l'analyse du wallet de Steven ce soir."""
    ts_all = [a["timestamp"] for a in updown if a.get("timestamp")]
    days_active = round((max(ts_all) - min(ts_all)) / 86400, 1) if ts_all else 0

    legs = {}
    sides_by_slug = {}
    redeem_by_slug = {}
    for a in updown:
        slug = a.get("slug")
        if a.get("type") == "REDEEM":
            redeem_by_slug[slug] = redeem_by_slug.get(slug, 0.0) + (a.get("usdcSize") or 0.0)
            continue
        if a.get("type") != "TRADE":
            continue
        k = (slug, a.get("outcome"))
        e = legs.setdefault(k, {"buy_usd": 0.0, "buy_sh": 0.0, "sell_usd": 0.0})
        if a.get("side") == "BUY":
            e["buy_usd"] += a.get("usdcSize") or 0.0
            e["buy_sh"] += a.get("size") or 0.0
            sides_by_slug.setdefault(slug, set()).add(a.get("outcome"))
        else:
            e["sell_usd"] += a.get("usdcSize") or 0.0

    bands = {}
    n_paired_legs = 0
    n_solo_legs = 0
    total_cost = 0.0
    total_return = 0.0
    for (slug, outcome), e in legs.items():
        if e["buy_sh"] <= 0.05 or e["buy_usd"] <= 0:
            continue
        avg_px = e["buy_usd"] / e["buy_sh"]
        if not (0.01 < avg_px < 0.99):
            continue
        redeem = redeem_by_slug.get(slug, 0.0)
        won = redeem > 0 and abs(redeem - e["buy_sh"]) < max(0.6, 0.3 * e["buy_sh"])
        ret = e["sell_usd"] + (e["buy_sh"] if won else 0.0)
        total_cost += e["buy_usd"]
        total_return += ret
        paired = len(sides_by_slug.get(slug, set())) == 2
        if paired:
            n_paired_legs += 1
        else:
            n_solo_legs += 1
        bname = _band_of(avg_px)
        if bname is None:
            continue
        b = bands.setdefault(bname, {"n": 0, "win": 0, "cost": 0.0, "ret": 0.0, "n_solo": 0})
        b["n"] += 1
        b["win"] += 1 if won else 0
        b["cost"] += e["buy_usd"]
        b["ret"] += ret
        if not paired:
            b["n_solo"] += 1

    band_rows = []
    for name in ("0.00-0.30", "0.30-0.50", "0.50-0.70", "0.70-0.85", "0.85-0.90", "0.90-0.95", "0.95-0.98", "0.98-1.00"):
        b = bands.get(name)
        if not b or b["n"] < min_band_n:
            continue
        band_rows.append(
            {
                "band": name,
                "n": b["n"],
                "win_rate_pct": round(100 * b["win"] / b["n"], 1),
                "cost": round(b["cost"], 2),
                "roi_pct": round(100 * (b["ret"] - b["cost"]) / b["cost"], 1) if b["cost"] else None,
                "solo_pct": round(100 * b["n_solo"] / b["n"], 1),
            }
        )

    total_legs = n_paired_legs + n_solo_legs
    nc = bands.get("0.95-0.98")
    return {
        "events_updown_5m": len(updown),
        "days_active": days_active,
        "total_cost_usd": round(total_cost, 2),
        "total_return_usd": round(total_return, 2),
        "overall_roi_pct": round(100 * (total_return - total_cost) / total_cost, 1) if total_cost else None,
        "arb_usage_pct": round(100 * n_paired_legs / total_legs, 1) if total_legs else None,
        "bands": band_rows,
        "near_cert_0_95_0_98": (
            {
                "n": nc["n"],
                "win_rate_pct": round(100 * nc["win"] / nc["n"], 1),
                "roi_pct": round(100 * (nc["ret"] - nc["cost"]) / nc["cost"], 1) if nc["cost"] else None,
            }
            if nc
            else None
        ),
    }


@app.route("/api/copy-analysis")
def api_copy_analysis():
    """ANALYSE COPY-TRADING (Steven 05/08, "inclu une fenetre dediee a
    l'analyse copy dans dash").

    Steven fournit un wallet (trouve via l'UI Polymarket, ou via
    /api/copy-discover ci-dessous) -- l'endpoint fait le meme travail de
    reconstruction on-chain qu'on a fait ce soir sur SON propre wallet :
    telecharge l'activite complete par pagination, ne garde que les marches
    "updown-5m" (le terrain de jeu du bot), et sort EXACTEMENT les memes
    metriques qui ont deja servi a diagnostiquer le bot cette nuit.

    Lecture seule, aucune ecriture, aucun ordre. Le wallet est un parametre
    utilisateur, jamais invente ni stocke."""
    import re

    wallet = (request.args.get("wallet") or "").strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet):
        return jsonify({"ok": False, "error": "adresse wallet invalide (attendu 0x + 40 hex)"}), 400

    try:
        max_pages = max(1, min(20, int(request.args.get("max_pages", 10))))
    except (TypeError, ValueError):
        max_pages = 10

    updown, err = _fetch_updown5m_events(wallet, max_pages, {"User-Agent": "Mozilla/5.0"})
    if err:
        return jsonify({"ok": False, "error": err}), 502
    events_seen_total = len(updown)  # deja filtre updown-only par _fetch_updown5m_events
    if not updown:
        return jsonify(
            {
                "ok": True,
                "wallet": wallet,
                "events_total": events_seen_total,
                "events_updown_5m": 0,
                "message": "aucune activite sur les marches Up/Down 5min pour ce wallet",
            }
        )
    result = _analyze_updown5m(updown)
    result.update({"ok": True, "wallet": wallet, "events_total": events_seen_total})
    return jsonify(result)


_copy_discover_cache = {"ts": 0.0, "data": None}
COPY_DISCOVER_TTL = 900  # 15min : le classement d'une session de scan ne bouge pas vite


@app.route("/api/copy-discover")
def api_copy_discover():
    """DECOUVERTE DE TRADERS 5MIN CRYPTO (Steven 05/08, "pour voir le
    leaderboard faut utiliser une cle je crois mais on peut y acceder").

    Verifie : lb-api.polymarket.com/profit repond SANS cle -- l'echec
    precedent venait d'un mauvais chemin d'URL (/leaderboard au lieu de
    /profit, deja utilise par core/copytrade.py). MAIS ce leaderboard est
    GLOBAL (tous marches confondus) : teste sur les 40 premiers traders 7j,
    1 SEUL touchait meme un peu au 5min crypto (5 evenements sur 500). Les
    gros traders du leaderboard general font leur volume sur sport/politique,
    pas sur cette niche -- le leaderboard global est donc inutile ici.

    Mecanisme retenu : data-api.polymarket.com/trades?market=<conditionId>
    liste TOUS les traders d'un marche donne, sans cle (verifie : 148
    wallets distincts sur un seul marche BTC 5min de 200 trades). On
    echantillonne plusieurs marches 5min recents (plusieurs symboles), on
    collecte les wallets les plus actifs dessus, puis on lance sur chacun la
    meme analyse que /api/copy-analysis (courte : peu de pages) pour ne
    garder que ceux avec un ROI positif et un echantillon suffisant.

    Resultat mis en cache 15min : scanner reste couteux (dizaines d'appels
    Polymarket), pas quelque chose a refaire a chaque chargement de page."""
    import requests

    now = time.time()
    force = request.args.get("refresh") == "1"
    if not force and _copy_discover_cache["data"] and (now - _copy_discover_cache["ts"] < COPY_DISCOVER_TTL):
        cached = dict(_copy_discover_cache["data"])
        cached["cached"] = True
        cached["cache_age_s"] = round(now - _copy_discover_cache["ts"])
        return jsonify(cached)

    headers = {"User-Agent": "Mozilla/5.0"}
    syms = ["btc", "eth", "sol", "xrp", "doge"]
    base = int(now // 300) * 300
    wallet_freq = {}
    markets_scanned = 0
    for sym in syms:
        for off in (-300, -600, -900):
            slug = f"{sym}-updown-5m-{base + off}"
            try:
                m = requests.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"slug": slug}, headers=headers, timeout=15,
                ).json()
            except Exception:
                continue
            mk = m[0] if isinstance(m, list) and m else None
            cid = mk.get("conditionId") if mk else None
            if not cid:
                continue
            try:
                trs = requests.get(
                    "https://data-api.polymarket.com/trades",
                    params={"market": cid, "limit": 200}, headers=headers, timeout=15,
                ).json()
            except Exception:
                continue
            if not isinstance(trs, list):
                continue
            markets_scanned += 1
            for t in trs:
                w = t.get("proxyWallet")
                if w:
                    wallet_freq[w] = wallet_freq.get(w, 0) + 1

    # les plus actifs d'abord : plus de signal, moins d'appels gaspilles sur
    # des wallets qui n'ont fait qu'un trade de passage
    candidates = sorted(wallet_freq.items(), key=lambda kv: -kv[1])[:25]

    results = []
    for wallet, freq in candidates:
        updown, err = _fetch_updown5m_events(wallet, max_pages=2, headers=headers)
        if err or not updown:
            continue
        an = _analyze_updown5m(updown, min_band_n=1)
        if an["total_cost_usd"] < 20 or an["overall_roi_pct"] is None:
            continue  # echantillon trop petit pour juger
        results.append({"wallet": wallet, "trades_seen_in_scan": freq, **an})

    results.sort(key=lambda r: -(r["overall_roi_pct"] or -999))
    payload = {
        "ok": True,
        "markets_scanned": markets_scanned,
        "wallets_seen": len(wallet_freq),
        "wallets_analyzed": len(candidates),
        "candidates": results[:15],
        "cached": False,
    }
    _copy_discover_cache["ts"] = now
    _copy_discover_cache["data"] = payload
    return jsonify(payload)


@app.route("/api/arb-quality")
def api_arb_quality():
    """QUALITE DES PAIRES D'ARB (Steven 05/08).

    Le PnL seul ne dit pas SI une paire etait un vrai arb. Cet endpoint
    mesure ce qui compte reellement, par paire :
      - combined NOMINAL  = somme des prix d'entree des 2 jambes
      - combined EFFECTIF = cout total / payout du pire cas
        (le gagnant paie 1$ PAR PART, donc le pire cas vaut min(parts))
      - verrouillee       = payout du pire cas > cout total
      - desequilibre      = max(parts)/min(parts), 1.0 = parfait

    L'ecart entre nominal et effectif isole exactement ce que coute un
    mauvais sizing : mesure sur l'historique on-chain du 05/08, median
    nominal 1.032 contre effectif 1.325, soit +0.293 de perte imputable au
    seul desequilibre de parts."""
    pairs = {}
    state = trader.state
    for sym, mk in state.get("markets", {}).items():
        rows = list(mk.get("trades", [])) + list(mk.get("open", {}).values())
        for t in rows:
            slug = t.get("slug")
            side = t.get("side")
            if not slug or not side or side == "ARB":
                continue
            if t.get("mode") != "real":
                continue
            sh = t.get("filled_shares") or 0
            if sh <= 0:
                continue
            p = pairs.setdefault(
                slug, {"slug": slug, "symbol": sym, "legs": {}, "opened_ts": t.get("opened_ts")}
            )
            leg = p["legs"].setdefault(side, {"shares": 0.0, "cost": 0.0})
            leg["shares"] += sh
            leg["cost"] += t.get("cost") or (sh * (t.get("entry_price") or 0))
            if t.get("is_risk_free"):
                p["tagged_risk_free"] = True
            if t.get("arb_locked") is not None:
                p["arb_locked_flag"] = t.get("arb_locked")

    out = []
    for slug, p in pairs.items():
        legs = p["legs"]
        if len(legs) != 2:
            continue
        (s1, l1), (s2, l2) = list(legs.items())
        worst = min(l1["shares"], l2["shares"])
        cost = l1["cost"] + l2["cost"]
        if worst <= 0 or cost <= 0:
            continue
        px1 = l1["cost"] / l1["shares"] if l1["shares"] else 0
        px2 = l2["cost"] / l2["shares"] if l2["shares"] else 0
        out.append(
            {
                "slug": slug,
                "symbol": p["symbol"],
                "opened_ts": p.get("opened_ts"),
                "combined_nominal": round(px1 + px2, 4),
                "combined_effective": round(cost / worst, 4),
                "locked": worst > cost,
                "lock_margin": round(worst - cost, 3),
                "imbalance": round(
                    max(l1["shares"], l2["shares"]) / min(l1["shares"], l2["shares"]), 3
                )
                if min(l1["shares"], l2["shares"]) > 0
                else None,
                "cost": round(cost, 2),
                "worst_payout": round(worst, 2),
                "tagged_risk_free": bool(p.get("tagged_risk_free")),
            }
        )

    out.sort(key=lambda r: r.get("opened_ts") or 0, reverse=True)
    n = len(out)
    nlock = sum(1 for r in out if r["locked"])

    def _median(vals):
        v = sorted(x for x in vals if x is not None)
        if not v:
            return None
        mid = len(v) // 2
        return round(v[mid] if len(v) % 2 else (v[mid - 1] + v[mid]) / 2, 4)

    # Incoherence a surveiller : paire tagguee risk-free alors qu'elle n'est
    # PAS verrouillee -> elle serait exemptee de TP/SL sans raison.
    mislabeled = [r for r in out if r["tagged_risk_free"] and not r["locked"]]
    return jsonify(
        {
            "ok": True,
            "pairs": out[:200],
            "summary": {
                "total_pairs": n,
                "locked_pairs": nlock,
                "lock_rate_pct": round(100 * nlock / n, 1) if n else None,
                "median_combined_nominal": _median([r["combined_nominal"] for r in out]),
                "median_combined_effective": _median([r["combined_effective"] for r in out]),
                "median_imbalance": _median([r["imbalance"] for r in out]),
                "mislabeled_risk_free": len(mislabeled),
                "guaranteed_margin_total": round(
                    sum(r["lock_margin"] for r in out if r["locked"]), 2
                ),
            },
        }
    )


@app.route("/api/history-summary")
def api_history_summary():
    """Resume par HEURE sur plusieurs heures (Steven 05/08, 'il faut que tu
    puisse voir + de historique, genre resumer plusieurs heures') : le
    journal texte ne garde que ~5000 lignes (~40min a ce rythme de log) --
    inutilisable pour une vue longue duree. Ceci reconstruit a la demande
    depuis mk['trades'] (deja en memoire pour toute la session, jamais
    purge), donc dispo immediatement, pas besoin d'attendre que des heures
    s'accumulent apres ce deploiement. Parametre ?hours=N (defaut 12)."""
    try:
        hours = max(1, min(72, int(request.args.get("hours", 12))))
    except (TypeError, ValueError):
        hours = 12
    cutoff = time.time() - hours * 3600
    trades = [t for t in _collect_all_trades() if (t.get("opened_ts") or 0) >= cutoff]

    buckets = {}
    for t in trades:
        ts = t.get("opened_ts") or 0
        bucket_ts = int(ts // 3600) * 3600
        b = buckets.setdefault(
            bucket_ts,
            {
                "bucket_start": bucket_ts,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl": 0.0,
                "by_symbol": {},
                "by_strat": {},
                "by_resolved_by": {},
            },
        )
        pnl = t.get("pnl") or 0
        b["trades"] += 1
        b["pnl"] = round(b["pnl"] + pnl, 4)
        if pnl > 0:
            b["wins"] += 1
        elif pnl < 0:
            b["losses"] += 1
        sym = t.get("_market_key", "?")
        strat = t.get("strat", "?")
        rb = t.get("resolved_by", "?")
        b["by_symbol"][sym] = round(b["by_symbol"].get(sym, 0) + pnl, 4)
        b["by_strat"][strat] = round(b["by_strat"].get(strat, 0) + pnl, 4)
        b["by_resolved_by"][rb] = round(b["by_resolved_by"].get(rb, 0) + pnl, 4)

    bucket_list = sorted(buckets.values(), key=lambda b: b["bucket_start"])
    total_pnl = round(sum(b["pnl"] for b in bucket_list), 4)
    total_trades = sum(b["trades"] for b in bucket_list)
    total_wins = sum(b["wins"] for b in bucket_list)
    return jsonify(
        {
            "ok": True,
            "hours_requested": hours,
            "buckets": bucket_list,
            "summary": {
                "total_trades": total_trades,
                "total_wins": total_wins,
                "win_rate_pct": round(total_wins / total_trades * 100, 1) if total_trades else None,
                "total_pnl": total_pnl,
            },
        }
    )


@app.route("/api/trades/export")
def api_trades_export():
    """Exporte les trades en CSV ou JSON (meme filtres que /api/trades)."""
    fmt = (request.args.get("format") or "csv").lower()
    trades = _collect_all_trades()
    paged, total, page, per_page, stats = _filter_sort_trades(trades, request.args)

    if fmt == "json":
        return jsonify({"ok": True, "trades": paged, "total": total, "stats": stats})

    # CSV
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(
        [
            "symbol",
            "slug",
            "mode",
            "side",
            "strat",
            "win",
            "pnl",
            "entry_price",
            "filled_shares",
            "cost",
            "opened_ts",
            "end_ts",
            "start_ts",
            "resolved_by",
            "loss_tag",
            "realized_pnl",
        ]
    )
    for t in paged:
        cw.writerow(
            [
                t.get("symbol", t.get("_market_key", "")),
                t.get("slug", ""),
                t.get("mode", ""),
                t.get("side", ""),
                t.get("strat", ""),
                t.get("win", ""),
                t.get("pnl", ""),
                t.get("entry_price", ""),
                t.get("filled_shares", ""),
                t.get("cost", ""),
                t.get("opened_ts", ""),
                t.get("end_ts", ""),
                t.get("start_ts", ""),
                t.get("resolved_by", ""),
                t.get("loss_tag", ""),
                t.get("realized_pnl", ""),
            ]
        )
    output = si.getvalue()
    return Response(
        "\ufeff" + output,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=ghost_trades_export.csv"},
    )


@app.route("/api/positions-stats")
def api_positions_stats():
    """Statistiques detaillees des positions ouvertes."""
    all_open = []
    state = trader.state
    for sym, mk in state.get("markets", {}).items():
        for p in mk.get("open", {}).values():
            p["_market_key"] = sym
            all_open.append(p)
    # POLY pairs
    pm = state.get("markets", {}).get("POLY", {})
    for p in pm.get("open", {}).values():
        p["_market_key"] = "POLY"
        all_open.append(p)

    total_cost = sum(p.get("cost", 0) for p in all_open)
    total_shares = sum(p.get("filled_shares", 0) for p in all_open)
    by_mode = {}
    by_symbol = {}
    by_side = {"Up": 0, "Down": 0}
    for p in all_open:
        m = p.get("mode", "?")
        s = p.get("_market_key", "?")
        sd = p.get("side", "?")
        by_mode[m] = by_mode.get(m, 0) + 1
        by_symbol[s] = by_symbol.get(s, 0) + 1
        by_side[sd] = by_side.get(sd, 0) + 1

    return jsonify(
        {
            "ok": True,
            "total_positions": len(all_open),
            "total_cost": round(total_cost, 2),
            "total_shares": round(total_shares, 2),
            "by_mode": by_mode,
            "by_symbol": by_symbol,
            "by_side": by_side,
            "positions": all_open,
        }
    )


# ── SSE : push temps reel uniquement quand l'etat change ──

_STATE_SNAPSHOT_INTERVAL = 1.0  # verification fingerprint chaque 1s
_STATE_CLOCK_INTERVAL = 5.0  # push clock toutes les 5s
_STATE_LOG_INTERVAL = 2.0  # push log toutes les 2s
_LOG_LINES_PUSH = 30  # dernieres lignes de log a pusher


def _state_fingerprint(state):
    """Empreinte legere de l'etat critique — ne change que quand
    un evenement metier se produit (trade, position, mode, stop…)."""
    parts = ["R" if state.get("running") else "F"]
    for sym in ("BTC", "ETH", "SOL", "XRP", "DOGE", "POLY"):
        mk = state.get("markets", {}).get(sym, {})
        # PRIX LIVE (fix : le fingerprint ne captait que des COMPTEURS -> une
        # position ouverte pouvait perdre 80% sans qu'aucun push SSE ne parte,
        # le dash restait fige sur le prix d'ouverture jusqu'au prochain
        # trade/close ailleurs). Le prix courant vient de price_log (WS).
        _open_px = "|".join(
            f"{p.get('side','')}:{(p.get('price_log') or [{}])[-1].get('price','')}"
            for p in mk.get("open", {}).values()
        )
        parts.append(
            f"{sym}:{len(mk.get('trades', []))}"
            f":{len(mk.get('open', {}))}"
            f":{mk.get('consec_losses', 0)}"
            f":{1 if mk.get('stopped') else 0}"
            f":{mk.get('mode', '')}"
            f":{_open_px}"
        )
    parts.append(f"C:{state.get('cash_usdc')}")
    parts.append(f"U:{1 if state.get('ultrapoly') else 0}")
    parts.append(f"UR:{1 if state.get('ultrapoly_real') else 0}")
    parts.append(f"DN:{1 if state.get('dn_enabled') else 0}")
    mm = state.get("mm", {})
    parts.append(f"MM:{1 if mm.get('enabled') else 0}:{1 if mm.get('killed') else 0}")
    return hashlib.md5("|".join(parts).encode()).hexdigest()


@app.route("/api/stream")
def stream():
    """Server-Sent Events : push snapshot, clock et log quand ca change."""
    from flask import Response as _Resp

    def _generate():
        last_fp = None
        last_clock_ts = 0
        last_log_ts = 0

        while True:
            now = time.time()

            # ── snapshot push (des que fingerprint change) ──
            try:
                fp = _state_fingerprint(trader.state)
                if fp != last_fp:
                    last_fp = fp
                    data = json.dumps(trader.snapshot(), separators=(",", ":"))
                    yield f"event: snapshot\ndata: {data}\n\n"
            except Exception:
                pass

            # ── clock push (toutes les N secondes) ──
            if now - last_clock_ts >= _STATE_CLOCK_INTERVAL:
                last_clock_ts = now
                try:
                    clk = reader.get_cycle_clock()
                    data = json.dumps(clk, separators=(",", ":"))
                    yield f"event: clock\ndata: {data}\n\n"
                except Exception:
                    pass

            # ── log push (dernieres lignes, toutes les N secondes) ──
            if now - last_log_ts >= _STATE_LOG_INTERVAL:
                last_log_ts = now
                try:
                    lines = reader.get_log_tail(_LOG_LINES_PUSH)
                    if lines:
                        data = json.dumps(lines)
                        yield f"event: log\ndata: {data}\n\n"
                except Exception:
                    pass

            # ── heartbeat toutes les 30s (keepalive proxy/nginx) ──
            yield ": heartbeat\n\n"

            time.sleep(_STATE_SNAPSHOT_INTERVAL)

    return _Resp(
        _generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    # 0.0.0.0 + $PORT (Steven 04/08, service Railway dedie MMTV1, separe de
    # DetailDesk) : joignable via le domaine public Railway, jamais en
    # 127.0.0.1 (injoignable de l'exterieur du conteneur).
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8787)), threaded=True, debug=False)
