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

from real_web.trader import MultiTrader  # noqa: E402
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

    return jsonify({
        "history": hist[-300:],
        "stats": {
            "total_ms": _stats("total_ms"),
            "avant_post_ms": _stats("avant_post_ms"),
            "baseline_ms": _stats("baseline_ms"),
            "signature_ms": _stats("signature_ms"),
            "post_orders_ms": _stats("post_orders_ms"),
        },
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
