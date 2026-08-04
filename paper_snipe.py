#!/usr/bin/env python3
"""GHOST V3 — vrai paper trading du sniper BTC/ETH Up/Down 5min.

Contrairement a main.py/demo.py (trouves sur le Bureau) qui affectaient un
gain/perte ARBITRAIRE a chaque signal sans jamais verifier si la prediction
etait juste, ce script:
  1) detecte un signal via evaluate() (core/btc_updown.py),
  2) ouvre une position VIRTUELLE au prix reel du marche (outcomePrices),
  3) a la resolution de la fenetre, verifie le VRAI resultat (bougie Binance
     1min a end_ts vs strike) et calcule le vrai P&L,
  4) persiste tout (positions ouvertes/fermees, solde) dans data/paper_state.json
     pour survivre aux redemarrages,
  5) log detaille dans data/ghost_v3_paper.log.

Objectif: valider que le "strike" utilise est correct AVANT tout passage en reel.
"""
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests

from core.btc_updown import find_active_markets, parse_updown_market, evaluate, _strike_at, _binance_price

ROOT = Path(__file__).parent
STATE_FILE = ROOT / "data" / "paper_state.json"
LOG_FILE = ROOT / "data" / "ghost_v3_paper.log"
LOCK_FILE = ROOT / "data" / "paper_snipe.lock"


def _pid_alive(pid):
    """Verification cross-platform (os.kill(pid,0) n'est pas fiable sur Windows)."""
    try:
        import subprocess
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                              capture_output=True, text=True, timeout=5)
        return str(pid) in out.stdout
    except Exception:
        return False  # en cas de doute, on considere mort plutot que de bloquer indefiniment


def acquire_lock_or_die():
    """Empeche 2 instances de tourner en meme temps et de corrompre l'etat partage."""
    if LOCK_FILE.exists():
        try:
            old_pid = int(LOCK_FILE.read_text().strip())
        except ValueError:
            old_pid = None
        if old_pid and _pid_alive(old_pid):
            print(f"[LOCK] une instance tourne deja (PID {old_pid}) - arret.", flush=True)
            sys.exit(1)
    LOCK_FILE.write_text(str(os.getpid()))

STAKE = 1.0          # mise de base par snipe (papier)
STAKE_MIN = 1.0
STAKE_MAX = 4.0      # plafond: jamais plus de 4x la mise de base, meme sur un ecart enorme


def size_stake(sig):
    """Mise dynamique: plus le buffer depasse la marge exigee, plus la conviction est
    forte -> mise plus grosse. Ratio buffer/margin borne pour eviter les extremes."""
    if sig["margin"] <= 0:
        return STAKE_MIN
    conviction = abs(sig["buffer"]) / sig["margin"]  # 1.0 = juste au seuil, plus haut = tres confiant
    stake = STAKE * conviction
    return round(min(STAKE_MAX, max(STAKE_MIN, stake)), 2)
RESOLVE_BUFFER = 8   # secondes apres end_ts avant de verifier la resolution
POLL_SECS = 1         # frequence de scan (edge Binance = limites larges, Gamma OK a ce rythme sur 6 marches)


def now_iso():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def log(msg):
    line = f"[{now_iso()}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_state():
    if STATE_FILE.exists():
        try:
            s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            s.setdefault("price_history", {})
            return s
        except Exception:
            pass
    return {"balance": 0.0, "open": {}, "closed": [], "traded_slugs": [], "price_history": {}}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def outcome_price(market, side):
    """Prix courant (cout d'entree) du cote 'Up' ou 'Down' via outcomePrices."""
    try:
        outcomes = json.loads(market.get("outcomes") or "[]")
        prices = json.loads(market.get("outcomePrices") or "[]")
        idx = outcomes.index(side)
        return float(prices[idx])
    except Exception:
        return None


def real_outcome(pair, start_ts, end_ts):
    """Resout le VRAI resultat: bougie 1min a start_ts (strike) vs prix a end_ts."""
    strike = _strike_at(pair, start_ts)
    close = _strike_at(pair, end_ts)  # ouverture de la bougie 1min a end_ts = prix a ce moment
    if strike is None or close is None:
        return None
    return "Up" if close > strike else "Down", strike, close


def resolve_open_positions(state):
    now = time.time()
    still_open = {}
    for slug, pos in state["open"].items():
        if now < pos["end_ts"] + RESOLVE_BUFFER:
            still_open[slug] = pos
            continue
        res = real_outcome(pos["pair"], pos["start_ts"], pos["end_ts"])
        if res is None:
            # pas encore de donnee Binance dispo, on retente au prochain tour
            still_open[slug] = pos
            continue
        actual_side, strike, close = res
        win = actual_side == pos["side"]
        entry = pos["entry_price"]
        stake = pos.get("stake", STAKE)
        shares = stake / entry if entry > 0 else 0
        pnl = (shares * 1.0 - stake) if win else -stake
        state["balance"] += pnl
        record = {**pos, "actual_side": actual_side, "close": close, "win": win, "pnl": round(pnl, 4),
                  "resolved_ts": now}
        state["closed"].append(record)
        icon = "✅ WIN " if win else "❌ LOSS"
        log(f"{icon} {pos['symbol']} {slug} predit={pos['side']} reel={actual_side} "
            f"entry={entry:.3f} strike={strike:.2f} close={close:.2f} pnl={pnl:+.3f} "
            f"| solde={state['balance']:+.3f}")
    state["open"] = still_open


def sample_price_curve(state, market, p):
    """Echantillonne le prix live vs strike pour la fenetre en cours -> courbe pour le dashboard."""
    slug = market.get("slug")
    now = time.time()
    secs_left = p["end_ts"] - now
    if secs_left < -RESOLVE_BUFFER or secs_left > 320:
        return
    strike = _strike_at(p["pair"], p["start_ts"])
    price = _binance_price(p["pair"])
    if strike is None or price is None:
        return
    hist = state["price_history"].setdefault(slug, {
        "symbol": p["symbol"], "start_ts": p["start_ts"], "end_ts": p["end_ts"],
        "strike": strike, "points": []})
    hist["points"].append({"ts": now, "price": price})
    hist["points"] = hist["points"][-200:]
    # menage: on ne garde que les fenetres recentes (< 20 min)
    for k in list(state["price_history"].keys()):
        if now - state["price_history"][k]["end_ts"] > 1200:
            del state["price_history"][k]


def scan_once(state):
    markets = find_active_markets()
    for m in markets:
        p = parse_updown_market(m)
        if not p:
            continue
        slug = m.get("slug")
        sample_price_curve(state, m, p)
        if slug in state["open"] or slug in state["traded_slugs"]:
            continue
        sig = evaluate(m)
        if not sig:
            continue
        entry = outcome_price(m, sig["side"])
        if entry is None or entry <= 0 or entry >= 1:
            log(f"⚠️ signal {sig['symbol']} {slug} mais prix outcome invalide ({entry}), skip")
            continue
        stake = size_stake(sig)
        pos = {**p, "slug": slug, "side": sig["side"], "entry_price": entry,
                "strike_sig": sig["strike"], "price_sig": sig["price"], "buffer": sig["buffer"],
                "margin": sig["margin"], "seconds_left_at_entry": sig["seconds_left"],
                "opened_ts": time.time(), "stake": stake}
        state["open"][slug] = pos
        state["traded_slugs"].append(slug)
        state["traded_slugs"] = state["traded_slugs"][-500:]  # garde-fou memoire
        log(f"🎯 SNIPE {sig['symbol']} {slug} cote={sig['side']} entry={entry:.3f} mise={stake:.2f}$ "
            f"strike={sig['strike']:.2f} live={sig['price']:.2f} buffer={sig['buffer']:+.2f} "
            f"(marge requise {sig['margin']:.2f}) | {sig['seconds_left']}s restantes")


def main():
    acquire_lock_or_die()
    state = load_state()
    log(f"=== GHOST V3 paper sniper demarre | solde={state['balance']:+.3f} | "
        f"{len(state['open'])} position(s) ouverte(s), {len(state['closed'])} historique ===")
    scan_n = 0
    while True:
        try:
            scan_n += 1
            scan_once(state)
            resolve_open_positions(state)
            save_state(state)
            if scan_n % 30 == 0:
                wins = sum(1 for c in state["closed"] if c["win"])
                total = len(state["closed"])
                wr = (wins / total * 100) if total else 0.0
                log(f"📊 heartbeat scan#{scan_n} | solde={state['balance']:+.3f} | "
                    f"{total} trades resolus (WR {wr:.0f}%) | {len(state['open'])} en cours")
        except Exception as e:
            log(f"💥 erreur boucle: {e}\n{traceback.format_exc()}")
        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
