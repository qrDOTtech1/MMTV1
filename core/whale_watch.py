"""SURVEILLANCE PASSIVE DES GROS ORDRES (Steven 29/07) — detecte les paris
massifs en toute fin de fenetre (pattern documente par l'etude Stanford/SMU
sur les marches BTC 5min Polymarket : un trader pousse un ordre spot Binance
geant dans les dernieres secondes pour faire pencher le prix Chainlink dans
son sens sur SON pari Polymarket, encaisse, le prix revient juste apres).

Usage EXCLUSIVEMENT DEFENSIF : ce module ne fait QUE lire des donnees
publiques on-chain (endpoint data-api.polymarket.com/trades, aucune cle,
aucune action) pour identifier quand NOUS pourrions etre exposes du mauvais
cote d'un pic suspect -> renforce le danger_score, ne genere JAMAIS d'ordre.
On ne "suit" jamais un pic pour en profiter : on evite d'etre en face."""
import json
import threading
import time
from pathlib import Path

import requests

TRADES_URL = "https://data-api.polymarket.com/trades"
_HDR = {"User-Agent": "GHOST/3"}

LATE_WINDOW_SECS = 45  # ne surveille que dans les 45 dernieres secondes
SPIKE_LOOKBACK_SECS = 12  # fenetre glissante pour cumuler la taille par wallet
SPIKE_NOTIONAL_USD = 150.0  # cumul $ par wallet sur la fenetre -> pic suspect
# LEVIER (Steven 29/07, "les gars qui manipulent peuvent mettre 1$ pour gagner
# quasi 20$") : le seuil $ ci-dessus rate EXACTEMENT le pattern le plus
# rentable pour un manipulateur -> miser peu sur le cote pas cher (5-10c) de
# Polymarket (notional minuscule, invisible au filtre $) et depenser le vrai
# argent a pousser le prix SPOT sur Binance (hors radar Polymarket). Le vrai
# tell n'est pas le $ depense ICI, c'est le PAYOUT POTENTIEL vise (parts x 1$).
LEVERAGE_PAYOUT_USD = 15.0  # payout potentiel (parts*1$) -> suspect meme si $ mise petit
CHEAP_SIDE_MAX_PRICE = 0.20  # "cote pas cher" = achete sous ce prix
# CORRELATION BINANCE (Steven 29/07, "art de la guerre" : ne pas juger un
# signal seul, correler) : un pari a fort levier COMBINE a une vitesse Binance
# anormale dans le meme sens, au meme moment, est un signal bien plus fort
# qu'un des deux isole (l'un peut etre un hasard, la combinaison beaucoup moins).
MOMENTUM_CORRELATION_THRESHOLD = 0.08  # %/s de vitesse Binance jugee anormale

REGISTRY_FILE = Path(__file__).parent.parent / "data" / "whale_registry.json"
_registry_lock = threading.Lock()
_trade_cache = {}  # condition_id -> (ts, trades)
_TRADE_CACHE_TTL = 3.0


def _load_registry():
    try:
        return json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_registry(reg):
    try:
        REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)
        REGISTRY_FILE.write_text(json.dumps(reg, indent=1), encoding="utf-8")
    except Exception:
        pass


def _fetch_trades(condition_id, limit=30):
    now = time.time()
    hit = _trade_cache.get(condition_id)
    if hit and now - hit[0] < _TRADE_CACHE_TTL:
        return hit[1]
    try:
        r = requests.get(
            TRADES_URL,
            params={"market": condition_id, "limit": str(limit)},
            timeout=4,
            headers=_HDR,
        )
        trades = r.json() if r.status_code == 200 else []
        if not isinstance(trades, list):
            trades = []
    except Exception:
        trades = []
    _trade_cache[condition_id] = (now, trades)
    return trades


def detect_late_spike(condition_id, end_ts, now=None, fav_side=None, binance_fast_pct_s=None):
    """Retourne dict {suspect, wallet, side, outcome, notional, payout, against_fav,
    correlated} si un pari suspect est detecte dans les derniers LATE_WINDOW_SECS.
    Deux facons d'etre suspect (Steven 29/07) :
      - GROS $ engage (notional >= SPIKE_NOTIONAL_USD) -> pari direct massif.
      - PETIT $ mais GROS LEVIER (payout potentiel >= LEVERAGE_PAYOUT_USD sur
        le cote pas cher) -> le pattern "1$ pour gagner 20$" qui echappe au
        filtre notional seul.
    `binance_fast_pct_s` (optionnel) : vitesse Binance courante -> si elle
    confirme le sens du pari suspect au meme moment, `correlated=True`
    (signal beaucoup plus fiable qu'isole, cf. Stanford study)."""
    now = now or time.time()
    secs_left = end_ts - now
    if secs_left > LATE_WINDOW_SECS or secs_left < 0 or not condition_id:
        return {"suspect": False}
    trades = _fetch_trades(condition_id)
    if not trades:
        return {"suspect": False}
    cutoff = now - SPIKE_LOOKBACK_SECS
    by_wallet = {}
    for t in trades:
        ts = t.get("timestamp", 0)
        if ts < cutoff:
            continue
        w = t.get("proxyWallet", "?")
        try:
            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
        except Exception:
            continue
        entry = by_wallet.setdefault(
            w, {"notional": 0.0, "payout": 0.0, "side": t.get("side"), "outcome": t.get("outcome")}
        )
        entry["notional"] += size * price
        if price <= CHEAP_SIDE_MAX_PRICE and t.get("side") == "BUY":
            entry["payout"] += size  # 1 part = 1$ si le cote pas cher gagne
    if not by_wallet:
        return {"suspect": False}
    # priorite : le plus suspect par $ engage OU par levier, celui qui depasse le plus son seuil
    wallet, info = max(
        by_wallet.items(),
        key=lambda kv: max(
            kv[1]["notional"] / SPIKE_NOTIONAL_USD, kv[1]["payout"] / LEVERAGE_PAYOUT_USD
        ),
    )
    is_big_notional = info["notional"] >= SPIKE_NOTIONAL_USD
    is_leveraged = info["payout"] >= LEVERAGE_PAYOUT_USD
    if not (is_big_notional or is_leveraged):
        return {"suspect": False}
    against_fav = (
        fav_side is not None
        and info.get("outcome") is not None
        and info["outcome"] != fav_side
        and info.get("side") == "BUY"
    )
    correlated = False
    if binance_fast_pct_s is not None and abs(binance_fast_pct_s) >= MOMENTUM_CORRELATION_THRESHOLD:
        # le pari suspect est sur quel outcome ? si sa vitesse Binance va dans
        # le MEME sens (Up = prix monte, Down = prix baisse) -> correlation.
        wants_up = info.get("outcome") == "Up"
        correlated = (wants_up and binance_fast_pct_s > 0) or (
            not wants_up and binance_fast_pct_s < 0
        )
    _record_sighting(wallet, secs_left, max(info["notional"], info["payout"]))
    return {
        "suspect": True,
        "leveraged": is_leveraged,
        "wallet": wallet,
        "side": info.get("side"),
        "outcome": info.get("outcome"),
        "notional": round(info["notional"], 2),
        "payout": round(info["payout"], 2),
        "secs_left": round(secs_left, 1),
        "against_fav": against_fav,
        "correlated": correlated,
    }


def _record_sighting(wallet, secs_left, notional):
    """Registre passif (Steven 29/07) : compte les apparitions en fin de
    fenetre par wallet -> permet un jour de distinguer un vrai pattern
    recurrent d'un simple gros trader occasionnel. Lecture seule ailleurs,
    aucune action prise automatiquement sur la base de ce compteur seul."""
    with _registry_lock:
        reg = _load_registry()
        entry = reg.setdefault(wallet, {"sightings": 0, "total_notional": 0.0, "first_seen": time.time()})
        entry["sightings"] += 1
        entry["total_notional"] = round(entry.get("total_notional", 0.0) + notional, 2)
        entry["last_seen"] = time.time()
        _save_registry(reg)


def get_watchlist(min_sightings=3):
    """Wallets ayant montre le pattern (gros trade en toute fin de fenetre)
    au moins `min_sightings` fois -> a titre informatif/dashboard."""
    reg = _load_registry()
    return {
        w: e for w, e in reg.items() if e.get("sightings", 0) >= min_sightings
    }
