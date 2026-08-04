"""
Copy-trading Polymarket — suit les traders PROUVÉS rentables et copie leurs
nouvelles prises. Edge : on ne parie pas sur notre analyse mais sur le
track-record démontré de gens qui gagnent (leaderboard public on-chain).

APIs publiques (vérifiées 2026-07-13) :
  - lb-api.polymarket.com/profit?window=7d&limit=N   -> top traders par profit
  - data-api.polymarket.com/activity?user=<wallet>   -> leurs trades récents
"""

import time

import requests

# Paris "prop" à résolution INSTANTANÉE (gappent à 0 sur un seul événement,
# le stop-loss ne peut jamais sortir à temps — perte totale garantie si ça
# tourne mal, ex: '1st Half O/U 1.5' 44¢->3.5¢ d'un but). On ne les trade PAS.
PROP_EXCLUDE = [
    "o/u", "over/under", "1st half", "first half", "2nd half", "half:",
    "exact score", "both teams to score", "corner", "correct score",
    "halftime", "ht/ft", "anytime", "first goal", "to score",
    "handicap", "set handicap", "game handicap",  # gap instantané, SL impossible
]


def is_untradeable_prop(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in PROP_EXCLUDE)


LB_URL = "https://lb-api.polymarket.com/profit"
ACTIVITY_URL = "https://data-api.polymarket.com/activity"
_HEADERS = {"User-Agent": "Mozilla/5.0"}

_traders_cache: list = [0.0, []]  # [ts, wallets]
TRADERS_TTL = 1800  # le classement bouge lentement, 30 min


def _leaderboard(window: str, limit: int) -> dict:
    """{wallet: profit} pour une fenêtre donnée."""
    try:
        r = requests.get(LB_URL, params={"window": window, "limit": str(limit)},
                         timeout=12, headers=_HEADERS)
        data = r.json()
        if not isinstance(data, list):
            return {}
        return {t["proxyWallet"]: {"profit": t.get("amount", 0), "name": t.get("name", "?")}
                for t in data if t.get("proxyWallet")}
    except Exception:
        return {}


def get_top_traders(window: str = "7d", limit: int = 10) -> list[dict]:
    """COPY v2 — sélection STRICTE : on ne garde que les wallets rentables sur
    PLUSIEURS fenêtres (7j ET 30j), pas les chanceux d'une semaine. Le profit
    30j (durabilité) sert de score de qualité pour pondérer la mise ensuite.
    C'est le cœur scalable : on emprunte l'edge de gagnants PROUVÉS sur la durée."""
    now = time.time()
    if now - _traders_cache[0] < TRADERS_TTL and _traders_cache[1]:
        return _traders_cache[1]
    lb7 = _leaderboard("7d", 40)
    lb30 = _leaderboard("30d", 60)
    if not lb7:
        return _traders_cache[1]
    # INTERSECTION : rentable 7j ET 30j (régularité, pas one-shot)
    traders = []
    for w, d in lb7.items():
        if d["profit"] <= 0:
            continue
        p30 = lb30.get(w, {}).get("profit", 0)
        if p30 <= 0:
            continue  # pas prouvé sur 30j -> on écarte (filtre durabilité)
        traders.append({"wallet": w, "name": d["name"],
                        "profit": d["profit"],       # 7j (récence)
                        "profit30": p30,             # 30j (durabilité = score qualité)
                        "quality": min(2.0, 0.7 + p30 / 500_000)})  # 0.7x..2.0x pondération mise
    traders.sort(key=lambda t: -t["profit30"])  # trie par durabilité, pas récence
    traders = traders[:limit]
    if traders:
        _traders_cache[0] = now
        _traders_cache[1] = traders
    return traders or _traders_cache[1]


def get_recent_buys(wallet: str, max_age_s: int = 900, limit: int = 15) -> list[dict]:
    """Achats (BUY) récents (< max_age_s) d'un trader. Retourne les infos
    nécessaires pour copier : asset (=token_id), price, title, conditionId."""
    try:
        r = requests.get(ACTIVITY_URL, params={"user": wallet, "limit": str(limit)},
                         timeout=12, headers=_HEADERS)
        acts = r.json()
    except Exception:
        return []
    now = time.time()
    buys = []
    for a in acts:
        if a.get("type") != "TRADE" or a.get("side") != "BUY":
            continue
        ts = a.get("timestamp", 0)
        if now - ts > max_age_s:
            continue
        if not a.get("asset"):
            continue
        buys.append({
            "token_id": str(a["asset"]),
            "price": float(a.get("price", 0)),
            "title": a.get("title", "?"),
            "condition_id": a.get("conditionId"),
            "usdc_size": float(a.get("usdcSize", 0)),
            "timestamp": ts,
        })
    return buys
