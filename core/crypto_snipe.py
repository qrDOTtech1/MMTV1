"""
Resolution-sniping CRYPTO — l'équivalent non-sportif du sniper ESPN.

Les marchés Polymarket "Will the price of Bitcoin be above $X on <date>?" se
résolvent selon le prix RÉEL de la crypto à l'échéance. Près de la résolution,
si le prix réel est franchement au-dessus (ou en-dessous) du seuil, l'issue
est quasi certaine — on achète le bon côté avant que le marché finisse de le
pricer à 1.00.

Prix réel : API Coinbase spot (gratuite, sans clé). Marge de sécurité
obligatoire : on ne snipe que si l'écart prix/seuil est assez grand pour
qu'un mouvement soudain ne puisse pas le renverser dans le temps restant.
"""

import re

import requests

COINBASE = "https://api.coinbase.com/v2/prices/{}-USD/spot"

# symboles crypto reconnus dans les questions
CRYPTO_MAP = {
    "bitcoin": "BTC", "btc": "BTC",
    "ethereum": "ETH", "eth": "ETH",
    "solana": "SOL", "sol": "SOL",
    "dogecoin": "DOGE", "doge": "DOGE",
    "xrp": "XRP", "ripple": "XRP",
    "binance-coin": "BNB", "bnb": "BNB",
}

_price_cache: dict = {}


def get_price(symbol: str) -> float | None:
    """Prix spot USD via Coinbase (caché 15s)."""
    import time
    now = time.time()
    if symbol in _price_cache and now - _price_cache[symbol][0] < 15:
        return _price_cache[symbol][1]
    try:
        r = requests.get(COINBASE.format(symbol), timeout=8)
        p = float(r.json()["data"]["amount"])
        _price_cache[symbol] = (now, p)
        return p
    except Exception:
        return None


def parse_crypto_market(question: str) -> dict | None:
    """Extrait (symbole, seuil, sens) d'une question crypto.
    Ex: 'Will the price of Bitcoin be above $62,000 on July 13?'
        -> {symbol: BTC, threshold: 62000, direction: 'above'}
    None si ce n'est pas un marché crypto seuil reconnaissable."""
    q = question.lower()
    symbol = None
    for kw, sym in CRYPTO_MAP.items():
        if kw in q:
            symbol = sym
            break
    if not symbol:
        return None

    direction = None
    if "above" in q or "reach" in q or "hit" in q or "over" in q:
        direction = "above"
    elif "below" in q or "under" in q or "dip" in q:
        direction = "below"
    if direction is None:
        return None

    # premier montant $ dans la question
    m = re.search(r"\$?\s?([\d,]+(?:\.\d+)?)\s?(k|m)?", q)
    if not m:
        return None
    val = float(m.group(1).replace(",", ""))
    if m.group(2) == "k":
        val *= 1_000
    elif m.group(2) == "m":
        val *= 1_000_000
    if val < 100:  # évite de capter une date ("13") ou un petit nombre
        return None

    return {"symbol": symbol, "threshold": val, "direction": direction}


def evaluate(question: str, hours_to_resolution: float, safety_margin_pct: float = 2.0) -> dict | None:
    """Décide si le résultat est quasi certain. Retourne
    {winner_side: 'YES'|'NO', symbol, price, threshold, margin_pct} ou None
    si trop incertain (écart insuffisant vu le temps restant).

    Logique : plus il reste de temps, plus la marge exigée est grande (le
    prix peut bouger davantage). safety_margin_pct est la marge de base pour
    une résolution imminente (< 15 min)."""
    parsed = parse_crypto_market(question)
    if not parsed:
        return None
    price = get_price(parsed["symbol"])
    if price is None:
        return None

    threshold = parsed["threshold"]
    gap_pct = (price - threshold) / threshold * 100  # >0 si prix au-dessus du seuil

    # marge exigée croît avec le temps restant. DESSERRÉ (Steven) : 1.5%/h était
    # trop strict -> rejetait 'ETH>1800' à 4% de marge sur 4h (near-certain évident,
    # ETH ne chute pas 4% en 4h en calme). Passé à 0.6%/h : capte le quasi-sûr réel.
    required = safety_margin_pct + max(0.0, hours_to_resolution) * 0.6

    if parsed["direction"] == "above":
        # YES gagne si prix > seuil. Quasi sûr si gap franchement positif.
        if gap_pct >= required:
            winner = "YES"
        elif gap_pct <= -required:
            winner = "NO"
        else:
            return None
    else:  # "below"
        if gap_pct <= -required:
            winner = "YES"  # question 'below' : YES si prix sous le seuil
        elif gap_pct >= required:
            winner = "NO"
        else:
            return None

    return {
        "winner_side": winner,
        "symbol": parsed["symbol"],
        "price": price,
        "threshold": threshold,
        "margin_pct": round(gap_pct, 2),
        "required_pct": round(required, 2),
    }
