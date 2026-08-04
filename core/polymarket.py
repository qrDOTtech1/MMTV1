"""
Client Polymarket — DONNÉES PUBLIQUES uniquement (aucun compte, aucune clé).

Deux APIs publiques :
  - Gamma  (https://gamma-api.polymarket.com) : catalogue des marchés, volumes,
    prix indicatifs, dates de résolution.
  - CLOB   (https://clob.polymarket.com)      : orderbooks temps réel (bids/asks
    avec tailles) par outcome-token.

Vérifié en direct le 2026-07-12 : les deux répondent sans authentification.

IMPORTANT — CADRE D'USAGE : ce module ne fait QUE de la lecture de données et
du paper trading. L'exécution réelle exige d'être physiquement dans une
juridiction autorisée (Polymarket est géobloqué en France ET au Portugal
depuis janvier 2026) — elle sera branchée le jour où c'est le cas, pas avant.
"""

import json
import re
import time

import aiohttp

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"

# Frais preneur Polymarket : 0 sur la plupart des marchés, mais le gas de
# règlement + le risque d'exécution partielle existent. On exige une marge
# minimale pour compter une opportunité d'arb comme "réelle".
MIN_ARB_EDGE = 0.005  # 0.5% sous 1.00 minimum

_cache: dict = {}
CACHE_TTL = 20  # les books bougent vite, cache court


async def _get_json(session: aiohttp.ClientSession, url: str, params: dict | None = None):
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None


async def get_active_markets(limit: int = 100, min_volume_24h: float = 10_000) -> list[dict]:
    """Marchés actifs triés par volume 24h décroissant. Chaque entrée garde :
    question, outcomes, outcomePrices, clobTokenIds, volume24hr, endDate,
    liquidity. Filtre le bruit (volume < min_volume_24h)."""
    key = f"markets_{limit}"
    now = time.time()
    if key in _cache and now - _cache[key][0] < CACHE_TTL * 3:
        return _cache[key][1]

    # L'API Gamma plafonne à 100 marchés/requête. Pour couvrir plus (on ne
    # veut pas rater des marchés exploitables), on pagine via offset.
    data = []
    async with aiohttp.ClientSession() as session:
        offset = 0
        while offset < limit:
            page = await _get_json(session, f"{GAMMA_URL}/markets", {
                "active": "true", "closed": "false",
                "limit": "100", "offset": str(offset),
                "order": "volume24hr", "ascending": "false",
            })
            if not page:
                break
            data.extend(page)
            if len(page) < 100:
                break  # dernière page
            offset += 100
    if not data:
        return []

    markets = []
    for m in data:
        try:
            vol = float(m.get("volume24hr") or 0)
            if vol < min_volume_24h:
                continue
            token_ids = json.loads(m.get("clobTokenIds") or "[]")
            outcomes = json.loads(m.get("outcomes") or "[]") if isinstance(m.get("outcomes"), str) else (m.get("outcomes") or [])
            prices = json.loads(m.get("outcomePrices") or "[]") if isinstance(m.get("outcomePrices"), str) else (m.get("outcomePrices") or [])
            if not token_ids or len(token_ids) != len(outcomes):
                continue
            markets.append({
                "id": m.get("id"),
                "question": m.get("question", "?"),
                "outcomes": outcomes,
                "prices": [float(p) for p in prices] if prices else [],
                "token_ids": token_ids,
                "volume_24h": vol,
                "liquidity": float(m.get("liquidity") or 0),
                "end_date": m.get("endDate"),
                # Regroupement par ÉVÉNEMENT (remarque Steven : Toronto & Montréal
                # pouvaient être le MÊME match via 2 marchés distincts -> le garde-fou
                # anti-both-sides qui compare les TITRES ne l'aurait pas vu). Gamma
                # rattache chaque marché à un event ; on garde son id pour bloquer
                # toute 2e jambe sur le même événement, quel que soit le côté.
                "event_id": ((m.get("events") or [{}])[0] or {}).get("id"),
                "event_slug": ((m.get("events") or [{}])[0] or {}).get("slug"),
                # DONNÉES STRUCTURÉES SPORT (trouvées par Steven 20/07) : timing + type
                # de marché fiables au lieu de parser la question. gameStartTime = début
                # du match (on saura s'il est fini) ; sportsMarketType = moneyline vs prop.
                "game_start": m.get("gameStartTime") or m.get("eventStartTime"),
                "sports_type": m.get("sportsMarketType"),
                "uma_status": m.get("umaResolutionStatus"),
                "game_id": m.get("gameId"),
            })
        except Exception:
            continue

    _cache[key] = (now, markets)
    return markets


async def get_book(session: aiohttp.ClientSession, token_id: str) -> dict | None:
    """Orderbook d'un outcome-token : {bids: [(prix, taille)...], asks: [...]}.
    bids triés du meilleur (plus haut) au pire, asks du meilleur (plus bas)."""
    data = await _get_json(session, f"{CLOB_URL}/book", {"token_id": token_id})
    if not data:
        return None
    try:
        bids = sorted(((float(b["price"]), float(b["size"])) for b in data.get("bids", [])), key=lambda x: -x[0])
        asks = sorted(((float(a["price"]), float(a["size"])) for a in data.get("asks", [])), key=lambda x: x[0])
        return {"bids": bids, "asks": asks}
    except Exception:
        return None


def best_ask(book: dict) -> tuple[float, float] | None:
    """(prix, taille) du meilleur ask, ou None si book vide."""
    return book["asks"][0] if book and book.get("asks") else None


def best_bid(book: dict) -> tuple[float, float] | None:
    return book["bids"][0] if book and book.get("bids") else None


# Le paramètre tag_slug de l'API Gamma est ignoré côté serveur (vérifié :
# renvoie exactement le même résultat pour un tag inventé) — le seul moyen
# fiable de trouver les marchés politique/guerre est un filtre par mots-clés
# sur un pool plus large trié par volume, pas juste le top-N global (qui est
# systématiquement dominé par le sport/crypto, les plus gros volumes).
# Sujets NEWS-DRIVEN où l'IA (recherche + LLM) peut avoir un edge réel : des
# événements décidés par l'actualité, pas par une cote de bookmaker. On
# élargit au-delà de la guerre — crypto/régulation, élections, justice,
# politique — SANS toucher à la barre de conviction (edge>=15pts, confiance
# non-faible), qui reste ce qui protège de miser sur du bruit.
POLITICS_WAR_KEYWORDS = [
    # guerre / géopolitique
    "war", "ukraine", "russia", "israel", "gaza", "iran", "nato", "ceasefire",
    "sanction", "military", "troops", "invasion", "nuclear", "coup", "syria",
    "china", "taiwan", "north korea", "hostage", "strike", "missile", "drone",
    # politique / élections
    "president", "election", "minister", "government", "parliament", "congress",
    "senate", "trump", "putin", "zelensky", "biden", "vote", "poll", "primary",
    "referendum", "candidate", "resign", "impeach", "prime minister", "chancellor",
    "cabinet", "governor", "mayor", "vance", "musk",
    # justice / événements
    "court", "ruling", "verdict", "indict", "supreme court", "trial", "arrest",
    "guilty", "lawsuit", "pardon", "investigation",
    # crypto / régulation (news-driven)
    "sec ", "etf", "regulation", "legalize", "ban ", "approve", "fed ",
    "interest rate", "shutdown", "debt ceiling", "tariff", "deal", "summit",
    "treaty", "agreement", "announce",
]


def _is_politics_or_war(question: str) -> bool:
    # \b = frontière de mot entier — sans ça "war" matchait "Warriors" (NBA),
    # un faux positif réel observé en prod.
    q = question.lower()
    return any(re.search(rf"\b{re.escape(kw)}\b", q) for kw in POLITICS_WAR_KEYWORDS)


def _days_until(end_date: str | None) -> float:
    """Jours restants avant résolution (999 si date absente/illisible).
    Sert à privilégier les marchés qui se dénouent VITE — capital qui tourne
    = gains réalisés chaque jour, au lieu d'être bloqué des mois (LeBron
    résout en octobre : inutile pour du gain quotidien)."""
    if not end_date:
        return 999.0
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        delta = (dt - datetime.now(timezone.utc)).total_seconds() / 86400.0
        return max(delta, 0.0)
    except Exception:
        return 999.0


async def get_diverse_markets(pool_size: int = 200, min_volume_24h: float = 2_000,
                              n_politics: int = 6, n_general: int = 6,
                              max_days: float | None = None) -> list[dict]:
    """Mix garanti : n_politics marchés politique/guerre (par mots-clés) +
    n_general marchés tous sujets confondus (souvent sport/crypto vu le
    volume) — sans ça, une sélection par top-volume brut est systématiquement
    100% sport, ce qui n'est pas ce qu'on veut pour l'analyse IA."""
    pool = await get_active_markets(limit=pool_size, min_volume_24h=min_volume_24h)
    if max_days is not None:
        pool = [m for m in pool if _days_until(m.get("end_date")) <= max_days]
    politics = [m for m in pool if _is_politics_or_war(m["question"])]
    general = [m for m in pool if m not in politics]
    # Tri par échéance croissante : les marchés qui se dénouent le plus vite
    # d'abord (gain réalisé plus tôt = capital qui retourne plus vite).
    politics.sort(key=lambda m: _days_until(m.get("end_date")))
    general.sort(key=lambda m: _days_until(m.get("end_date")))
    return politics[:n_politics] + general[:n_general]


async def get_price_history(session: aiohttp.ClientSession, token_id: str,
                            interval: str = "1h", fidelity: int = 5) -> list[tuple[float, float]]:
    """Historique (timestamp, prix) d'un outcome-token, le plus récent en dernier."""
    data = await _get_json(session, f"{CLOB_URL}/prices-history",
                            {"market": token_id, "interval": interval, "fidelity": str(fidelity)})
    if not data or not data.get("history"):
        return []
    return [(pt["t"], pt["p"]) for pt in data["history"]]


async def scan_momentum(markets: list[dict], min_move_pct: float = 4.0) -> list[dict]:
    """Détecte les marchés dont le prix YES a bougé de plus de min_move_pct
    (en points de %) sur la dernière heure — signal que quelque chose se
    passe et que le marché est en train de le pricer, potentiellement pas
    encore fini de bouger. Direction du signal = sens du mouvement récent
    (on suit la tendance, on ne la parie pas à contre-courant)."""
    signals = []
    async with aiohttp.ClientSession() as session:
        for m in markets:
            if len(m["token_ids"]) != 2:
                continue
            hist = await get_price_history(session, m["token_ids"][0], interval="1h", fidelity=5)
            if len(hist) < 4:
                continue
            first_price = hist[0][1]
            last_price = hist[-1][1]
            move_pts = (last_price - first_price) * 100
            if abs(move_pts) < min_move_pct:
                continue
            side = "YES" if move_pts > 0 else "NO"
            token_id = m["token_ids"][0] if side == "YES" else m["token_ids"][1]
            # Le prix d'historique (dernière transaction) n'est PAS forcément
            # marketable — un ordre GTC posté à ce prix peut rester passif
            # indéfiniment sans se remplir, bloquant le capital pour rien
            # (vécu : 2 ordres coincés des heures). Le VRAI prix actionnable
            # est le meilleur ask du carnet en direct.
            book = await get_book(session, token_id)
            ask = best_ask(book)
            if not ask:
                continue
            price_now = ask[0]
            # Risque/récompense : payer 0.97$ pour gagner 0.03$ potentiel n'est
            # pas un "momentum" tradable, c'est un marché déjà quasi-résolu —
            # stratégie différente (quasi-résolution), pas celle-ci.
            if price_now > 0.90 or price_now < 0.05:
                continue
            signals.append({
                "question": m["question"],
                "token_yes": m["token_ids"][0],
                "token_no": m["token_ids"][1],
                "side": side,
                "move_pts": round(move_pts, 1),
                "price_now": price_now,
                "volume_24h": m["volume_24h"],
            })
    return signals


# Mots-clés pour repérer les marchés sportifs in-play OÙ le mouvement de cote
# reflète un avantage DURABLE (un joueur qui mène a une vraie proba accrue de
# gagner). Le momentum y est un vrai signal — Grigor Dimitrov 45¢ -> 100¢.
SPORT_KEYWORDS = [
    "vs.", " vs ", "open:", "atp", "wta", " fc ", "win on 2026", "ufc",
    "nba", "match", "grand prix", "o/u", "both teams", "tour de france",
    "mlb", "nhl", "premier league", "champions", "odi", "test match",
]

# Esports EXCLUS : en BO3, gagner une manche ne prédit quasi rien pour le
# match — les cotes swinguent, le momentum se retourne, on se fait prendre à
# contre-pied. Pertes réelles constatées : CS eSuba -21%, Dota Team Liquid
# -18%. On ne trade que le sport où l'avantage est durable.
ESPORT_KEYWORDS = [
    "dota", "cs2", "counter-strike", "counter strike", "league of legends",
    "lol:", "valorant", "esport", "rocket league", "overwatch", "rainbow six",
]

# TENNIS : notre point faible PROUVÉ (data on-chain propre : -4.46$, le boulet).
# Trop swingy (chaque point retourne le momentum), on se fait whipsaw. Décision
# Steven : on COUPE le tennis en momentum in-play ET en copy. (Le snipe near-
# certain sur du tennis DÉJÀ décidé reste OK — c'est une autre bête, basse variance.)
TENNIS_KEYWORDS = [
    "itf", "atp", "wta", "tennis", " open:", "roland", "wimbledon", "challenger",
    # tournois "ville: Joueur vs Joueur" qui leakaient dans 'autre' (mal étiquetés,
    # -11$ caché). Data on-chain : ce sont tous du tennis perdant.
    "lincoln:", "granby:", "iasi", "gandia", "astana", "sao paulo", "brisbane",
    "swedish open", "swiss open", "athens open", "bucharest",
]

# POLITIQUE / GÉOPOLITIQUE : l'IA s'y fait démolir (parlement israélien, Iran =
# gros du carnage 'autre'). Son edge est sur le SPORT/match (Spain +3.55$ via IA).
# On lui interdit la politique pure.
POLITICS_KEYWORDS = [
    "parliament", "election", "president", "minister", "government", "congress",
    "senate", "trump", "putin", "zelensky", "war", "iran", "israel", "gaza",
    "ukraine", "russia", "court", "impeach", "sanction", "nato", "coup", "hostage",
]


def _is_tennis(question: str) -> bool:
    return any(kw in (question or "").lower() for kw in TENNIS_KEYWORDS)


# Props à résolution instantanée : gappent à 0 sur un seul événement, le
# stop-loss ne peut pas sortir (perte totale). Exclus du trading.
_PROP_EXCLUDE = [
    "o/u", "over/under", "1st half", "first half", "2nd half", "half:",
    "exact score", "both teams to score", "corner", "correct score",
    "halftime", "ht/ft", "first goal", "to score", "total ",
    # Handicaps (tennis "Set Handicap", "Game Handicap"...) : résolvent set par
    # set -> gap instantané 48.8¢->5¢, SL impossible (perte -2.19$ vécue).
    "handicap", "set handicap", "game handicap",
    # "Up or Down" (crypto intraday) : coin-flip pur qui gappe -> l'IA a perdu
    # -3.46$ sur "Bitcoin Up or Down". Aucun edge, variance max, on exclut.
    "up or down", "up/down",
]


def _is_sport(question: str) -> bool:
    q = question.lower()
    if any(kw in q for kw in ESPORT_KEYWORDS):
        return False
    if any(kw in q for kw in _PROP_EXCLUDE):
        return False  # prop instantané, SL impossible
    return any(kw in q for kw in SPORT_KEYWORDS)


async def scan_sport_inplay(markets: list[dict], min_move_pct: float = 8.0) -> list[dict]:
    """Suit les marchés SPORTIFS dont la cote bouge fort en direct (in-play).
    Contrairement au momentum générique (désactivé car il pariait sur
    n'importe quoi), celui-ci ne cible QUE le sport, où un mouvement de cote
    reflète un vrai événement en cours (un joueur qui prend l'ascendant) — et
    la tendance a de bonnes chances de continuer. Ne garde que le tradable
    (prix 0.20-0.80) et suit le sens du mouvement."""
    signals = []
    async with aiohttp.ClientSession() as session:
        for m in markets:
            if len(m["token_ids"]) != 2 or not _is_sport(m["question"]):
                continue
            hist = await get_price_history(session, m["token_ids"][0], interval="1h", fidelity=1)
            if len(hist) < 5:
                continue
            recent = hist[-15:]  # ~15 dernières minutes
            move_pts = (recent[-1][1] - recent[0][1]) * 100
            if abs(move_pts) < min_move_pct:
                continue
            side = "YES" if move_pts > 0 else "NO"
            token_id = m["token_ids"][0] if side == "YES" else m["token_ids"][1]
            book = await get_book(session, token_id)
            ask = best_ask(book)
            if not ask:
                continue
            price = ask[0]
            if price > 0.80 or price < 0.20:
                continue  # trop proche d'être décidé, potentiel de gain faible
            signals.append({
                "question": m["question"],
                "token_yes": m["token_ids"][0],
                "token_no": m["token_ids"][1],
                "side": side,
                "move_pts": round(move_pts, 1),
                "price_now": price,
                "volume_24h": m["volume_24h"],
            })
    signals.sort(key=lambda s: -abs(s["move_pts"]))  # plus gros mouvements d'abord
    return signals


async def scan_complementary_arb(markets: list[dict]) -> list[dict]:
    """Cherche les arbs YES+NO : acheter les DEUX côtés d'un marché binaire
    pour moins de 1.00$ garantit mathématiquement 1.00$ à la résolution,
    quel que soit le résultat. La taille exploitable est bornée par la
    profondeur des books des deux côtés.

    Retourne les opportunités avec edge >= MIN_ARB_EDGE, tailles réelles."""
    opportunities = []
    async with aiohttp.ClientSession() as session:
        for m in markets:
            if len(m["token_ids"]) != 2:
                continue  # v1 : binaires uniquement (le dutching multi-outcome viendra après)
            book_yes = await get_book(session, m["token_ids"][0])
            book_no = await get_book(session, m["token_ids"][1])
            ask_yes = best_ask(book_yes)
            ask_no = best_ask(book_no)
            if not ask_yes or not ask_no:
                continue
            total = ask_yes[0] + ask_no[0]
            if total < 1.0 - MIN_ARB_EDGE:
                size = min(ask_yes[1], ask_no[1])  # bornée par le côté le moins profond
                opportunities.append({
                    "question": m["question"],
                    "token_yes": m["token_ids"][0],
                    "token_no": m["token_ids"][1],
                    "ask_yes": ask_yes[0], "ask_no": ask_no[0],
                    "total_cost": round(total, 4),
                    "edge_pct": round((1.0 - total) * 100, 2),
                    "max_size_shares": round(size, 2),
                    "max_profit_usd": round(size * (1.0 - total), 2),
                    "volume_24h": m["volume_24h"],
                })
    return opportunities
