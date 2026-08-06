"""V3 — Sniper BTC/ETH "Up or Down" 5 minutes (idée Steven : la poule aux œufs d'or).

Marchés Polymarket 'btc-updown-5m-<start_unix>' : sur une fenêtre de 5 min, résout
"Up" si le prix (Binance 1-min close) à la FIN > au DÉBUT. Le "prix à battre" (strike)
= prix au début de la fenêtre (déductible du timestamp du slug).

EDGE (source de vérité) : dans les DERNIÈRES secondes, si le prix live est FRANCHEMENT
au-dessus/sous le strike (écart >> ce que le prix peut bouger dans le temps restant),
l'issue est quasi certaine -> on snipe LE SEUL bon côté, jamais les deux.

Discipline codée (ce qui manquait en manuel, cf. round -10,49$) :
 - snipe UNIQUEMENT en fin de fenêtre (< WINDOW_SECS restantes),
 - marge de sécurité DYNAMIQUE (croît avec le temps restant : BTC peut bouger),
 - un seul côté, petite mise.
"""

import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests

_pool = ThreadPoolExecutor(
    max_workers=6
)  # partage entre appels -> pas de recreation a chaque scan

BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/price"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
COINBASE = "https://api.coinbase.com/v2/prices/{}-USD/spot"

SYMS = {
    "bitcoin": ("BTC", "BTCUSDT"),
    "btc": ("BTC", "BTCUSDT"),
    "ethereum": ("ETH", "ETHUSDT"),
    "eth": ("ETH", "ETHUSDT"),
    "solana": ("SOL", "SOLUSDT"),
    "sol": ("SOL", "SOLUSDT"),
    "xrp": ("XRP", "XRPUSDT"),
    "ripple": ("XRP", "XRPUSDT"),
    "dogecoin": ("DOGE", "DOGEUSDT"),
    "doge": ("DOGE", "DOGEUSDT"),
    # BNB ajoute 06/08 : paire BNBUSDT verifiee cotee sur Binance.
    "binance-coin": ("BNB", "BNBUSDT"),
    "bnb": ("BNB", "BNBUSDT"),
}

# ── SYNCHRO HORLOGE (decouverte 21/07) ──
# L'horloge locale de la machine trainait de ~6.7s derriere l'heure serveur
# Polymarket -> "secs_left" affiche etait FAUX (on croyait avoir plus de temps
# qu'en realite), causant des tentatives d'achat sur des fenetres deja closes
# cote Polymarket ("ask=None" a repetition) et le decalage que Steven a
# observe (log dit "10s restantes", le marche venait de fermer en vrai).
_clock_offset = 0.0
_clock_lock = threading.Lock()


def _sync_clock():
    global _clock_offset
    try:
        from email.utils import parsedate_to_datetime

        r = requests.get(GAMMA, params={"limit": 1}, timeout=5, headers=_HDR)
        server_ts = parsedate_to_datetime(r.headers["Date"]).timestamp()
        with _clock_lock:
            _clock_offset = server_ts - time.time()
    except Exception:
        pass  # garde l'offset precedent plutot que de repartir a 0 sur un hoquet reseau


def synced_now() -> float:
    """Heure actuelle CALEE sur le serveur Polymarket (pas l'horloge locale,
    qui peut trainer). A utiliser pour tout calcul de temps-restant-avant-fin."""
    with _clock_lock:
        return time.time() + _clock_offset


def clock_offset() -> float:
    """Decalage mesure (secondes) entre l'horloge locale et le serveur
    Polymarket. Expose pour affichage dashboard (diagnostic)."""
    with _clock_lock:
        return _clock_offset


def _clock_sync_loop():
    while True:
        time.sleep(300)  # resynchro toutes les 5 min (derive lente possible)
        _sync_clock()


_price_cache: dict = {}
_strike_cache: dict = {}
_poly_strike_cache: dict = {}  # slug -> priceToBeat (strike officiel Chainlink/Polymarket)

# ── VOLATILITE PAR MARCHE (mesuree, recalculee en tache de fond) ──
# Une marge en % identique pour tous donnait MOINS de securite aux marches les
# plus volatils : mesure du 21/07 -> ETH x1.24, SOL x1.27, XRP x1.27, DOGE x1.19
# par rapport a BTC. La marge exigee est donc mise a l'echelle de la volatilite
# REELLE de chaque actif, pour un niveau de securite EQUIVALENT partout.
# BNB mesure a 1.34x BTC le 06/08 (60 bougies 1min) ; valeur de depart,
# _measure_vol_ratios la recalcule ensuite periodiquement comme les autres.
_vol_ratio: dict = {"BTC": 1.0, "ETH": 1.24, "SOL": 1.27, "XRP": 1.27, "DOGE": 1.19,
                    "BNB": 1.34}
_vol_lock = threading.Lock()

# ── CONVICTION MINIMALE PAR MARCHE (22/07) ──
# Il ne suffit pas que l'ecart DEPASSE la marge : il doit la depasser NETTEMENT.
# Mesure sur les logs : BTC declenche naturellement a 2.0-2.9x la marge, alors que
# XRP se declenchait a 1.00-1.50x, c.-a-d. en effleurant le seuil. Or le prix XRP
# (~1.16$, 4 decimales) bouge par pas de 0.0001 : un ecart de 0.0005 ne represente
# que 5 pas de cotation = quasi du BRUIT de cotation, pas un vrai signal.
# On exige donc que XRP depasse la marge d'une marge confortable.
# BTC reste a 1.0 (aucun changement -> son 21W/0L n'est pas touche).
_MIN_CONVICTION: dict = {"BTC": 1.0, "ETH": 1.0, "SOL": 1.0, "XRP": 1.6, "DOGE": 1.0,
                         "BNB": 1.0}


def _measure_vol_ratios():
    """Mesure le mouvement absolu moyen sur 5min de chaque paire et en deduit
    un ratio de volatilite relatif a BTC. Recalcule periodiquement : la
    volatilite change (marche calme vs agite), les seuils suivent."""
    import statistics

    pairs = {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
        "SOL": "SOLUSDT",
        "XRP": "XRPUSDT",
        "DOGE": "DOGEUSDT",
        "BNB": "BNBUSDT",
    }
    out = {}
    for sym, pair in pairs.items():
        try:
            r = requests.get(
                BINANCE_KLINES,
                params={"symbol": pair, "interval": "5m", "limit": 300},
                timeout=8,
            )
            moves = [abs(float(k[4]) - float(k[1])) / float(k[1]) for k in r.json()]
            if moves:
                out[sym] = statistics.mean(moves)
        except Exception:
            continue
    if "BTC" in out and out["BTC"] > 0:
        with _vol_lock:
            for sym, v in out.items():
                _vol_ratio[sym] = max(0.5, min(3.0, v / out["BTC"]))


def _vol_loop():
    while True:
        time.sleep(1800)  # re-mesure toutes les 30 min
        _measure_vol_ratios()


# ── PROBABILITE CALIBREE (Steven 29/07, "arrete de deviner, calcule") ──
# Remplace le danger_score heuristique (0-100 arbitraire) par une vraie
# probabilite issue d'un mouvement brownien geometrique (meme math que
# Black-Scholes pour "P(le prix finit au-dessus du strike)") : drift ~0 sur
# 5min crypto (raisonnable, pas de biais directionnel connu a cette echelle),
# volatilite MESUREE (pas devinee) sur l'historique reel Binance 5min de
# chaque actif. Remplace enfin les seuils magiques par un chiffre qu'on peut
# comparer directement au prix Polymarket pour un vrai edge = P_calc - prix.
_sigma5m: dict = {}  # sym -> ecart-type des rendements 5min (mesure reelle)
_sigma_lock = threading.Lock()


def _measure_sigma5m():
    """Ecart-type reel des rendements 5min (log-return), par actif, sur les
    300 dernieres bougies Binance -> volatilite EFFECTIVEMENT observee, pas
    une constante devinee une fois et jamais revue."""
    import math
    import statistics

    pairs = {
        "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT",
        "XRP": "XRPUSDT", "DOGE": "DOGEUSDT", "BNB": "BNBUSDT",
    }
    out = {}
    for sym, pair in pairs.items():
        try:
            r = requests.get(
                BINANCE_KLINES, params={"symbol": pair, "interval": "5m", "limit": 300},
                timeout=8,
            )
            rets = [
                math.log(float(k[4]) / float(k[1]))
                for k in r.json() if float(k[1]) > 0
            ]
            if len(rets) > 10:
                out[sym] = statistics.pstdev(rets)
        except Exception:
            continue
    if out:
        with _sigma_lock:
            _sigma5m.update(out)


def _sigma_loop():
    while True:
        time.sleep(1800)  # re-mesure toutes les 30 min, meme cadence que vol_ratio
        _measure_sigma5m()


def _norm_cdf(x):
    """CDF normale standard, sans dependance scipy (erf de la lib standard)."""
    import math

    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def probability_above_strike(pair, current_price, strike, secs_remaining):
    """P(le prix Binance finit >= strike a la resolution), calculee via
    mouvement brownien geometrique : d2 = ln(S/K) / (sigma_t), sigma_t =
    sigma_5min * sqrt(temps_restant/300). Retourne None si volatilite pas
    encore mesuree (fallback : appelant garde son ancien signal)."""
    import math

    if current_price is None or strike is None or strike <= 0 or current_price <= 0:
        return None
    sym = None
    for s, p in (("BTC", "BTCUSDT"), ("ETH", "ETHUSDT"), ("SOL", "SOLUSDT"),
                 ("XRP", "XRPUSDT"), ("DOGE", "DOGEUSDT"), ("BNB", "BNBUSDT")):
        if p == pair:
            sym = s
            break
    with _sigma_lock:
        sigma5 = _sigma5m.get(sym)
    if not sigma5 or sigma5 <= 0:
        return None
    frac = max(secs_remaining, 1.0) / 300.0
    sigma_t = sigma5 * math.sqrt(frac)
    if sigma_t <= 1e-9:
        # temps restant ~0 -> quasi deterministe selon le signe actuel
        return 1.0 if current_price >= strike else 0.0
    d2 = math.log(current_price / strike) / sigma_t
    return _norm_cdf(d2)


def _binance_price(pair: str):
    """Prix spot live via Binance (source de résolution). Coinbase en secours."""
    now = time.time()
    if pair in _price_cache and now - _price_cache[pair][0] < 1.5:
        return _price_cache[pair][1]
    try:
        r = requests.get(BINANCE_TICKER, params={"symbol": pair}, timeout=5)
        p = float(r.json()["price"])
        _price_cache[pair] = (now, p)
        _record_price(pair, now, p)
        return p
    except Exception:
        try:  # secours Coinbase
            sym = pair.replace("USDT", "")
            r = requests.get(COINBASE.format(sym), timeout=5)
            p = float(r.json()["data"]["amount"])
            _record_price(pair, now, p)
            return p
        except Exception:
            return None


# ── SCORE DE DANGER (idée Steven 22/07) : un écart instantané suffisant pour
# déclencher un signal ne dit rien sur la STABILITÉ récente du marché. Un prix
# qui a traversé le strike plusieurs fois dans la dernière minute est un
# marché indécis -> même si l'écart passe la marge PILE au moment T, le risque
# de retournement avant résolution est plus élevé qu'un marché qui a dérivé
# tranquillement d'un seul côté. Protection SUPPLÉMENTAIRE (pas un remplacement
# de la marge dynamique existante).
_price_history: dict = {}  # pair -> deque[(ts, price)]
_history_lock = threading.Lock()
_HISTORY_KEEP_S = 120.0  # garde 2 min d'historique, largement assez pour window_s=60


def _record_price(pair: str, ts: float, price: float):
    with _history_lock:
        dq = _price_history.setdefault(pair, deque())
        dq.append((ts, price))
        cutoff = ts - _HISTORY_KEEP_S
        while dq and dq[0][0] < cutoff:
            dq.popleft()


def get_price_history(pair: str) -> list:
    """Accesseur PUBLIC en LECTURE SEULE de l'historique de prix (Steven 23/07,
    pour le market maker) : evite qu'un module externe pioche directement dans
    _price_history (prive). Retourne une COPIE (liste de tuples (ts, price))."""
    with _history_lock:
        return list(_price_history.get(pair, ()))


def danger_score(pair: str, strike: float | None, window_s: float = 60.0) -> int:
    """Score 0-100 : instabilité récente du marché près du strike.
    - flips : nombre de fois où le prix a traversé le strike dans window_s ->
      6 flips ou plus = score de flip maximal (marché en pile-ou-face).
    - vélocité : amplitude moyenne entre points consécutifs, normalisée au
      prix -> un marché qui bouge vite peut retourner un écart qui semblait
      sûr au moment du signal.
    0 = calme/tranché, 100 = très instable. N'affecte rien tant qu'il n'est
    pas branché à une décision (cf. trader.py DANGER_MAX)."""
    if strike is None or strike <= 0:
        return 0
    with _history_lock:
        dq = list(_price_history.get(pair, ()))
    if len(dq) < 3:
        return 0
    now = dq[-1][0]
    recent = [(t, p) for t, p in dq if now - t <= window_s]
    if len(recent) < 3:
        return 0
    signs = [1 if p >= strike else -1 for _, p in recent]
    flips = sum(1 for i in range(1, len(signs)) if signs[i] != signs[i - 1])
    flip_score = min(1.0, flips / 6.0)
    diffs = [abs(recent[i][1] - recent[i - 1][1]) for i in range(1, len(recent))]
    avg_move = sum(diffs) / len(diffs) if diffs else 0.0
    vel_score = min(1.0, (avg_move / strike) / 0.0005)  # 0.05%/tick de moyenne = max
    return round(100 * (0.65 * flip_score + 0.35 * vel_score))


def momentum(pair: str, fast_s: float = 2.5, slow_s: float = 12.0) -> dict | None:
    """LOG-ONLY (Steven 22/07) : vitesse du prix sur une fenetre COURTE (sursaut
    recent, le vrai signal qu'on veut capter) et une fenetre LONGUE (filtre anti-
    bruit -> si le court dit 'ca accelere' mais le long dit 'c'est plat depuis
    12s', c'est probablement du bruit de cotation). N'affecte AUCUNE decision de
    trading pour l'instant -> juste loggue pour voir ce que ca aurait donne avant
    d'envisager de l'utiliser comme signal d'entree anticipee.

    Retourne {fast_pct_s, slow_pct_s, confirms} ou None si historique insuffisant.
    'confirms' = True si le court et le long vont dans le MEME sens (pas juste
    du bruit court-terme)."""
    with _history_lock:
        dq = list(_price_history.get(pair, ()))
    if len(dq) < 3:
        return None
    now = dq[-1][0]
    price_now = dq[-1][1]

    def _rate(window_s):
        # point de reference : le plus RECENT parmi ceux vieux d'au moins
        # window_s*0.8 (tolerance -> pas toujours un point pile a -window_s).
        candidates = [(t, p) for t, p in dq if now - t >= window_s * 0.8]
        if not candidates:
            return None
        ref_t, ref_p = candidates[-1]
        elapsed = max(0.5, now - ref_t)
        return (price_now - ref_p) / ref_p / elapsed * 100  # %/s

    fast = _rate(fast_s)
    slow = _rate(slow_s)
    if fast is None or slow is None:
        return None
    confirms = (fast > 0 and slow > 0) or (fast < 0 and slow < 0)
    return {
        "fast_pct_s": round(fast, 5),
        "slow_pct_s": round(slow, 5),
        "confirms": confirms,
    }


def poly_strike(slug: str) -> float | None:
    """Strike officiel Polymarket (Chainlink priceToBeat), extrait de l'API Gamma.
    Plus fiable que le strike Binance car c'est la vraie source de resolution."""
    return _poly_strike_cache.get(slug)


def _strike_at(pair: str, start_ts: int, slug: str = None):
    """Prix de référence (strike) : priorise le strike Poly (Chainlink) si dispo,
    fallback sur l'open Binance de la bougie 1min du début."""
    # Priorité 1 : strike officiel Polymarket (Chainlink)
    if slug:
        poly_px = _poly_strike_cache.get(slug)
        if poly_px and poly_px > 0:
            return poly_px
    # Fallback 2 : open Binance (legacy)
    key = (pair, start_ts)
    if key in _strike_cache:
        return _strike_cache[key]
    try:
        r = requests.get(
            BINANCE_KLINES,
            params={
                "symbol": pair,
                "interval": "1m",
                "startTime": start_ts * 1000,
                "limit": 1,
            },
            timeout=6,
        )
        k = r.json()
        if k and len(k[0]) > 4:
            strike = float(k[0][1])  # open de la bougie 1min du début
            _strike_cache[key] = strike
            return strike
    except Exception:
        pass
    return None


GAMMA = "https://gamma-api.polymarket.com/markets"
_HDR = {"User-Agent": "Mozilla/5.0"}

_sync_clock()  # sync initiale (GAMMA/_HDR maintenant definis)
threading.Thread(target=_clock_sync_loop, daemon=True).start()
# mesure de volatilite en tache de fond (ne bloque pas le demarrage : les valeurs
# par defaut mesurees le 21/07 servent tant que la 1re mesure n'est pas revenue)
threading.Thread(
    target=lambda: (_measure_vol_ratios(), _vol_loop()), daemon=True
).start()
# sigma reelle par actif (Steven 29/07) : meme pattern, pour le modele de
# probabilite brownien (probability_above_strike).
threading.Thread(
    target=lambda: (_measure_sigma5m(), _sigma_loop()), daemon=True
).start()


# ── CACHE DES METADONNEES DE MARCHE ──
# Les champs qui nous servent (slug, clobTokenIds, outcomes, endDate, question)
# sont FIXES pour un slug donne : ils ne changent jamais de la naissance a la
# resolution de la fenetre. Or on les re-telechargeait a CHAQUE scan (15 requetes
# HTTP, ~3.5s) -> la boucle ne repassait sur un marche que toutes les ~9s, ne
# laissant que ~3 tentatives d'achat sur les 30 dernieres secondes. C'est LA cause
# des echecs d'achat repetes. On les met en cache : un slug n'est telecharge
# qu'UNE SEULE FOIS. Scan suivant = quasi instantane.
_market_cache: dict = {}
_market_cache_lock = threading.Lock()
_NEG_TTL = (
    20.0  # un slug introuvable (fenetre pas encore ouverte) est reteste apres 20s
)


def _fetch_one_market(slug: str):
    with _market_cache_lock:
        hit = _market_cache.get(slug)
    if hit is not None:
        kind, payload, ts = hit
        if kind == "ok":
            return payload
        if time.time() - ts < _NEG_TTL:
            return None  # negatif recent : inutile de re-taper l'API
    try:
        r = requests.get(GAMMA, params={"slug": slug}, timeout=6, headers=_HDR)
        d = r.json()
        m = (d[0] if isinstance(d, list) else d) if d else None
        ok = bool(m) and m.get("active") and not m.get("closed")
        with _market_cache_lock:
            _market_cache[slug] = (
                ("ok", m, time.time()) if ok else ("no", None, time.time())
            )
            if len(_market_cache) > 400:  # purge simple : garde les 200 plus recents
                for k in sorted(_market_cache, key=lambda k: _market_cache[k][2])[:200]:
                    _market_cache.pop(k, None)
            # Extraire le strike officiel Polymarket (Chainlink priceToBeat)
            if ok and m:
                try:
                    events = m.get("events") or []
                    if events:
                        ptb = (events[0].get("eventMetadata") or {}).get("priceToBeat")
                        if ptb and ptb > 0:
                            _poly_strike_cache[slug] = float(ptb)
                except Exception:
                    pass
        return m if ok else None
    except Exception:
        return None  # erreur reseau : on ne met PAS en cache negatif (on retentera)


def find_active_markets(active_tags: list[str] | None = None) -> list[dict]:
    """Trouve les marchés up/down 5min ACTIFS par slug DÉTERMINISTE (fenêtres alignées
    sur 5 min) — pas de recherche, fetch direct = rapide et fiable pour une boucle serrée.
    Renvoie la fenêtre COURANTE + la précédente (encore en résolution).

    Les requêtes sont lancées EN PARALLÈLE (au lieu de sequentiel) : gagne
    plusieurs centaines de ms a plusieurs secondes par scan.

    FAST MODE (Laguna XS 24/07) : si active_tags est fourni, ne fetch QUE les
    coins actifs (ex: ['xrp'] = 3 slugs au lieu de 15). Economise ~80% des
    appels API quand 1-2 coins sont actifs."""
    now = int(synced_now())
    base = (now // 300) * 300
    tags = active_tags or ["btc", "eth", "sol", "xrp", "doge"]
    slugs = [
        f"{tag}-updown-5m-{ws}" for ws in (base, base - 300, base + 300) for tag in tags
    ]
    results = _pool.map(_fetch_one_market, slugs)
    return [m for m in results if m is not None]


def parse_updown_market(market: dict) -> dict | None:
    """Extrait (symbol, pair, start_ts, end_ts) d'un marché up/down. None sinon."""
    slug = (market.get("slug") or "").lower()
    q = (market.get("question") or "").lower()
    sym = pair = None
    for kw, (s, p) in SYMS.items():
        if kw in slug or kw in q:
            sym, pair = s, p
            break
    if not sym or "up or down" not in q and "updown" not in slug:
        return None
    # start_ts depuis le slug 'btc-updown-5m-<unix>' ; sinon via endDate - 300
    m = re.search(r"updown-\w+-(\d{9,11})", slug)
    start_ts = int(m.group(1)) if m else None
    end = market.get("endDate")
    end_ts = None
    if end:
        from datetime import datetime, timezone

        try:
            end_ts = int(datetime.fromisoformat(end.replace("Z", "+00:00")).timestamp())
        except Exception:
            pass
    if start_ts is None and end_ts:
        start_ts = end_ts - 300
    if start_ts is None or end_ts is None:
        return None
    return {"symbol": sym, "pair": pair, "start_ts": start_ts, "end_ts": end_ts}


def evaluate(
    market: dict,
    window_secs: float = 45.0,
    base_margin: float = 6.0,
    margin_per_sec: float = 0.6,
    min_secs_left: float = 6.0,
) -> dict | None:
    """Décide s'il faut sniper (et quel côté) SUR CE marché, MAINTENANT.
    Retourne {side: 'Up'|'Down', seconds_left, strike, price, buffer, margin} ou None.

    - trade uniquement si secondes_restantes <= window_secs (fin de fenêtre),
    - ET >= min_secs_left : en dessous, un ordre RÉEL n'a pas le temps de se placer
      et se confirmer (latence réseau/CLOB) -> ces cas gonflent artificiellement le
      win rate papier avec des trades irréalisables en réel,
    - marge exigée = base + secondes_restantes × margin_per_sec (le prix peut bouger),
    - écart |price - strike| >= marge -> issue quasi certaine -> on snipe le bon côté.

    MARGES EN POURCENTAGE (21/07) : les seuils etaient en DOLLARS ABSOLUS, calibres
    pour BTC (~66k$). Resultat : XRP (1.16$) aurait du bouger de 85$ pour declencher
    un signal -> mathematiquement impossible, ces marches ne tradaient JAMAIS.
    Les seuils sont desormais proportionnels au prix : BTC garde exactement la meme
    marge qu'avant (33$ a 45s, streak preserve) et chaque marche obtient son
    equivalent proportionnel."""
    p = parse_updown_market(market)
    if not p:
        return None
    now = (
        synced_now()
    )  # heure calee serveur (pas l'horloge locale, cf. decouverte 21/07)
    secs_left = p["end_ts"] - now
    if secs_left < min_secs_left or secs_left > window_secs:
        return (
            None  # trop tard pour executer en reel, ou trop tot (edge pas encore sur)
        )
    strike = _strike_at(p["pair"], p["start_ts"], slug=market.get("slug"))
    price = _binance_price(p["pair"])
    if strike is None or price is None:
        return None
    gap = price - strike
    # conversion des seuils historiques BTC en fraction du prix (reference 66410$)
    _BTC_REF = 66410.0
    # ... puis mise a l'echelle par la VOLATILITE MESUREE du marche : un actif qui
    # bouge 1.27x plus que BTC exige une marge 1.27x plus large pour offrir la
    # MEME securite. Sans ca, les altcoins prenaient plus de risque a marge egale.
    with _vol_lock:
        vol = _vol_ratio.get(p["symbol"], 1.0)
    margin = (
        price * (base_margin / _BTC_REF)
        + max(0.0, secs_left) * price * (margin_per_sec / _BTC_REF)
    ) * vol
    # exigence de conviction : l'ecart doit depasser la marge d'un facteur suffisant
    # pour ce marche (cf. _MIN_CONVICTION) -> ecarte les signaux "au ras du seuil"
    # qui, sur les actifs a faible precision de cotation (XRP), sont du bruit.
    margin *= _MIN_CONVICTION.get(p["symbol"], 1.0)
    if gap >= margin:
        side = "Up"
    elif gap <= -margin:
        side = "Down"
    else:
        return None  # écart insuffisant vu le temps restant : trop risqué
    # precision d'affichage adaptee au prix : round(x, 2) ecraserait a 0 les
    # ecarts de DOGE (0.07$) ou XRP (1.16$).
    nd = 2 if price >= 100 else (4 if price >= 1 else 6)
    return {
        "side": side,
        "symbol": p["symbol"],
        "seconds_left": round(secs_left),
        "strike": round(strike, nd),
        "price": round(price, nd),
        "buffer": round(gap, nd),
        "margin": round(margin, nd),
        "danger": danger_score(p["pair"], strike),
    }
