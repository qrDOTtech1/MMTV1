"""GHOST V3 — MARKET MAKER CONDITIONNEL (Steven 23/07).

OBJECTIF : coter un BID et un ASK sur le MEME token (ex. "Up") pour capturer
le SPREAD, distinct de l'arbitrage two-leg (qui achete UP+DOWN pour un gain
garanti). Le market making gagne si le prix oscille autour de la fair value
sans tendance forte ; il perd si un mouvement directionnel le traverse (d'ou
les regimes qui coupent les quotes des que le marche n'est plus "calme").

V1 (cette passe) : donnees REST (polling existant btc_updown.py), fair value
heuristique explicable (pas de ML), 4 regimes deterministes, inventaire
strictement capsule, kill switch. EXECUTION REELLE a taille MINIMALE des le
depart (Steven 23/07 : "testons en reel pour avoir des vraies donnees" — pas
de shadow mode separe, mais les memes garde-fous s'appliquent comme CAPS DE
TAILLE plutot que comme simulation).

DEFERE (v2, note dans le plan, pas fait ici pour rester dans un scope
raisonnable pour une 1re passe sur argent reel) :
  - flux WebSocket Polymarket + Binance (actuellement REST polling, latence
    ~1-2s au lieu de <100ms — suffisant pour valider la logique, pas pour
    scaler le size plus tard)
  - persistance structuree (parquet/sqlite) des snapshots + markout analytics
  - modele de fair value calibre (remplace l'heuristique)
  - suite de tests dediee (des invariants sont verifies inline : bid<ask,
    tick clamp, jamais de quote croisee — voir _clamp_quote)
"""

import math
import time
from collections import deque

MIN_ORDER_SIZE_SHARES = 5.0  # meme plancher CLOB que le reste du bot

# ── PARAMETRES (delibrement prudents pour la 1re validation reelle) ──
MM_ENABLED_DEFAULT = False
MM_QUOTE_NOTIONAL_USD = 1.0      # taille visee PAR QUOTE (bid ou ask), en $ —
                                 # minimal, on valide l'execution avant de scaler.
MM_MAX_NOTIONAL_PER_MARKET = 3.0   # exposition max (bid+ask ouverts) par marche/token
MM_MAX_NOTIONAL_TOTAL = 6.0        # exposition max cumulee tous marches MM confondus
MM_MIN_HALF_SPREAD = 0.02        # jamais moins que 2c de demi-spread (frais + marge)
MM_MAX_HALF_SPREAD = 0.12
MM_REQUOTE_MIN_DELTA = 0.015     # ne recote que si le prix desire bouge d'au moins ca
                                 # (evite le churn / perte de priorite de file)
MM_DATA_MAX_AGE_S = 4.0          # si le dernier prix Binance date de plus que ca -> stale, annule
MM_CLOSE_CUTOFF_S = 30.0         # n'ouvre plus de nouvelle quote sous ce temps restant
MM_CLOSE_CANCEL_S = 15.0         # annule les quotes passives sous ce temps restant
MM_TIME_HANDOFF_S = 60.0         # Steven 23/07 (correctif ETH/SOL perdus a 100% : un
                                 # marche reste CALM toute la fenetre sans jamais
                                 # declencher PANIC/MOMENTUM, mais peut quand meme
                                 # deriver contre nous) : au-dela de ce seuil, une
                                 # position tenue est transferee a l'orphan manager
                                 # MEME si le regime est toujours CALM -> protection
                                 # active garantie avant chaque resolution, pas
                                 # seulement sur un changement de regime.
MM_PANIC_DIVERGENCE = 0.15       # |poly_mid - fair_value| au-dela duquel c'est une
                                 # dislocation (PANIC), pas juste du bruit de cotation
MM_LEG_MIN = 0.15                # Steven 23/07 ("on gagnait tout avant, plus maintenant") :
MM_LEG_MAX = 0.85                # meme garde-fou que l'arb (BOTH_SIDE_LEG_MIN/MAX) — ne PAS
                                 # coter/acheter en dehors de cette zone. Sous 0.15 ou au-dessus
                                 # de 0.85, le marche est quasi tranche -> plus un ticket de
                                 # loterie a haute variance qu'une vraie capture de spread. La
                                 # perte ETH/XRP/SOL (achat "Up" a 0.02$, marche qui donnait
                                 # "Down" gagnant) vient exactement de ce trou : le MM cotait
                                 # sans limite jusqu'a 0.02/0.98, contrairement a l'arb qui a
                                 # toujours eu ce plancher.
# ── ZONE RENTABLE (Steven 23/07, backtest sur 256 fenetres reelles avec sortie
# realiste au bid) : le SEUL intervalle d'entree a P&L positif net du spread est
# [0.55, 0.80] (favori modere) + TP/SL symetrique 0.15. Hors de cette zone :
#   - poly_mid > 0.80 (favori fort) : -60$ (le pire, WR 80% trompeur -> flips ruineux)
#   - poly_mid < 0.55 (coin-flip / outsider) : variance pure, negatif au bid reel
# On ne cote donc QUE dans cette fenetre etroite -> moins de volume mais le seul
# regime ou l'edge survit au spread bid-ask (~14c) de ces marches.
MM_SWEET_LOW = 0.55
MM_SWEET_HIGH = 0.80
MM_MOMENTUM_THRESHOLD = 0.008    # %/s de momentum fast au-dela duquel on coupe (MOMENTUM)
MM_DRIFT_WINDOW_S = 90.0         # Steven 23/07 ("selection adverse sur cotes perimees") :
MM_DRIFT_THRESHOLD = 0.10        # une derive LENTE (sous les seuils PANIC/MOMENTUM tick-par-
                                 # tick, donc invisible instantanement) mais qui deplace le
                                 # marche de >= ce seuil sur MM_DRIFT_WINDOW_S est traitee
                                 # comme une dislocation : le regime CALM ne protege QUE
                                 # contre le bruit court terme, pas contre une tendance
                                 # soutenue qui grignote lentement contre notre cotation
                                 # figee (anti-churn 1.5c -> notre bid reste immobile
                                 # pendant qu'un vendeur informe nous vend juste avant
                                 # que le marche parte dans le mauvais sens).
MM_MIN_RELATIVE_SIGMA = 0.00005  # plancher de volatilite RELATIF au prix (0.005%/sqrt(s)) :
                                 # un plancher ABSOLU unique serait ecrase pour BTC (~66000$)
                                 # et disproportionne pour SOL (~78$) -> chacun a son echelle.
MM_TICK = 0.01
MM_MIN_PRICE = 0.02
MM_MAX_PRICE = 0.98

# ── RISQUE / KILL SWITCH ──
MM_DAILY_LOSS_LIMIT_USD = -3.0   # kill switch automatique si le P&L MM du jour descend sous ca
MM_MAX_CONSEC_ADVERSE = 4        # N fills consecutifs "adverse" (markout negatif) -> pause


def _norm_cdf(x):
    """CDF de la loi normale standard (approx Abramowitz-Stegun, pas de scipy)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def fair_value(spot_now, target, secs_left, sigma_per_sqrt_s, momentum_pct_s=0.0):
    """Probabilite (0-1) que le marche resolve 'Up' (spot_now > target a la
    fin de la fenetre), estimee par un modele DE DIFFUSION SIMPLE :
    - distance normalisee = (spot - target) / (sigma * sqrt(temps restant))
      -> plus le temps restant est court, moins un ecart donne peut se
      retourner (le denominateur retrecit -> la proba tend vers 0 ou 1).
    - legere correction de momentum : un mouvement recent confirme pousse
      un peu la proba dans son sens (mais reste mineur, pas dominant).
    Retourne une probabilite bornee [0.01, 0.99] (jamais 0/1 exact : garde
    toujours une marge d'incertitude irreductible).
    """
    if target is None or target <= 0 or secs_left is None or secs_left <= 0:
        return 0.5
    sigma_t = max(1e-9, sigma_per_sqrt_s * math.sqrt(secs_left))
    z = (spot_now - target) / sigma_t
    p = _norm_cdf(z)
    # correction momentum : nudge borne (+/-0.05 max) dans le sens confirme
    nudge = max(-0.05, min(0.05, momentum_pct_s * 3.0))
    p = p + nudge
    return round(max(0.01, min(0.99, p)), 4)


def estimate_sigma(price_history, window_s=30.0):
    """Volatilite (ecart-type des rendements) estimee sur l'historique de prix
    deja collecte par btc_updown._price_history (pas de nouvel appel reseau).
    Retourne un sigma PAR SECONDE (a multiplier par sqrt(temps) dans fair_value).
    Fallback prudent (sigma large -> proba proche de 0.5) si pas assez de points.

    REVERT 23/07 (Steven) : la version "corrigee" (dedup + mediane + plancher
    relatif) rendait la fair value trop "correcte" (~0.5, au milieu) -> le MM
    cotait des coin-flips 50/50 = variance pure = machine a perdre. La version
    d'origine ci-dessous sous-estime la volatilite (garde les doublons de cache
    Binance -> sigma minuscule -> fair sature vers 0.99/0.01), ce qui poussait
    les quotes du MM vers les FAVORIS (bid ~0.87 / ask ~0.98) : acheter un
    favori qui dip et le revendre = gain regulier. C'est le comportement qui
    RAPPORTAIT avant. On le restaure a l'identique."""
    if not price_history or len(price_history) < 4:
        return 0.01  # defaut prudent : incertitude elevee tant qu'on n'a pas mesure
    now = price_history[-1][0]
    recent = [(t, p) for t, p in price_history if now - t <= window_s]
    if len(recent) < 4:
        recent = list(price_history)[-8:]
    diffs = []
    for i in range(1, len(recent)):
        dt = recent[i][0] - recent[i - 1][0]
        if dt <= 0:
            continue
        diffs.append(abs(recent[i][1] - recent[i - 1][1]) / max(dt, 0.01) ** 0.5)
    if not diffs:
        return 0.01
    avg = sum(diffs) / len(diffs)
    return max(1e-6, avg)


def classify_regime(secs_left, momentum_signal, danger, poly_mid, fair, depth_ok, data_age_s):
    """Regime deterministe et loggable (Steven : "classifieur simple,
    deterministe et logge"). Ordre de priorite volontaire : CLOSE > PANIC >
    MOMENTUM > CALM (le plus prudent gagne en cas d'ambiguite)."""
    if data_age_s is not None and data_age_s > MM_DATA_MAX_AGE_S:
        return "STALE"
    if secs_left is not None and secs_left <= MM_CLOSE_CANCEL_S:
        return "CLOSE"
    if poly_mid is not None and fair is not None and abs(poly_mid - fair) >= MM_PANIC_DIVERGENCE:
        # Binance (fair) ne confirme pas le prix Polymarket -> dislocation.
        # Spec Steven : PANIC = jamais d'execution reelle MM ici (pas notre
        # strategie two-leg/mean-reversion), on coupe simplement les quotes.
        return "PANIC"
    if momentum_signal and momentum_signal.get("confirms") and \
            abs(momentum_signal.get("fast_pct_s", 0.0)) >= MM_MOMENTUM_THRESHOLD:
        return "MOMENTUM"
    if not depth_ok:
        return "THIN"
    return "CALM"


def detect_drift(mid_history, window_s=MM_DRIFT_WINDOW_S, threshold=MM_DRIFT_THRESHOLD):
    """Derive LENTE (Steven 23/07) : compare le point le plus ANCIEN encore
    dans la fenetre au point le plus RECENT. Contrairement au momentum (fast/
    slow rate, sensible au bruit instantane), ceci capte un glissement
    SOUTENU meme s'il est trop progressif pour jamais depasser les seuils
    PANIC/MOMENTUM tick-par-tick. `mid_history` : liste [(ts, poly_mid), ...]
    deja triee par ts croissant. Retourne le deplacement signe, ou None si
    pas assez de recul (fenetre pas encore couverte)."""
    if len(mid_history) < 2:
        return None
    now = mid_history[-1][0]
    oldest_in_window = None
    for t, v in mid_history:
        if now - t <= window_s:
            oldest_in_window = (t, v)
            break
    if oldest_in_window is None or now - oldest_in_window[0] < window_s * 0.5:
        return None  # pas assez de recul pour juger d'une tendance soutenue
    return mid_history[-1][1] - oldest_in_window[1]


def _clamp_quote(bid, ask):
    """Contraintes IMPERATIVES (Steven) : tick size, bornes prix, bid<ask
    strict apres arrondi. Retourne (bid, ask) valides ou None si impossible
    de maintenir un spread positif apres clamp (mieux vaut ne pas coter)."""
    def _tick(x):
        return round(round(x / MM_TICK) * MM_TICK, 2)
    bid = max(MM_MIN_PRICE, min(MM_MAX_PRICE, _tick(bid)))
    ask = max(MM_MIN_PRICE, min(MM_MAX_PRICE, _tick(ask)))
    if ask - bid < MM_TICK:  # spread nul ou croise apres clamp -> invalide
        return None
    return bid, ask


def compute_quote(fair, half_spread, inventory_skew):
    """bid = fair - half_spread - skew ; ask = fair + half_spread - skew.
    Le skew (positif si trop long, negatif si trop court) deplace les 2 cotes
    dans le MEME sens pour re-equilibrer l'inventaire (Steven : "si trop de
    UP achetes, abaisser le bid UP et rendre l'ask UP plus agressif")."""
    half_spread = max(MM_MIN_HALF_SPREAD, min(MM_MAX_HALF_SPREAD, half_spread))
    bid = fair - half_spread - inventory_skew
    ask = fair + half_spread - inventory_skew
    return _clamp_quote(bid, ask)


def half_spread_for(sigma_per_sqrt_s, data_age_s, depth_ratio, secs_left):
    """Demi-spread FONCTION de la volatilite, de la fraicheur des donnees, de
    la profondeur de carnet et du temps restant (Steven : formule explicite,
    pas une constante). Chaque facteur ELARGIT le spread (plus prudent) quand
    les conditions se degradent."""
    base = MM_MIN_HALF_SPREAD
    vol_term = min(0.06, sigma_per_sqrt_s * 4.0)          # + volatil -> + large
    age_term = min(0.03, max(0.0, data_age_s - 1.0) * 0.02)  # data vieillissante -> + large
    depth_term = min(0.03, max(0.0, (1.0 - depth_ratio)) * 0.04)  # carnet fin -> + large
    time_term = 0.0
    if secs_left is not None and secs_left < 60:
        time_term = min(0.04, (60 - secs_left) / 60 * 0.04)  # approche de la fin -> + large
    return base + vol_term + age_term + depth_term + time_term


def inventory_skew(net_notional, max_notional):
    """Skew proportionnel a l'exposition nette / le plafond -> jamais > le
    demi-spread max pour rester coherent (pas de skew qui inverse bid/ask)."""
    if max_notional <= 0:
        return 0.0
    ratio = max(-1.0, min(1.0, net_notional / max_notional))
    return round(ratio * MM_MAX_HALF_SPREAD * 0.5, 4)


class MarketMakerState:
    """Etat persistant (dans multi_state.json, bucket 'mm') d'un market maker
    conditionnel. Une instance gere TOUS les symboles ; l'etat par symbole vit
    dans le dict passe par le trader (self.state['mm'])."""

    @staticmethod
    def blank():
        return {
            "enabled": MM_ENABLED_DEFAULT,
            "killed": False,
            "kill_reason": None,
            "daily_pnl": 0.0,
            "daily_pnl_date": None,
            "consec_adverse": 0,
            "quotes": {},      # sym -> {bid_order_id, ask_order_id, bid, ask, ts, token_id, shares, avg_entry}
            "inventory": {},   # sym -> net notional $ de la fenetre COURANTE uniquement (positif = long Up)
            "fills": [],       # historique des fills (markout differe)
            "regime_log": {},  # sym -> dernier regime + ts (pour le dashboard)
            "pending": [],     # positions laissees par un ROULEMENT de fenetre (nouveau
                               # token_id chaque 5min) : {sym, slug, token_id, shares,
                               # avg_entry, end_ts} en attente de resolution Polymarket
                               # (Steven 23/07, correctif du bug "balance 0" -> l'inventaire
                               # etait garde par SYMBOLE alors que chaque fenetre est un
                               # token CTF DIFFERENT, d'ou des tentatives de vente sur un
                               # token qu'on ne detient plus).
            "mid_history": {},   # sym -> [(ts, poly_mid), ...] pour detect_drift (Steven 23/07)
            "markout_pending": [],  # {sym, side, fill_price, token_id, check_at} (Steven 23/07,
                                     # "aucun controle de qualite de fill" -> verifie si le
                                     # marche a bouge contre nous APRES le fill, alimente
                                     # consec_adverse (donc le kill switch existant).
        }
