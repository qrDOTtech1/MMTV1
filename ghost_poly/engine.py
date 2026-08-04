"""
GHOST POLY — moteur de scan/paper-trading Polymarket.

Boucle : rafraîchit le catalogue des marchés (2 min), scanne les orderbooks
des N plus gros marchés (30 s), et pour chaque arb YES+NO < 1.00 trouvé :
  - le logge en DB (mesure de fréquence réelle du filon),
  - l'« exécute » en paper : dépense simulée = coût des deux jambes au ask,
    payout garanti = 1.00 $ x taille à la résolution du marché.

Le paper P&L « locked » est donc du profit mathématiquement garanti SI les
fills avaient eu lieu — la seule hypothèse optimiste est le fill au ask
affiché (risque réel : un autre bot prend l'arb avant nous). C'est exactement
ce que les données diront : fréquence x taille x contention.

L'exécution réelle (py-clob-client, wallet Polygon) se branche dans
execute_real() — volontairement non implémentée tant qu'il n'y a pas de
compte configuré ; aucune autre barrière.
"""

import asyncio
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import aiohttp  # noqa: E402
from core.polymarket import (get_active_markets, scan_complementary_arb, scan_momentum,  # noqa: E402
                              scan_sport_inplay, get_book, best_ask, best_bid, get_price_history)
from core.news_ai import estimate_probability  # noqa: E402
from ghost_poly import db  # noqa: E402

import os  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
load_dotenv(Path(__file__).parent.parent / ".env")

MAX_LIVE_USD_PER_ARB = 50.0        # plafond par arb en réel — garanti mathématiquement
MIN_DIRECTIONAL_USD = 1.0          # mise plancher (faible confiance)
FAILED_RETRY_COOLDOWN_S = 180      # après un échec, on ne retente pas le même marché avant ça
MAX_DIRECTIONAL_USD = 16.0         # relevé 10->16 (Steven 22/07, plein gaz) : laisse le
                                   # sizing Kelly respirer. BET_CAP_FRACTION reste la vraie
                                   # sécurité (jamais >X% de l'equity sur un pari).
KELLY_FRACTION = 0.25              # 1/4 Kelly (Steven 22/07) : WR réel encore incertain
                                   # (78-95% selon estimation sur 86 trades) -> Kelly plein
                                   # ruineux si edge surestimé, 1/4 absorbe l'erreur.
KELLY_ASSUMED_EDGE = 0.06          # edge de proba supposé au max de conviction (6pts au-dessus
                                   # du prix marché) — conservateur, à recalibrer avec plus de données.
# --- Sizing proportionnel au capital, calibré pour le VOLUME ---
# But (Steven) : mettre toutes les chances de notre côté = maximiser le NOMBRE
# de paris en parallèle, pas concentrer le capital. On vise donc ~N positions
# simultanées : chaque mise ≈ capital_total / N, ajustée par la conviction, puis
# bornée. Petit solde -> petites mises -> plus de positions tiennent (plus de
# volume/diversification). Gros solde -> mises qui grandissent proportionnellement.
TARGET_CONCURRENT_POSITIONS = 6    # relevé 4->6 (Steven : "pas de maximum de positions", + de volume) :
                                   # plus de paris en parallèle = plus de tirages sur l'edge positif
BET_CAP_FRACTION = 0.40            # relevé 0.30->0.40 (Steven 22/07, plein gaz) : jamais plus
                                   # de 40% du capital total sur un seul pari
CASH_RESERVE_FRACTION = 0.12       # presence garde ~12% de l'equity en cash (poudre sèche) pour que
                                   # le near-certain (favorite-longshot, + de valeur) puisse sniper —
                                   # sinon presence déploie TOUT et on rate les quasi-sûrs (Steven).
# --- Améliorations "menu" (Steven : tout appliquer) ---
SPREAD_MAX = 0.06                  # ③ garde liquidité : skip si (ask-bid) > 6¢ à l'entrée — à notre
                                   # petite taille un spread large mange l'edge (mauvais fill).
CAT_EXPOSURE_CAP_FRACTION = 0.45   # ④ jamais plus de 45% de l'equity sur UNE catégorie (ex. foot) :
                                   # un mauvais jour sur une famille ne coule pas le compte.
DEAD_HOURS = 18                    # ⑤ position "morte" : ouverte depuis > DEAD_HOURS et quasi flat
DEAD_FLAT_PCT = 0.04               #    (|variation| < 4%) = capital dormant -> on recycle (vélocité).
CATPERF_REFRESH_SCANS = 15         # ① méta-edge : rafraîchit la perf par catégorie tous les N scans
CATPERF_MIN_CLOSES = 8             #    (nb mini de clôtures pour piloter l'edge_mult par la data)
MIN_AI_EDGE_PTS = 15               # écart mini estimation IA vs marché (points de %) pour trader
MAX_AI_EDGE_PTS = 45               # au-delà, on plafonne le scaling (edge suspect = pas plus de confiance)
AI_SCAN_EVERY_N_CYCLES = 10        # ~5 min : économise le quota IA gratuit (50 req/j)
AI_NEWS_ENABLED = False            # COUPÉ (Steven 18/07) : analyse LLM broad = pas d'edge à source de vérité
# --- SNIPER IA VÉRIFIÉ (idée Steven) : IA + outils, décide sur faits vérifiables ---
AI_VERIFIED_ENABLED = True
AI_VERIFIED_EVERY_N = 8            # throttle appels LLM (~4 min) : coût/quota
AI_VERIFIED_MIN_CONF = 0.85       # ne trade QUE si confiance IA >= 85% ET grounded=true
AI_VERIFIED_MAX_USD = 4.0         # mise PLAFONNÉE (non prouvé — on mesure sa calibration d'abord)
AI_VERIFIED_MAX_CALLS = 3         # max 3 candidats évalués par passage (coût LLM)
AI_VERIFIED_MIN_PRICE = 0.35      # zone tradeable (edge = marché mésestime vs la preuve)
AI_VERIFIED_MAX_PRICE = 0.90
AI_MANAGE_EVERY_N = 6             # boucle de surveillance IA : re-check des positions IA tous les N scans
AI_CONSENSUS_MODELS = 3          # ② nb de modèles interrogés pour le consensus (doivent être d'accord)
AI_SINGLE_MIN_CONF = 0.90        # fallback : si pas de consensus, 1 modèle grounded à >=90% -> demi-mise
CALIB_MIN_CLOSES = 10            # ① nb mini de clôtures IA avant de piloter la mise par la calibration
KELLY_MULT = 6.0                # sizing Kelly-flavor : dial = 1 + ROI_mesuré × KELLY_MULT (borné 0.3-2.0)
MOMENTUM_ENABLED = False           # désactivé : ne filtre pas par sujet, a produit un A/R perdant sur du sport
SPORT_INPLAY_ENABLED = True        # RÉACTIVÉ (Steven 18/07 : "laisse le sport"). Whitelist foot/basket
                                   # UNIQUEMENT = SL gérable (déclin graduel). PAS de combat/UFC (KO
                                   # instantané = ingérable, voir l'incident -99% du 18/07). Commentaire :
                                   # + de trades, mais sur l'EDGE). Avant, il ramassait du tennis/cricket
                                   # qui churnait -> maintenant filtre positif SPORT_INPLAY_WHITELIST.
SPORT_INPLAY_WHITELIST = [         # SEULS ces marchés d'ÉQUIPE passent (inertie = notre edge). PAS de " vs "
                                   # générique (attrape les joueurs de tennis type 'Cordenons: X vs Y').
                                   # On exige un marqueur SPÉCIFIQUE sport d'équipe.
    "nba", "wnba", "fifa", "world cup", "premier league", "champions",
    "la liga", "serie a", "bundesliga", "ligue 1", "mls", "copa", "euro",
    "afcon", "soccer", "football", "team to advance", "team to win",
    " fc ", " fc:", " united", " city ", " sc ", "whitecaps", "galaxy",
    # AJOUTÉ (Steven 18/07) — mesurés proprement, méta-edge coupe si perdants :
    "basketball", "euroleague", "nbl", "eurobasket",              # basket élargi
    # TENNIS RETIRÉ (20/07) : give-back systématique — les positions piquent positif
    # puis s'effondrent à 0 (gap point par point, impossible à stopper). Cause n°1 du
    # skew négatif (Iasi -3,25, Lincoln -2,00...). On ne le trade plus.
    "odi", "t20", "cricket", "test match", "the hundred", "ipl", "bbl",     # cricket
]
_SPORT_INPLAY_OLD = True           # RÉACTIVÉ : le -9.27$ historique était POLLUÉ par des bugs corrigés
                                   # (both-sides, props, cooldown effacé aux redémarrages, mesure à 0,
                                   # mode paper) — il ne reflète pas l'edge réel sous le code actuel.
                                   # Ce matin sport+IA ont fait 10->17$. On le garde MAIS amélioré :
                                   # scalp serré symétrique (TP +5% / SL -6%) = plein de petits gains,
                                   # pertes minuscules. Jugé sur mesure PROPRE (stats remises à zéro).
SPORT_SL_SCALP = 0.06              # sport : coupe vite à -6% (au lieu du SL large 18-40%) — c'est ça
                                   # qui manquait : les perdants traînaient. Band serrée + TP+5% =
                                   # scalping symétrique. (le TP sport reste SPORT_TP_PCT ci-dessus.)
SPORT_HARD_SL = 0.15               # plancher DUR : au-delà de -15%, on coupe même si le score est
                                   # favorable (l'anti-swing ne peut plus tenir). Vécu : Pablo Aunion
                                   # -44% tenu à tort car il "menait un set" mais s'effondrait.
# (Pas de disjoncteur global : choix Steven — on ne fige jamais la machine par
# peur, on gère le risque au trade. "Jamais assez" ne s'arrête pas, il s'adapte.)
SPORT_MATCH_COOLDOWN_S = 3600      # 1h : après avoir tradé un match, on n'y retouche plus — évite le
                                   # whipsaw (Tsitsipas : 5 trades dans les 2 sens sur le même match)
# Resolution-sniping (idée B) : acheter le vainqueur d'un match DÉJÀ DÉCIDÉ
# (confirmé par ESPN) que le marché Polymarket n'a pas fini de pricer à 1.00.
SNIPE_MAX_PRICE = 0.97            # au-dessus, le gain (<3%) ne vaut pas le risque résiduel
SNIPE_MIN_PRICE = 0.60           # en-dessous, incohérence (marché en désaccord avec ESPN ?) -> on s'abstient
CRYPTO_SNIPE_MIN_PRICE = {"SOL": 0.94, "DOGE": 0.94}  # plancher dédié (Steven 22/07) : SOL/DOGE
                              # plus volatils -> n'achète qu'aux favoris quasi-certains
SNIPE_SL = 0.25                  # FILET DE SÉCURITÉ large (Steven 19/07) : un snipe qui chute de >25% =
                                 # coupé quoi qu'il arrive (cas catastrophe / IA indispo). Le jugement FIN
                                 # "on tient ou on sort" est confié à l'IA (elle lit le score live : 0-2 en
                                 # 1re manche = récupérable HOLD ; 0-5 en 9e = plié EXIT). Idée Steven.
SNIPE_AI_MANAGE_FROM = 0.08      # dès -8%, l'IA re-juge le snipe via le score live (hold/exit contextuel).
# ARBITRAGE-IA des snipes borderline (Steven 19/07) : quand ESPN dit "décidé" mais le
# marché price le vainqueur SOUS SNIPE_MIN_PRICE (désaccord), l'IA lit le score live et
# confirme si c'est vraiment plié avant de sniper l'edge (au lieu de s'abstenir aveuglément).
SNIPE_ARB_ENABLED = True
SNIPE_ARB_MIN = 0.40            # sous ce prix, désaccord trop fort -> on s'abstient même avec l'IA
SNIPE_ARB_MIN_CONF = 0.85       # l'IA doit confirmer le MÊME vainqueur, grounded, à >=85%
SNIPE_ARB_MAX_CALLS = 2         # max 2 arbitrages IA par scan (coût LLM)
SNIPE_MIN_GAME_HOURS = 2.0      # DONNÉES STRUCTURÉES (Steven 20/07) : on ne snipe un "décidé" que si
                                # le match a commencé il y a >= 2h (plausiblement fini). Sinon = match en
                                # cours / doubleheader Game 2 -> on s'abstient (fin du bug SF/Seattle).
SNIPE_MAX_USD = 14.0             # near-certain = le jeu des TOP traders (ils chargent le 90-100¢ en
                                 # taille). Basse variance -> on ose plus gros ici qu'ailleurs. Agression
                                 # placée là où edge ET sécurité coïncident (étude des meilleurs, Steven).
# Copy-trading : suit les traders prouvés rentables (leaderboard 7j).
COPY_MAX_USD = 7.0              # + agressif : le copy = notre source FOOT (edge 80%), on charge plus
COPY_MAX_PER_WALLET_USD = 10.0  # plafond d'exposition SIMULTANÉE par wallet source (point ferme
                                # Perplexity) : un bon wallet peut avoir une série perdante, on ne
                                # concentre pas le risque sur un seul, même prouvé.
# ── ZONE DE PRIX GAGNANTE (analyse on-chain, intuition Steven confirmée) ──
# PnL par prix d'entrée sur l'historique réel : 0-30¢ = 0% réussite / -15$ (poison
# absolu, tickets de loterie) ; 70-85¢ (favoris) = 50% réussite (meilleure zone) ;
# 85¢+ = 0% (aucun upside). La zone rentable est 50-85¢. On y CONCENTRE toutes
# les stratégies directionnelles (sport/copy/IA). Le SNIPE est EXEMPT (il achète
# des vainqueurs déjà connus, donc prix élevé = certitude, pas risque).
DIRECTIONAL_MIN_PRICE = 0.50   # sous 0.50 : trop d'outsiders perdants (0-30¢ = 0% !)
EXPLORE_MIN_PRICE = 0.68        # plancher RELEVÉ pour les catégories NON prouvées (edge_mult<1) —
                               # Steven a vu des opens à 58/54/47c : sans edge catégoriel, une
                               # entrée mid-price n'est que de la variance. L'edge prouvé (foot/
                               # Team-to-Advance) garde le mid-price (ces marchés vivent à 0.55-0.65).
DIRECTIONAL_MAX_PRICE = 0.85   # au-dessus : gros favori sans upside, mauvais rapport
# ── GARDE-PRÉSENCE (idée Steven) : toujours >=1 position ouverte sur le compte ──
# But : ne jamais afficher 0 trade (présence visible sur Polymarket, construire un
# track-record), SANS churn. On tient UNE position longue (<48h) sur un favori
# solide de la zone gagnante, uniquement quand le compte est VIDE. Ce n'est pas
# du pari au hasard : favori 0.68-0.84 + gros volume + pas prop/esport.
PRESENCE_ENABLED = False           # RE-COUPÉ (Steven 20/07) : pari de favori = pas d'edge (EV~0 - spread),
                                   # les protections limitent la perte mais NE créent PAS d'edge. L'activité
                                   # doit venir de l'IA/snipes (edge à source de vérité), pas de presence.
                                   # moins le spread. Aucun edge. Remplacé par le sniping vérité.
PRESENCE_MIN_HOURS = 1
PRESENCE_MAX_HOURS = 72        # court terme : évite les futures parkés (World Cup à 1 an),
                               # privilégie les matchs proches qui clôturent VITE (preuve edge rapide)
PRESENCE_FAV_MIN = 0.55        # élargi : favori un peu plus large pour trouver des candidats
PRESENCE_FAV_MAX = 0.90
PRESENCE_SL = 0.12             # stop serré présence (idée Steven) : favori sans edge, on coupe
                               # à -12% au lieu du stop générique large (-31%), pas de saignée.
PRESENCE_TP = 0.08             # take-profit RAPIDE (idée Steven) : encaisse dès +8%, pas d'attente
                               # de résolution (crucial pour futures longs type World Cup 2026).
MOVER_TRIGGER_PTS = 7.0        # SCAN INTELLIGENT (Steven) : un mouvement inter-scan >=7pts sur un
                               # marché sport déclenche une VÉRIFICATION IA prioritaire (le delta dit
                               # OÙ regarder, l'IA dit s'il y a de l'edge). Réactif, pas parieur de momentum.
MOMENTUM_SHADOW_PTS = 3.0      # shadow logger (Perplexity) : loggue tout mouvement >=3pts SANS trader,
                               # pour mesurer où le momentum continue vraiment (R&D gratuit).
# RATCHET / VERROU DE GAINS (leçon du +17£ rendu) : quand le compte marque un
# NOUVEAU SOMMET à +15% du pic précédent, on réduit les mises de moitié pendant
# 6h. On encaisse les runs au lieu de les rejouer intégralement — attaque
# directement notre faiblesse prouvée (gagner puis tout rendre).
RATCHET_GAIN_PCT = 0.15
RATCHET_HOURS = 6
RATCHET_MULT = 0.5
RATCHET_DEPOSIT_PCT = 0.30   # saut de total > +30% en un scan = DÉPÔT de Steven, pas un
                             # gain de trading (impossible à notre sizing/fréquence). On
                             # rebase le pic SANS armer la protection (sinon le dépôt bride
                             # le capital frais — bug repéré par Steven le 2026-07-17).

# SNIPE NEAR-CERTAIN généralisé (favorite-longshot bias, jeu des whales) :
NEARCERT_ENABLED = False           # COUPÉ (Steven 18/07) : favorite-longshot SANS source de vérité =
                                   # pari de favori, edge trop mince/mangé par le spread. Le crypto
                                   # near-cert VÉRIFIABLE passe par _scan_crypto_snipe (vérité Coinbase).
NEARCERT_MIN = 0.88           # favori extrême (favorite-longshot bias = souvent sous-coté)
NEARCERT_MAX = 0.96           # plafond abaissé de 0.985 -> 0.96 (décision Kapitane 2026-07-17) :
                              # au-dessus, payout catastrophique (risquer 97-98 pour gagner 2-3),
                              # une seule perte efface 20 gains. Sweet-spot risque/rendement 0.88-0.96
NEARCERT_MAX_H = 24           # résolution <24h (élargi pour capter + de candidats)
NEARCERT_MAX_USD = 5.0        # petite mise (expérimental, mesuré)
# --- SNIPE MÉTÉO (idée Steven) : edge via source de vérité Open-Meteo ---
WEATHER_SNIPE_ENABLED = True
WEATHER_MAX_USD = 6.0            # petite mise (edge réel mais prévision faillible ~1.5°C)
WEATHER_MAX_HOURS = 14          # ne trade que si résolution < 14h (prévision fiable)
WEATHER_NO_MAX_PRICE = 0.90     # NO : on n'achète que si le marché donne >=10% au bucket
                                # (sinon déjà correctement pricé, pas d'edge). Payout mince sinon.
WEATHER_NO_MIN_PRICE = 0.55     # en dessous, le marché doute déjà trop = pas assez sûr pour nous
WEATHER_YES_MAX_PRICE = 0.75    # YES : on n'achète le bon bucket que s'il est sous-coté
NEARCERT_OK_CATS = {"crypto", "foot", "basket", "cricket", "combat"}
                              # near-certain UNIQUEMENT sur catégories à résolution VÉRIFIABLE (issue
                              # claire). Le géopo/macro ("Houthis..." @0.97 -> -0.35) = pas de source de
                              # vérité + risque de queue = un near-certain sans edge, à payout affreux.
RESIDUAL_SWEEP_MAX = 8.0      # BALAYAGE DU RÉSIDU (idée Steven) : sous ce cash, le reste est trop
                              # petit pour être fractionné (dort = monnaie inexploitable). On le
                              # déploie presque en entier — mais UNIQUEMENT sur une near-certain
                              # (favori 0.88-0.985 vérifié), jamais un all-in spéculatif.
PRESENCE_MAX_POS = 4           # présence débridée : jusqu'à 4 favoris foot/basket (déploie le cash,
                               # + de trades sur l'edge à mesurer en réel). Zone 0.68-0.84, stop -12%.
SPIKE_MAX_GAP = 0.15           # ANTI-SPIKE (idée Steven) : si le prix actuel dépasse sa MÉDIANE
                               # sur 1h de plus de 0.15, c'est un PIC transitoire (faux positif de
                               # momentum) — le match était en réalité serré (~50¢) et on paierait
                               # le sommet d'un croisement de courbes. On n'entre PAS sur un spike.
COPY_MIN_PRICE = DIRECTIONAL_MIN_PRICE
COPY_MAX_PRICE = DIRECTIONAL_MAX_PRICE
COPY_MAX_AGE_S = 900           # ne copie que les achats < 15 min (sinon on est en retard)
COPY_N_TRADERS = 12            # nombre de top traders prouvés suivis — copy = SEULE stratégie
                               # rentable (+40% WR), on élargit le pool pour capter plus de leurs
                               # gros paris (filtre conviction >=300$ inchangé : quantité SANS baisser
                               # la qualité). Plus de tickets issus d'un edge prouvé.
COPY_MIN_TRADER_SIZE = 1500    # ne copie que la VRAIE conviction. Constat data (Steven) : les top
                               # traders profitent de 0.5M à 10M$ et leur médiane d'achat est 3-60$ —
                               # 300$ était leur BRUIT (hedge/market-making/appât). Leur conviction
                               # réelle est à 2k-500k$. À >=2000$ on capte leurs vrais gros paris ET
                               # on exclut d'office les market-makers (qui parient des deux côtés en
                               # petits tickets < 2k$, jamais de la conviction directionnelle).

# TP/SL dynamiques selon la conviction (même ratio que le sizing) : plus le
# signal est fort, plus on laisse respirer la position (on croit à la thèse) ;
# plus il est faible, plus on coupe vite (peu de conviction = sortie rapide,
# gain ou perte, plutôt que de rester exposé sur un pari qu'on ne croit qu'à
# moitié).
TP_MIN, TP_MAX = 0.12, 0.35        # +12% (conviction faible) à +35% (conviction max)
SL_MIN, SL_MAX = 0.18, 0.40        # -18% (conviction faible) à -40% (conviction max)
PRERESO_MINUTES = 8                # SORTIE PRÉ-RÉSOLUTION (Steven 20/07) : à <=8min de la fin du match
PRERESO_MIN_PROFIT = 0.03          # on verrouille un gain (>=+3%) avant le gap de fin qui met les gagnants à 0.
# COUPE-CIRCUIT (idée Kapitane) : si le PnL réalisé propre chute de > CIRCUIT_LOSS_USD sur
# la fenêtre CIRCUIT_WINDOW_H, on met les NOUVEAUX trades en pause CIRCUIT_PAUSE_H (on gère
# quand même les sorties). Empêche une mauvaise série de compounder (protection systémique).
CIRCUIT_LOSS_USD = 8.0
CIRCUIT_WINDOW_H = 3.0
CIRCUIT_PAUSE_H = 1.0
GIVEBACK_PEAK = 0.06               # STOP GIVE-BACK (Steven 20/07) : si une position a piqué >=+6%
                                   # puis rend tout (revient <= entrée), on SORT ~breakeven au lieu de
                                   # tenir jusqu'à 0. Casse le skew négatif (petits gains / pertes pleines).
LADDER_TP1_PCT = 0.10              # 1er palier : à +10%, vendre la moitié — capte la marge du
                                   # mouvement (le "bruit") plus tôt, sans attendre la résolution.
SPORT_TP_PCT = 0.05                # sport in-play : take-profit TÔT et EN ENTIER à +5% (choix
                                   # Steven) — verrouille le gain avant le whipsaw (Isabella).

SCAN_INTERVAL_S = 15
MARKETS_REFRESH_S = 120
TOP_MARKETS = 60
MIN_VOLUME_24H = 5_000
PAPER_BANKROLL_START = 1_000.0  # USD virtuels
MAX_LOG_LINES = 400


class PolyEngine:
    def __init__(self):
        db.init_db()
        self._running = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._log_lines: list[dict] = []
        self._log_lock = threading.Lock()

        self._markets: list[dict] = []
        self._last_markets_fetch = 0.0
        self.scan_count = 0
        self.paper_bankroll = PAPER_BANKROLL_START
        self._start_time: float | None = None
        self._cash_cache: list = [0.0, None]  # [ts, cash_usdc] — évite un appel réseau par trade tenté
        self._failed_recently: dict = {}  # question -> timestamp du dernier échec (cooldown anti-spam)
        self._sport_traded: dict = {}     # match -> timestamp du dernier trade (anti-whipsaw : 1 prise/match)
        self._account_snapshot: dict = {"cash": 0.0, "positions": [], "total": 0.0, "pnl": 0.0, "ts": 0.0}
        self._copied: set = set()  # (wallet, token_id) déjà copiés — pas de doublon
        self._logged_keys: dict = {}  # anti-répétition des logs de SKIP
        self._cat_perf: dict = {}  # ① méta-edge : cat -> multiplicateur data-driven (category_report)
        self._sniped_matches: set = set()  # matchs ESPN déjà snipés (1 prise/match, pas de re-achat)
        self._ai_meta: dict = {}  # token_id -> {sl_pct, question, side, entry} : SL choisi par l'IA + gestion
        self._ai_calib: dict = {"dial": 1.0, "min_conf": AI_VERIFIED_MIN_CONF}  # ① calibration auto
        self._mover_qs: dict = {}  # SCAN INTELLIGENT : questions sport ayant gros-bougé -> prioritaires IA
        self._mkt_by_token_cache: dict = {}  # token -> marché (état match, sortie pré-résolution)
        self._circuit_until: float = 0.0     # coupe-circuit : pause des nouveaux trades jusqu'à ce ts

        # Exécution réelle : instanciée seulement si clé présente, activée
        # seulement par le bouton LIVE de l'UI.
        self.live_enabled = False
        self._live = None
        self._live_error = None
        try:
            pk = os.environ.get("PRIVATE_KEY", "")
            if pk:
                from ghost_poly.live import PolyLive
                self._live = PolyLive(pk, os.environ.get('POLY_FUNDER_ADDRESS', ''))
        except Exception as e:
            self._live_error = str(e)[:200]

        # POLY_AUTOSTART=1 : le moteur démarre seul à l'ouverture de l'app.
        # POLY_LIVE=1      : le mode réel s'active seul si le wallet est prêt.
        # Demande Steven : tourne 24/7 sans aucune interaction.
        if os.environ.get("POLY_AUTOSTART") == "1":
            self.start()
        if os.environ.get("POLY_LIVE") == "1" and self._live is not None:
            try:
                if self._live.status()["ready"]:
                    self.live_enabled = True
                    self._log("LIVE auto-activé (POLY_LIVE=1) — exécution réelle armée")
                else:
                    self._log("POLY_LIVE=1 mais wallet pas prêt — resté en paper")
            except Exception as e:
                self._log(f"POLY_LIVE=1 mais vérification wallet échouée: {str(e)[:80]}")

    # ── logging UI ──
    def _log(self, text: str, kind: str | None = None):
        """Journal en mémoire (UI) + FICHIER PERSISTANT (data/ghost.log, survit aux
        redémarrages). 'kind' colore la ligne dans l'UI (auto-déduit de l'emoji sinon)."""
        now = datetime.now()
        if kind is None:
            kind = self._infer_log_kind(text)
        entry = {"ts": now.strftime("%H:%M:%S"), "text": text, "kind": kind}
        with self._log_lock:
            self._log_lines.append(entry)
            if len(self._log_lines) > MAX_LOG_LINES:
                self._log_lines = self._log_lines[-MAX_LOG_LINES:]
        # fichier persistant (append, best-effort)
        try:
            from pathlib import Path
            if PolyEngine._LOG_FILE is None:
                PolyEngine._LOG_FILE = Path(__file__).parent.parent / "data" / "ghost.log"
                PolyEngine._LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(PolyEngine._LOG_FILE, "a", encoding="utf-8") as fh:
                fh.write(f"{now.strftime('%Y-%m-%d %H:%M:%S')} [{kind:>4}] {text}\n")
        except Exception:
            pass

    @staticmethod
    def _infer_log_kind(text: str) -> str:
        """Déduit le type de ligne (pour la couleur UI) à partir du contenu/emoji."""
        t = text.lower()
        if any(e in text for e in ("🎯", "🚀", "✅", "🌡️", "⚖️", "🤖")) or "achat" in t or "snipe" in t and "skip" not in t:
            return "buy"
        if "sortie" in t or "stop" in t or "vend" in t or "🔒" in text or "🧹" in text:
            return "exit"
        if "pnl=+" in t or "gagn" in t or "+$" in text or "encaisse" in t:
            return "win"
        if "pnl=-" in t or "perd" in t or "erreur" in t or "⚠️" in text:
            return "loss"
        if "skip" in t or "s'abstient" in t or "abstient" in t or "🔁" in text:
            return "skip"
        if "📊" in text or "scan" in t or "observe" in t or "méta-edge" in t or "calib" in t:
            return "scan"
        return "info"

    def _holds_position_on(self, question: str) -> bool:
        """Détient-on DÉJÀ une position sur ce marché (n'importe quel camp) ?
        Vérifie les VRAIES positions Polymarket (account snapshot), qui
        survivent aux redémarrages — contrairement au cooldown en mémoire.
        C'EST le garde-fou anti-both-sides : sans ça, un redémarrage efface la
        mémoire et le bot re-trade l'autre camp du même match (Isabella YES +
        Reasco NO = perte garantie, bug vécu)."""
        if not question:
            return False
        qn = "".join(ch for ch in question.lower() if ch.isalnum())[:40]
        for p in self._account_snapshot.get("positions", []):
            tn = "".join(ch for ch in (p.get("title") or "").lower() if ch.isalnum())[:40]
            if qn and tn and qn == tn:
                return True
        return False

    def _holds_losing_position_on(self, question: str) -> bool:
        """Détient-on sur ce marché une position ACTUELLEMENT EN PERTE ? Sert à
        bloquer le renfort IA sur une thèse qui va déjà contre nous (vécu : 3×
        NO sur 'Israeli parliament' perdus d'affilée = compounding de la perte)."""
        if not question:
            return False
        qn = "".join(ch for ch in question.lower() if ch.isalnum())[:40]
        for p in self._account_snapshot.get("positions", []):
            tn = "".join(ch for ch in (p.get("title") or "").lower() if ch.isalnum())[:40]
            if qn and tn and qn == tn and (p.get("pnl") or 0) < 0:
                return True
        return False

    def _recently_traded(self, question: str) -> bool:
        """Cooldown : marché tradé récemment (mémoire) OU position déjà
        détenue on-chain (persistant). Le 2e couvre le cas restart."""
        if self._holds_position_on(question):
            return True
        last = self._sport_traded.get(question, 0)
        return bool(last and time.time() - last < SPORT_MATCH_COOLDOWN_S)

    def _mark_traded(self, question: str):
        self._sport_traded[question] = time.time()

    def _log_once(self, key: str, text: str, ttl: int = 600):
        """Log throttlé : n'écrit le message que si ce 'key' n'a pas déjà été
        loggé dans les ttl dernières secondes. Évite que les raisons de SKIP
        (cooldown, hors fourchette...) se répètent à chaque scan (30s) et
        noient le journal en boucle."""
        now = time.time()
        last = self._logged_keys.get(key, 0)
        if now - last < ttl:
            return
        self._logged_keys[key] = now
        # purge occasionnelle pour ne pas faire grossir le dict indéfiniment
        if len(self._logged_keys) > 500:
            self._logged_keys = {k: v for k, v in self._logged_keys.items() if now - v < ttl}
        self._log(text)

    # ── boucle ──
    async def _refresh_markets(self):
        if time.time() - self._last_markets_fetch < MARKETS_REFRESH_S and self._markets:
            return
        markets = await get_active_markets(limit=TOP_MARKETS, min_volume_24h=MIN_VOLUME_24H)
        if markets:
            self._markets = markets
            self._last_markets_fetch = time.time()
            self._log(f"catalogue rafraîchi : {len(markets)} marchés (vol24h > {MIN_VOLUME_24H:,}$)")

    def _record_trade(self, strategy, question, side, price, cost, size, reasoning,
                      live_done, token_id=None, conviction=0.5,
                      src_wallet=None, src_price=None, src_ts=None):
        """N'enregistre PAS une tentative live ÉCHOUÉE (solde insuffisant, ordre
        rejeté) : elle s'afficherait à tort comme un trade '(paper)' dans le
        feed alors que le bot est bien en live. En live, seul un ordre RÉELLEMENT
        exécuté est journalisé. En mode paper pur, on journalise tout (simulation).
        """
        if not live_done and self.live_enabled:
            return
        # Polymarket impose un MINIMUM de 5 parts : l'exécuteur gonfle tout ordre
        # plus petit à 5 parts. On enregistre donc la taille/coût RÉELLEMENT
        # exécutés (max(size,5) * prix), pas ceux visés — sinon la DB sous-estime
        # la dépense (vécu : mise voulue 1.17$ mais 5 parts @ 0.71 = 3.57$ réel).
        if live_done:
            from ghost_poly.live import MIN_ORDER_SIZE_SHARES
            size = max(size, MIN_ORDER_SIZE_SHARES)
            cost = round(size * price, 2)
        db.log_directional_trade(strategy, question, side, price, cost, size,
                                 reasoning, live_done, token_id=token_id, conviction=conviction,
                                 src_wallet=src_wallet, src_price=src_price, src_ts=src_ts)

    async def _scan_once(self):
        self.scan_count += 1
        # AUTO-RÉPARATION du mode LIVE : si POLY_LIVE=1 mais le wallet n'était pas
        # prêt au démarrage (timing VPN/réseau), on reste bloqué en paper à vie.
        # On re-tente d'armer le live ici tant qu'il n'est pas actif — self-heal.
        if (not self.live_enabled and self._live is not None
                and os.environ.get("POLY_LIVE") == "1" and self.scan_count % 4 == 0):
            try:
                import asyncio as _aio
                ready = (await _aio.to_thread(self._live.status)).get("ready")
                if ready:
                    self.live_enabled = True
                    self._log("LIVE auto-réarmé (wallet devenu prêt) — exécution réelle active")
            except Exception:
                pass
        # Charge les VRAIES positions AVANT de trader (surtout au 1er scan
        # après un redémarrage) — sinon le garde-fou anti-both-sides est
        # aveugle et le bot peut re-trader un match déjà détenu.
        if self.live_enabled and self._live is not None and self._account_snapshot["ts"] == 0:
            try:
                import asyncio as _aio
                await _aio.to_thread(self._refresh_account)
            except Exception:
                pass
        await self._refresh_markets()
        if not self._markets:
            self._log("catalogue vide — API Polymarket injoignable ?")
            return

        # HEARTBEAT NARRATIF (Steven) : le bot "raconte" ce qu'il fait, façon chatbot.
        if self.scan_count % 3 == 0:
            snap = self._account_snapshot
            npos = len(snap.get("positions", []))
            mv = len(getattr(self, "_mover_qs", {}) or {})
            extra = f" · 📡 {mv} marché(s) qui bougent fort → je vérifie" if mv else ""
            self._log(f"🔍 Scan #{self.scan_count} — j'observe {len(self._markets)} marchés · "
                      f"{snap.get('cash', 0):.2f}$ dispo · {npos} position(s) ouverte(s){extra}", kind="scan")

        # ① MÉTA-EDGE : rafraîchit les multiplicateurs data-driven périodiquement.
        if self.scan_count % CATPERF_REFRESH_SCANS == 0:
            try:
                import asyncio as _aio
                await _aio.to_thread(self._refresh_cat_perf)
                self._refresh_ai_calibration()  # ① molette de confiance IA data-driven
            except Exception:
                pass

        opps = await scan_complementary_arb(self._markets)
        db.log_scan(len(self._markets), len(opps))

        if opps:
            for o in opps:
                db.log_opportunity("arb", o)
                last_fail = self._failed_recently.get(o["question"])
                if last_fail and time.time() - last_fail < FAILED_RETRY_COOLDOWN_S:
                    continue

                # ── EXÉCUTION RÉELLE (si LIVE activé) ──
                if self.live_enabled and self._live is not None:
                    usd = min(o["max_size_shares"] * o["total_cost"], MAX_LIVE_USD_PER_ARB)
                    # Bug réel trouvé : ce plafond ne regardait jamais le
                    # solde RÉEL du wallet — un arb pouvait viser 4$ avec
                    # seulement 1,27$ disponibles, rejeté en boucle sans
                    # jamais rien exécuter. usd = coût total des 2 jambes.
                    available = self._available_cash()
                    if available < MIN_DIRECTIONAL_USD:
                        continue
                    usd = min(usd, available * 0.9)
                    size = usd / o["total_cost"] if o["total_cost"] > 0 else 0
                    try:
                        res = self._live.execute_arb(
                            o["token_yes"], o["token_no"],
                            o["ask_yes"], o["ask_no"], size,
                        )
                        if res.get("error"):
                            self._failed_recently[o["question"]] = time.time()
                            self._log(f"LIVE ARB partiel/refusé: {res['error'][:120]}")
                        else:
                            self._cash_cache[1] = None
                            self._log(
                                f"LIVE ARB EXÉCUTÉ {o['question'][:40]} — {usd:.2f}$ engagés, "
                                f"profit garanti {(size * (1 - o['total_cost'])):.2f}$ à résolution"
                            )
                    except Exception as e:
                        self._failed_recently[o["question"]] = time.time()
                        self._log(f"LIVE ARB erreur: {str(e)[:120]}")
                # paper-exécution : toute la taille dispo, bornée par la bankroll
                cost_full = o["max_size_shares"] * o["total_cost"]
                size = o["max_size_shares"]
                if cost_full > self.paper_bankroll:
                    size = self.paper_bankroll / o["total_cost"] if o["total_cost"] > 0 else 0
                    cost_full = size * o["total_cost"]
                if size <= 0:
                    self._log(f"ARB vu mais bankroll paper épuisée : {o['question'][:40]}")
                    continue
                payout = size * 1.0
                self.paper_bankroll -= cost_full
                # le payout revient à la résolution ; en attendant il est 'locked'
                db.log_paper_trade(o["question"], cost_full, payout, size, None)
                self._log(
                    f"ARB PAPER {o['question'][:45]} — coût {cost_full:.2f}$ → payout {payout:.2f}$ "
                    f"(edge {o['edge_pct']}%, profit {payout - cost_full:.2f}$ garanti si fill)"
                )
        else:
            if self.scan_count % 10 == 0:
                self._log(f"scan #{self.scan_count}: {len(self._markets)} marchés, 0 arb (les pros snipent vite — on mesure)")

        # Si le live est actif et qu'il n'y a plus rien à miser, inutile de
        # scanner momentum/IA — surtout l'IA (recherche DuckDuckGo + appel
        # Nemotron par marché) : ça consomme du quota gratuit pour rien tant
        # qu'aucun trade ne peut de toute façon être exécuté. Le monitoring
        # de sortie (TP/SL) plus bas continue quoi qu'il arrive — les
        # positions déjà ouvertes doivent rester surveillées.
        no_budget = (self.live_enabled and self._live is not None
                    and self._available_cash() < MIN_DIRECTIONAL_USD)
        # GARDE ANTI-BOTH-SIDES au démarrage : tant que le snapshot compte n'a pas
        # été chargé au moins une fois (ts==0, juste après une relance), on NE prend
        # AUCUN nouveau pari — sinon le garde-fou anti-both-sides est aveugle (il ne
        # voit pas encore qu'on tient déjà un camp) et le bot peut acheter l'autre
        # camp du même match (vécu : Astana Nina + Sandugash après un redémarrage).
        snapshot_pret = not (self.live_enabled and self._live is not None
                             and self._account_snapshot["ts"] == 0)
        if not snapshot_pret:
            if self.scan_count % 4 == 0:
                self._log("⏳ snapshot compte pas encore chargé — pas de nouveau pari (protection anti-both-sides au démarrage)")
        # Pas de disjoncteur/cooldown (choix Steven) : on ne fige jamais la machine
        # par peur. On coupe les vrais perdants par position (hard-stop, scalp) et
        # on continue d'attaquer là où il y a un edge. Le risque se gère au trade,
        # pas en éteignant tout.
        if no_budget or not snapshot_pret:
            if self.scan_count % 10 == 0:
                self._log(f"solde de trading épuisé ({self._available_cash():.2f}$) — momentum/IA en pause, dépose plus de fonds pour reprendre")
        else:
            # ── MOMENTUM désactivé : demande explicite de Steven, réagit à
            # n'importe quel sujet (sport/crypto) sans distinction — a produit
            # un aller-retour LeBron James (achat 0.814 -> revente 0.799,
            # perte sèche) au lieu de se concentrer sur guerre/politique. ──
            if MOMENTUM_ENABLED:
                try:
                    await self._scan_momentum()
                except Exception as e:
                    self._log(f"erreur momentum: {str(e)[:100]}")

            # ── RESOLUTION-SNIPING : achète les vainqueurs de matchs déjà
            # décidés (confirmés ESPN) pas encore résolus par Polymarket —
            # gains faibles mais quasi sûrs. ──
            try:
                await self._scan_resolution_snipe()
            except Exception as e:
                self._log(f"erreur snipe: {str(e)[:100]}")

            # ── SNIPE CRYPTO : marchés 'BTC/ETH au-dessus de X' proches de
            # l'échéance dont le prix réel décide déjà l'issue. ──
            try:
                await self._scan_crypto_snipe()
            except Exception as e:
                self._log(f"erreur snipe crypto: {str(e)[:100]}")

            # ── SNIPE MÉTÉO : marchés température, vérité Open-Meteo (edge réel) ──
            try:
                await self._scan_weather_snipe()
            except Exception as e:
                self._log(f"erreur snipe météo: {str(e)[:100]}")

            # ── SNIPER IA VÉRIFIÉ : throttlé (tous les N) OU RÉACTIF (dès qu'un gros
            # mouvement inter-scan a marqué un marché sport — scan intelligent Steven).
            # Garde coût : les movers ne relancent l'IA qu'au + 1x/3 scans. ──
            _mover_ok = self._mover_qs and (self.scan_count - getattr(self, "_last_ai_scan", -99) >= 3)
            if AI_VERIFIED_ENABLED and (self.scan_count % AI_VERIFIED_EVERY_N == 0 or _mover_ok):
                self._last_ai_scan = self.scan_count
                try:
                    await self._scan_ai_verified()
                except Exception as e:
                    self._log(f"erreur snipe IA: {str(e)[:100]}")

            # ── SURVEILLANCE IA (self wake-up) : l'IA re-check ses positions ──
            if AI_VERIFIED_ENABLED and self.scan_count % AI_MANAGE_EVERY_N == 0:
                try:
                    await self._manage_ai_positions()
                except Exception as e:
                    self._log(f"erreur gestion IA: {str(e)[:100]}")

            # ── SNIPE NEAR-CERTAIN GÉNÉRALISÉ : favoris extrêmes 0.90-0.96 toutes
            # catégories vérifiables (favorite-longshot bias, jeu des whales). ──
            try:
                await self._scan_nearcertain()
            except Exception as e:
                self._log(f"erreur near-certain: {str(e)[:100]}")

            # ── COPY-TRADING : copie les top traders rentables (toutes les
            # ~2 min, l'API leaderboard est cachée). ──
            if self.scan_count % 4 == 0:
                try:
                    await self._scan_copytrade()
                except Exception as e:
                    self._log(f"erreur copytrade: {str(e)[:100]}")

            # ── SPORT IN-PLAY : suit les cotes qui bougent en direct pendant
            # les matchs — la source de gain la plus rapide constatée (Grigor
            # 45¢->88¢). Pas d'appel IA, juste le mouvement de cote : léger,
            # peut tourner à chaque scan. ──
            if SPORT_INPLAY_ENABLED:
                try:
                    await self._scan_sport_inplay()
                except Exception as e:
                    self._log(f"erreur sport in-play: {str(e)[:100]}")

            # ── IA/NEWS : COUPÉ (Steven 18/07) — analyse LLM broad = pas d'edge
            # (a produit la perte baseball). On ne garde que le sniping vérité + copy. ──
            if AI_NEWS_ENABLED and self.scan_count % AI_SCAN_EVERY_N_CYCLES == 0:
                try:
                    await self._scan_ai_news()
                except Exception as e:
                    self._log(f"erreur IA/news: {str(e)[:100]}")

            # ── GARDE-PRÉSENCE : EN DERNIER (après toutes les autres stratégies)
            # et peu fréquent — ne comble le vide QUE si rien d'autre n'a de
            # position. Garantit toujours >=1 trade actif sur le compte. ──
            if self.scan_count % 2 == 0:
                try:
                    await self._scan_presence()
                except Exception as e:
                    self._log(f"erreur garde-présence: {str(e)[:100]}")

        # ── SHADOW LOGGER MOMENTUM : mesure gratuite (aucun trade) d'où le
        # momentum continue, dans TOUTES les catégories. R&D pur (Perplexity). ──
        try:
            await self._scan_momentum_shadow()
        except Exception as e:
            self._log(f"erreur shadow momentum: {str(e)[:80]}")

        # ── SYNC ON-CHAIN : enrichit la DB depuis la vérité du compte (bot +
        # manuel), demande Steven "DB = historique réel". FRÉQUENT (~chaque scan,
        # aussi temps réel que l'API le permet — elle lague côté serveur, inutile
        # d'aller plus vite et risque de rate-limit). ──
        if self._live is not None and self.scan_count % 2 == 0:
            try:
                import asyncio as _aio
                await _aio.to_thread(self._sync_onchain)
            except Exception as e:
                self._log(f"erreur sync on-chain: {str(e)[:100]}")

        # ── nettoyage : un ordre GTC non rempli après 2 min bloque du capital
        # pour rien (vécu : 2 ordres coincés des heures, plus de capital pour
        # les vrais arbs). On annule tout ce qui traîne. ──
        if self.live_enabled and self._live is not None:
            try:
                self._live.cancel_stale_orders()
            except Exception as e:
                self._log(f"erreur nettoyage ordres: {str(e)[:100]}")

        # ── sortie : prend le profit ou coupe la perte sur les positions
        # directionnelles ouvertes (l'arb n'a pas besoin de ça, son payout est
        # garanti à résolution). Sans ça, parier sur France ET Espagne au
        # même tournoi ne sert à rien — il faut vendre quand ça monte. ──
        if self.live_enabled and self._live is not None:
            try:
                await self._check_directional_exits()
            except Exception as e:
                self._log(f"erreur suivi positions: {str(e)[:100]}")

        # Rafraîchit le vrai compte (solde+positions) toutes les ~1 min pour l'UI
        if self._live is not None:  # rafraichit le compte a CHAQUE scan (au plus pres du direct)
            try:
                import asyncio as _aio
                await _aio.to_thread(self._refresh_account)
            except Exception:
                pass

        # Auto-redeem : réclame les gains des marchés résolus en notre faveur
        # pour recycler le capital vite (plus de cash dispo = plus de trades).
        if self.live_enabled and self._live is not None and self.scan_count % 6 == 0:
            try:
                import asyncio as _aio
                n = await _aio.to_thread(self._live.redeem_resolved)
                if n:
                    self._cash_cache[1] = None
                    self._log(f"{n} position(s) gagnante(s) résolue(s) réclamée(s) — capital libéré")
            except Exception as e:
                self._log(f"erreur redeem: {str(e)[:80]}")

    def _conviction_ratio(self, edge_pts: float, max_edge: float = MAX_AI_EDGE_PTS) -> float:
        """0..1 — même mesure utilisée pour dimensionner la mise ET les
        seuils TP/SL, pour que les deux restent cohérents entre eux."""
        return min(abs(edge_pts) / max_edge, 1.0)

    def _available_cash(self) -> float:
        """Solde de trading réel, caché 20s — évite un appel réseau par
        tentative de trade et les tirs répétés dans le vide quand le solde
        est bas (vécu : boucle de 'not enough balance' toutes les 30s)."""
        now = time.time()
        if self._cash_cache[1] is not None and now - self._cash_cache[0] < 8:
            return self._cash_cache[1]
        try:
            cash = self._live.status().get("cash_usdc") or 0.0
        except Exception:
            cash = self._cash_cache[1] or 0.0
        self._cash_cache[0] = now
        self._cash_cache[1] = cash
        return cash

    async def _price_median_1h(self, token: str) -> float | None:
        """Médiane du prix (côté détenu) sur la dernière heure — sert à repérer
        un PIC transitoire : si le prix actuel est très au-dessus de sa médiane,
        c'est un croisement de courbes momentané, pas une vraie valeur soutenue."""
        try:
            import statistics
            async with aiohttp.ClientSession() as s:
                hist = await get_price_history(s, token, interval="1h", fidelity=5)
            prices = [p for (_t, p) in hist if p is not None]
            if len(prices) < 4:
                return None
            return statistics.median(prices)
        except Exception:
            return None

    _RATCHET_FILE = None  # init paresseux (chemin data/ratchet.txt)
    _LOG_FILE = None      # init paresseux (data/ghost.log, journal persistant)

    def _ratchet_state(self):
        """(peak, protect_until) persistés — survivent aux redémarrages."""
        from pathlib import Path
        if PolyEngine._RATCHET_FILE is None:
            PolyEngine._RATCHET_FILE = Path(__file__).parent.parent / "data" / "ratchet.txt"
        try:
            peak, until = PolyEngine._RATCHET_FILE.read_text().strip().split(",")
            return float(peak), float(until)
        except Exception:
            return 0.0, 0.0

    def _update_ratchet(self, total: float):
        """Nouveau sommet +15% au-dessus du pic verrouillé -> mode protection 6h
        (mises /2). Le pic ne redescend jamais (high-water-mark)."""
        peak, until = self._ratchet_state()
        if total > peak:
            gain = (total / peak - 1) if peak > 0 else 0.0
            if peak > 0 and gain >= RATCHET_DEPOSIT_PCT:
                # DÉPÔT : on monte le pic mais on N'ARME PAS la protection.
                self._log(f"💰 dépôt détecté (+{gain*100:.0f}%) — pic rebasé à {total:.2f}$, PAS de gain-lock (capital frais laissé libre de travailler)")
            elif peak > 0 and total >= peak * (1 + RATCHET_GAIN_PCT):
                until = time.time() + RATCHET_HOURS * 3600
                self._log(f"🔒 RATCHET : nouveau sommet {total:.2f}$ (+{gain*100:.0f}%) — mises réduites de moitié {RATCHET_HOURS}h, on ENCAISSE le run")
            try:
                PolyEngine._RATCHET_FILE.write_text(f"{total},{until}")
            except Exception:
                pass

    def _ratchet_mult(self) -> float:
        _, until = self._ratchet_state()
        return RATCHET_MULT if time.time() < until else 1.0

    def _equity(self) -> float:
        """Capital total (cash + valeur des positions ouvertes). Base du sizing
        proportionnel — retombe sur le cash seul si le snapshot n'est pas prêt."""
        eq = self._account_snapshot.get("total") or 0.0
        return eq if eq > 0 else self._available_cash()

    def _confidence_size(self, ratio: float, price: float | None = None) -> float:
        """Sizing KELLY FRACTIONNÉ 1/4 (Steven 22/07, remplace l'ancien sizing
        fixe/capital). Sur Polymarket le prix payé EST la probabilité implicite du
        marché (b = (1-price)/price = cote). On estime notre proba de gain réelle q
        en ajoutant à ce prix un edge supposé (KELLY_ASSUMED_EDGE), modulé par la
        conviction 0..1 déjà calculée par chaque appelant (favori quasi-certain,
        momentum fort, etc). f* = (b*q - (1-q)) / b, puis on ne mise que
        KELLY_FRACTION de f* : le WR réel du bot est encore incertain (86 trades,
        78-95% selon l'estimation) et le Kelly plein est ruineux si l'edge est
        surestimé -> 1/4 Kelly absorbe l'erreur d'estimation tout en gardant le
        sizing proportionnel à l'edge (gros favori = grosse mise, zone floue =
        mise réduite), contrairement à l'ancien sizing qui ignorait le prix payé.
        Fallback sans price (callers historiques) : ancien comportement fixe."""
        equity = self._equity()
        hard_cap = min(MAX_DIRECTIONAL_USD, equity * BET_CAP_FRACTION)
        if price is None or price <= 0 or price >= 1:
            base = equity / TARGET_CONCURRENT_POSITIONS
            usd = base * (0.6 + ratio * 0.8)
            return round(max(MIN_DIRECTIONAL_USD, min(usd, hard_cap) * self._ratchet_mult()), 2)

        b = (1 - price) / price
        q = min(0.995, price + KELLY_ASSUMED_EDGE * max(0.0, min(1.0, ratio)))
        f_star = (b * q - (1 - q)) / b
        f_star = max(0.0, f_star) * KELLY_FRACTION
        usd = equity * f_star
        return round(max(MIN_DIRECTIONAL_USD, min(usd, hard_cap) * self._ratchet_mult()), 2)

    def _can_afford_min_order(self, price: float) -> bool:
        """La commande minimale Polymarket = 5 parts. À prix p, ça coûte
        5*p — si le solde ne couvre pas ça, inutile de tenter (c'était la
        cause des 'not enough balance' : ordre bumpé à 5 parts qui dépasse
        le solde). +5% de marge pour frais/slippage."""
        from ghost_poly.live import MIN_ORDER_SIZE_SHARES
        return self._available_cash() >= MIN_ORDER_SIZE_SHARES * price * 1.05

    def _dynamic_tp_sl(self, conviction: float) -> tuple[float, float]:
        """(take_profit_pct, stop_loss_pct) — plus large dans les deux sens
        quand la conviction est haute, plus serré quand elle est faible."""
        conviction = conviction if conviction is not None else 0.5
        tp = TP_MIN + conviction * (TP_MAX - TP_MIN)
        sl = SL_MIN + conviction * (SL_MAX - SL_MIN)
        return tp, sl

    async def _check_directional_exits(self):
        positions = db.open_live_directional_positions()
        if not positions:
            return
        async with aiohttp.ClientSession() as session:
            for p in positions:
                if not p.get("token_id"):
                    # Vieilles positions sans token_id (avant le tracking) :
                    # invendables par code, on les ferme en base pour ne plus
                    # les réévaluer inutilement à chaque scan.
                    db.close_directional_trade(p["id"], 0, 0)
                    continue

                # SOURCE DE VÉRITÉ = solde réel détenu, PAS la DB. La DB dérive
                # (position résolue/vendue/jamais remplie encore marquée open).
                # Vendre selon la DB = erreur 400 en boucle sur des parts
                # inexistantes (les 'sortie refusée' du journal).
                real_size = self._live.position_size(p["token_id"])
                if real_size < 0:
                    continue  # lecture indéterminée, on réessaiera au prochain scan
                if real_size < 1.0 and self._holds_position_on(p["question"]):
                    # INCOHÉRENCE : la lecture on-chain dit <1 part MAIS le snapshot
                    # compte (data-api, source + fiable) montre encore la position.
                    # C'est une lecture FLAKY (hoquet RPC) — on NE clôture PAS (sinon
                    # on marque la position résolue à tort avec un PnL inféré et on
                    # l'abandonne alors qu'elle est détenue : bug vécu Pablo Aunion
                    # -42% orphelin). On réessaiera au prochain scan.
                    continue
                if real_size < 1.0:
                    # plus rien détenu → la position n'existe plus (résolue,
                    # vendue, ou jamais remplie). On DÉDUIT l'issue du dernier prix
                    # connu (côté détenu) : prix→1 = notre camp a gagné (parts
                    # remboursées à 1$), prix→0 = perdu. Sinon on comptait 0 et les
                    # stats étaient fausses (on pilotait à l'aveugle).
                    entry_price = p["price"]
                    last = p.get("last_price")
                    if last is None:
                        # Jamais observée détenue (ordre probablement non rempli) :
                        # on ne devine pas d'issue, on ferme neutre.
                        db.close_directional_trade(p["id"], 0, 0)
                        self._log(f"position {p['question'][:32]} fermée (jamais détenue/non remplie) — neutre")
                        continue
                    size_held = p.get("size_shares") or 0
                    exit_price = 1.0 if last >= 0.5 else 0.0
                    pnl = round((exit_price - entry_price) * size_held, 2)
                    db.close_directional_trade(p["id"], exit_price, pnl)
                    issue = "GAGNÉ" if exit_price == 1.0 else "perdu"
                    self._log(f"position {p['question'][:32]} résolue ({issue}, dernier prix {last:.2f}) — pnl {pnl:+.2f}$")
                    continue

                book = await get_book(session, p["token_id"])
                bid = best_bid(book)
                if not bid:
                    # Pas de carnet. Si on tient pourtant des parts (size>=1) MAIS
                    # que la position n'apparaît PLUS dans le snapshot compte
                    # (valeur ~0, data-api), c'est un marché RÉSOLU CONTRE nous
                    # (parts à 0) : on clôture à la vraie perte, sinon ça reste
                    # bloqué 'open' à jamais (size=5, aucun acheteur) et la mesure
                    # ignore la perte. Sinon (dans le snapshot) = illiquidité
                    # transitoire, on réessaie.
                    if not self._holds_position_on(p["question"]):
                        loss = round(-(p.get("cost_usd") or 0), 2)
                        db.close_directional_trade(p["id"], 0.0, loss)
                        self._log(f"position {p['question'][:30]} résolue PERDANTE (parts à 0, sans carnet) — pnl {loss:+.2f}$")
                    continue
                current_price = bid[0]
                entry_price = p["price"]
                pct_change = (current_price - entry_price) / entry_price if entry_price > 0 else 0
                tp_pct, sl_pct = self._dynamic_tp_sl(p.get("conviction"))
                partial_done = bool(p.get("partial_taken"))

                # Suivi du PIC de prix (high-water-mark) pour le trailing stop.
                peak = max(p.get("peak_price") or entry_price, current_price)
                if peak > (p.get("peak_price") or 0):
                    db.update_peak_price(p["id"], peak)
                db.update_last_price(p["id"], current_price)  # trace pour déduire l'issue à la résolution
                peak_gain = (peak - entry_price) / entry_price if entry_price > 0 else 0
                drop_from_peak = (peak - current_price) / peak if peak > 0 else 0
                # SORTIE PRÉ-RÉSOLUTION (idée Steven) : à l'approche de la fin du match
                # (données structurées end_date), on VERROUILLE un gain avant le gap de
                # fin qui transforme les gagnants en 0. Prioritaire sur le reste.
                _mk = getattr(self, "_mkt_by_token_cache", {}).get(str(p.get("token_id")), {})
                _ed = self._parse_dt(_mk.get("end_date")) if _mk else None
                if _ed:
                    from datetime import datetime as _dt, timezone as _tz
                    _mins_end = (_ed - _dt.now(_tz.utc)).total_seconds() / 60
                    if 0 < _mins_end <= PRERESO_MINUTES and pct_change >= PRERESO_MIN_PROFIT:
                        res = self._live.sell_position(p["token_id"], current_price, real_size)
                        if res.get("success"):
                            pnl = (current_price - entry_price) * real_size
                            db.close_directional_trade(p["id"], current_price, round(pnl, 2))
                            self._cash_cache[1] = None
                            self._ai_meta.pop(str(p["token_id"]), None)
                            self._log(f"🔒 LOCK pré-résolution {p['question'][:30]} — match finit ~{int(_mins_end)}min, +{pct_change*100:.0f}% verrouillé avant le gap (pnl {pnl:+.2f}$)")
                        continue

                # ── TAKE-PROFIT EN ESCALIER ──
                if p["strategy"] != "sport" and not partial_done and pct_change >= LADDER_TP1_PCT and pct_change < tp_pct:
                    half = round(real_size / 2, 2)
                    if half >= 5.0:
                        res = self._live.sell_position(p["token_id"], current_price, half)
                        if res.get("success"):
                            realized = (current_price - entry_price) * half
                            db.mark_partial_taken(p["id"], real_size - half, realized)
                            self._cash_cache[1] = None
                            self._log(
                                f"TP ESCALIER {p['strategy']} {p['question'][:32]} — moitié vendue à "
                                f"+{pct_change*100:.0f}% (pnl {realized:+.2f}$), reste protégé"
                            )
                    continue

                # ── PROTECTIONS "QUASI RISK-FREE" (idée Steven) ──
                # 1) TRAILING STOP : si la position a bien monté (+15% de pic)
                #    puis redescend de 8% depuis son sommet, on vend — on
                #    verrouille le gain, un gagnant ne redevient jamais perdant.
                # 2) VERROU BREAKEVEN : après la prise partielle, si le reste
                #    revient à ton prix d'entrée, on sort à 0 (le runner ne
                #    peut plus rien te coûter, la moitié est déjà encaissée).
                if p["strategy"] == "sport":
                    # SPORT (analyse on-chain : 37% réussite, l'edge est dans les
                    # RUNNERS type Grigor 45->88¢). Donc : PAS de TP fixe (il
                    # tuerait les gros gains). On LAISSE COURIR avec un trailing
                    # tôt (verrouille dès qu'un pic +8% redescend de 5%), et on
                    # COUPE VITE les perdants à -6%. Asymétrie inversée : petites
                    # pertes, gros gains — c'est ce qui manquait pour être net+.
                    if peak_gain >= 0.08 and drop_from_peak >= 0.05 and current_price > entry_price:
                        reason = f"trailing sport (pic +{peak_gain*100:.0f}%, verrouille +{pct_change*100:.0f}%)"
                    elif pct_change <= -SPORT_HARD_SL:
                        # PLANCHER DUR : au-delà de -15%, on coupe QUEL QUE SOIT le
                        # score. Motivé par un vécu (Pablo Aunion -44% tenu par
                        # l'anti-swing alors qu'il menait un set mais s'effondrait) :
                        # en tennis, "mener au score" (grossier) n'empêche pas de
                        # perdre le match. Passé ce seuil, le signal score était faux.
                        reason = f"STOP DUR sport {pct_change*100:.0f}% (score ignoré, vrai effondrement)"
                    elif pct_change <= -SPORT_SL_SCALP:
                        # ANTI-SWING (idée Steven, ma sauce) : entre -6% et -15%, avant
                        # de couper un perdant sport, on regarde le SCORE LIVE ESPN. Si
                        # notre joueur MÈNE ENCORE au tableau, le prix a juste swingué
                        # (bruit) — on GARDE. S'il est mené/inconnu, on coupe.
                        import asyncio as _aio
                        from core.livescores import inplay_leader_status
                        we_first = (p.get("side") == "YES")
                        try:
                            score_status = await _aio.to_thread(inplay_leader_status, p["question"], we_first)
                        except Exception:
                            score_status = "unknown"
                        if score_status in ("leading", "tied"):
                            self._log_once(
                                f"antiswing_{p['id']}_{int(current_price*100)}",
                                f"[sport] {p['question'][:28]} à {pct_change*100:.0f}% MAIS mène encore au score ({score_status}) — ON GARDE (anti-swing, plancher -15%)")
                            continue
                        reason = f"stop scalp sport {pct_change*100:.0f}% (score {score_status}, vrai effondrement)"
                    else:
                        continue
                elif p["strategy"] == "presence" and pct_change >= PRESENCE_TP:
                    # TAKE-PROFIT RAPIDE (idée Steven) : on encaisse le bénéf dès +8%
                    # sans attendre la résolution — crucial pour les futures LONGS
                    # (Spain World Cup résout en 2026 !). Grab le gain quand il est là.
                    reason = f"TP rapide présence +{pct_change*100:.0f}% (on encaisse, pas d'attente résolution)"
                elif peak_gain >= 0.15 and drop_from_peak >= 0.08 and current_price > entry_price:
                    reason = f"trailing stop (pic +{peak_gain*100:.0f}%, redescendu à +{pct_change*100:.0f}%)"
                elif (p["strategy"] != "sport" and peak_gain >= GIVEBACK_PEAK and pct_change <= 0.0
                      and current_price > 0.02):
                    # STOP GIVE-BACK (fix Steven 20/07 : LE tueur du skew négatif) : une
                    # position qui a piqué positif (+6%) puis rend TOUT est sortie ~breakeven
                    # AU LIEU d'être tenue jusqu'à 0. Ça casse le "petits gains / pertes pleines".
                    reason = f"stop give-back (pic +{peak_gain*100:.0f}% rendu → on sort avant 0, {pct_change*100:+.0f}%)"
                elif partial_done and pct_change <= 0.0:
                    reason = "verrou breakeven (moitié déjà encaissée, reste protégé à 0)"
                elif pct_change >= tp_pct:
                    reason = f"take-profit plein +{pct_change*100:.0f}%"
                elif p["strategy"] == "snipe" and pct_change <= -SNIPE_SL:
                    # STOP SNIPE (Steven 19/07) : un snipe "certain" qui s'effondre = notre
                    # identification est fausse (ESPN prématuré / mauvais côté). On coupe VITE
                    # au lieu de "tenir jusqu'à résolution" et saigner (vécu SF/Seattle -45%).
                    reason = f"stop snipe {pct_change*100:.0f}% (thèse 'certaine' cassée, un vrai gagnant ne chute pas)"
                elif p["strategy"] == "ai_verified" and pct_change <= -self._ai_meta.get(str(p.get("token_id")), {}).get("sl_pct", 0.15):
                    # SL CHOISI PAR L'IA (idée Steven) : chaque position IA a son propre
                    # stop, fixé par l'IA selon sa certitude (match fini = large, en cours = serré).
                    ai_sl = self._ai_meta.get(str(p.get("token_id")), {}).get("sl_pct", 0.15)
                    reason = f"stop IA {pct_change*100:.0f}% (SL {ai_sl:.0%} fixé par l'IA)"
                elif p["strategy"] == "presence" and pct_change <= -PRESENCE_SL:
                    # STOP SERRÉ présence (idée Steven "SL escalier") : la présence
                    # est un favori sans edge (novelty type Musk-tweets), son stop
                    # générique était trop large (-31%). On coupe à -12% pour ne
                    # pas la laisser saigner (vécu : Musk -10.86% qui pouvait filer).
                    reason = f"stop présence {pct_change*100:.0f}% (favori sans edge, on coupe serré)"
                elif pct_change <= -sl_pct:
                    reason = f"stop-loss {pct_change*100:.0f}%"
                elif (p["strategy"] not in ("sport", "nearcert")
                      and (time.time() - (p.get("ts") or time.time())) > DEAD_HOURS * 3600
                      and abs(pct_change) < DEAD_FLAT_PCT):
                    # ⑤ POSITION MORTE : ouverte depuis longtemps et quasi-flat = capital
                    # dormant. On recycle vers de meilleures cibles (vélocité). near-cert
                    # et sport exclus (tenus jusqu'à résolution / logique propre).
                    reason = f"position morte ({DEAD_HOURS}h quasi-flat {pct_change*100:+.0f}%) — recycle capital"
                else:
                    continue

                # vend la taille RÉELLE détenue, pas celle de la DB
                res = self._live.sell_position(p["token_id"], current_price, real_size)
                if res.get("success"):
                    pnl = (current_price - entry_price) * real_size
                    db.close_directional_trade(p["id"], current_price, pnl)
                    self._cash_cache[1] = None
                    self._log(
                        f"SORTIE {p['strategy']} {p['question'][:35]} — {reason}, "
                        f"pnl={pnl:+.2f}$ ({entry_price:.3f}->{current_price:.3f})"
                    )
                else:
                    self._failed_recently[p["question"]] = time.time()
                    self._log(f"sortie refusée pour {p['question'][:30]}: {str(res)[:80]}")

    async def _scan_resolution_snipe(self):
        """Idée B : achète le vainqueur d'un match DÉJÀ DÉCIDÉ (confirmé ESPN)
        que Polymarket n'a pas encore résolu à 1.00. Gain faible mais quasi
        sûr — on connaît le résultat. Garde-fous : le côté vainqueur doit
        aussi être coté par le marché entre SNIPE_MIN et SNIPE_MAX (cross-check
        qu'on a bien matché le bon marché/côté ; si le marché est en désaccord
        franc avec ESPN, on s'abstient)."""
        from core.livescores import get_decided_games, match_polymarket_question, get_inplay_games, _norm
        from core.polymarket import _is_sport, _PROP_EXCLUDE
        import asyncio as _aio
        # BUG FIX (Steven 19/07) : le snipe a parié un PROP ("run in first inning")
        # car son sous-titre contenait "vs". On exige un marché QUI GAGNE et on
        # exclut tout prop (inning, total, spread, handicap, score exact...).
        _PROP_MORE = ["inning", "run scored", "run in the", "first goal", "to score",
                      "total ", "how many", "spread", "handicap", "-1.5", "+1.5", "-2.5",
                      "o/u", "over ", "under ", "points", "corner", "half", "exact",
                      "margin", "double chance", "both teams"]
        decided = await _aio.to_thread(get_decided_games)
        if not decided:
            return
        # GARDE DOUBLEHEADER (Steven 19/07) : mêmes équipes qui jouent 2x le même jour
        # -> le résultat du Game 1 ne doit PAS s'appliquer au marché du Game 2. Si les
        # mêmes équipes sont AUSSI en match EN COURS, on s'abstient (ambiguïté).
        inplay = await _aio.to_thread(get_inplay_games)
        inplay_pairs = [frozenset({_norm(g["p1"]), _norm(g["p2"])}) for g in inplay]
        self._snipe_arb_used = 0  # budget d'arbitrages IA pour ce scan
        pool = await get_active_markets(limit=400, min_volume_24h=500)
        held = {p["question"] for p in db.open_live_directional_positions()}
        async with aiohttp.ClientSession() as session:
            for m in pool:
                if len(m["token_ids"]) != 2 or not _is_sport(m["question"]):
                    continue
                if m["question"] in held:
                    continue
                ql = m["question"].lower()
                # DONNÉES STRUCTURÉES (Steven) : filtre FIABLE. sportsMarketType != moneyline
                # = prop/spread/total -> jamais le marché du vainqueur, on écarte.
                if m.get("sports_type") and m["sports_type"] != "moneyline":
                    continue
                # match plausiblement FINI (via gameStartTime) : pas un match en cours / Game 2
                gs = m.get("game_start")
                if gs:
                    try:
                        from datetime import datetime, timezone
                        s = str(gs).replace("Z", "+00:00")
                        if " " in s and "T" not in s:
                            s = s.replace(" ", "T", 1)
                        if s.endswith("+00"):
                            s += ":00"
                        gsdt = datetime.fromisoformat(s)
                        if (datetime.now(timezone.utc) - gsdt).total_seconds() < SNIPE_MIN_GAME_HOURS * 3600:
                            continue  # match trop récent -> encore en cours, on ne snipe pas
                    except Exception:
                        pass
                # filtre PROP de secours (si sportsMarketType absent) + marché "qui gagne"
                if any(k in ql for k in _PROP_EXCLUDE) or any(k in ql for k in _PROP_MORE):
                    continue
                if not ("win" in ql or "to advance" in ql or " vs. " in ql or " vs " in ql):
                    continue
                mm = match_polymarket_question(m["question"], decided)
                if not mm:
                    continue
                # doubleheader : ces 2 équipes ont-elles AUSSI un match en cours ? -> ambigu, skip
                pair = frozenset({_norm(mm["winner"]), _norm(mm["loser"])})
                if pair in inplay_pairs:
                    self._log_once(f"dh_{m['question']}", f"[snipe] {m['question'][:34]} — SKIP: mêmes équipes en match EN COURS (ambiguïté doubleheader)")
                    continue
                # UN SEUL snipe par match (fix : rachetait Pittsburgh 4x à prix montant)
                match_key = frozenset({mm["winner"], mm["loser"]})
                if match_key in self._sniped_matches:
                    continue
                # Déterminer le token du vainqueur. Le marché est binaire
                # YES/NO sur le 1er nommé — on regarde quel côté correspond au
                # vainqueur ESPN via la question.
                q = m["question"].lower()
                w = mm["winner"].lower()
                # heuristique : si le vainqueur est nommé AVANT "vs" -> YES
                before_vs = q.split(" vs")[0] if " vs" in q else q
                winner_is_first = any(part in before_vs for part in w.split() if len(part) > 3)
                token = m["token_ids"][0] if winner_is_first else m["token_ids"][1]

                book = await get_book(session, token)
                ask = best_ask(book)
                if not ask:
                    continue
                price = ask[0]
                if price > SNIPE_MAX_PRICE:
                    continue  # déjà pricé ~1.00 -> gain nul
                if price < SNIPE_MIN_PRICE:
                    # ARBITRAGE-IA (idée Steven) : marché conteste ESPN. Dans la zone
                    # borderline, l'IA lit le score live et confirme le vainqueur avant
                    # de sniper. Hors zone / IA non confirmée -> on s'abstient.
                    if not (SNIPE_ARB_ENABLED and price >= SNIPE_ARB_MIN
                            and self._snipe_arb_used < SNIPE_ARB_MAX_CALLS):
                        continue
                    self._snipe_arb_used += 1
                    from core.ai_verified import evaluate as _ai_eval
                    want_side = "YES" if winner_is_first else "NO"
                    av = await _aio.to_thread(_ai_eval, m["question"],
                                              m["prices"][0] if m.get("prices") else price)
                    if not (av and av.get("grounded") and av["confidence"] >= SNIPE_ARB_MIN_CONF
                            and av["side"] == want_side):
                        self._log_once(f"arb_no_{m['question']}", f"[snipe] {m['question'][:30]} @{price:.2f} — marché conteste ESPN, IA NON confirmée -> skip")
                        continue
                    self._log(f"⚖️ SNIPE ARBITRÉ IA {m['question'][:30]} @{price:.2f} — IA confirme {mm['winner'][:14]} ({av['confidence']:.0%}, {av.get('summary','')[:36]})")

                if self.live_enabled and self._live and not self._can_afford_min_order(price):
                    return  # solde ne couvre pas la commande minimale (5 parts)
                usd = min(SNIPE_MAX_USD, self._equity() * 0.45, self._available_cash() * 0.9)
                if usd < MIN_DIRECTIONAL_USD:
                    return
                size = usd / price
                res = self._live.execute_directional(token, price, size) if (self.live_enabled and self._live) else {"success": False}
                live_done = bool(res.get("success"))
                if live_done:
                    self._cash_cache[1] = None
                    self._sport_traded[m["question"]] = time.time()
                    self._sniped_matches.add(match_key)  # ne plus racheter ce match
                    self._log(
                        f"SNIPE {mm['winner']} (gagné, confirmé ESPN) {m['question'][:34]} — "
                        f"achat @ {price:.3f} pour ~1.00 ({(1-price)*100:.0f}% quasi sûr), {usd:.2f}$"
                    )
                elif self.live_enabled:
                    self._failed_recently[m["question"]] = time.time()
                self._record_trade(
                    "snipe", m["question"], "YES" if winner_is_first else "NO", price, usd, size,
                    f"vainqueur ESPN {mm['winner']} — résolution non finie", live_done, token_id=token, conviction=1.0,
                )
                if not live_done and not self.live_enabled:
                    self._log(f"SNIPE PAPER {mm['winner']} @ {price:.3f} sur {m['question'][:34]}")

    async def _scan_copytrade(self):
        """Copy-trading : copie les nouveaux achats des top traders rentables
        (leaderboard 7j). On parie sur leur track-record prouvé, pas sur notre
        analyse. Garde-fous : prix 0.15-0.90 (pas de loterie), achat < 15 min
        (pas en retard), une copie par (trader, marché)."""
        from core.copytrade import get_top_traders, get_recent_buys
        import asyncio as _aio

        traders = await _aio.to_thread(get_top_traders, "7d", COPY_N_TRADERS)
        if not traders:
            return
        held = {p["question"] for p in db.open_live_directional_positions()}
        async with aiohttp.ClientSession() as session:
            for tr in traders:
                buys = await _aio.to_thread(get_recent_buys, tr["wallet"], COPY_MAX_AGE_S)
                from core.copytrade import is_untradeable_prop
                # Détection MARKET-MAKER (remarque Steven : certains parient des
                # DEUX côtés). Si le trader a acheté >=2 tokens distincts sur le
                # MÊME marché, il tient le carnet des deux côtés = pas de
                # conviction directionnelle -> on ne copie AUCUN de ses paris là.
                _tokens_by_title: dict = {}
                for _b in buys:
                    _tokens_by_title.setdefault(_b["title"], set()).add(_b["token_id"])
                both_sided_titles = {t for t, toks in _tokens_by_title.items() if len(toks) > 1}
                for b in buys:
                    key = (tr["wallet"], b["token_id"])
                    if key in self._copied:
                        continue
                    if b["title"] in both_sided_titles:
                        self._copied.add(key)
                        self._log_once(f"copy_mm_{key}", f"[copy] {tr['name']} parie des 2 côtés sur {b['title'][:30]} — SKIP: market-maker, pas de conviction")
                        continue
                    # PLAFOND D'EXPOSITION PAR WALLET (point ferme Perplexity) : un
                    # bon wallet peut avoir une série perdante -> on ne concentre pas.
                    # Max COPY_MAX_PER_WALLET_USD engagé simultanément par wallet source.
                    if db.open_exposure_on_wallet(tr["wallet"]) >= COPY_MAX_PER_WALLET_USD:
                        self._log_once(f"copy_wcap_{tr['wallet']}", f"[copy] {tr['name']} — SKIP: plafond exposition wallet atteint ({COPY_MAX_PER_WALLET_USD}$)")
                        continue
                    # paris à résolution instantanée (O/U, mi-temps, score
                    # exact...) : le stop-loss ne peut pas sortir, on n'y touche pas
                    if is_untradeable_prop(b["title"]):
                        self._copied.add(key)
                        self._log_once(f"prop_{key}", f"[copy] {b['title'][:34]} — SKIP: prop à résolution instantanée (SL impossible)")
                        continue
                    # TENNIS (notre boulet -4.46$) et ESPORTS (high-variance) exclus
                    # du copy aussi (décision Steven) — cohérent avec le sport in-play.
                    from core.polymarket import _is_tennis, ESPORT_KEYWORDS
                    tl = (b["title"] or "").lower()
                    if _is_tennis(b["title"]) or any(k in tl for k in ESPORT_KEYWORDS):
                        self._copied.add(key)
                        self._log_once(f"cat_{key}", f"[copy] {b['title'][:34]} — SKIP: tennis/esport (catégories perdantes/volatiles)")
                        continue
                    # ne copier que les GROS paris (vraie conviction du trader)
                    if b["usdc_size"] < COPY_MIN_TRADER_SIZE:
                        self._copied.add(key)  # marque comme vu, on ne le reverra pas
                        self._log_once(f"copy_small_{key}", f"[copy] {tr['name']} a parié {b['usdc_size']:.0f}$ sur {b['title'][:30]} — SKIP: trop petit (min {COPY_MIN_TRADER_SIZE}$)")
                        continue
                    if b["title"] in held:
                        self._copied.add(key)
                        continue
                    if self._recently_traded(b["title"]):
                        self._copied.add(key)
                        continue  # déjà tradé par une autre stratégie, cooldown partagé
                    # prix ACTUEL du carnet (pas le prix d'entrée du trader,
                    # qui peut dater de 14 min)
                    book = await get_book(session, b["token_id"])
                    ask = best_ask(book)
                    if not ask:
                        continue
                    price = ask[0]
                    if price < COPY_MIN_PRICE or price > COPY_MAX_PRICE:
                        self._copied.add(key)
                        self._log_once(f"copy_range_{key}", f"[copy] {tr['name']} -> {b['title'][:30]} @ {price:.2f} — SKIP: hors fourchette (évite loterie)")
                        continue
                    # pas trop loin du prix où le trader est rentré (sinon on chase)
                    if b["price"] > 0 and abs(price - b["price"]) / b["price"] > 0.10:
                        self._log_once(f"copy_moved_{key}", f"[copy] {tr['name']} -> {b['title'][:30]} — SKIP: prix a bougé ({b['price']:.2f}→{price:.2f}), on ne chase pas")
                        continue

                    self._copied.add(key)
                    if self.live_enabled and self._live and not self._can_afford_min_order(price):
                        return
                    # COPY v2 : pondération par QUALITÉ du wallet (rentabilité 30j).
                    # Un gagnant prouvé sur la durée -> on lui fait + confiance (mise
                    # jusqu'à 2x, plafonnée). C'est le cœur scalable : suivre + fort
                    # les meilleurs, + prudemment les moins prouvés.
                    # MÉTA-EDGE RESPECTÉ PARTOUT (Steven 19/07) : le copy fuyait le foot
                    # (catégorie prouvée perdante) alors que le sport in-play le bridait.
                    # On coupe le copy sur toute catégorie que la data marque perdante.
                    if self._cat_perf.get(self._categorize(b["title"]), 1.0) <= 0.5:
                        self._log_once(f"copy_metae_{key}", f"[copy] {b['title'][:30]} — SKIP: catégorie prouvée perdante (méta-edge)")
                        self._copied.add(key)
                        continue
                    quality = tr.get("quality", 1.0)
                    edge = self._edge_mult(b["title"])  # concentre le capital sur le foot/Team-to-Advance
                    usd = min(COPY_MAX_USD, self._confidence_size(0.7, price) * quality * edge, self._available_cash() * 0.9)
                    if usd < MIN_DIRECTIONAL_USD:
                        return
                    # ④ CAP D'EXPOSITION PAR CATÉGORIE (le copy empilait du foot -> 72%)
                    if not self._cat_cap_ok(b["title"], usd):
                        self._log_once(f"copy_catcap_{key}", f"[copy] {b['title'][:30]} — SKIP: cap catégorie atteint")
                        self._copied.add(key)
                        continue
                    size = usd / price
                    res = self._live.execute_directional(b["token_id"], price, size) if (self.live_enabled and self._live) else {"success": False}
                    live_done = bool(res.get("success"))
                    if live_done:
                        self._cash_cache[1] = None
                        self._mark_traded(b["title"])  # cooldown partagé
                        self._log(
                            f"✅ COPY {tr['name']} (+{tr['profit']/1000:.0f}k$/7j) — {b['title'][:34]} "
                            f"@ {price:.3f}, {usd:.2f}$"
                        )
                    elif self.live_enabled:
                        pass
                    self._record_trade(
                        "copy", b["title"], "YES", price, usd, size,
                        f"copie {tr['name']} (30j:+{tr.get('profit30',0)/1000:.0f}k$, q{tr.get('quality',1):.1f})", live_done,
                        token_id=b["token_id"], conviction=0.7,
                        src_wallet=tr["wallet"], src_price=b.get("price"), src_ts=b.get("timestamp"),
                    )
                    if not live_done and not self.live_enabled:
                        self._log(f"COPY PAPER {tr['name']} -> {b['title'][:32]} @ {price:.3f}")

    @staticmethod
    def _categorize(q: str) -> str:
        tl = (q or "").lower()
        if any(k in tl for k in ["bitcoin", "ethereum", "solana", "crypto", " btc", " eth"]): return "crypto"
        if any(k in tl for k in ["itf", "atp", "wta", "tennis", " open:", "wimbledon", "challenger"]): return "tennis"
        if any(k in tl for k in ["counter", "dota", "cs2", "valorant", "esport"]): return "esport"
        if any(k in tl for k in ["odi", "t20", "cricket"]): return "cricket"
        if any(k in tl for k in ["nba", "wnba", "basket", "summer league", "storm", "mystics", "aces", " sky"]): return "basket"
        if any(k in tl for k in ["ufc", "boxing", "fight night"]): return "combat"
        # FOOT élargi (découverte : 76% du volume des whales = 'Team to Advance',
        # 'X vs. Y', O/U buts... que le catégoriseur ratait). C'est LEUR terrain.
        if any(k in tl for k in ["fifa", "world cup", "premier league", "la liga", "serie a",
                                 "bundesliga", "ligue 1", "soccer", "football", "win on",
                                 "team to advance", "to advance", " advance", "end in a draw",
                                 "o/u", "total corners", " vs. "]): return "foot"
        if any(k in tl for k in ["parliament", "election", "president", "trump", "iran", "israel", "war"]): return "politique"
        if any(k in tl for k in ["fed", "rate", "inflation", "gdp", "cpi", "jobs"]): return "macro"
        if any(k in tl for k in ["ai ", "openai", "gpt", "tesla", "apple", "nvidia", "company"]): return "tech"
        if any(k in tl for k in ["tweet", "musk post", "weather", "temperature"]): return "novelty"
        return "autre"

    def _edge_mult(self, question: str) -> float:
        """Multiplicateur de mise par catégorie (découverte : les gagnants prouvés
        sont ~90% FOOT, surtout 'Team to Advance'). On CONCENTRE le capital sur
        notre edge (foot boosté) et on EXPLORE le reste à mise réduite — sans se
        disperser. C'est 'trader sélectivement' avec la taille au bon endroit."""
        ql = (question or "").lower()
        cat = self._categorize(question)
        if "team to advance" in ql or "to advance" in ql:
            base = 1.5   # marché ROI des whales (France vs Spain = 2.1M$ chez eux)
        elif cat == "foot":
            base = 1.3   # notre edge terrain
        elif cat == "basket":
            base = 1.0
        else:
            base = 0.6   # autres catégories : exploration à mise réduite (on mesure)
        # ① MÉTA-EDGE : module par la perf réelle mesurée (data > présupposé), borné.
        adj = self._cat_perf.get(cat, 1.0)
        return round(max(0.2, min(2.0, base * adj)), 3)

    def _refresh_cat_perf(self):
        """① MÉTA-EDGE AUTO : lit la perf NET-DE-COÛTS par catégorie (category_report,
        vérité on-chain) et en déduit un multiplicateur data-driven par catégorie.
        Le bot booste tout seul ce qui GAGNE et coupe ce qui PERD — au lieu d'un
        edge_mult codé en dur. Le vrai avantage durable : il s'améliore avec la data."""
        try:
            rep = db.category_report(since=db.get_stats_since()).get("par_categorie", [])
        except Exception:
            return
        perf = {}
        for d in rep:
            n, pnl = d.get("clotures", 0), d.get("pnl_net", 0.0)
            if n < CATPERF_MIN_CLOSES:
                continue  # pas assez de données -> neutre (1.0 implicite)
            if pnl > 1:
                perf[d["categorie"]] = 1.4      # PROUVÉ gagnant -> on charge
            elif pnl > 0:
                perf[d["categorie"]] = 1.15     # prometteur
            elif pnl < -1:
                perf[d["categorie"]] = 0.3      # PROUVÉ perdant -> on étouffe
            else:
                perf[d["categorie"]] = 0.7
        self._cat_perf = perf
        if perf:
            self._log_once("catperf", f"[méta-edge] multiplicateurs data: {perf}", ttl=3600)

    def _spread_ok(self, book) -> bool:
        """③ GARDE LIQUIDITÉ : True si le spread (meilleur ask - meilleur bid) est
        assez serré. À notre petite taille, un spread large = fill pourri qui mange
        l'edge. Si pas de bid (marché sans profondeur), on refuse."""
        ask, bid = best_ask(book), best_bid(book)
        if not ask or not bid:
            return False
        return (ask[0] - bid[0]) <= SPREAD_MAX

    def _cat_exposure(self, category: str) -> float:
        """④ Somme du coût engagé (positions live ouvertes) sur une CATÉGORIE."""
        return sum((p.get("cost_usd") or 0) for p in db.open_live_directional_positions()
                   if self._categorize(p.get("question", "")) == category)

    def _cat_cap_ok(self, question: str, add_usd: float) -> bool:
        """④ True si ajouter add_usd ne dépasse pas CAT_EXPOSURE_CAP_FRACTION de
        l'equity sur cette catégorie (anti-concentration sur une seule famille)."""
        cat = self._categorize(question)
        cap = self._equity() * CAT_EXPOSURE_CAP_FRACTION
        return (self._cat_exposure(cat) + add_usd) <= cap

    def _price_floor_ok(self, question: str, price: float) -> bool:
        """Plancher de prix conditionnel à la catégorie (idée Steven). Edge prouvé
        (foot/Team-to-Advance, edge_mult>=1) : garde le mid-price (0.55-0.65, c'est
        là que vit le Team-to-Advance). Exploration (edge_mult<1, non prouvé) : exige
        un vrai favori >= EXPLORE_MIN_PRICE — sans edge, mid-price = variance à EV~0-."""
        if self._edge_mult(question) < 1.0:
            return price >= EXPLORE_MIN_PRICE
        return True

    @staticmethod
    def _match_signature(question: str):
        """Signature 'même match' par NOMS d'équipes. Le garde-fou event_id rate les
        doubles legs quand Gamma donne des event_id DIFFÉRENTS aux marchés 'Team to
        Win' vs 'Draw' d'un même match (cas France-England repéré par Steven :
        ~14$ corrélés sur une seule rencontre). On extrait la paire d'un 'A vs B'
        -> frozenset normalisé. None si pas de paire identifiable."""
        import re
        q = (question or "").lower().replace("will ", " ")
        m = re.search(r"([a-z0-9 .&'-]+?)\s+vs\.?\s+([a-z0-9 .&'-]+)", q)
        if not m:
            return None
        def norm(s):
            s = re.split(r"[:?]| end | win | to advance| to win| on ", s)[0]
            w = s.split()
            return " ".join(w[:3]).strip()
        a, b = norm(m.group(1)), norm(m.group(2))
        if not a or not b:
            return None
        return frozenset({a, b})

    def _held_event_ids(self, pool, held_questions):
        """Ids des ÉVÉNEMENTS déjà détenus (remarque Steven : Toronto & Montréal
        pouvaient être 2 marchés du MÊME match -> parier sur les deux, c'est se
        contredire, et le garde-fou par TITRE ne le voit pas). On mappe chaque
        question détenue vers son event_id via le pool Gamma fraîchement chargé.
        Zéro migration DB : reconstruit à chaque scan. None ignoré (pas de faux
        blocage si Gamma n'a pas rattaché d'event)."""
        return {m.get("event_id") for m in pool
                if m["question"] in held_questions and m.get("event_id")}

    async def _scan_nearcertain(self):
        """SNIPE NEAR-CERTAIN GÉNÉRALISÉ (idée Steven + jeu des whales) : acheter
        les favoris EXTRÊMES (0.90-0.96) proches de résolution, dans TOUTES les
        catégories vérifiables (pas tennis/esport/props/novelties non vérifiables).
        Exploite le favorite-longshot bias (les 90-100¢ sont souvent sous-cotés).
        Basse variance, petite mise, EXPÉRIMENTAL -> mesuré via category_report.
        Held to resolution (courte, <NEARCERT_MAX_H h)."""
        if not (NEARCERT_ENABLED and self.live_enabled and self._live is not None):
            return
        if self._available_cash() < MIN_DIRECTIONAL_USD:
            return
        from core.polymarket import _days_until, ESPORT_KEYWORDS, TENNIS_KEYWORDS, _PROP_EXCLUDE, POLITICS_KEYWORDS
        pool = await get_active_markets(limit=400, min_volume_24h=500)
        held = {p["question"] for p in db.open_live_directional_positions()}
        held_eids = self._held_event_ids(pool, held)
        held_sigs = {self._match_signature(q) for q in held} - {None}
        async with aiohttp.ClientSession() as session:
            best = None
            for m in pool:
                if len(m.get("token_ids", [])) != 2 or not m.get("prices"):
                    continue
                # GARDE MÊME-ÉVÉNEMENT (event_id) + MÊME MATCH (noms d'équipes) : jamais
                # une 2e jambe corrélée d'un match déjà en position.
                if m.get("event_id") and m["event_id"] in held_eids:
                    continue
                _sig = self._match_signature(m["question"])
                if _sig and _sig in held_sigs:
                    continue
                ql = m["question"].lower()
                # catégories NON vérifiables / perdantes exclues (météo/novelty non
                # snipable sans source de vérité ; esport/tennis/props/politique out)
                if (any(k in ql for k in ESPORT_KEYWORDS) or any(k in ql for k in TENNIS_KEYWORDS)
                        or any(k in ql for k in _PROP_EXCLUDE) or any(k in ql for k in POLITICS_KEYWORDS)
                        or any(k in ql for k in ["tweet", "musk post", "weather", "temperature", "up or down"])):
                    continue
                # VÉRIFIABLE UNIQUEMENT : catégorie à source de vérité / issue claire.
                if self._categorize(m["question"]) not in NEARCERT_OK_CATS:
                    continue
                if m["question"] in held or self._recently_traded(m["question"]):
                    continue
                # ④ CAP D'EXPOSITION PAR CATÉGORIE (foot near-cert + presence foot = concentration)
                if not self._cat_cap_ok(m["question"], 0):
                    continue
                h = _days_until(m.get("end_date")) * 24
                if h <= 0 or h > NEARCERT_MAX_H:
                    continue
                p = m["prices"][0]
                fav = max(p, 1 - p)
                if not (NEARCERT_MIN <= fav <= NEARCERT_MAX):
                    continue
                # ROTATION COURTE : on préfère la résolution la + PROCHE (le capital
                # tourne 2-4x/jour au lieu de dormir) — vélocité > volume.
                if best is None or h < best[0]:
                    best = (h, m, p >= (1 - p), fav)
            if not best:
                return
            _, m, fav_is_yes, favp = best
            token = m["token_ids"][0] if fav_is_yes else m["token_ids"][1]
            side = "YES" if fav_is_yes else "NO"
            book = await get_book(session, token)
            ask = best_ask(book)
            if not ask:
                return
            # ③ GARDE LIQUIDITÉ : spread trop large = fill pourri, on passe.
            if not self._spread_ok(book):
                return
            price = ask[0]
            if not (NEARCERT_MIN <= price <= NEARCERT_MAX + 0.02):
                return
            if not self._can_afford_min_order(price):
                return
            cash = self._available_cash()
            # BALAYAGE DU RÉSIDU (idée Steven) : un reste de ~6-7$ déployé à moitié
            # laisse ~3$ qui dorment (trop petit pour re-bouger). Sous RESIDUAL_SWEEP_MAX
            # on balaie presque tout le cash sur CETTE near-certain plutôt que de laisser
            # de la monnaie inexploitable. Au-dessus, moitié (on garde de quoi tourner).
            if cash <= RESIDUAL_SWEEP_MAX:
                # PAS de plafond NEARCERT_MAX_USD ici : le but EST de vider le résidu
                # (sinon on re-capperait à 5$ et ~1-3$ dormiraient encore). Ratchet
                # respecté (déploie moins pendant une protection de gain).
                usd = cash * 0.95 * self._ratchet_mult()
                self._log_once(f"sweep_{m['question']}", f"[near-certain] balayage résidu {cash:.2f}$ -> {usd:.2f}$ (monnaie sinon dormante)")
            else:
                # PROPORTIONNEL au bankroll (Steven) : plafond = max(fixe, 13% equity)
                # -> déploie plus quand le compte grossit, garde le fixe en plancher.
                cap = max(NEARCERT_MAX_USD, self._equity() * 0.13)
                usd = min(cap * self._ratchet_mult(), cash * 0.5)
            if usd < MIN_DIRECTIONAL_USD:
                return
            size = usd / price
            res = self._live.execute_directional(token, price, size)
            live_done = bool(res.get("success"))
            if live_done:
                self._cash_cache[1] = None
                self._mark_traded(m["question"])
                self._log(f"🎯 NEAR-CERTAIN {side} {m['question'][:34]} favori @{price:.3f} (favorite-longshot bias, {(1-price)*100:.1f}% à gagner)")
            elif self.live_enabled:
                self._failed_recently[m["question"]] = time.time()
            self._record_trade("nearcert", m["question"], side, price, usd, size,
                               f"near-certain favori @{price:.2f}", live_done, token_id=token, conviction=0.9)

    async def _scan_momentum_shadow(self):
        """SHADOW LOGGER MOMENTUM (idée Perplexity) : détecte les mouvements de prix
        (>=MOMENTUM_SHADOW_PTS pts) dans TOUTES les catégories SANS trader, les
        loggue, et mesure ~5min après si le mouvement a CONTINUÉ. Révèle
        GRATUITEMENT où un momentum serait exploitable — zéro capital risqué.
        Détection sans appel API : compare au snapshot de prix du scan précédent."""
        prev = getattr(self, "_prev_mom_prices", {})
        cur = {}
        self._mover_qs = {}  # movers de CE scan (consommés par la vérif IA au cycle suivant)
        for m in self._markets:
            if len(m.get("token_ids", [])) != 2 or not m.get("prices"):
                continue
            tok = str(m["token_ids"][0])
            cur[tok] = (m["prices"][0], m["question"])
            pv = prev.get(tok)
            if pv is not None:
                move = (m["prices"][0] - pv) * 100
                if abs(move) >= MOMENTUM_SHADOW_PTS:
                    db.log_momentum_signal(m["question"], self._categorize(m["question"]),
                                           round(move, 1), m["prices"][0], tok)
                # SCAN INTELLIGENT (idée Steven) : un GROS mouvement inter-scan sur un
                # marché sport = il s'est passé qqch -> on le marque prioritaire pour la
                # VÉRIFICATION IA (le delta dit OÙ regarder, l'IA dit s'il y a de l'edge).
                from core.polymarket import _is_sport
                if abs(move) >= MOVER_TRIGGER_PTS and _is_sport(m["question"]) \
                        and self._categorize(m["question"]) != "combat":
                    self._mover_qs[m["question"]] = round(move, 1)
        self._prev_mom_prices = {tok: p for tok, (p, q) in cur.items()}
        # suivi : le mouvement a-t-il continué ~5min après ?
        for sig in db.momentum_signals_to_check():
            p1 = cur.get(sig["token_id"], (None, None))[0]
            if p1 is not None:
                db.record_momentum_check(sig["id"], p1)

    async def _scan_presence(self):
        """GARDE-PRÉSENCE (idée Steven) : si le compte n'a AUCUNE position
        ouverte, on en tient UNE (favori solide, zone gagnante, résolution
        <48h). Objectif : toujours une activité visible sur Polymarket, sans
        churn. Ne se déclenche QUE quand tout est vide — dès qu'une autre
        stratégie a une position, cette garde se met en veille."""
        if not (PRESENCE_ENABLED and self.live_enabled and self._live is not None):
            return
        # PAS DE MAXIMUM DE POSITIONS (Steven 2026-07-17) : on déploie le cash tant
        # qu'il en reste d'exploitable. Presence ouvre 1 position/scan, donc c'est
        # naturellement paced (pas de flood). MAIS on garde une RÉSERVE (poudre sèche)
        # pour le near-certain : presence ne touche que le cash au-dessus de la réserve.
        _reserve = self._equity() * CASH_RESERVE_FRACTION
        _deployable = self._available_cash() - _reserve
        if _deployable < MIN_DIRECTIONAL_USD:
            return
        from core.polymarket import ESPORT_KEYWORDS, TENNIS_KEYWORDS, _PROP_EXCLUDE, _days_until
        pool = await get_active_markets(limit=400, min_volume_24h=400)
        held_q = {p["question"] for p in db.open_live_directional_positions()}
        held_eids = self._held_event_ids(pool, held_q)
        held_sigs = {self._match_signature(q) for q in held_q} - {None}
        candidates_pool = []  # [(volume, market, fav_is_yes)] non détenus
        for m in pool:
            if len(m.get("token_ids", [])) != 2 or not m.get("prices"):
                continue
            # GARDE MÊME-ÉVÉNEMENT (event_id) + MÊME MATCH (noms d'équipes) : pas de
            # 2e jambe corrélée sur une rencontre déjà en position (cas France-England).
            if m.get("event_id") and m["event_id"] in held_eids:
                continue
            _sig = self._match_signature(m["question"])
            if _sig and _sig in held_sigs:
                continue
            ql = m["question"].lower()
            # PRÉSENCE-EXPLORATION (demande Steven : ouvrir à + de catégories, mesurer
            # séparément). On exclut UNIQUEMENT les perdants PROUVÉS (tennis, esport,
            # politique, props) + les novelties inutiles (tweets/météo). Tout le reste
            # (foot, basket, crypto, tech, UFC, entreprises...) est exploré en réel et
            # tagué par catégorie via db.category_report().
            from core.polymarket import POLITICS_KEYWORDS
            # COMBAT/UFC EXCLU (incident -99% du 18/07) : KO = résolution instantanée,
            # le prix gappe par-dessus le SL, ingérable. On ne tient QUE du gérable.
            if (any(k in ql for k in _PROP_EXCLUDE) or any(k in ql for k in ESPORT_KEYWORDS)
                    or any(k in ql for k in TENNIS_KEYWORDS) or any(k in ql for k in POLITICS_KEYWORDS)
                    or self._categorize(m["question"]) == "combat"
                    or any(k in ql for k in ["ufc", "mma", "boxing", "knockout", "fight night",
                                             "tweet", "musk post", "weather", "temperature", "rain "])):
                continue
            # MÉTA-EDGE : skip toute catégorie que la data prouve perdante (ex. foot)
            if self._cat_perf.get(self._categorize(m["question"]), 1.0) <= 0.5:
                continue
            h = _days_until(m.get("end_date")) * 24
            if h < PRESENCE_MIN_HOURS or h > PRESENCE_MAX_HOURS:
                continue
            p = m["prices"][0]
            fav_price = max(p, 1 - p)
            if not (PRESENCE_FAV_MIN <= fav_price <= PRESENCE_FAV_MAX):
                continue
            # exclut d'office ce qu'on tient déjà / vient de trader (sinon on
            # sélectionnait le meilleur puis on abandonnait car déjà détenu = 0 trade)
            if self._recently_traded(m["question"]) or self._holds_position_on(m["question"]):
                continue
            # ④ CAP D'EXPOSITION PAR CATÉGORIE : skip si la famille est déjà pleine
            if not self._cat_cap_ok(m["question"], 0):
                continue
            vol = m.get("volume_24h", 0) or 0
            candidates_pool.append((vol, m, p >= (1 - p)))
        if not candidates_pool:
            return
        # PRIORITÉ FOOT/Team-to-Advance (le terrain des gagnants), puis volume.
        from core.polymarket import _days_until as _du
        candidates_pool.sort(key=lambda x: (-self._edge_mult(x[1]["question"]), _du(x[1].get("end_date")), -x[0]))
        _, m, fav_is_yes = candidates_pool[0]
        token = m["token_ids"][0] if fav_is_yes else m["token_ids"][1]
        side = "YES" if fav_is_yes else "NO"
        async with aiohttp.ClientSession() as session:
            book = await get_book(session, token)
        ask = best_ask(book)
        if not ask:
            return
        # ③ GARDE LIQUIDITÉ : spread trop large = fill pourri, on passe.
        if not self._spread_ok(book):
            self._log_once(f"spread_{m['question']}", f"[presence] {m['question'][:28]} — SKIP: spread trop large")
            return
        price = ask[0]
        if not (PRESENCE_FAV_MIN <= price <= PRESENCE_FAV_MAX):
            return
        if not self._can_afford_min_order(price):
            return
        # mise PROPORTIONNELLE au capital (Steven), boostée par l'edge de la catégorie :
        # conviction dérivée de _edge_mult (foot/Team-to-Advance = + haute conviction).
        conv = min(1.0, 0.45 * self._edge_mult(m["question"]))
        usd = min(self._confidence_size(conv, price), _deployable * 0.9)
        size = usd / price
        res = self._live.execute_directional(token, price, size)
        live_done = bool(res.get("success"))
        if live_done:
            self._cash_cache[1] = None
            self._mark_traded(m["question"])
            self._log(f"🛡️ PRÉSENCE {side} {m['question'][:36]} — favori @{price:.2f}, tenu jusqu'à résolution (<48h)")
        elif self.live_enabled:
            self._failed_recently[m["question"]] = time.time()
        self._record_trade("presence", m["question"], side, price, usd, size,
                           "garde-présence : favori solide <48h", live_done, token_id=token, conviction=0.6)

    async def _scan_crypto_snipe(self):
        """Resolution-sniping CRYPTO (non-sport) : achète le côté quasi
        certain d'un marché 'BTC/ETH au-dessus de X à telle heure' quand le
        prix réel (Coinbase) est franchement du bon côté et l'échéance proche.
        Le pendant crypto du sniper ESPN — réponse au 'pas que le sport'."""
        from core.crypto_snipe import evaluate, parse_crypto_market
        from core.polymarket import _days_until
        import asyncio as _aio

        pool = await get_active_markets(limit=400, min_volume_24h=500)
        held = {p["question"] for p in db.open_live_directional_positions()}
        async with aiohttp.ClientSession() as session:
            for m in pool:
                if len(m["token_ids"]) != 2 or not parse_crypto_market(m["question"]):
                    continue
                if m["question"] in held:
                    continue
                last = self._sport_traded.get(m["question"], 0)
                if last and time.time() - last < SPORT_MATCH_COOLDOWN_S:
                    continue
                hours = _days_until(m.get("end_date")) * 24
                if hours > 12:
                    continue  # trop loin : le prix peut bouger, pas encore "sûr"
                verdict = await _aio.to_thread(evaluate, m["question"], hours)
                if not verdict:
                    continue
                side = verdict["winner_side"]
                token = m["token_ids"][0] if side == "YES" else m["token_ids"][1]
                book = await get_book(session, token)
                ask = best_ask(book)
                if not ask:
                    continue
                price = ask[0]
                sym_floor = CRYPTO_SNIPE_MIN_PRICE.get(verdict["symbol"], SNIPE_MIN_PRICE)
                if price > SNIPE_MAX_PRICE or price < sym_floor:
                    continue

                if self.live_enabled and self._live and not self._can_afford_min_order(price):
                    return
                usd = min(SNIPE_MAX_USD, self._equity() * 0.45, self._available_cash() * 0.9)
                if usd < MIN_DIRECTIONAL_USD:
                    return
                size = usd / price
                res = self._live.execute_directional(token, price, size) if (self.live_enabled and self._live) else {"success": False}
                live_done = bool(res.get("success"))
                if live_done:
                    self._cash_cache[1] = None
                    self._sport_traded[m["question"]] = time.time()
                    self._log(
                        f"SNIPE CRYPTO {side} {verdict['symbol']} {verdict['price']:.0f}$ vs seuil {verdict['threshold']:.0f}$ "
                        f"(marge {verdict['margin_pct']:+.1f}%) — achat @ {price:.3f}, {usd:.2f}$"
                    )
                elif self.live_enabled:
                    self._failed_recently[m["question"]] = time.time()
                self._record_trade(
                    "snipe", m["question"], side, price, usd, size,
                    f"{verdict['symbol']} {verdict['price']:.0f}$ vs {verdict['threshold']:.0f}$ (marge {verdict['margin_pct']:+.1f}%)",
                    live_done, token_id=token, conviction=1.0,
                )
                if not live_done and not self.live_enabled:
                    self._log(f"SNIPE CRYPTO PAPER {side} {m['question'][:34]} @ {price:.3f}")

    async def _scan_weather_snipe(self):
        """Resolution-sniping MÉTÉO (idée Steven) : marchés 'Highest temperature
        in <Ville> be N°C on <date>?' résolus sur la météo réelle. Source de
        vérité Open-Meteo (gratuit). On achète le côté quasi-sûr QUAND le marché
        le mésestime (edge informationnel réel). NO sur un bucket loin du max
        prévu ; YES sur le bucket == round(max prévu) s'il est sous-coté."""
        if not (WEATHER_SNIPE_ENABLED and self.live_enabled and self._live is not None):
            return
        if self._available_cash() < MIN_DIRECTIONAL_USD:
            return
        import asyncio as _aio
        from datetime import datetime, timezone
        from core.weather_snipe import evaluate, parse_weather_market

        def _fetch_weather_markets():
            import requests
            out = []
            r = requests.get("https://gamma-api.polymarket.com/public-search",
                             params={"q": "highest temperature", "limit": 60},
                             timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            for e in r.json().get("events", []):
                if "temperature" not in (e.get("title", "") or "").lower():
                    continue
                for m in e.get("markets", []):
                    out.append(m)
            return out

        try:
            markets = await _aio.to_thread(_fetch_weather_markets)
        except Exception:
            return
        held = {p["question"] for p in db.open_live_directional_positions()}
        import json as _json
        for m in markets:
            q = m.get("question", "")
            if not parse_weather_market(q) or q in held or self._recently_traded(q):
                continue
            toks = m.get("clobTokenIds")
            try:
                toks = _json.loads(toks) if isinstance(toks, str) else toks
                prices = _json.loads(m.get("outcomePrices") or "[]") if isinstance(m.get("outcomePrices"), str) else (m.get("outcomePrices") or [])
                prices = [float(x) for x in prices]
            except Exception:
                continue
            if not toks or len(toks) != 2 or len(prices) != 2:
                continue
            end = m.get("endDate", "")
            try:
                hrs = (datetime.fromisoformat(end.replace("Z", "+00:00")) - datetime.now(timezone.utc)).total_seconds() / 3600
            except Exception:
                continue
            if hrs <= 0 or hrs > WEATHER_MAX_HOURS:
                continue
            verdict = await _aio.to_thread(evaluate, q, hrs, end[:10] if end else None)
            if not verdict:
                continue
            side = verdict["winner_side"]
            # GATE MISPRICING : on ne trade que si le marché nous donne de l'edge.
            if side == "NO":
                no_price = prices[1]
                if not (WEATHER_NO_MIN_PRICE <= no_price <= WEATHER_NO_MAX_PRICE):
                    continue
                token = toks[1]
            else:  # YES
                yes_price = prices[0]
                if yes_price > WEATHER_YES_MAX_PRICE:
                    continue
                token = toks[0]
            if not self._cat_cap_ok(q, WEATHER_MAX_USD):
                continue
            async with aiohttp.ClientSession() as session:
                book = await get_book(session, token)
            ask = best_ask(book)
            if not ask:
                continue
            price = ask[0]
            # re-vérifie le prix réel après fetch book (le side doit rester dans la zone edge)
            ceiling = WEATHER_NO_MAX_PRICE if side == "NO" else WEATHER_YES_MAX_PRICE
            if price > ceiling or not self._can_afford_min_order(price):
                continue
            usd = min(WEATHER_MAX_USD, self._equity() * 0.30, self._available_cash() * 0.9)
            if usd < MIN_DIRECTIONAL_USD:
                return
            size = usd / price
            res = self._live.execute_directional(token, price, size)
            live_done = bool(res.get("success"))
            if live_done:
                self._cash_cache[1] = None
                self._mark_traded(q)
                self._log(f"🌡️ SNIPE MÉTÉO {side} {verdict['city']} bucket {verdict['threshold']}°{verdict['unit']} (max prévu {verdict['predicted']}°, écart {verdict['gap']}°) — @{price:.3f}, {usd:.2f}$")
            elif self.live_enabled:
                self._failed_recently[q] = time.time()
            self._record_trade("weather", q, side, price, usd, size,
                               f"{verdict['city']} bucket {verdict['threshold']}° vs max prévu {verdict['predicted']}° (écart {verdict['gap']}°)",
                               live_done, token_id=token, conviction=0.9)
            return  # une prise météo par scan (paced)

    async def _scan_ai_verified(self):
        """SNIPER IA VÉRIFIÉ (idée Steven) : l'IA lit des PREUVES (ESPN + news) et
        rend un score de confiance. On ne trade QUE si confiance >= seuil ET grounded
        (ancré sur un fait), en mise plafonnée, tagué 'ai_verified' pour MESURER sa
        calibration. Additif et borné : ne peut pas dégrader les autres stratégies."""
        if not (AI_VERIFIED_ENABLED and self.live_enabled and self._live is not None):
            return
        if self._available_cash() < MIN_DIRECTIONAL_USD:
            return
        import asyncio as _aio
        from core.ai_verified import evaluate_consensus, evaluate as _ai_single
        from core.polymarket import _is_sport, _PROP_EXCLUDE
        pool = await get_active_markets(limit=400, min_volume_24h=1000)
        held = {p["question"] for p in db.open_live_directional_positions()}
        held_sigs = {self._match_signature(q) for q in held} - {None}
        min_conf = self._ai_calib.get("min_conf", AI_VERIFIED_MIN_CONF)   # ① seuil piloté par calibration
        cands = []
        for m in pool:
            if len(m.get("token_ids", [])) != 2 or not m.get("prices"):
                continue
            ql = m["question"].lower()
            cat = self._categorize(m["question"])
            # ③ GÉNÉRALISÉ (prudent) : sport OU catégories à FAIT vérifiable (macro/tech).
            # Exclus : combat (KO), props, et le non-vérifiable (politique/novelty/autre).
            # combat (KO gap) + tennis (give-back à 0, gap point par point) exclus
            if cat in ("combat", "tennis") or not (_is_sport(m["question"]) or cat in ("macro", "tech")):
                continue
            if any(k in ql for k in _PROP_EXCLUDE) or any(k in ql for k in ["ufc", "mma", "boxing"]):
                continue
            if m["question"] in held or self._recently_traded(m["question"]):
                continue
            sig = self._match_signature(m["question"])
            if sig and sig in held_sigs:
                continue
            p = m["prices"][0]
            if not (AI_VERIFIED_MIN_PRICE <= max(p, 1 - p) <= AI_VERIFIED_MAX_PRICE):
                continue
            cands.append(m)
        # SCAN INTELLIGENT (Steven) : les marchés qui viennent de GROS-BOUGER passent en
        # PREMIER (un mouvement soudain = un fait vient de tomber = edge à vérifier vite).
        cands.sort(key=lambda mk: 0 if mk["question"] in self._mover_qs else 1)
        for m in cands[:AI_VERIFIED_MAX_CALLS]:
            # ② CONSENSUS : plusieurs modèles doivent être d'accord + grounded
            verdict = await _aio.to_thread(evaluate_consensus, m["question"], m["prices"][0], AI_CONSENSUS_MODELS)
            if not verdict:
                # FALLBACK MODÈLE UNIQUE FORT (fix Steven : le consensus est si strict que
                # l'IA ne tirait JAMAIS). Si 1 modèle est grounded à >= AI_SINGLE_MIN_CONF,
                # on trade en DEMI-mise (moins de certitude qu'un consensus). Ainsi l'IA
                # fire enfin sur les cas clairs, on la mesure, le méta-edge la calibre.
                single = await _aio.to_thread(_ai_single, m["question"], m["prices"][0])
                if not (single and single.get("grounded") and single["confidence"] >= AI_SINGLE_MIN_CONF):
                    continue
                verdict = {"side": single["side"], "confidence": single["confidence"],
                           "sl_pct": single.get("sl_pct", 0.15), "summary": single.get("summary", ""),
                           "models": [single.get("model", "?")], "n_agree": 1, "n_votes": 1, "single": True}
            if verdict["confidence"] < min_conf:
                continue
            side = verdict["side"]
            token = m["token_ids"][0] if side == "YES" else m["token_ids"][1]
            if not self._cat_cap_ok(m["question"], AI_VERIFIED_MAX_USD):
                continue
            async with aiohttp.ClientSession() as session:
                book = await get_book(session, token)
            ask = best_ask(book)
            if not ask:
                continue
            price = ask[0]
            if price < 0.05 or price > 0.97 or not self._can_afford_min_order(price):
                continue
            # ① MISE pilotée par la CALIBRATION (dial borné 0.3-2.0) — plafond dur gardé.
            # Fallback modèle unique = DEMI-mise (moins de certitude qu'un consensus).
            dial = max(0.3, min(2.0, self._ai_calib.get("dial", 1.0))) * (0.5 if verdict.get("single") else 1.0)
            usd = min(AI_VERIFIED_MAX_USD * dial, self._equity() * 0.20, self._available_cash() * 0.9)
            if usd < MIN_DIRECTIONAL_USD:
                return
            size = usd / price
            res = self._live.execute_directional(token, price, size)
            live_done = bool(res.get("success"))
            models = "+".join(verdict.get("models", []))[:24]
            if live_done:
                self._cash_cache[1] = None
                self._mark_traded(m["question"])
                # l'IA fixe SON stop-loss (idée Steven) — utilisé dans la boucle de sortie
                self._ai_meta[str(token)] = {"sl_pct": verdict.get("sl_pct", 0.15),
                                             "question": m["question"], "side": side, "entry": price}
                self._log(f"🤖 SNIPE IA {side} consensus {verdict.get('n_agree','?')}/{verdict.get('n_votes','?')} conf {verdict['confidence']:.0%} SL {verdict.get('sl_pct',0.15):.0%} — {verdict['summary'][:40]} (@{price:.3f}, {usd:.2f}$)")
            elif self.live_enabled:
                self._failed_recently[m["question"]] = time.time()
            self._record_trade("ai_verified", m["question"], side, price, usd, size,
                               f"IA consensus {verdict.get('n_agree','?')}/{verdict.get('n_votes','?')} conf {verdict['confidence']:.0%} [{models}] {verdict['summary'][:34]}",
                               live_done, token_id=token, conviction=verdict["confidence"])
            return  # 1 prise IA par passage (paced + coût LLM)

    def _refresh_ai_calibration(self):
        """① CALIBRATION AUTO (idée Steven) : molette de confiance pilotée par la data.
        Bien calibrée/rentable -> on augmente sa mise (dial↑). Sur-confiante/perdante ->
        on réduit la mise ET on relève le seuil de confiance. Neutre tant que peu de data."""
        try:
            rep = db.ai_calibration_report()
        except Exception:
            return
        n, wr, ac, net, roi = rep["n"], rep["win_rate"], rep["avg_conf"], rep["net"], rep.get("roi", 0.0)
        if n < CALIB_MIN_CLOSES:
            self._ai_calib = {"dial": 1.0, "min_conf": AI_VERIFIED_MIN_CONF}
            return
        # SIZING KELLY-FLAVOR (idée Steven) : la mise suit l'EDGE MESURÉ (ROI réel) au lieu
        # de paliers grossiers. dial = 1 + ROI × KELLY_FRACTION, borné [0.3, 2.0]. ROI+15%
        # -> mise ×1.9 ; ROI -10% -> mise ×0.6. Le seuil de confiance suit aussi la calibration.
        dial = max(0.3, min(2.0, 1.0 + roi * KELLY_MULT))
        if net < 0 or wr < ac * 0.7:
            min_conf = min(0.95, AI_VERIFIED_MIN_CONF + 0.05)   # sur-confiante -> on exige +
        elif net > 0 and wr >= ac * 0.9:
            min_conf = max(0.80, AI_VERIFIED_MIN_CONF - 0.05)   # bien calibrée -> on ose +
        else:
            min_conf = AI_VERIFIED_MIN_CONF
        self._ai_calib = {"dial": round(dial, 2), "min_conf": min_conf}
        self._log_once("ai_calib", f"[IA Kelly] {n} clos, win {wr:.0%}, ROI {roi:+.0%}, net {net:+.2f}$ -> mise ×{dial:.2f}, seuil {min_conf:.0%}", ttl=3600)

    def _refresh_circuit(self):
        """COUPE-CIRCUIT (idée Kapitane) : si le PnL réalisé chute de > CIRCUIT_LOSS_USD
        sur les dernières CIRCUIT_WINDOW_H heures, on met les stratégies de VOLUME en
        pause CIRCUIT_PAUSE_H (les snipes vérité continuent). Empêche une mauvaise série
        de compounder. Se base sur les clôtures récentes."""
        try:
            import sqlite3
            now = time.time()
            with sqlite3.connect(db.DB_PATH) as c:
                r = c.execute("SELECT COALESCE(SUM(pnl_usd),0) FROM directional_trades "
                              "WHERE live=1 AND status='closed' AND ts>=?",
                              (now - CIRCUIT_WINDOW_H * 3600,)).fetchone()
            recent = r[0] if r else 0.0
            if recent <= -CIRCUIT_LOSS_USD and now >= self._circuit_until:
                self._circuit_until = now + CIRCUIT_PAUSE_H * 3600
                self._log(f"⛔ COUPE-CIRCUIT : {recent:+.2f}$ sur {CIRCUIT_WINDOW_H:.0f}h -> pause des stratégies de volume {CIRCUIT_PAUSE_H:.0f}h (snipes vérité continuent)", kind="loss")
        except Exception:
            pass

    def _volume_paused(self) -> bool:
        return time.time() < self._circuit_until

    async def _manage_ai_positions(self):
        """BOUCLE DE SURVEILLANCE IA (idée Steven, self wake-up) : l'IA re-regarde
        des positions avec des preuves FRAÎCHES (score live ESPN + news) et SORT si
        la thèse est cassée. Gère : (1) les positions ai_verified, (2) les SNIPE
        UNDERWATER (idée Steven : au lieu d'un SL brut, l'IA juge 'on tient ou on
        sort' via le score — 0-2 en 1re manche = HOLD récupérable, 0-5 en 9e = EXIT)."""
        if not (AI_VERIFIED_ENABLED and self.live_enabled and self._live is not None):
            return
        import asyncio as _aio
        from core.ai_verified import reevaluate
        allpos = db.open_live_directional_positions()
        managed = [p for p in allpos if p.get("strategy") in ("ai_verified", "snipe")]
        if not managed:
            return
        async with aiohttp.ClientSession() as session:
            for p in managed[:3]:  # borne le coût LLM
                book = await get_book(session, p["token_id"])
                bid = best_bid(book)
                if not bid:
                    continue
                cur = bid[0]
                entry = p.get("price") or 0
                pct = (cur - entry) / entry if entry else 0
                # SNIPE : on ne dérange l'IA QUE si la position est underwater (sinon
                # elle gagne, on la laisse aller vers 1.0). ai_verified : toujours re-jugée.
                if p.get("strategy") == "snipe" and pct > -SNIPE_AI_MANAGE_FROM:
                    continue
                verdict = await _aio.to_thread(reevaluate, p["question"], p.get("side", "YES"),
                                               entry, cur)
                if not verdict or verdict["action"] != "EXIT":
                    continue
                # l'IA veut sortir : on vend la taille RÉELLE détenue on-chain
                real = self._live.position_size(p["token_id"])
                if real < 1.0:
                    continue
                res = self._live.sell_position(p["token_id"], cur, real)
                if res.get("success"):
                    pnl = (cur - (p.get("price") or 0)) * real
                    db.close_directional_trade(p["id"], cur, round(pnl, 2))
                    self._cash_cache[1] = None
                    self._ai_meta.pop(str(p["token_id"]), None)
                    self._log(f"🤖 SORTIE IA (surveillance) {p['question'][:34]} @{cur:.3f} — {verdict['summary'][:44]} (pnl {pnl:+.2f}$)")

    async def _scan_sport_inplay(self):
        # Univers plus large que le catalogue courant : on veut TOUS les
        # marchés sport, pas juste le top-volume (les matchs en direct ne sont
        # pas toujours les plus gros volumes).
        pool = await get_active_markets(limit=400, min_volume_24h=500)
        signals = await scan_sport_inplay(pool, min_move_pct=8.0)
        # WHITELIST FOOT/BASKET UNIQUEMENT (demande Steven : + de trades sur l'EDGE).
        # Le momentum in-play ne passe QUE sur les sports d'équipe à inertie (foot
        # +6.11$/80%, basket +0.99/100%). Tennis/cricket/obscur = bloqués.
        def _ok_inplay(q):
            ql = (q or "").lower()
            # COMBAT/UFC toujours bloqué (KO = gap instantané, ingérable).
            if (self._categorize(q) == "combat"
                    or any(k in ql for k in ["ufc", "mma", "boxing", "knockout", "fight night"])):
                return False
            # MÉTA-EDGE RESPECTÉ (fix 2026-07-19) : le sport in-play misait en taille fixe
            # SANS écouter le méta-edge -> le foot (prouvé perdant -5,51$/11 sur base propre)
            # continuait de saigner. On coupe toute catégorie que la data marque perdante
            # (mult <= 0.5). Ainsi le bot cesse TOUT SEUL de trader ce qui perd.
            if self._cat_perf.get(self._categorize(q), 1.0) <= 0.5:
                return False
            return any(k in ql for k in SPORT_INPLAY_WHITELIST)
        signals = [s for s in signals if _ok_inplay(s.get("question", ""))]
        if not signals:
            if self.scan_count % 6 == 0:
                self._log("[sport] 0 match foot/basket avec cote qui bouge assez dans la fourchette")
            return
        held = {p["question"] for p in db.open_live_directional_positions()}
        held_eids = self._held_event_ids(self._markets, held)
        held_sigs = {self._match_signature(q) for q in held} - {None}
        # question -> event_id (via le pool marché courant) pour tester les signaux
        _eid_by_q = {m["question"]: m.get("event_id") for m in self._markets}
        rej = {"cooldown": 0, "en_position": 0, "echec_recent": 0, "plafond": 0, "solde": 0}
        pris = 0
        for s in signals[:3]:
            if s["question"] in held:
                rej["en_position"] += 1
                continue
            # GARDE MÊME-ÉVÉNEMENT : bloque une 2e jambe du même match (Toronto+
            # Montréal via 2 marchés distincts que le test par titre ne voyait pas)
            _eid = _eid_by_q.get(s["question"])
            _sig = self._match_signature(s["question"])
            if (_eid and _eid in held_eids) or (_sig and _sig in held_sigs):
                rej["en_position"] += 1
                self._log_once(f"sport_evt_{s['question']}", f"[sport] {s['question'][:34]} — SKIP: même match/événement qu'une position ouverte")
                continue
            # Garde-fou anti-both-sides RENFORCÉ : cooldown mémoire OU position
            # déjà détenue on-chain (survit aux redémarrages — c'est ce qui
            # manquait quand Isabella+Reasco ont été pris sur le même match).
            if self._recently_traded(s["question"]):
                rej["cooldown"] += 1
                self._log_once(f"sport_cd_{s['question']}", f"[sport] {s['question'][:34]} ({s['side']} {s['move_pts']:+.0f}pts) — SKIP: match déjà tradé/détenu")
                continue
            last_fail = self._failed_recently.get(s["question"])
            if last_fail and time.time() - last_fail < FAILED_RETRY_COOLDOWN_S:
                rej["echec_recent"] += 1
                continue
            last_fail = self._failed_recently.get(s["question"])
            if last_fail and time.time() - last_fail < FAILED_RETRY_COOLDOWN_S:
                rej["echec_recent"] += 1
                continue
            token = s["token_yes"] if s["side"] == "YES" else s["token_no"]
            price = s["price_now"]
            # ZONE GAGNANTE (data on-chain) : on ne parie QUE dans 0.50-0.85. Les
            # outsiders (<0.50) ont fait 0% sur 13 paris (-15$), les gros favoris
            # (>0.85) 0% aussi. L'OKC à 20¢ qui a inquiété Steven = typiquement banni.
            if price < DIRECTIONAL_MIN_PRICE or price > DIRECTIONAL_MAX_PRICE or not self._price_floor_ok(s["question"], price):
                self._log_once(f"sport_zone_{s['question']}", f"[sport] {s['question'][:30]} @ {price:.2f} — SKIP: hors zone / plancher exploration (edge_mult<1 exige >={EXPLORE_MIN_PRICE})")
                continue
            # ENTRÉE CONFIRMÉE PAR LE SCORE (idée Kapitane) : le prix qui bouge
            # peut être du bruit qui CONTREDIT le jeu réel (tennis = 39% car ça
            # swingue à chaque point). On ne prend PAS un pari dont le joueur est
            # en train de PERDRE au tableau. 'trailing' = on passe ; leading/tied/
            # unknown (pas de score live) = on autorise (fallback prix seul).
            import asyncio as _aio2
            from core.livescores import inplay_leader_status
            try:
                entry_score = await _aio2.to_thread(inplay_leader_status, s["question"], s["side"] == "YES")
            except Exception:
                entry_score = "unknown"
            if entry_score == "trailing":
                self._log_once(f"sport_scoreconf_{s['question']}", f"[sport] {s['question'][:28]} — SKIP: notre camp MÈNE PAS au score (entrée non confirmée)")
                continue
            # ANTI-SPIKE (idée Steven) : ne pas acheter un PIC transitoire. Si le
            # prix actuel dépasse sa médiane 1h de > SPIKE_MAX_GAP, c'est un
            # croisement de courbes momentané sur un match en réalité serré
            # (~50¢) — un faux positif de momentum. On paie le sommet, ça retombe.
            median1h = await self._price_median_1h(token)
            if median1h is not None and (price - median1h) > SPIKE_MAX_GAP:
                self._log_once(f"sport_spike_{s['question']}", f"[sport] {s['question'][:26]} @ {price:.2f} — SKIP: pic transitoire (médiane 1h {median1h:.2f}), faux positif")
                continue
            # ④ CAP D'EXPOSITION PAR CATÉGORIE (le sport empilait du foot -> 72%)
            if not self._cat_cap_ok(s["question"], MIN_DIRECTIONAL_USD):
                self._log_once(f"sport_catcap_{s['question']}", f"[sport] {s['question'][:30]} — SKIP: cap catégorie atteint")
                continue
            conviction = self._conviction_ratio(s["move_pts"], max_edge=40.0)
            # Sport in-play = stratégie la plus risquée (9% de réussite, whipsaw
            # qu'on ne peut pas couper à temps : Isabella +3.22$ retournée en
            # perte). Choix Steven : MISE MINI (plancher, = 5 parts Polymarket) +
            # take-profit très tôt (+5%, cf. _dynamic_tp_sl) pour verrouiller le
            # gain AVANT le retournement. On ne parie plus gros là-dessus.
            usd = MIN_DIRECTIONAL_USD
            if self.live_enabled and self._live is not None:
                already = db.open_exposure_on_question(s["question"])
                room = MAX_DIRECTIONAL_USD - already
                if room < MIN_DIRECTIONAL_USD:
                    rej["plafond"] += 1
                    self._log_once(f"sport_cap_{s['question']}", f"[sport] {s['question'][:34]} — SKIP: déjà au plafond de mise ({already:.1f}$)")
                    continue
                usd = min(usd, room)
                available = self._available_cash()
                if available < MIN_DIRECTIONAL_USD:
                    self._log_once("sport_nobudget", f"[sport] solde libre épuisé ({available:.2f}$) — en pause jusqu'à ce que des positions se dénouent")
                    return  # plus de budget, inutile de continuer la boucle
                if not self._can_afford_min_order(price):
                    rej["solde"] += 1
                    self._log_once(f"sport_min_{s['question']}", f"[sport] {s['question'][:30]} — SKIP: solde ne couvre pas 5 parts à {price:.2f}$")
                    continue
                usd = min(usd, available * 0.9)
            size = usd / price if price > 0 else 0
            cost = size * price

            live_done = False
            if self.live_enabled and self._live is not None:
                res = self._live.execute_directional(token, price, size)
                if res.get("success"):
                    live_done = True
                    pris += 1
                    self._cash_cache[1] = None
                    self._sport_traded[s["question"]] = time.time()  # anti-whipsaw : 1h de cooldown sur ce match
                    self._log(
                        f"✅ LIVE SPORT {s['side']} {s['question'][:38]} — cote bouge {s['move_pts']:+.0f}pts, "
                        f"{cost:.2f}$ @ {price:.3f}"
                    )
                else:
                    self._failed_recently[s["question"]] = time.time()
                    self._log(f"[sport] SPORT live refusé: {str(res)[:90]}")

            self._record_trade(
                "sport", s["question"], s["side"], price, cost, size,
                f"cote in-play {s['move_pts']:+.0f}pts", live_done, token_id=token, conviction=conviction,
            )
            if not live_done and not self.live_enabled:
                # Cooldown posé MÊME en paper : sans ça, en mode paper le même
                # match se re-signalait à chaque scan (spam de 27 trades vu).
                self._sport_traded[s["question"]] = time.time()
                self._log(f"SPORT PAPER {s['side']} {s['question'][:38]} — {s['move_pts']:+.0f}pts @ {price:.3f}")

        if pris == 0 and any(rej.values()):
            détail = ", ".join(f"{k}:{v}" for k, v in rej.items() if v)
            self._log(f"[sport] {len(signals)} signal(aux) détecté(s), 0 pris — raisons: {détail}")

    async def _scan_momentum(self):
        signals = await scan_momentum(self._markets, min_move_pct=4.0)
        if not signals:
            return
        for s in signals[:3]:  # les plus gros mouvements seulement, pas tout le lot
            last_fail = self._failed_recently.get(s["question"])
            if last_fail and time.time() - last_fail < FAILED_RETRY_COOLDOWN_S:
                continue  # échec récent sur ce marché, on laisse le temps de se stabiliser
            token = s["token_yes"] if s["side"] == "YES" else s["token_no"]
            price = s["price_now"]
            conviction = self._conviction_ratio(s["move_pts"], max_edge=30.0)
            usd = self._confidence_size(conviction, price)
            if self.live_enabled and self._live is not None:
                # Anti-empilement : plusieurs signaux successifs sur le MÊME
                # marché partagent le plafond, ils ne l'additionnent pas.
                already = db.open_exposure_on_question(s["question"])
                room = MAX_DIRECTIONAL_USD - already
                if room < MIN_DIRECTIONAL_USD:
                    continue  # déjà au plafond sur ce marché
                usd = min(usd, room)
                available = self._available_cash()
                if available < MIN_DIRECTIONAL_USD:
                    continue  # solde trop bas, inutile de tenter (évite le spam d'erreurs)
                if not self._can_afford_min_order(price):
                    continue  # solde ne couvre pas 5 parts à ce prix
                usd = min(usd, available * 0.9)  # marge de sécurité (frais/slippage)
            size = usd / price if price > 0 else 0
            cost = size * price
            tp_pct, sl_pct = self._dynamic_tp_sl(conviction)

            live_done = False
            if self.live_enabled and self._live is not None:
                res = self._live.execute_directional(token, price, size)
                if res.get("success"):
                    live_done = True
                    self._cash_cache[1] = None  # force un refresh au prochain check, on vient de depenser
                    self._log(
                        f"LIVE MOMENTUM {s['side']} {s['question'][:40]} — mouvement {s['move_pts']:+.1f}pts, "
                        f"{cost:.2f}$ engagés @ {price:.3f} (TP +{tp_pct*100:.0f}%/SL -{sl_pct*100:.0f}%)"
                    )
                else:
                    self._failed_recently[s["question"]] = time.time()
                    self._log(f"MOMENTUM live refusé: {str(res)[:100]}")

            self._record_trade(
                "momentum", s["question"], s["side"], price, cost, size,
                f"mouvement {s['move_pts']:+.1f}pts/1h", live_done, token_id=token, conviction=conviction,
            )
            if not live_done:
                self._log(f"MOMENTUM PAPER {s['side']} {s['question'][:40]} — mouvement {s['move_pts']:+.1f}pts @ {price:.3f}")

    async def _scan_ai_news(self):
        # Mix guerre/politique PRIORITAIRE (8) + autres sujets (6) — Steven veut
        # de la guerre incluse mais sans ignorer le reste. max_days=4 : marchés
        # qui se résolvent sous 4 jours, pour que le capital tourne et que les
        # gains se réalisent vite (pas d'argent bloqué jusqu'en octobre).
        from core.polymarket import get_diverse_markets
        try:
            candidates = await get_diverse_markets(n_politics=12, n_general=10, max_days=5)
        except Exception:
            candidates = []

        # PRÉ-SÉLECTION (idée de Steven) : on filtre AVANT tout appel IA, pour
        # ne pas gaspiller les recherches DuckDuckGo + Nemotron (gratuits mais
        # limitables) sur des marchés qu'on rejetterait de toute façon. Ne
        # reste que le tradable : prix dans 0.20-0.80, pas déjà en position,
        # pas en cooldown d'échec. Constaté : sans ça, le bot appelait l'IA
        # sur des marchés à 0.01/1.00 (guerre quasi-résolue) pour rien.
        from core.polymarket import _PROP_EXCLUDE, POLITICS_KEYWORDS, TENNIS_KEYWORDS
        held_questions = {p["question"] for p in db.open_live_directional_positions()}
        prefiltered = []
        for m in candidates:
            ql0 = m["question"].lower()
            # IA : politique/géo COUPÉE (data on-chain : -11$ 'autre', carnage
            # géopolitique) + tennis coupé. L'IA garde le sport/match (Spain +3.55).
            if any(k in ql0 for k in POLITICS_KEYWORDS) or any(k in ql0 for k in TENNIS_KEYWORDS):
                continue
            if len(m["token_ids"]) != 2 or not m.get("prices"):
                continue
            # Demande Steven : l'IA PEUT trader un prop (O/U, both teams...) si
            # elle est convaincue, contrairement au copy (aveugle). On le
            # marque pour appliquer plus bas des garde-fous renforcés (mise
            # réduite + barre plus haute), car le stop-loss reste impossible.
            m["_is_prop"] = any(kw in m["question"].lower() for kw in _PROP_EXCLUDE)
            mp = m["prices"][0]
            if mp > 0.80 or mp < 0.20:
                continue  # extrême : mauvais risque/récompense, l'IA n'y est pas fiable
            if m["question"] in held_questions:
                continue  # déjà une position dessus
            # Blocage re-entrée sur thèse perdante (choix Steven) : ni si on tient
            # déjà une position en perte dessus, ni si on vient d'y perdre (stop).
            if self._holds_losing_position_on(m["question"]) or db.recently_lost_on(m["question"]):
                self._log_once(f"ia_relance_{m['question']}", f"[IA] {m['question'][:34]} — SKIP: thèse déjà perdante ici, pas de renfort")
                continue
            if self._recently_traded(m["question"]):
                continue  # cooldown partagé
            last_fail = self._failed_recently.get(m["question"])
            if last_fail and time.time() - last_fail < FAILED_RETRY_COOLDOWN_S:
                continue
            prefiltered.append(m)

        if not prefiltered:
            if self.scan_count % 20 == 0:
                self._log("IA en pause : aucun marché tradable (prix 0.20-0.80) parmi les candidats — normal quand les marchés guerre sont quasi-résolus")
            return

        for m in prefiltered[:3]:  # max 3/scan : économise le quota LLM gratuit
            market_price = m["prices"][0]
            est = estimate_probability(m["question"], market_price)
            if est is None:
                self._log_once(f"ia_nonews_{m['question']}", f"[IA] {m['question'][:34]} — SKIP: pas d'actu exploitable")
                continue
            if est["confidence"] == "low":
                self._log_once(f"ia_lowconf_{m['question']}", f"[IA] {m['question'][:34]} — SKIP: IA peu sûre (confiance faible)")
                continue
            is_prop = m.get("_is_prop", False)
            # Prop (stop-loss impossible) : barre renforcée — confiance HAUTE
            # obligatoire ET edge >= 25pts (au lieu de 15). On n'y va que sur
            # une conviction IA très forte, vu qu'on ne pourra pas couper.
            if is_prop and est["confidence"] != "high":
                self._log_once(f"ia_prop_conf_{m['question']}", f"[IA] {m['question'][:34]} — SKIP: prop, confiance IA pas assez haute (SL impossible)")
                continue
            edge_min = 25 if is_prop else MIN_AI_EDGE_PTS
            edge_pts = (est["probability"] - market_price) * 100
            if abs(edge_pts) < edge_min:
                self._log_once(f"ia_edge_{m['question']}", f"[IA] {m['question'][:34]} — SKIP: écart insuffisant ({edge_pts:+.0f}pts < {edge_min})")
                continue

            side = "YES" if edge_pts > 0 else "NO"
            token = m["token_ids"][0] if side == "YES" else m["token_ids"][1]
            # Le prix "outcomePrices" de Gamma est indicatif, pas forcément
            # marketable (même souci que momentum) — on va chercher le vrai
            # meilleur ask du carnet pour garantir un remplissage immédiat.
            async with aiohttp.ClientSession() as _sess:
                book = await get_book(_sess, token)
            ask = best_ask(book)
            if not ask:
                continue
            price = ask[0]
            # Même garde-fou risque/récompense que momentum : pas de mise sur
            # un prix déjà extrême, même avec un edge IA élevé (le potentiel
            # de gain reste minuscule par rapport à la mise).
            # Fourchette resserrée à 0.20-0.80 : aux extrêmes de prix (events
            # quasi-certains ou quasi-impossibles), l'estimation de proba de
            # l'IA est la moins fiable — parier NO à 0.063 sur Moscou (event à
            # 94% côté marché) a coûté -1.26$ en perte totale. On ne joue que
            # là où il y a une vraie incertitude ET un vrai potentiel de gain.
            # IA = pari de VALEUR (mispricing) : on lui garde sa marge basse pour
            # jouer un sous-coté qu'elle juge sur-vendu, MAIS on bannit la zone
            # poison < 0.35 (0% de réussite sur l'historique) et > 0.85 (pas d'upside).
            if price < 0.35 or price > DIRECTIONAL_MAX_PRICE:
                continue
            # Confiance "high" pèse plus dans la mise que "medium" — un edge
            # brut de 20pts avec confiance faible en sources n'a pas la même
            # valeur qu'avec confiance haute confirmée par plusieurs sources.
            confidence_mult = 1.0 if est["confidence"] == "high" else 0.6
            conviction = self._conviction_ratio(edge_pts) * confidence_mult
            usd = self._confidence_size(conviction, price)
            if is_prop:
                usd = min(usd, 2.0)  # prop = stop-loss impossible, on limite la casse
            if self.live_enabled and self._live is not None:
                already = db.open_exposure_on_question(m["question"])
                room = MAX_DIRECTIONAL_USD - already
                if room < MIN_DIRECTIONAL_USD:
                    continue
                usd = min(usd, room)
                available = self._available_cash()
                if available < MIN_DIRECTIONAL_USD:
                    continue
                usd = min(usd, available * 0.9)
            size = usd / price if price > 0 else 0
            cost = size * price
            tp_pct, sl_pct = self._dynamic_tp_sl(conviction)

            live_done = False
            if self.live_enabled and self._live is not None:
                res = self._live.execute_directional(token, price, size)
                if res.get("success"):
                    live_done = True
                    self._cash_cache[1] = None  # force un refresh au prochain check, on vient de depenser
                    self._log(
                        f"LIVE IA {side} {m['question'][:35]} — IA={est['probability']*100:.0f}% "
                        f"vs marché={market_price*100:.0f}% ({est['confidence']}) — {cost:.2f}$ engagés "
                        f"(TP +{tp_pct*100:.0f}%/SL -{sl_pct*100:.0f}%)"
                    )
                else:
                    self._failed_recently[m["question"]] = time.time()
                    self._log(f"IA live refusé: {str(res)[:100]}")

            reasoning = f"IA={est['probability']*100:.0f}% vs marché={market_price*100:.0f}% | {est['reasoning'][:150]}"
            self._record_trade("ai_news", m["question"], side, price, cost, size, reasoning, live_done,
                               token_id=token, conviction=conviction)
            if not live_done:
                self._log(f"IA PAPER {side} {m['question'][:40]} — {reasoning[:80]}")

    def _main(self):
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)

        async def runner():
            self._log("GHOST POLY démarré — scan des cotes réelles Polymarket (lecture publique)")
            while self._running:
                try:
                    await self._scan_once()
                except Exception as e:
                    self._log(f"erreur scan: {str(e)[:120]}")
                await asyncio.sleep(SCAN_INTERVAL_S)

        try:
            loop.run_until_complete(runner())
        finally:
            loop.close()

    # ── contrôle ──
    def start(self) -> dict:
        if self._running:
            return {"ok": False, "message": "déjà démarré"}
        self._running = True
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._main, daemon=True)
        self._thread.start()
        return {"ok": True}

    def stop(self) -> dict:
        if not self._running:
            return {"ok": False, "message": "déjà arrêté"}
        self._running = False
        return {"ok": True}

    # ── état pour l'UI ──
    def _forecast(self) -> dict:
        """Projection INDICATIVE des prochains jours si le rythme actuel se
        maintient. Basée sur le PnL réalisé rapporté au temps écoulé de
        session. Volontairement prudent : c'est une extrapolation linéaire,
        PAS une promesse — le trading est de la variance."""
        perf = db.performance_stats()
        since = db.get_stats_since()
        n = perf["trades_closed"]
        if n < 1:
            return {"ready": False, "reason": "en attente du 1er trade clôturé — la prévision s'affichera dès qu'il y en a un, puis s'affinera"}

        # Temps écoulé = depuis le reset des stats jusqu'à maintenant. Marche
        # dès 1 seul trade (grossier), s'affine mécaniquement avec le temps.
        elapsed_days = max((time.time() - since) / 86400.0, 0.04)  # min ~1h
        pnl_per_day = perf["realized_pnl"] / elapsed_days
        trades_per_day = n / elapsed_days
        # fiabilité : faible sous 5 trades, correcte à partir de ~20
        if n < 5:
            conf = "faible"
        elif n < 20:
            conf = "moyenne"
        else:
            conf = "bonne"
        return {
            "ready": True,
            "elapsed_hours": round(elapsed_days * 24, 1),
            "n_trades": n,
            "confidence": conf,
            "pnl_per_day": round(pnl_per_day, 2),
            "trades_per_day": round(trades_per_day, 1),
            "proj_1d": round(pnl_per_day, 2),
            "proj_3d": round(pnl_per_day * 3, 2),
            "proj_7d": round(pnl_per_day * 7, 2),
            "win_rate": perf["win_rate"],
            "note": f"Basé sur {n} trade(s) clôturé(s) en {round(elapsed_days*24,1)}h — fiabilité {conf}. "
                    f"Extrapolation linéaire, s'affine avec le temps. Indicatif, pas une garantie.",
        }

    def _sync_onchain(self):
        """Enrichit la DB depuis la VÉRITÉ on-chain (activité réelle du compte),
        demande Steven : chaque trade capturé — bot ET manuel (Steven lui-même) —
        taggé par source. Fini la DB qui ne voit que ce que le bot loggue avec
        ses trous. Re-syncable sans doublon (clé tx_hash+asset+side)."""
        if self._live is None:
            return
        try:
            import requests
            funder = self._live.funder
            bot_tokens = db.onchain_bot_tokens(within_s=604800)  # [(token_id, ts)] sur 7j
            # FIX MESURE (2026-07-17) : on tague par TOKEN, pas par fenêtre de 120s.
            # Une VENTE/REDEEM d'une position bot arrive des HEURES après l'achat
            # (résolution/TP) -> l'ancien test <120s la ratait et la taguait "manual",
            # donc onchain_realized_pnl JETAIT nos recettes (bot_pnl faussé). Tout
            # event sur un token que le bot a ACHETÉ = 'bot' (c'est la PnL de CETTE
            # position, même si Steven l'a fermée à la main).
            bot_assets = {str(tok) for tok, _bts in bot_tokens if tok}
            acts = []
            off = 0
            while off < 1500:
                r = requests.get("https://data-api.polymarket.com/activity",
                                 params={"user": funder, "limit": 500, "offset": off},
                                 timeout=15, headers={"User-Agent": "Mozilla/5.0"})
                d = r.json()
                if not d:
                    break
                acts += d
                if len(d) < 500:
                    break
                off += 500
            # conditionIds des marchés que le bot a tradés (via asset match) : sert à
            # taguer les REDEEM, qui ont un asset VIDE mais le conditionId du marché.
            bot_conditions = {str(a.get("conditionId", "")) for a in acts
                              if str(a.get("asset", "")) in bot_assets and a.get("conditionId")}
            for a in acts:
                asset = str(a.get("asset", ""))
                cond = str(a.get("conditionId", ""))
                ts = a.get("timestamp", 0) or 0
                # source : 'bot' si le token OU le conditionId du marché appartient à
                # une position que le bot a achetée (le conditionId rattrape les REDEEM
                # à asset vide) ; sinon 'manual' (ex. l'ETH-NO @0.12 de Steven).
                source = "bot" if (asset in bot_assets or (cond and cond in bot_conditions)) else "manual"
                db.upsert_onchain_trade(
                    a.get("transactionHash", ""), asset, a.get("side") or a.get("type", ""),
                    ts, a.get("type", ""), a.get("title", ""), a.get("outcome", ""),
                    float(a.get("price", 0) or 0), float(a.get("usdcSize", 0) or 0),
                    float(a.get("size", 0) or 0), a.get("conditionId", ""), source,
                )
        except Exception:
            pass

    @staticmethod
    def _parse_dt(s):
        """Parse un timestamp gamma ('2026-07-20 07:55:00+00' ou ISO) -> datetime aware."""
        from datetime import datetime
        if not s:
            return None
        try:
            x = str(s).replace("Z", "+00:00")
            if " " in x and "T" not in x:
                x = x.replace(" ", "T", 1)
            if x.endswith("+00"):
                x += ":00"
            return datetime.fromisoformat(x)
        except Exception:
            return None

    def _match_state(self, mkt: dict) -> str:
        """ÉTAT DU MATCH d'une position (données structurées Steven) : à venir / EN COURS
        (avec temps avant la fin) / se termine / terminé. '' si pas de données."""
        from datetime import datetime, timezone
        gs = self._parse_dt(mkt.get("game_start"))
        ed = self._parse_dt(mkt.get("end_date"))
        now = datetime.now(timezone.utc)
        if gs and now < gs:
            mins = int((gs - now).total_seconds() / 60)
            return f"à venir ({mins}min)" if mins < 240 else "à venir"
        if ed and now < ed:
            mins = int((ed - now).total_seconds() / 60)
            if gs and now >= gs:
                return f"🔴 EN COURS (finit ~{mins}min)" if mins < 300 else "🔴 EN COURS"
            return f"finit ~{mins}min" if mins < 300 else ""
        if ed and now >= ed:
            return "✅ terminé (résolution)"
        return ""

    def _export_positions(self, positions: list, cash: float, total: float):
        """DEMANDE STEVEN : rendre les positions LISIBLES partout — un résumé dans
        les logs + un fichier-journal (data/positions.json + positions.txt) avec,
        pour chaque position : stratégie, camp, parts achetées, prix d'entrée, mise,
        valeur actuelle, PnL. Écrit à chaque refresh compte."""
        from pathlib import Path
        base = Path(__file__).parent.parent / "data"
        try:
            import json as _json
            base.mkdir(parents=True, exist_ok=True)
            (base / "positions.json").write_text(_json.dumps(
                {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "cash": cash, "total": total,
                 "n_positions": len(positions), "positions": positions}, indent=2, ensure_ascii=False))
            lines = [f"POSITIONS — {time.strftime('%Y-%m-%d %H:%M:%S')} | {len(positions)} ouverte(s) | cash {cash}$ | total {total}$", ""]
            lines.append(f"{'STRAT':10s} {'CAMP':13s} {'PARTS':>6s} {'MISE$':>6s} {'VALEUR$':>8s} {'PNL$':>7s}  {'MATCH':20s} MARCHÉ")
            for p in positions:
                lines.append(f"{p.get('strategy','?')[:10]:10s} {str(p.get('outcome',''))[:13]:13s} "
                             f"{p.get('shares',0):>6} {p.get('cost',0):>6} "
                             f"{p.get('value',0):>8} {p.get('pnl',0):>+7}  {str(p.get('match',''))[:20]:20s} {str(p.get('title',''))[:34]}")
            (base / "positions.txt").write_text("\n".join(lines))
        except Exception:
            pass
        # résumé dans les logs (throttlé : 1 fois / 4 refresh pour ne pas spammer)
        self._pos_log_count = getattr(self, "_pos_log_count", 0) + 1
        if positions and self._pos_log_count % 4 == 1:
            top = "  ·  ".join(f"{p.get('strategy','?')[:6]} {str(p.get('outcome',''))[:10]} "
                              f"{p.get('shares',0)}pts @{p.get('avg_price',0)} → {p.get('value',0)}$ ({p.get('pnl',0):+.2f})"
                              for p in positions[:6])
            self._log(f"📊 {len(positions)} positions | {top}")

    def _refresh_account(self):
        """Récupère le VRAI compte Polymarket (solde libre + positions avec
        PnL) via leur data-API. Appelé périodiquement dans la boucle, pas à
        chaque poll UI (réseau). Cache dans self._account_snapshot."""
        if self._live is None:
            return
        try:
            import requests
            funder = self._live.funder
            cash = self._available_cash()
            r = requests.get(f"https://data-api.polymarket.com/positions?user={funder}",
                             timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            # enrichissement : token -> (stratégie, mise investie, parts) via nos trades
            bot_by_token = {}
            for t in db.open_live_directional_positions():
                if t.get("token_id"):
                    bot_by_token[str(t["token_id"])] = {
                        "strategy": t.get("strategy", "?"),
                        "cost": round(t.get("cost_usd", 0) or 0, 2),
                        "shares": round(t.get("size_shares", 0) or 0, 1),
                    }
            # ÉTAT DU MATCH par position (données structurées Steven) : map token -> marché
            mkt_by_token = {}
            for mk in self._markets:
                for tid in mk.get("token_ids", []):
                    mkt_by_token[str(tid)] = mk
            self._mkt_by_token_cache = mkt_by_token  # réutilisé par la sortie pré-résolution
            positions = []
            pos_val = 0.0
            pnl = 0.0
            held_assets = []  # token_ids réellement détenus -> réconciliation DB
            for p in r.json():
                v = p.get("currentValue", 0) or 0
                if v < 0.02:
                    continue
                asset = str(p.get("asset", ""))
                if asset:
                    held_assets.append(asset)
                info = bot_by_token.get(asset, {})
                positions.append({
                    "title": p.get("title", "?"),
                    "outcome": p.get("outcome", ""),
                    "value": round(v, 2),
                    "pnl": round(p.get("cashPnl", 0) or 0, 2),
                    "pnl_pct": round(p.get("percentPnl", 0) or 0, 1),
                    "avg_price": round(p.get("avgPrice", 0) or 0, 3),
                    "cur_price": round(p.get("curPrice", 0) or 0, 3),
                    # DEMANDE STEVEN : combien acheté, à quel prix, valeur actuelle, par qui
                    "strategy": info.get("strategy", "manuel"),
                    "cost": info.get("cost", 0.0),          # mise investie ($)
                    "shares": info.get("shares", round(p.get("size", 0) or 0, 1)),  # nb de parts
                    # ÉTAT DU MATCH EN DIRECT (données structurées) : à venir / en cours / finit / fini
                    "match": self._match_state(mkt_by_token.get(asset, {})),
                })
                pos_val += v
                pnl += p.get("cashPnl", 0) or 0
            # IDÉE b (Steven) : les positions hors top-volume (foot spécifique) n'ont pas
            # de match state (marché absent du pool). On récupère leur timing individuellement.
            missing = [(str(p.get("asset", "")), i) for i, p in enumerate(positions)
                       if not positions[i].get("match")]
            miss_assets = [a for a, _ in missing if a and a not in mkt_by_token]
            if miss_assets:
                try:
                    rr = requests.get("https://gamma-api.polymarket.com/markets",
                                      params={"clob_token_ids": ",".join(miss_assets[:20])},
                                      timeout=8, headers={"User-Agent": "Mozilla/5.0"})
                    import json as _mj
                    fetched = {}
                    for mk in (rr.json() or []):
                        toks = mk.get("clobTokenIds")
                        toks = _mj.loads(toks) if isinstance(toks, str) else (toks or [])
                        info = {"game_start": mk.get("gameStartTime") or mk.get("eventStartTime"),
                                "end_date": mk.get("endDate")}
                        for t in toks:
                            fetched[str(t)] = info
                    for a, i in missing:
                        if a in fetched:
                            positions[i]["match"] = self._match_state(fetched[a])
                except Exception:
                    pass
            positions.sort(key=lambda x: -x["value"])
            self._export_positions(positions, round(cash, 2), round(cash + pos_val, 2))
            self._account_snapshot = {
                "cash": round(cash, 2),
                "positions": positions,
                "total": round(cash + pos_val, 2),
                "pnl": round(pnl, 2),
                "ts": time.time(),
            }
            # RÉCONCILIATION (fix orpheline définitif) : toute position marquée
            # closed en DB mais ENCORE détenue on-chain est ré-ouverte -> le bot
            # la re-gère (stops). La DB se recale sur la vérité à chaque scan.
            reop = db.reopen_held_but_closed(held_assets)
            if reop:
                self._log(f"🔁 réconciliation : {reop} position(s) ré-ouverte(s) (détenue(s) mais marquée(s) closed à tort)")
            # Historique du portefeuille (courbe du solde, style appli bancaire) —
            # throttlé à 1 point / 5 min côté db.record_portfolio_snapshot.
            db.record_portfolio_snapshot(cash + pos_val, cash, pos_val, len(positions))
            # RATCHET : détecte les nouveaux sommets, verrouille les gains (mises /2)
            self._update_ratchet(cash + pos_val)
        except Exception:
            pass

    def get_state(self) -> dict:
        stats = db.get_stats()
        with self._log_lock:
            logs = list(self._log_lines[-200:])
        # activité récente = tous les trades directionnels (sport/IA/snipe),
        # pas seulement les arbs paper — c'est là que se passe la vraie action.
        return {
            "running": self._running,
            "uptime_s": int(time.time() - self._start_time) if (self._running and self._start_time) else 0,
            "scan_count": self.scan_count,
            "stats": stats,
            "perf": db.performance_stats(),
            "forecast": self._forecast(),
            "account": self._account_snapshot,
            "portfolio_history": db.portfolio_history(300),
            "portfolio_summary": db.portfolio_summary(),
            "onchain_pnl": db.onchain_realized_pnl(),
            "onchain_recent": db.recent_onchain_trades(30),
            "directional_trades": db.recent_directional_trades(40),
            "opportunities": db.recent_opportunities(15),
            "logs": logs,
            "live_enabled": self.live_enabled,
            "live_configured": self._live is not None,
            "live_error": self._live_error,
        }

    # ── contrôle LIVE ──

    def live_status(self) -> dict:
        if self._live is None:
            return {"ok": False, "message": self._live_error or "PRIVATE_KEY absente du .env"}
        try:
            st = self._live.status()
            return {"ok": True, **st}
        except Exception as e:
            return {"ok": False, "message": str(e)[:200]}

    def setup_allowances(self) -> dict:
        if self._live is None:
            return {"ok": False, "message": "clé non chargée"}
        try:
            txs = self._live.setup_allowances()
            return {"ok": True, "txs": txs, "message": f"{len(txs)} approbations envoyées" if txs else "déjà tout approuvé"}
        except Exception as e:
            return {"ok": False, "message": str(e)[:200]}

    def enable_live(self) -> dict:
        if self._live is None:
            return {"ok": False, "message": "PRIVATE_KEY absente ou invalide"}
        st = self._live.status()
        if not st["ready"]:
            cash = st.get("cash_usdc")
            missing = f"solde de trading Polymarket insuffisant ({cash if cash is not None else '?'}$) — dépose via le bouton 'Déposer' du site"
            return {"ok": False, "message": missing}
        self.live_enabled = True
        self._log(f"MODE LIVE ACTIVÉ — proxy {st['funder_proxy'][:10]}..., {st['cash_usdc']}$ dispo, plafond {MAX_LIVE_USD_PER_ARB}$/arb")
        return {"ok": True, **st}

    def disable_live(self) -> dict:
        self.live_enabled = False
        self._log("mode live désactivé — retour paper")
        return {"ok": True}
