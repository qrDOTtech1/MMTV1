"""GHOST V3 — moteur MULTI-MARCHE (BTC/ETH/..., mode reel OU paper par marche).

Demande Steven (21/07) :
 - selecteur reel/paper PAR marche (BTC reel, ETH paper le temps de valider si
   chaque marche a besoin d'une strategie differente ; Solana etc. ajoutables),
 - resolution WIN/LOSS via la VRAIE settlement Polymarket (data-api), plus jamais
   via Binance (prouve non fiable : un trade Binance='Down' paye 'Up'),
 - garde-fous : plancher solde 5$, stop a 5 pertes CONSECUTIVES (remis a 0 des
   qu'un win), limite de nombre de trades RETIREE,
 - sizing dynamique : miser PLUS sur les entrees pas cheres a forte conviction
   (comme le trade a 0,31 -> +5,74$), thin sur les entrees deja a 0,97.

Le trading REEL ne demarre que sur appel explicite start() (clic bouton).
"""

import json
import math
import os
import shutil
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from core.btc_updown import find_active_markets, parse_updown_market, synced_now
from ghost_poly.live import PolyLive, MIN_ORDER_SIZE_SHARES
from real_web import market_maker as mm
from real_web.ws_feed import get_feed


ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")
LOG_FILE = ROOT / "data" / "ghost_v3_real.log"
STATE_FILE = ROOT / "data" / "multi_state.json"

# ── garde-fous & sizing (valeurs validees par Steven) ──
FLOOR_USD = 20.0  # ne jamais engager de capital sous ce plancher (protege le capital,
# ne risque que les profits au-dessus de 20$)
STOP_CONSEC_LOSSES = 2  # stop apres 2 pertes consecutives (resserre le temps de valider
# l'entree plus tot, plus agressive ; reset des un win)

# ── KILL-SWITCH GLOBAL (Steven 04/08, "set and forget" -> "kill switch reglable")
# : contrairement a STOP_CONSEC_LOSSES qui n'arrete qu'UN symbole, ceci coupe
# TOUS les symboles reels d'un coup si un seuil global est franchi. Sans ca,
# "oublier" le bot signifie zero garde-fou si le comportement derive hors de
# vue -- exactement le risque signale avant ce fix (session a -34$ ce soir).
# Reglable a chaud via GET/POST /api/killswitch, pas besoin de redeployer.
KILLSWITCH_DEFAULTS = {
    "enabled": True,
    "cash_floor_usd": 3.0,       # sous ce cash reel -> stop (capital trop bas pour ouvrir 1 paire)
    "max_session_loss_usd": 15.0,  # perte de cash depuis le demarrage du process -> stop
    "max_global_consec_losses": 5,  # pertes reelles d'affilee, tous symboles confondus -> stop
}
MAX_ENTRY_PRICE = 0.97  # au-dessus : trop peu d'upside
MAX_ENTRY_PRICE_OPPORTUNITY = (
    0.99  # plafond leve quand "Opportunité" est ON pour ce marche
)
# (Steven 22/07) : achete meme tres cher si le marche est
# convaincu (jusqu'a 99cts), pas seulement le plancher bas.
ENTRY_WINDOW_SECS = (
    90  # entrer PLUS TOT (jusqu'a 90s de la fin) : la marge exigee croit
)
# avec le temps restant -> on n'entre tot QUE si conviction Binance
# forte (ex: a 90s il faut ~60 pts d'ecart). Capte les entrees pas
# cheres a fort edge (le trade a 0,28 -> +5,74$).
POLL_SECS = 1
SETTLE_DELAY = 45  # attendre la settlement Polymarket apres fin de fenetre

# ── MODE VALIDATION REEL (Steven 22/07) : premiers vrais achats, tout petits,
# pour VALIDER l'execution (fills, prix, rollback) avant de rebrancher le sizing.
# True -> chaque trade REEL est force au MINIMUM (5 parts, le plancher Polymarket)
# quel que soit le sizing calcule. Sur une jambe pas chere ~0.20-0.45 ca fait
# ~1-2$ ; sur un near-certain ~0.95 ca fait ~4.75$ (5 parts, plancher incompressible).
# Repasser a False quand valide -> restaure le sizing Kelly/arb normal.
REAL_VALIDATION_MODE = True
REAL_VALIDATION_SHARES = 5.0  # = MIN_ORDER_SIZE_SHARES (plancher Polymarket)
# ── CAP $ STRICT (Steven 23/07, "je veux QUE des trades a max 1$") : pour les
# ordres MARKET (arb crypto + ULTRAPOLY, pas de plancher de parts contrairement
# au GTC limite), la notional par jambe est plafonnee a ce montant, quel que
# soit le prix. Remplace le sizing "5 parts" (qui pouvait couter 4.75$ sur un
# favori a 0.95) le temps de valider en conditions reelles a exposition minimale.
REAL_VALIDATION_LEG_USD = 1.6

# ── PLANCHER NEAR-CERTAIN (Steven 22/07) : une position SEULE (directionnel, non
# hedgee) ne doit JAMAIS etre prise sous ce prix -> uniquement de VRAIS favoris
# quasi-certains. Le cheap ne se prend qu'en PAIRE (arb both-side, les 2 jambes).
# Declencheur : SOL achete seul a 0.49 (pile-ou-face nu) que Steven a du couper.
NEAR_CERTAIN_MIN_PRICE = 0.94
# ── SEUIL DANGER POUR L'UNDERDOG (Steven 23/07, "verif signal Binance pas de
# retournement possible") : danger_score (0-100, deja calcule ailleurs = flips
# recents + velocite pres du strike) doit depasser ce seuil pour qu'on achete
# l'assurance underdog. En dessous = marche stable, aucun retournement
# plausible signale -> pas d'underdog, favori seul protege par le stop-loss.
NEAR_CERTAIN_DANGER_MIN_FOR_HEDGE = 8
# SKIP TOTAL (Steven 29/07, "les 3% du temps il faut forcer underdog... si le
# marche se reverse bcp ou proche du target alors on ne fait pas de near
# certain") : au-dela de ce seuil, meme l'assurance underdog ne suffit pas a
# rendre le trade interessant -> on ne prend PAS le favori du tout ce cycle.
NEAR_CERTAIN_SKIP_DANGER_MIN = 25
# ── MISE UNDERDOG (Steven 23/07, "50c ou meme 1$ -> plusieurs dizaines de $ sur
# un flip") : mise fixe visee (en $), convertie en parts, plancher 5 parts (CLOB).
# Plus de plafond "cout <= gain favori" -> l'underdog paie enorme sur un vrai
# flip, largement de quoi couvrir la perte du favori.
HEDGE_DOG_STAKE_USD = 0.50
# COUVERTURE PROPORTIONNELLE (Steven 29/07, "pour chaque loss on doit avoir
# un bothside qui gagne enormement + que la perte") : au lieu d'une mise
# underdog FIXE (0.50$, arbitraire, pouvait etre trop petite pour couvrir un
# gros favori, ou l'assurance meme absente si danger sous-estime le risque -
# vu en reel : danger=2 mais flip quand meme, perte totale non couverte),
# l'underdog est dimensionne pour payer au moins DOG_COVERAGE_MULT x la mise
# du favori SI le favori perd -> chaque perte potentielle est structurellement
# compensee par un gain plus gros, pas juste "on espere que le danger a raison".
DOG_COVERAGE_MULT = 1.5
# FILTRE PROBABILITE MINIMALE (Steven 29/07, "on peut viser mieux que 85% de
# win rate") : n'entre en favori QUE si le modele brownien calcule une
# probabilite reelle >= ce seuil. Contrairement au danger_score (heuristique),
# c'est un chiffre calibre sur la volatilite MESUREE -> un vrai filtre de
# qualite, pas un slogan. Ne bloque QUE quand le calcul est disponible
# (fallback sur l'ancien comportement si pas encore mesure, jamais de blocage
# total du bot pour ca).
MIN_CALIBRATED_PROB = 0.9  # Steven 29/07 : suggestion SUPRAM Kapitane (grounded, valeur reelle verifiee) -> 0.85->0.9, resserre le filtre near-certain suite a la performance SOL
NEAR_CERTAIN_ENABLED = (
    False  # Steven 22/07 : le near-certain (favori 96-99c) a fait perdre
)
# -9.71$ REELS (ETH+SOL Up flippes en meme temps). R/R pourri
# (risque 96c pour 4c -> il faut >96% WR). COUPE sur les marches
# d'arb -> ils ne font QUE de l'arb GARANTI. BTC garde son
# directionnel (marche a part). True pour reactiver.

# ── PARI UNDERDOG (Steven 22/07, meilleure idee que le hedge $1) : quand un cote
# est un near-certain (>= 0.94), au lieu d'acheter le favori nu (mauvais R/R) ou
# $1 sur chaque cote (gaspille $1 sur le favori), on met juste UNDERDOG_BET_USD
# sur le cote CHEAP (celui qui va probablement perdre). Perte MAX = ce petit
# montant (qq centimes) si le favori gagne ; GROS gain si ca flippe au strike
# ($0.20 a 0.04 = 5 parts = 5$). Capped-downside, flip-upside.
UNDERDOG_BET_USD = 0.20
# ── SIZING FAVORI DYNAMIQUE (Steven 22/07 : "mettre + de 1$ pour recuperer les
# 20cts + viser ~0.30 de gain sur favori"). Constat : a $1 fixe sur un favori
# 0.96, gagner ne rapportait que +0.04 - 0.20 (dog) = NET NEGATIF. On calcule
# donc la mise pour un NET cible quand le favori gagne :
#   F = (CIBLE + UNDERDOG_BET) * prix/(1-prix)   (ex: 0.96 -> ~12$)
# Revers assume (these Steven, le paper juge) : si flip, on perd F - payout_dog
# (~-7$ a 0.96). Rentable ssi flips < ~4-6% des fenetres.
FAV_TARGET_NET_USD = 0.30  # gain net vise quand le favori gagne (dog deja deduit)
# RETRY FORCE-PAIR (Steven 05/08) : nombre d'essais pour completer la 2e jambe
# avant d'abandonner et revendre la 1ere -- toujours au meme plafond de prix
# (max_payable), jamais surpaye pour forcer la paire.
FORCE_PAIR_MAX_RETRIES = 3
FORCE_PAIR_RETRY_SLEEP_S = 0.4
FAVORITE_BET_MAX_USD = 12.0  # plafond dur de mise favori (post-validation)
FAV_MAX_PRICE = 0.97  # au-dela, la mise requise explose pour 0.30$ -> skip hedge
# ── VALIDATION HEDGE (Steven 23/07, "uniquement des mises en $, max 1$/pos en
# attendant de valider") : les 2 jambes (favori + underdog) passent par
# post_market_order (ordre $, AUCUN plancher de parts contrairement au GTC
# limite -> confirme par l'historique reel, achats de 0.02 part deja vus).
# Plafond dur 1$/jambe le temps de prouver le mecanisme en conditions reelles ;
# repasser a FAVORITE_BET_MAX_USD une fois valide.
HEDGE_VALIDATION_MODE = True
HEDGE_FAV_MAX_USD_VALIDATION = 1.6
HEDGE_DOG_MAX_USD_VALIDATION = 1.6
# ── FAVORITE BUDGET MULT (Steven 28/07, releve 1.6->2.5 le 04/08) : la jambe
# favorite recoit plus de budget que l'underdog. FACTOR = 2.5 -> favorite
# $2.50 vs underdog $1.00, meme ratio que PRICE_TIER_BUDGET_MULT["above_070"].
FAVORITE_BUDGET_MULT = 2.5

# ── ARB-ONLY (Steven 22/07 : "quasi 100% WR au global") : True = on ne prend QUE
# les arbs GARANTIS (crypto both-side + ULTRAPOLY). Le hedge favori+dog est COUPE :
# c'est le seul composant qui casse le 100% (1 flip a -6$ efface ~20 wins a +0.30,
# et on a vu 2 flips en 1 journee -> equation fragile). False = reactive le hedge.
# REACTIVE (Steven 23/07) : hedge repense en mises $ plafonnees 1$/jambe
# (HEDGE_VALIDATION_MODE) -> le flip qui cassait l'equation coute maintenant
# ~1$ max au lieu de ~6-12$, le temps de valider en conditions reelles.
ARB_ONLY = False

# ── ULTRAPOLY (Steven 22/07) : chasse a l'ARB sur TOUT Polymarket, pas juste
# les 5 up/down crypto. Scanner de fond : top marches binaires par volume, lit
# les VRAIS asks des 2 cotes, ouvre une paire parts-egales si comb_ask <= seuil.
# PAPER-ONLY pour l'instant (validation du volume d'opportunites avant le reel).
# Resolution via gamma (outcomePrices extremes), pas de date fixe -> le capital
# peut rester engage longtemps sur les marches long-terme (d'ou le cap).
ULTRAPOLY_SCAN_INTERVAL_S = (
    15  # decouverte de nouveaux marchés (la detection ARB est desormais via WS stream)
)
ULTRAPOLY_TOP_N = 40  # nb de marches (tries par volume) inspectes par cycle
ULTRAPOLY_MIN_VOL24 = 2000.0  # $ de volume 24h minimum (liquidite)
ULTRAPOLY_COMB_MAX = (
    0.95  # arb si ask_yes + ask_no <= 0.95 (aligne sur BOTH_SIDE_COMBINED_MAX)
)
ULTRAPOLY_SHARES = 5.0  # parts par jambe (petit, volume > taille)
ULTRAPOLY_MAX_OPEN_PAIRS = 10  # cap de paires ouvertes (capital bloque jusqu'a reso)
ULTRAPOLY_COOLDOWN_S = 900  # pas 2x la meme paire en < 15 min
# ── ULTRAPOLY REEL (Steven 23/07) : mode reel plus strict que le paper, cap de
# paires plus serre le temps de valider sur un univers de marches bien plus
# large/varie que les 5 crypto (liquidite/qualite tres inegales selon les sujets).
ULTRAPOLY_REAL_MAX_OPEN_PAIRS = 3
ULTRAPOLY_REAL_MAX_COMBINED = 0.95  # plus strict que le paper (0.97), meme logique
# que REAL_MAX_COMBINED pour l'arb crypto.
ULTRAPOLY_REAL_MIN_DEPTH_RATIO = 0.6

# ── DELTA-NEUTRE BOTH-SIDE AU BID (Steven 23/07, "on est reellement armes") :
# le WS a revele que sur les 5 crypto, Up_ask+Down_ask ~1.01 (pas d'arb au market)
# MAIS Up_bid+Down_bid ~0.99 -> si on POSTE un bid des DEUX cotes et qu'ils sont
# remplis, on entre a combined < 1 = ARB GARANTI. On ne paie plus le spread, on
# le CAPTURE. Aucune position nue par conception : si une seule jambe fill, la
# 2e reste postee (on veut l'autre cote), et l'orphan manager gere le residuel.
DN_ENABLED_DEFAULT = False
DN_COMBINED_TARGET = 0.985  # on ne poste la paire que si Up_bid+Down_bid <= ca
# (marge >= 1.5% une fois les 2 remplis). Le WS montre
# ~0.99 souvent -> on vise juste sous 1.
DN_BID_OFFSET = 0.00  # on poste PILE au best bid (rejoint la file). >0 = plus
# agressif (bid plus haut, remplit plus vite mais marge -).
DN_SHARES = 5.0  # parts par jambe (plancher CLOB)
DN_MAX_OPEN_PAIRS = 2  # cap de paires en cours (capital engage jusqu'a reso)
DN_MIN_SECS_LEFT = 60  # ne poste plus de nouvelle paire sous 60s (pas le temps
# de faire remplir les 2 jambes proprement)
DN_REQUOTE_DELTA = 0.01  # ne recote une jambe que si le best bid a bouge d'au moins ca
# ── FIX 23/07 (Steven, audit run reel) : avec un petit solde, tenir des bids
# simultanement sur 3 symboles (6 ordres = 6 reservations de collateral) fait
# rejeter presque tous les 2e-jambes ("balance insuffisante" en boucle) ->
# jambes seules jamais completees, coupees a petite perte par l'orphan. On
# CONCENTRE le capital sur UN SEUL symbole a la fois -> chaque paire a une
# vraie chance de se completer avant d'en tenter une autre.
DN_MAX_ACTIVE_SYMBOLS = 1
DN_MIN_FREE_CASH = 4.0  # sous ce seuil, n'ouvre meme pas une nouvelle paire
# (evite de poster une 1ere jambe qui restera forcement
# seule faute de fonds pour la 2e).

# sizing dynamique
# RETOUR AUX VALEURS D'ORIGINE (demande Steven 21/07) : mon boost (18$/55%/2$)
# est retire pour ne pas fausser les stats. On garde la base prouvee (10W/0L)...
HARD_CAP_USD = 16.0  # cout max d'un trade (releve 10->16, Steven 22/07 : plein gaz)
MAX_FRACTION = 0.40  # ... et au plus 40% du capital investissable (releve 0.30->0.40)
MIN_BUDGET_USD = 1.0
PAPER_START_BAL = 20.0  # solde papier de depart (par marche paper)

# ── sizing KELLY FRACTIONNE 1/4 (Steven 22/07, remplace score+STAKE_MULTIPLIER x2) ──
# Le prix ask paye = probabilite implicite du marche (b = (1-ask)/ask = cote).
# On estime notre proba de gain reelle q = ask + KELLY_ASSUMED_EDGE * conviction du
# signal, puis f* = (b*q-(1-q))/b, et on ne mise que KELLY_FRACTION de f*. Le Kelly
# plein serait ruineux si l'edge est surestime (WR encore incertain sur petit
# echantillon) -> 1/4 Kelly absorbe l'erreur tout en gardant la logique "mise plus
# sur les entrees pas cheres a forte conviction" (b grandit quand ask baisse).
KELLY_FRACTION = 0.25
KELLY_ASSUMED_EDGE = 0.06  # edge de proba suppose au max de conviction, a recalibrer
# avec plus de donnees reelles sur le WR par marche.

# ── MULTIPLICATEUR EMPIRIQUE PAR PALIER DE PRIX (Steven 04/08) ──
# Analyse on-chain de 221 jambes reelles (301$ engages) : ROI par tranche de
# prix d'achat allait dans le sens INVERSE de ce que produit le Kelly ci-dessus
# (edge fixe suppose) -- <0.30 : -24.4% ROI (19% win rate) ; 0.30-0.50 :
# -18.2% (34%) ; 0.50-0.70 : -3.8% (54%) ; >0.70 : +9.1% ROI (77% win rate,
# SEULE tranche rentable, la moins financee). Resserre l'allocation vers ce
# qui a reellement marche -- voir _budget_usd(). A recalibrer avec plus de
# donnees ; ce n'est pas un vrai recalibrage du modele de proba, juste un
# correctif empirique en attendant.
PRICE_TIER_BUDGET_MULT = {
    "below_030": 0.15,
    "p030_050": 0.35,
    "p050_070": 1.0,
    # >0.70 releve a 2.5x le 04/08 (Steven, "une vraie diff de prix entre la
    # pos chere qui rapporte bcp et la pos pas chere qui rapporte peu") --
    # etait 1.6x. Toujours borne par HARD_CAP_USD/investable en aval, donc
    # sans risque de depasser le capital ou le plafond dur meme agressif ici.
    "above_070": 2.5,
}
# ── plancher de prix d'achat DEDIE par symbole (Steven 22/07) : SOL/DOGE plus
# volatils -> n'achete que sur des favoris quasi-certains (>=0.94) PAR DEFAUT.
# Le bouton "Opportunité" (par marche) permet de LEVER ce plancher pour laisser
# le marche descendre bas et profiter du sizing Kelly comme BTC (prix bas +
# forte conviction = grosse mise). BTC n'a jamais eu de plancher -> le bouton
# ne le concerne pas (toujours autorise, cf. UI).
SYMBOL_MIN_ENTRY = {"SOL": 0.94, "DOGE": 0.94}
# ── SYMBOLS DISABLED (Steven 26/07) : inutile a comb_max=0.93, tous sont gagnants ──
DISABLED_SYMBOLS = set()  # a 0.93 meme DOGE/XRP font 100% WR

# ── SCORE DE DANGER (Steven 22/07) : protection SUPPLEMENTAIRE a la marge
# dynamique existante. Un marche qui a beaucoup zigzague pres du strike dans
# la derniere minute est skip meme si son ecart passe la marge au moment T
# (cf. core.btc_updown.danger_score, 0=calme, 100=tres instable).
DANGER_MAX = 70

# ── STRATEGIE BOTH-SIDE (Steven 22/07) : achete CHAQUE cote (Up ET Down)
# INDEPENDAMMENT des qu'il devient pas cher, sans exiger de conviction
# directionnelle (evaluate()). Idee : un marche AGITE (danger eleve) a de
# bonnes chances de faire passer les DEUX cotes sous le seuil a des moments
# differents pendant la fenetre -> transforme une periode qu'on evitait
# jusqu'ici en periode exploitable. Actif uniquement si Opportunité ON pour
# ce marche (jamais BTC). Le danger sert ICI de FEU VERT (mouvement = plus
# de chances de capter les 2 jambes), l'inverse de son usage habituel.
BOTH_SIDE_MAX_ENTRY = (
    0.52  # ↑ de 0.48 -> attrape les 1eres jambes plus tot (Laguna XS 24/07)
)
BOTH_SIDE_MIN_DANGER = 20  # ↓ de 25 -> activation plus tôt sur marchés volatils
# deux cotes passent sous 0.48 rapidement

# ── ACHAT SIMULTANE DES 2 COTES (Steven 22/07, mode PAPER de test) : "both
# side = both side" -> quand le marche bouge, on achete les DEUX jambes en
# MEME TEMPS (pas d'attente que chacune devienne cheap independamment), puis
# on scalpe le bruit (TP la jambe qui monte, SL celle qui plonge). Garde-fous :
BOTH_SIDE_SIMULTANEOUS = False  # False -> achat independant, seul underdog ouvre 2e pos
BOTH_SIDE_SCALP = False  # False = ARB PUR (Steven 22/07) : parts egales, on TIENT
# les 2 jambes jusqu'a resolution -> profit GARANTI, 100%
# win, pire fenetre encore POSITIVE (backtest +25$/18 fen).
# True = scalp du bruit (TP precoce + SL) : +/- de P&L mais
# reintroduit une variance/perte possible. Le pur arb colle
# a l'objectif "gonfler ET verrouiller", pas parier.
BOTH_SIDE_LEG_MIN = 0.15  # ne JAMAIS acheter une jambe sous ce prix : c'est un
# ticket de loterie (marche deja tranche), pas un hedge.
# Corrige aussi les "fills fantomes" du paper (500 parts
# a 0.01 -> +495 fictif) qui gonflaient le P&L.
BOTH_SIDE_LEG_MAX = 0.85  # PARTS-EGALES (Steven 22/07, retour pour + de volume) : pour
# un arb PARTS-EGALES tenu jusqu'a resolution, seul le COMBINE
# compte (garanti si <0.95), le prix d'UNE jambe importe peu.
# 0.85 capte 82% des arbs (vs 54% a 0.60) — les asymetriques
# inclus (un cote favori 0.65-0.80 + un cote cheap).
BOTH_SIDE_LEG_BUDGET = (
    1.0  # (inutilise en mode parts-egales ; garde pour le mode $1-egal)
)
BOTH_SIDE_COMBINED_MAX = (
    0.95  # skip si combined depasse ca. 100% WR, ROI +13.3% (Steven 26/07)
    # 0.95 = ~2x plus de trades que 0.93, vise ~6 trades/h
)
PAPER_MAX_FILL_SHARES = 40.0  # plafond de parts simulables en PAPER (evite les fills
# irrealistes qui faussent le P&L de test)
BOTH_SIDE_PAIR_BUDGET = 10.0  # $ engages par PAIRE en PAPER (les 2 jambes). En parts
# egales : parts = budget / combined -> profit arb garanti
# = parts * (1 - combined) a la resolution.
BOTH_SIDE_PAIR_BUDGET_REAL = (
    6.0  # $ par PAIRE en REEL (~3$/jambe, Steven 22/07 : "quand on
)
# attrape un arb on peut miser 3$+"). L'arb etant GARANTI,
# on le charge plus que le near-certain (qui, lui, reste au
# minimum de validation tant que REAL_VALIDATION_MODE=True).

# ── EXECUTION REELLE : CORRECTIFS 23/07 (Steven, apres audit de 5 arbs reels
# tous rates + 1 rollback qui a coute -0.35$ de spread pur) ──
# Constat log : le carnet SOL/XRP (thin) bouge de 0.04 a 0.10 en a peine 2s,
# MEME en milieu de fenetre (pas juste en fin de course). Le cap +0.02 fixe
# etait donc systematiquement trop serre -> quasi 100% d'echec en reel alors
# que le paper (meme logique, sans latence reseau) gagne +94$.
PREFLIGHT_DISABLED = False  # (Steven 04/08, "pret a mettre des $", avant un
# nouveau depot) : REACTIVE. Bilan chiffre de la session avec ce flag a True :
# 141 marches, 70 mono-jambe (50%!) a seulement 20% de reussite = -35.12$,
# c'etait le vrai trou noir (l'arb propre a parts egales, lui, etait legerement
# positif : +1.87$, 54%). Le preflight + reevaluation au combine FRAIS
# (PREFLIGHT-REEVAL, code deja en place plus bas) n'a JAMAIS tourne en
# conditions reelles cette nuit (0 occurrence en log) -- premiere vraie mesure
# a faire avec ce redemarrage. Si ca retombe a ~0 trade comme le test precedent
# (preflight strict SANS reeval), le reeval n'aura pas suffi et il faudra
# retravailler la marge de tolerance plutot que redesactiver aveuglement.
_PREFLIGHT_FORCE_BOTHSIDE_HISTORIQUE = False  # (Steven 04/08, "il faut forcer both side pour
# vraiment etre risk free") : REACTIVE apres preuve chiffree. Sur la session
# 22:32->01:19 UTC, donnees REELLES Polymarket : 198.08$ d'achats (119 ordres)
# pour ~20$ de capital = capital tourne ~10 fois, 40 ventes d'unwind, NET
# -22.40$. Ce n'est pas l'arb qui perd, c'est le cycle achat->1 jambe
# seule->revente qui paie le spread a chaque tour. Le preflight verifie les
# 2 jambes AVANT de poster : si les 2 ne sont pas prenables, on n'achete RIEN
# (zero capital engage) au lieu d'acheter 1 jambe puis la brader.
# NB : depuis le 30/07 ce preflight passe par le flux WS (book_depth deja en
# memoire, ~0ms) et ne retombe sur le REST que si le WS est absent -> il ne
# reintroduit PAS la latence qui avait motive sa desactivation.
# Repasser a True annule ce garde-fou (aucun autre changement requis).
_PREFLIGHT_HISTORIQUE = True  # (Steven 30/07, "c'est mon bot je fais ce que je
# veux... si j'ai envie de tester m'en empeche pas") : coupe le check
# prix+profondeur juste avant l'achat groupe des 2 jambes reelles.
# ATTENTION explicite : avec ca a True, un arb detecte a comb=0.97 peut
# s'executer alors que le prix a deja bouge et que le combine REEL au
# fill depasse 1.0 -> PERTE possible malgre le tag is_risk_free (aucun
# capital reste protege sur l'ecart de prix, seul l'unwind atomique
# reste actif pour le cas ou une seule jambe se remplit). Repasser a
# False annule ce risque immediatement (aucun autre changement requis).
REAL_MAX_COMBINED = 0.98  # (Steven 30/07, accord explicite malgre le tradeoff
# explique : le seuil 0.95 etait valide par backtest (100% WR,
# ROI +13.3%) mais bloquait trop d'occasions en reel (ex: SOL
# comb=0.960 rejete -> paper only) alors que le preflight
# WS-first + le cap de slippage restent la protection prix.
# Aligne sur le plafond paper (0.98) pour ne plus perdre ces
# fenetres ; accepte le risque que des arbs a marge fine (<2%)
# paient moins bien qu'a 0.95 en execution reelle.
REAL_SLIPPAGE_MIN = 0.03  # (Steven 30/07, "juste milieu") 0.02 -> 0.03 : plancher
# legerement releve pour absorber le bruit normal du carnet.
# Sans danger : le hard-cap "slip_total <= edge*0.9" (voir plus
# bas, _open_pair_parallel_real) empeche ce plancher de jamais
# manger plus de 90% de l'edge, quel que soit son reglage ici.
REAL_SLIPPAGE_MAX = 0.10  # 0.06 -> 0.10 : plafond elargi pour debloquer les arbs
# a bonne marge (edge large) qui ratent le preflight de peu -
# jamais illimite, et jamais > 90% de l'edge (garde ci-dessous).
REAL_SLIPPAGE_EDGE_FRACTION = (
    0.6  # part de l'edge (1-combined) allouee au slippage total
)
# (les 2 jambes). Le reste (40%) reste un profit garanti
# meme dans le pire cas de fill au cap. Auto-proportionnel :
# un arb a marge fine tolere PEU de slippage (bon, evite de
# payer plus que le gain), un arb a grosse marge en tolere +
# (aligne avec la volatilite observee sur ces marches).
REAL_MIN_DEPTH_RATIO = 1.0  # (Steven 04/08, "forcer both side") : EXIGE que le
# carnet couvre 100% de la taille visee SUR LES 2 JAMBES avant de poster.
# Historique : 0.6 a l'origine -> abaisse a 0.25 le 30/07 en pensant que
# l'unwind atomique suffisait de filet. Resultat mesure sur donnees REELLES
# Polymarket (22:32->01:19 UTC) : 119 ordres d'achat, 40 ventes d'unwind,
# NET -22.40$. Accepter de poster quand le carnet ne peut couvrir qu'un quart
# de la taille, c'est fabriquer des orphelins : la jambe fine ne se remplit
# pas, l'autre si, et on brade. A 1.0 on ne poste que si les DEUX cotes ont
# reellement de quoi nous servir en entier -> le both-side devient effectif
# au lieu d'etre un voeu pieux repare apres coup.
_REAL_MIN_DEPTH_RATIO_HISTORIQUE = 0.25  # (Steven 30/07, "les 2 en meme temps donc pas besoin
# de garde-fou") : abaisse de 0.6 -> 0.25. Redondant avec l'unwind
# atomique (jambe seule remplie -> revente immediate) qui backstop
# deja le vrai risque (mismatch de fill), donc ce seuil peut etre
# bas sans reintroduire de risque nu. Historique (Steven "carnet
# plus profond avant de tenter en reel") : la taille dispo au
# best ask doit couvrir au moins 60% des parts visees, sinon
# on saute ce marche (carnet trop fin = slippage garanti).

# ── GARDIEN BOTH-SIDE (Steven 22/07) : but = gonfler le portefeuille ET
# VERROUILLER le gain max, pas juste eviter la perte. Protege UNIQUEMENT la
# jambe SOLO (l'autre cote pas encore achete) : une fois les 2 jambes en
# poche, le P&L est deja FIXE (une gagne $1, l'autre $0, total connu d'avance)
# -> vendre a ce stade REINTRODUIRAIT du risque au lieu d'en retirer, donc le
# gardien ne touche JAMAIS une position deja couverte.
BOTH_SIDE_STOP_PRICE = 0.25  # ↑ de 0.20 -> protège mieux le solo
BOTH_SIDE_STOP_MIN_SECS_LEFT = 20  # ↓ de 30 -> kill zone plus tôt

# ── TP AGRESSIF ORPHAN (Steven 23/07, "TP plus agressif") : au lieu de tenir
# une jambe gagnante jusqu'a resolution complete (risque de retournement tardif
# du signal Binance), on VERROUILLE une partie du gain des que le prix devient
# confortable. Le reste continue de viser le payout complet.
ORPHAN_TP_PRICE = 0.80  # prix mini pour declencher la prise de profit
ORPHAN_TP_MIN_PROFIT = 0.15  # marge mini vs prix d'entree pour que ca vaille le coup
ORPHAN_TP_SELL_FRACTION = 0.6  # part vendue au TP (le reste continue vers le $1)

# ── STOP-LOSS DUR (Steven 23/07, "les 3", cause racine des pertes -1.90/-2.90$) :
# le vrai probleme n'etait PAS la vitesse de vente mais le SEUIL de declenchement.
# On tenait un favori tant que Binance le disait gagnant -> mais le prix Polymarket
# s'effondre AVANT/PENDANT que Binance flippe (14:23 : achat 0.91, Binance encore
# "gagnant", prix deja tombe a 0.33 quand la vente part). Ce stop coupe TOUT
# IMMEDIATEMENT des que le bid descend de ORPHAN_HARD_STOP sous le prix d'entree,
# SANS attendre la confirmation Binance. Transforme un -2.90$ en ~-0.75$.
ORPHAN_HARD_STOP = 0.15  # perte max toleree par part avant coupe seche totale

# ── STOP LOSS ARB NEAR RESOLUTION (Steven 23/07) : sur les positions bothside
# (ARB), vendre la jambe PERDANTE quand le prix bid < seuil ET il reste < ARB_SL_SECS_LEFT
# avant resolution. La jambe gagnante reste ouverte (payout 1$). NE TOUCHE PAS
# aux positions orphan, hedge solo, ou swing — uniquement les ARB bothside.
ARB_SL_SECS_LEFT = 45  # secondes avant resolution pour activer le SL ARB
ARB_SL_BID_THRESHOLD = 0.12  # si bid < ce seuil, la jambe est consideree comme morte

# ── JOURNAL DES PRIX (Steven 22/07) : loggue le prix de CHAQUE position
# tenu a intervalle regulier -> version texte relisible dans le log +
# historique structure (price_log) pour affichage courbe cote dashboard.
# Permet de revivre un trade apres coup, comprendre ce qui s'est passe.
PRICE_LOG_INTERVAL_S = 2  # frequence de journalisation par position (WS, via fast-exit loop)
PRICE_LOG_MAX_POINTS = 60  # historique conserve par position (~5 min a 5s d'intervalle)

# ── PRISE DE PROFIT PAR PALIERS (Steven 22/07) : une jambe both-side (solo
# OU couverte) qui s'envole avant resolution est vendue PROGRESSIVEMENT au
# lieu d'attendre passivement -> verrouille le gain sans tout risquer sur un
# seul palier. Palier 1 : vend la moitie. Palier 2 : vend le reste.
# Paliers de TP (3 niveaux, appliques dans l'ordre). Chaque palier vend une
# FRACTION du RESTANT ; le dernier vend tout ce qui reste.
BOTH_SIDE_TP1_PRICE = 0.65
BOTH_SIDE_TP1_FRACTION = 0.40
BOTH_SIDE_TP2_PRICE = 0.78
BOTH_SIDE_TP2_FRACTION = 0.50  # 50% du RESTANT apres TP1
BOTH_SIDE_TP3_PRICE = 0.88  # 3e palier, desormais VRAIMENT branche (vend le reste)

# SL par jambe (scalp simultane, Steven 22/07 "en sl et gard pos gagnante") :
# coupe une jambe qui plonge SOUS ce prix SI le momentum confirme la baisse
# (pas juste un creux de bruit qui pourrait rebondir). Protege le capital sans
# tuer la these sur un simple soubresaut.
BOTH_SIDE_SL_PRICE = 0.15
BOTH_SIDE_SL_MIN_SECS_LEFT = 20

# ── PHASE 1 : HEDGE FORCÉ & FERMETURE URGENCE (Steven 25/07) ──
# Si jambe 1 remplie mais jambe 2 non confirmée après ce délai -> mode agressif
HEDGE_FORCE_TIMEOUT_MS = 500  # 500ms : force agressif si 2e jambe pas encore remplie
# Prix agressif max : 1-2 ticks ou 0.5-1.0% de l'edge attendu
HEDGE_AGGR_MAX_TICKS = 2
HEDGE_AGGR_MAX_SLIP_PCT_OF_EDGE = 0.01  # 1.0% de l'edge
# Si échec total après ce délai (incluant force) -> fermeture urgence IMMEDIATE
HEDGE_EMERGENCY_TIMEOUT_MS = (
    300  # 300ms : zero-lag cut, on ne garde JAMAIS de position orpheline
)
# Seuils adaptatifs selon liquidité
HEDGE_LIQUID_THRESHOLD_DEPTH_RATIO = 1.0  # > 1.0 = marché très liquide
HEDGE_LIQUID_FORCE_TIMEOUT_MS = 400  # 400ms pour marchés liquides
HEDGE_THIN_FORCE_TIMEOUT_MS = 800  # 800ms pour marchés fins (un peu plus de marge)
HEDGE_THIN_AGGR_MAX_SLIP_PCT_OF_EDGE = 0.005  # 0.5% plus strict sur marchés fins
# ZÉRO-LAG CUT : slippage max sur vente urgence (perte acceptée pour sortir vite)
EMERGENCY_SELL_SLIPPAGE_PCT = 0.03  # 3% max de slippage sur vente urgence
# EDGE MINIMUM : ne pas entrer si l'edge ne couvre pas le risque d'orphan
MIN_EDGE_FOR_SEQUENTIAL = 0.03  # 3% minimum entre combined et 1.0 pour entrer
# MICRO-SIZING : cap la mise par jambe quand le solde est petit
MICRO_SIZING_MAX_LEG_USD = 1.0  # max 1$/jambe quand solde < 10$

# ── WATCHDOG FILL-RATE (Steven 25/07) ──
FILL_RATE_WINDOW = 20  # nombre de tentatives récentes à surveiller
FILL_RATE_MAX_FAILS = 3  # max échecs 2e jambe / fenêtre avant cooldown
FILL_RATE_COOLDOWN_S = 300  # 5 min pause sur ce symbole/marché

# ── SIZING ADAPTATIF (Steven 25/07, boost symetrique ajoute 04/08) ──
SIZING_MIN_DEPTH_RATIO = 0.5  # ratio profondeur/taille visée pour réduire la taille
SIZING_MAX_SPREAD_BPS = 200  # spread max (bps) au-delà duquel on réduit la taille
SIZING_REDUCTION_FACTOR = 0.5  # facteur de réduction de taille si liquidité faible
# BOOST (Steven 04/08, "mise + sur le gagnant, - sur le perdant") : jusqu'ici
# _adaptive_size ne savait QUE reduire (mauvaise liquidite -> taille /2), jamais
# augmenter meme quand le carnet est excellent -> asymetrie qui allait dans le
# meme sens que le probleme corrige sur _budget_usd (jamais assez agressif sur
# les conditions favorables). Le seul signal que possede CETTE fonction est la
# liquidite (pas le prix/probabilite, deja traite dans _budget_usd) -- "gagnant"
# ici = carnet profond + spread serre (conditions d'execution favorables),
# "perdant" = carnet fin + spread large (deja gere par la reduction existante).
# RECALIBRE (Steven 05/08, "adaptatif sizing pas assez visible... regarde
# log") : verifie sur 28 mesures reelles de la nuit, TOUJOURS des reductions
# (0 boost), et pour cause -- les spreads reels observes vont de 200 a
# 976bps, tous largement au-dessus de l'ancien seuil de 40bps qui n'avait
# donc AUCUNE chance de se declencher. Attention honnete : ces 28 mesures
# sont uniquement les cas REDUITS (la fonction ne loggue que reduce/boost,
# jamais le cas neutre) -> pas de vraie distribution du "bon" carnet
# dispo pour calibrer precisement. Nouveaux seuils volontairement moins
# extremes que les valeurs devinees au depart (150bps/1.5x reste nettement
# mieux que la moyenne des cas reduits ~260bps, sans etre hors d'atteinte).
SIZING_BOOST_MAX_SPREAD_BPS = 150  # spread serre (< ce seuil) pour booster
SIZING_BOOST_MIN_DEPTH_RATIO = 1.5  # profondeur large (> ce seuil) pour booster
SIZING_BOOST_FACTOR = 1.25  # facteur d'augmentation si liquidite excellente

# ── EDGE TIERS pour sizing adaptatif a l'edge (Steven 25/07) ──
# Confirme par Steven : BOLD>=12%, NORMAL 8-12%, REDUCED 4-8%, SKIP<4%
EDGE_BOLD_THRESHOLD = 0.12  # >= 12% edge (combined <= 0.88) -> BOLD x1.5
EDGE_NORMAL_THRESHOLD = 0.08  # >= 8% edge (combined <= 0.92) -> NORMAL x1.0
EDGE_REDUCE_THRESHOLD = 0.04  # >= 4% edge (combined <= 0.96) -> REDUCED x0.6
EDGE_BOLD_MULTIPLIER = (
    2.0  # ↑ de 1.5 -> double la mise sur les gros edges (Steven 26/07)
)
EDGE_NORMAL_MULTIPLIER = 1.0
EDGE_REDUCE_MULTIPLIER = 0.6

# ── BINANCE WS SIZING : momentum/danger influencent la taille (Steven 25/07) ──
BINANCE_MOMENTUM_BOOST = 1.2  # momentum confirme (fast+slow meme sens) -> x1.2
BINANCE_DANGER_REDUCE = 0.8  # danger > 50 (retournement possible) -> x0.8

# ── RISK UNIFIÉ PAR STRATÉGIE (Steven 25/07) ──
# Structure centralisée pour SL/TP par stratégie
STRATEGY_RISK_PARAMS = {
    "bothside": {
        "hard_stop": 0.15,  # ORPHAN_HARD_STOP
        "tp_levels": [
            (0.65, 0.40),
            (0.78, 0.50),
            (0.88, 1.0),
        ],  # (price, fraction_of_remaining)
        "trail_trigger": 1.08,  # TRAIL_TRIGGER
        "trail_pct": 0.05,  # TRAIL_PCT
        "breakeven_trigger": 1.04,  # BREAKEVEN_TRIGGER
        "max_hold_hours": 6,  # MAX_HOLD_HOURS
        "arb_sl_secs_left": 45,  # ARB_SL_SECS_LEFT
        "arb_sl_bid_threshold": 0.12,  # ARB_SL_BID_THRESHOLD
    },
    "hedge": {
        "hard_stop": 0.15,
        "tp_levels": [(0.65, 0.40), (0.78, 0.50), (0.88, 1.0)],
        "trail_trigger": 1.08,
        "trail_pct": 0.05,
        "breakeven_trigger": 1.04,
        "max_hold_hours": 6,
    },
    "near-certain": {
        "hard_stop": 0.12,  # plus serré pour favoris nus
        "tp_levels": [(0.65, 0.40), (0.78, 0.50), (0.88, 1.0)],
        "trail_trigger": 1.08,
        "trail_pct": 0.05,
        "breakeven_trigger": 1.04,
        "max_hold_hours": 6,
    },
    "orphan": {
        "hard_stop": 0.15,  # ORPHAN_HARD_STOP
        "tp_price": 0.80,  # ORPHAN_TP_PRICE
        "tp_min_profit": 0.15,  # ORPHAN_TP_MIN_PROFIT
        "tp_sell_fraction": 0.6,  # ORPHAN_TP_SELL_FRACTION
        "max_hold_hours": 6,
    },
    "dn": {
        "hard_stop": 0.15,
        "tp_levels": [(0.65, 0.40), (0.78, 0.50), (0.88, 1.0)],
        "max_hold_hours": 6,
    },
}

# ── LIMITES RISQUE JOURNALIÈRES / HORAIRES (Steven 25/07) ──
MAX_DAILY_LOSS_PER_SYM = 5.0  # $ max perte par symbole/jour
MAX_HOURLY_LOSS_PER_SYM = 2.0  # $ max perte par symbole/heure
MAX_DAILY_LOSS_PER_STRAT = 10.0  # $ max perte par stratégie/jour
MAX_TRADES_PER_HOUR_PER_SYM = 10  # max trades/heure/symbole

# ── MULTI-TRADE PAR TRANCHE (optimisation bruit, VRAIMENT branche) :
MAX_TRADES_PER_SLOT = 3  # max 3 cycles de scalp both-side par tranche 5min (par slug)
# ── 2e JAMBE FORCEE si le temps presse (Steven 22/07) : si on reste SOLO
# (jamais couvert) et qu'il ne reste plus assez de temps pour que l'autre
# cote passe sous BOTH_SIDE_MAX_ENTRY naturellement, on force l'achat de
# couverture a un prix plus haut plutot que de rester expose sans filet.
BOTH_SIDE_FORCE_HEDGE_SECS_LEFT = 15  # ↓ de 25 -> couverture + tôt
BOTH_SIDE_FORCE_HEDGE_MAX_PRICE = 0.72  # ↑ de 0.70 -> tolère un peu plus haut

# ══════════════════════════════════════════════════════════════════════════════
# ── GHOST V3.1 — CONSTANTS (Steven 26/07) ──
# ══════════════════════════════════════════════════════════════════════════════

# ── AXE 1 : PRE-FLIGHT VALIDATION + DEAD MARKET CHECK ──
COMB_ASK_FEE_ESTIMATE = 0.02  # 1% taker par jambe (~2% total)
COMB_ASK_TINY_MARGIN = 0.005  # marge souple sur le seuil dynamique
DEAD_MARKET_THRESHOLD = 0.05  # en dessous = marche probablement resolu
DEAD_MARKET_CAUTION = 0.95  # au dessus = l'autre cote est presque morte

# ── AXE 2 : COOLDOWN SLUG/SYMBOLE + TIMEOUT ADAPTATIF ──
FORCE_PAIR_TIMEOUT_BASE = 1.0  # secondes min timeout jambe 2
FORCE_PAIR_TIMEOUT_MULT = 3  # x mediane latence recente
SLUG_COOLDOWN_SECS = 120  # 2 minutes apres abort sur un slug
SYMBOL_ABORT_COOLDOWN_SECS = 300  # 5 minutes apres 2+ aborts consec
MAX_CONSEC_ABORTS = 2  # avant cooldown symbole
LATENCE_HISTORY_SIZE = 20  # buffer mediane latences

# ── AXE 3 : TAILLE VARIABLE SELON SIGNAL ──
TIER_EDGE_PREMIUM = 0.12  # >= 12% edge -> premium
TIER_EDGE_NORMAL = 0.08  # >= 8% edge -> normal
TIER_D_PREMIUM = 120  # secondes restantes pour premium
TIER_D_NORMAL = 60  # secondes restantes pour normal
TIER_SIZE_FRAGILE = 0.50  # $ par jambe
TIER_SIZE_NORMAL = 1.00  # $ par jambe
TIER_SIZE_PREMIUM = 1.50  # $ par jambe (max)
TIER_SIZE_ULTRA = 2.00  # $ par jambe (marche ultra liquide + edge > 15%)

# ── INSTANT-ARB SCALE-UP (Steven 29/07) : "acheter 5 positions de chaque
# cote ou plus si les fonds le permettent, a comb_ask < 0.99" -> l'ARB-PARALLEL
# garanti (les 2 jambes postees EN MEME TEMPS, profit fige des l'ouverture) ne
# doit PAS rester plafonne aux petits tiers fixes ($0.50-2/jambe) quand du
# capital dort : on scale la taille sur le capital dispo, PLANCHER a
# MIN_ORDER_SIZE_SHARES (5 parts), tant que combined < INSTANT_ARB_MAX_COMBINED.
INSTANT_ARB_MAX_COMBINED = 0.99  # au-dela : pas assez de marge garantie
# GROSSE MISE SUR RISK-FREE (Steven 29/07, "je veux grosse mise sur les arb
# risk free") : contrairement au directionnel, l'arb garanti (2 jambes achetees
# EN MEME TEMPS, profit fige quel que soit le resultat) n'expose PAS a un
# retournement -> ca justifie une mise bien plus grosse que sur du hedge/
# near-certain. Fraction du capital ET plafond $ montes en consequence.
INSTANT_ARB_CAPITAL_FRACTION = 0.35  # jusqu'a 35% de l'investable par jambe (etait 20%)
INSTANT_ARB_MAX_USD = 50.0  # plafond en $ (etait un plafond en PARTS -> incoherent avec le prix)
INSTANT_ARB_MAX_SHARES = 50  # garde-fou secondaire (parts), rarement le facteur limitant desormais

# ── DYNAMIC SIZING V3.2 : facteurs de vitesse + PnL flottant (Steven 27/07) ──
DYN_SPEED_BOOST = 1.15  # momentum tres fort (|fast|>0.10%/s) -> x1.15
DYN_SPEED_REDUCE = 0.85  # momentum oppose -> x0.85
DYN_STREAK_BONUS = 1.10  # 3+ wins consec -> x1.10 (confiance, pas folie)
DYN_STREAK_PENALTY = 0.75  # 2+ losses consec -> x0.75 (protection)
DYN_FLOATING_REDUCE = 0.80  # >10$ unrealized neg -> x0.80 (proteger capital)
DYN_MAX_FLOORED_MULT = 2.5  # max multiplicateur total (ne depasse pas 2.5x le tier)

# ── TP/SL PALIERS PNL-BASED V3.2 (Steven 27/07) ──
# Sortie progressive basee sur le PnL% du contrat (pas le prix absolu).
# 4 niveaux : 25% a +25%, 25% a +50%, 25% a +75%, 25% runner avec trailing stop.
PNL_TP_FRACTIONS = (0.25, 0.25, 0.25, 0.25)  # fraction de la taille INITIALE par palier
PNL_TP_TARGETS = (0.25, 0.50, 0.75)  # paliers de PnL% pour TP1/TP2/TP3
PNL_TRAIL_ACTIVATION = 0.25  # trailing s'armee des TP1 (+25%)
PNL_TRAIL_GIVEBACK = 0.10  # 10% du pic depuis le palier atteint -> vente runner
PNL_SL_PCT = 0.20  # stop loss (Steven 29/07, resserre de -30% -> -20% : perte
# moyenne realisee -1.49$ contre gain moyen +1.25$ sur TP -> le SL breche
# largement son seuil nominal avant declenchement (pire perte vue -3.8$ pour
# un seuil "-30%"), le prix crashe plus vite que le check ne l'attrape. Coupe
# plus tot -> perte moyenne plus petite, meme avec le check rapide (1.5s).
PNL_SL_MIN_SECS_LEFT = 20  # trop peu de temps = pas de SL
# ESCALADE TP (Steven 05/08, "on voit l'argent filer entre nos doigts") :
# apres ce nombre d'echecs consecutifs a vendre la fraction du palier vise,
# on vend TOUT ce qui reste au lieu de continuer a retenter la meme petite
# fraction indefiniment. Observe en reel : 6 echecs consecutifs sur 2.5min
# (0 part vendue a chaque fois) sur une position SOL pendant que le marche
# continuait de bouger -- ce plafond coupe court bien avant.
PNL_TP_ESCALATE_AFTER = 3

# FAST EXIT LOOP (Steven 28/07) : le SL/TP tournait au rythme du scan complet
# (~7-12s/symbole, trop lent pour un marche 5min) -> thread dedie, rythme WS.
FAST_EXIT_POLL_S = 1.5

# ── REVERSAL TRACKING V3.2 : bonus statistique (Steven 27/07) ──
REVERSAL_MIN_DRAWDOWN = -0.10  # position a fait -10% minimum pour compter
REVERSAL_STATS_WINDOW = 50  # dernieres 50 resolutions par tier pour calibrer
REVERSAL_UPGRADE_THRESHOLD = 0.40  # si >40% des fragiles se retournent -> upgrade tier

# ── RL EXIT MANAGER V3.2 (Steven 27/07) ──
RL_EXIT_ENABLED = True  # active le RL pour les sorties positionnelles
RL_EXIT_SHADOW = False  # True = log only, False = execute
RL_EXIT_INTERVAL_S = 10  # intervalle entre les propositions RL (sec)
RL_EXIT_MIN_SECS_LEFT = 60  # pas de RL si < 60s restantes (laisse resolution)
RL_EXIT_MIN_SHARES = 5.0  # minimum pour qu'on le RL agisse

# ── AXE 4 : TP PARTIEL + RUNNER ──
SWING_TP1_SELL_FRACTION = 0.50  # 50% sorti a TP1
SWING_TP1_TARGET = 0.10  # +$0.10 profit
SWING_TP2_TARGET = 0.30  # +$0.30 profit
SWING_STOP_PCT = 0.05  # -5% stop (a monitorer)
ORPHAN_SELL_TIMEOUT = 60  # secondes avant vente market forcee

# ── AXE 5 : STOPS LOGIQUES ──
STOP_DAILY_LOSS_PCT = 0.10  # 10% du bankroll -> arret journee
STOP_CONSEC_SYMBOL_V31 = 3  # pertes consecutives par symbole -> pause
STOP_CONSEC_GLOBAL_V31 = 5  # pertes consecutives globales -> arret journee
STOP_CONSEC_PAUSE_MINS = 30  # duree de pause
STOP_WINDOW_REMAIN_SECS = 30  # secondes restantes -> stop si perdant
STOP_UNREALIZED_PCT = 0.30  # -30% du cout en < 30s -> abort
STOP_MARKET_SPEED = 0.20  # mouvement > 0.20 en < 3s -> vente

SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
# SOL/XRP/DOGE ajoutes 21/07 en PAPER uniquement : on collecte des donnees par
# marche (chacun a sa propre volatilite -> ses propres seuils, cf. ETH qui bouge
# trop peu pour les marges calibrees BTC) avant d'envisager le reel.
DEFAULT_MODES = {
    "BTC": "real",
    "ETH": "paper",
    "SOL": "paper",
    "XRP": "paper",
    "DOGE": "paper",
}
# strategie par marche : "hold" = pari tenu jusqu'a resolution (BTC) ;
# "swing" = achete le contrat pas cher pendant le bruit et REVEND avant
# resolution sur un objectif de prix (ETH : profite des oscillations du contrat,
# pas d'un gros mouvement du sous-jacent — ideal pour ETH qui bouge peu).
# ETH passe de "swing" a "hold" (Steven 22/07) : en hold + Opportunité ON, ETH
# fait du BOTH-SIDE comme SOL/XRP/DOGE. BTC branche sur la MEME strategie
# (Steven 23/07) : l'exclusion `sym != "BTC"` dans _try_market a ete retiree.
DEFAULT_STRATS = {
    "BTC": "hold",
    "ETH": "hold",
    "SOL": "hold",
    "XRP": "hold",
    "DOGE": "hold",
}
# parametres swing (paper)
SWING_MIN_EDGE_PCT = (
    0.0004  # ecart Binance minimal (0.04% du prix) pour ouvrir un swing :
)
# en dessous c'est du bruit -> on s'abstient. Sans ce filtre,
# toutes les entrees se faisaient a ~0.50 (pile ou face).
SWING_MAX_ENTRY = (
    0.55  # n'achete que si le contrat du cote favori est <= 0.55 (pas cher)
)
# (nettoye 22/07 : doublons Laguna XS supprimes, valeurs finales conservees)
SWING_TARGET = 0.72  # arme le trailing (TP +8% plus tot que l'origine 0.78)
SWING_TRAIL_GIVEBACK = (
    0.05  # une fois arme : vend si le prix retombe de 5c sous son PIC
)
SWING_STOP = 0.25  # coupe sous 0.25 (protection +13% vs origine 0.22)
SWING_MIN_SECS = 25  # ne pas ouvrir un swing a moins de 25s (pas le temps de revendre)
SWING_ENTER_MAX_SECS = 240  # peut entrer tot (jusqu'a 240s de la fin) pendant le bruit

# ── MOMENTUM FALLBACK (Steven 26/07) : quand l'ARB est bloquee (combined > 0.95),
# achete le côté cheap directionnel si le momentum Binance confirme -> scalp le bruit.
MOMENTUM_FALLBACK_ENABLED = True
MOMENTUM_FALLBACK_MAX_ENTRY = 0.50  # n'achete que si le contrat < 0.50
MOMENTUM_FALLBACK_MIN_FAST_PCT = 0.01  # momentum court minimum (%/s) pour confirmer
MOMENTUM_FALLBACK_BUDGET = 1.0  # $ par trade momentum (petit, risque dir.)
MOMENTUM_FALLBACK_MIN_SECS = 30  # pas d'entree < 30s (pas le temps de scalper)


def _now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]


class MultiTrader:
    def __init__(self):
        self._live = None
        self._thread = None
        self._running = threading.Event()
        self._last_good_cash = None
        # ── PARALLELISATION PAR MARCHE (22/07, demande Steven) ──
        # chaque marche traite son workflow dans son propre thread a chaque tick :
        # la detection/lecture carnet/resolution/paper de TOUS les marches tournent
        # en meme temps -> un achat reel (attente de fill jusqu'a ~8s) ne bloque
        # plus les autres marches (c'etait la "file d'attente" qui faisait rater
        # des achats en fin de fenetre).
        from concurrent.futures import ThreadPoolExecutor

        # +2 workers : le scanner ULTRAPOLY + le market maker tournent en fond
        # sans voler un slot aux 5 marches crypto du tick.
        self._pool = ThreadPoolExecutor(max_workers=len(SYMBOLS) + 2)
        # serialise UNIQUEMENT l'execution d'un ordre REEL (signature+envoi+fill) :
        # le client CLOB n'est pas garanti thread-safe et il ne faut jamais engager
        # deux fois le meme capital. Tout le reste reste parallele.
        self._order_lock = threading.Lock()
        # SERIALISE tout un HEDGE (lecture cash -> favori -> underdog), pas juste
        # l'envoi d'ordre unitaire (Steven 23/07, correctif course entre marches) :
        # ETH/SOL/XRP/BTC/DOGE tournent en PARALLELE et lisaient tous le meme
        # solde partage au meme instant -> plusieurs pensaient avoir 1$ de reserve
        # disponible simultanement, un seul l'avait reellement -> "not enough
        # balance" sur l'underdog des autres. Un seul hedge complet a la fois.
        self._hedge_lock = threading.Lock()
        # VERROU ARB GARANTI (Steven 29/07, "sa verrouille pour pas qu'un autre
        # trade prend le £ necessaire au arb garanti") : meme bug de course que
        # le hedge -> 5 symboles en parallele pouvaient lire le MEME solde au
        # meme instant et sur-engager le capital sur plusieurs arb a la fois.
        self._arb_lock = threading.Lock()
        # tokens dont les caches tick_size/neg_risk sont deja prechauffes
        # (Steven 04/08, latence detection->post) - voir _prewarm_order_cache
        self._prewarmed = set()
        # debut de session : reference pour le PnL REEL on-chain (Steven 04/08)
        self._session_start_ts = time.time()
        self._session_start_cash = None  # capture au 1er read reussi (client pas encore pret ici)
        self._global_consec_losses = 0
        self._log_lock = (
            threading.Lock()
        )  # ecritures log concurrentes -> pas d'entrelacement
        _state_file_existed = STATE_FILE.exists()
        self.state = self._load()
        self.state.setdefault("killswitch", dict(KILLSWITCH_DEFAULTS))
        self.state.setdefault("killswitch_triggered", None)  # {"reason":..., "ts":...} si declenche
        self.state.setdefault("latency_history", [])  # mesures CHRONO structurees, voir /api/latency
        self.state.setdefault("execution_quality_history", [])  # fill ratio / EV / freshness, voir /api/execution-quality
        # RESTAURATION DEPUIS DB (Steven 04/08) : uniquement si le fichier
        # local etait ABSENT (volume Railway perdu/non monte -- exactement ce
        # qui vient d'arriver cette nuit). Si le fichier existe deja, il est
        # prioritaire -- pas de raison d'ecraser un etat local valide.
        if not _state_file_existed:
            _db_cfg = self._db_load_config_state()
            if _db_cfg:
                self.state["floor_usd"] = _db_cfg["floor_usd"]
                self.state["killswitch"] = _db_cfg["killswitch"]
                self._log("♻️ [CONFIG] plancher + kill-switch restaures depuis Postgres (fichier local absent)")
        # ── FLUX WEBSOCKET TEMPS REEL (Steven 23/07) : demarre les connexions
        # Binance + Polymarket. Alimente le cache que _book_quote / _mm_tick /
        # l'arb lisent en <100ms au lieu du REST ~1s. Lecture seule, aucun ordre. ──
        self._ws = get_feed()
        # ── ULTRAPOLY (Steven 22/07) : bucket dedie hors SYMBOLS + etat scanner ──
        self.state["markets"].setdefault("POLY", self._blank_market("POLY"))
        self.state.setdefault("ultrapoly", False)
        self.state.setdefault("ultrapoly_real", False)
        # ── DELTA-NEUTRE both-side au bid (Steven 23/07) ──
        self.state.setdefault("dn_enabled", DN_ENABLED_DEFAULT)
        self.state.setdefault("dn", {"pairs": {}, "trades": [], "pnl": 0.0})
        self._dn_quotes = {}  # sym -> {slug, Up:{token,bid,oid}, Down:{token,bid,oid}}
        self._ultra_last_scan = 0.0
        self._ultra_future = None
        self._ultra_cooldown = {}
        self._tlog_ts = {}  # throttle des logs repetitifs (cle -> dernier ts)
        # ── ARB STREAM (Steven 26/07) : callback push quand WS detecte combined <= seuil ──
        self._ws.set_arb_callback(
            self._arb_stream_callback,
            threshold=REAL_MAX_COMBINED,
            debounce_s=30,
        )
        self._arb_stream_opened = set()  # slugs ouverts via stream (eviter double-open)
        # ── DIAGNOSTIC (Steven 22/07) : temps de traitement, pour le dashboard ──
        self._diag = {"scan_ms": 0, "per_symbol_ms": {}, "tick_ts": 0, "scan_count": 0}
        # ── WATCHDOG FILL-RATE (Steven 25/07) : surveille les échecs de 2e jambe ──
        self._fill_watchdog = {}  # sym -> {fails: int, attempts: list[bool], cooldown_until: float}
        # ── LIMITES RISQUE JOURNALIÈRES / HORAIRES (Steven 25/07) ──
        self._risk_limits = {}  # sym -> {daily_pnl: float, hourly_pnl: float, daily_ts: float, hourly_ts: float, trades_this_hour: int}
        # ── REVERSAL TRACKING V3.2 (Steven 27/07) : stats retournements tardifs ──
        self.state.setdefault(
            "reversal_stats", {"fragile": [], "normal": [], "premium": []}
        )
        # ── RL EXIT MANAGER V3.2 (Steven 27/07) : assistant positionnel ──
        self._rl_last_proposal = {}  # sym -> timestamp de la derniere proposition RL
        try:
            from rl_exit import get_rl_manager

            self._rl = get_rl_manager(enabled=RL_EXIT_ENABLED, shadow=RL_EXIT_SHADOW)
            if RL_EXIT_ENABLED:
                self._log(f"🤖 [RL] EXIT MANAGER active (shadow={RL_EXIT_SHADOW})")
        except Exception as e:
            self._log(f"⚠️ [RL] Erreur init: {e}")
            self._rl = None
        # ── MARKET MAKER CONDITIONNEL (Steven 23/07) : bucket dedie, hors SYMBOLS ──
        # FIX 23/07 : setdefault("mm", ...) ne s'applique QUE si la cle "mm"
        # est totalement absente -> un etat sauvegarde AVANT l'ajout d'une
        # nouvelle sous-cle (ex: markout_pending, mid_history) gardait un
        # dict "mm" incomplet, provoquant un KeyError au premier acces. On
        # merge desormais chaque sous-cle individuellement.
        self.state.setdefault("mm", {})
        for k, v in mm.MarketMakerState.blank().items():
            self.state["mm"].setdefault(k, v)
        for sym in SYMBOLS:
            self.state["mm"]["inventory"].setdefault(sym, 0.0)

    # ── persistance ──
    def _blank_market(self, sym):
        return {
            "trades": [],
            "open": {},
            "consec_losses": 0,
            "stopped": False,
            "stop_reason": None,
            "paper_balance": PAPER_START_BAL,
        }

    def _load(self):
        if STATE_FILE.exists():
            try:
                s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
                s.setdefault("modes", {})
                s.setdefault("strategies", {})
                s.setdefault("markets", {})
                s.setdefault("opportunity", {})
                # complete PAR SYMBOLE : un etat sauvegarde avant l'ajout de
                # SOL/XRP/DOGE n'a ni leur mode ni leur strategie -> setdefault
                # sur le dict entier ne suffisait pas (KeyError 'SOL').
                for sym in SYMBOLS:
                    s["modes"].setdefault(sym, DEFAULT_MODES.get(sym, "paper"))
                    s["strategies"].setdefault(sym, DEFAULT_STRATS.get(sym, "hold"))
                    s["markets"].setdefault(sym, self._blank_market(sym))
                    s["opportunity"].setdefault(sym, False)
                return s
            except Exception:
                pass
        return {
            "modes": dict(DEFAULT_MODES),
            "strategies": dict(DEFAULT_STRATS),
            "opportunity": {sym: False for sym in SYMBOLS},
            "markets": {sym: self._blank_market(sym) for sym in SYMBOLS},
        }

    def _save(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(self.state, indent=2), encoding="utf-8")

    def _log(self, msg):
        line = f"[{_now()}] {msg}"
        with self._log_lock:  # threads concurrents -> lignes non entrelacees
            print(line, flush=True)
            # try/except (Steven 04/08) : trouve en testant un deploiement --
            # cette ecriture etait SANS PROTECTION, et un _log() appele depuis
            # un handler d'exception (ex: RL init qui echoue proprement et
            # essaie de LOGUER l'echec) plantait le process ENTIER si le
            # dossier data/ n'existait pas encore/plus (ex: volume pas encore
            # monte au tout premier demarrage). print() ci-dessus reste le
            # filet minimal (stdout capture par Railway) meme si le fichier
            # echoue -> jamais silencieux, jamais fatal.
            try:
                with open(LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception as e:
                print(f"[{_now()}] ⚠️ [LOG] ecriture fichier echouee : {e}", flush=True)

    def _tlog(self, key, msg, every=15.0):
        """Log THROTTLE : au plus 1 fois toutes les `every` secondes par cle.
        Pour les messages repetitifs (solde insuffisant, carnet vide) qui doivent
        etre EXPLICITES (Steven 22/07) sans noyer le log."""
        now = time.time()
        if now - self._tlog_ts.get(key, 0) >= every:
            self._tlog_ts[key] = now
            self._log(msg)

    # ── solde reel robuste (retry + cache court pour ne pas marteler l'API) ──
    def _read_cash(self, max_age=6.0):
        import os

        if (
            self._last_good_cash is not None
            and (time.time() - getattr(self, "_cash_ts", 0)) < max_age
        ):
            return self._last_good_cash, "cache"
        pk, funder = (
            os.environ.get("PRIVATE_KEY", ""),
            os.environ.get("POLY_FUNDER_ADDRESS", ""),
        )
        if not pk or not funder:
            return None, "PRIVATE_KEY/FUNDER absente du .env"
        for _ in range(3):
            try:
                if self._live is None:
                    self._live = PolyLive(pk, funder)
                    # CANAL USER WS (Steven 30/07, "on a WS aussi") : demarre
                    # une seule fois, des que le client reel existe -> fills
                    # pousses en direct disponibles pour toute la suite.
                    try:
                        self._ws.start_user_channel(self._live.ws_auth(), log_fn=self._log)
                    except Exception as e:
                        self._log(f"⚠️ [WS-USER] demarrage echec (fallback REST actif) : {str(e)[:100]}")
                # FAST PATH (Steven 30/07, latence detection->achat) : seul le
                # solde USDC compte ici -> evite les 2 appels reseau inutiles
                # (gas POL + valeur proxy data-api, jusqu'a 10s de timeout) que
                # status() faisait en sequence pour un resultat non utilise.
                cash = self._live.get_cash_usdc_fast()
                if cash is not None:
                    self._last_good_cash = cash
                    self._cash_ts = time.time()
                    if self._session_start_cash is None:
                        self._session_start_cash = cash
                    self._check_global_killswitch(cash)
                    return cash, "ok"
            except Exception as e:
                err = str(e)[:150]
                time.sleep(0.5)
        if self._last_good_cash is not None:
            return self._last_good_cash, "cache (lecture live instable)"
        return None, "lecture solde echouee"

    def _record_execution_quality(
        self, sym, slug, edge_pct, ev_net_fees_pct, feed_age_ms, filled: bool,
        ev_net_slippage_pct=None, fill_pct=None,
    ):
        """Fill ratio / EV net de fees+slippage / fraicheur des donnees /
        partial-fill (Steven 04/08). Purement de la capture, jamais dans le
        chemin de decision."""
        self.state.setdefault("execution_quality_history", []).append({
            "ts": time.time(),
            "symbol": sym,
            "slug": slug,
            "edge_pct": edge_pct,
            "ev_net_fees_pct": ev_net_fees_pct,
            "ev_net_slippage_pct": ev_net_slippage_pct,
            "feed_age_ms": feed_age_ms,
            "filled": filled,
            "fill_pct": fill_pct,
        })
        if len(self.state["execution_quality_history"]) > 1000:
            del self.state["execution_quality_history"][: len(self.state["execution_quality_history"]) - 1000]

    def _check_global_killswitch(self, cash: float):
        """Coupe TOUS les symboles reels si un seuil global est franchi
        (Steven 04/08, "kill switch reglable" avant de laisser tourner sans
        surveillance). Appele a chaque lecture de cash reussie (~chaque scan).
        Ne se redeclenche pas une fois deja triggered -> pas besoin de spammer
        les logs, l'etat reste visible via GET /api/killswitch jusqu'a reset
        manuel (qui remet aussi les modes a "off", jamais "real" automatiquement)."""
        ks = self.state.get("killswitch") or dict(KILLSWITCH_DEFAULTS)
        if not ks.get("enabled", True) or self.state.get("killswitch_triggered"):
            return
        reason = None
        if cash < ks.get("cash_floor_usd", 3.0):
            reason = f"cash {cash:.2f}$ < plancher {ks['cash_floor_usd']:.2f}$"
        elif (
            self._session_start_cash is not None
            and (self._session_start_cash - cash) > ks.get("max_session_loss_usd", 15.0)
        ):
            perte = self._session_start_cash - cash
            reason = f"perte session {perte:.2f}$ > max {ks['max_session_loss_usd']:.2f}$"
        elif self._global_consec_losses >= ks.get("max_global_consec_losses", 5):
            reason = f"{self._global_consec_losses} pertes reelles d'affilee (tous symboles)"
        if reason:
            any_real = any(m == "real" for m in self.state["modes"].values())
            for s in SYMBOLS:
                self.state["modes"][s] = "off"
            self.state["killswitch_triggered"] = {"reason": reason, "ts": time.time()}
            if any_real:
                self._log(f"⛔ [KILL-SWITCH] DECLENCHE : {reason} -> TOUS les symboles passes a 'off'")
            self._save()
            # AUDIT DB (Steven 04/08, "on utilise la DB de DetailDesk") : best-
            # effort, EN ARRIERE-PLAN, apres coup -> l'arret reel (ci-dessus)
            # ne depend JAMAIS de la DB. Si Postgres est injoignable, le
            # kill-switch a quand meme fait son travail, seul le journal
            # d'audit est manquant (log local en secours dans ce cas).
            try:
                self._pool.submit(self._audit_killswitch_to_db, reason, cash)
            except Exception:
                pass

    def _audit_killswitch_to_db(self, reason: str, cash: float):
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            return
        try:
            import uuid

            import psycopg2

            conn = psycopg2.connect(dsn, connect_timeout=5)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        'INSERT INTO "mmtrade_killswitch_events" (id, reason, cash_at_trigger) '
                        "VALUES (%s, %s, %s)",
                        (str(uuid.uuid4()), reason, cash),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            self._log(f"⚠️ [KILL-SWITCH] audit DB echoue (non bloquant) : {str(e)[:120]}")

    # ── PERSISTANCE PARAMETRES (Steven 04/08, "on save nos params en DB") ──
    # Uniquement plancher + seuils kill-switch : petit volume, ecritures
    # rares, jamais dans le hot path. PAS le journal (ecrit a chaque tick,
    # une DB synchrone ici ajouterait de la latence dans la boucle live) ni
    # l'etat de trading complet (positions/trades, encore instable). Survit
    # meme si le volume Railway est perdu (deja arrive cette nuit -> log
    # entierement efface a un redemarrage).
    def _db_save_config_state(self):
        """Best-effort, TOUJOURS en arriere-plan (voir appelants via
        self._pool.submit) -- une ecriture Postgres ratee ne doit jamais
        empecher le changement de parametre de s'appliquer localement."""
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            return
        ks = self.state.get("killswitch") or {}
        try:
            import psycopg2

            conn = psycopg2.connect(dsn, connect_timeout=5)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO "mmtrade_config_state"
                            (id, floor_usd, killswitch_enabled, killswitch_cash_floor_usd,
                             killswitch_max_session_loss_usd, killswitch_max_global_consec_losses, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (id) DO UPDATE SET
                            floor_usd = EXCLUDED.floor_usd,
                            killswitch_enabled = EXCLUDED.killswitch_enabled,
                            killswitch_cash_floor_usd = EXCLUDED.killswitch_cash_floor_usd,
                            killswitch_max_session_loss_usd = EXCLUDED.killswitch_max_session_loss_usd,
                            killswitch_max_global_consec_losses = EXCLUDED.killswitch_max_global_consec_losses,
                            updated_at = now()
                        """,
                        (
                            "current",
                            self.floor(),
                            bool(ks.get("enabled", True)),
                            float(ks.get("cash_floor_usd", 3.0)),
                            float(ks.get("max_session_loss_usd", 15.0)),
                            int(ks.get("max_global_consec_losses", 5)),
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            self._log(f"⚠️ [CONFIG] sauvegarde DB echouee (non bloquant) : {str(e)[:120]}")

    def _db_log_config_event(self, kind: str, detail: str):
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            return
        try:
            import uuid

            import psycopg2

            conn = psycopg2.connect(dsn, connect_timeout=5)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        'INSERT INTO "mmtrade_config_events" (id, kind, detail) VALUES (%s, %s, %s)',
                        (str(uuid.uuid4()), kind, detail),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            self._log(f"⚠️ [CONFIG] audit DB echoue (non bloquant) : {str(e)[:120]}")

    def _db_load_config_state(self):
        """Appele UNE FOIS au demarrage (__init__), avant que quoi que ce
        soit d'autre tourne -- restaure floor/killswitch depuis Postgres si
        le fichier local etait absent/perdu (volume Railway manquant ou mal
        monte). Le fichier local reste prioritaire s'il existe deja avec des
        valeurs (voir appel conditionnel dans __init__)."""
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            return None
        try:
            import psycopg2

            conn = psycopg2.connect(dsn, connect_timeout=5)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        'SELECT floor_usd, killswitch_enabled, killswitch_cash_floor_usd, '
                        'killswitch_max_session_loss_usd, killswitch_max_global_consec_losses '
                        'FROM "mmtrade_config_state" WHERE id = %s',
                        ("current",),
                    )
                    row = cur.fetchone()
            finally:
                conn.close()
            if not row:
                return None
            return {
                "floor_usd": row[0],
                "killswitch": {
                    "enabled": row[1],
                    "cash_floor_usd": row[2],
                    "max_session_loss_usd": row[3],
                    "max_global_consec_losses": row[4],
                },
            }
        except Exception:
            return None  # jamais fatal -- le bot demarre quand meme avec les defauts locaux

    def precheck(self):
        cash, msg = self._read_cash()
        if cash is None:
            return {"ok": False, "message": msg}
        fl = self.floor()
        return {
            "ok": cash >= fl + 0.1,
            "cash_usdc": cash,
            "message": "pret" if cash >= fl + 0.1 else f"solde <= plancher {fl}$",
        }

    # ── controle ──
    def is_running(self):
        return self._thread is not None and self._thread.is_alive()

    def set_mode(self, sym, mode):
        if sym in SYMBOLS and mode in ("real", "paper", "off"):
            old = self.state["modes"].get(sym)
            self.state["modes"][sym] = mode
            self._save()
            self._log(f"⚙️ mode {sym} -> {mode}")
            try:
                self._pool.submit(self._db_log_config_event, "mode", f"{sym} {old} -> {mode}")
            except Exception:
                pass
            return {"ok": True}
        return {"ok": False, "message": "marche/mode invalide"}

    def set_opportunity(self, sym, enabled):
        """ON/OFF (Steven 22/07) : autorise ou coupe le sizing KELLY agressif
        (grosse mise sur entree pas chere a forte conviction) pour ce marche,
        independamment du mode reel/paper/off. OFF -> sizing plat/conservateur
        (cf. _budget_usd et la branche paper) meme si l'edge Kelly serait grand."""
        if sym not in SYMBOLS:
            return {"ok": False, "message": "marche invalide"}
        self.state.setdefault("opportunity", {})[sym] = bool(enabled)
        self._save()
        self._log(f"⚙️ opportunité {sym} -> {'ON' if enabled else 'OFF'}")
        return {"ok": True}

    def _risk_free_on(self, sym):
        return bool(self.state.get("risk_free", {}).get(sym, False))

    def set_risk_free(self, sym, enabled):
        """RISK-FREE ON/OFF (Steven 29/07, bouton dashboard) : quand actif sur
        un symbole, seul l'arb garanti (ARB-BYPASS, 2 jambes simultanees,
        profit fige quel que soit le resultat) est autorise -> le hedge favori/
        underdog et le near-certain directionnel sont SKIPPES. Moins de trades,
        mais chacun sans risque directionnel -> WR proche de 100% par design
        (le seul risque residuel est l'echec d'execution, pas le marche)."""
        if sym not in SYMBOLS:
            return {"ok": False, "message": "marche invalide"}
        self.state.setdefault("risk_free", {})[sym] = bool(enabled)
        self._save()
        self._log(f"⚙️ RISK-FREE {sym} -> {'ON (arb garanti uniquement)' if enabled else 'OFF'}")
        return {"ok": True}

    def set_ultrapoly(self, enabled):
        """ULTRAPOLY ON/OFF (Steven 22/07) : active le scanner d'arb sur TOUT
        Polymarket. Persiste."""
        self.state["ultrapoly"] = bool(enabled)
        self._save()
        self._log(f"🌐 ULTRAPOLY -> {'ON' if enabled else 'OFF'}")
        return {"ok": True, "ultrapoly": self.state["ultrapoly"]}

    def set_ultrapoly_real(self, enabled):
        """ULTRAPOLY REEL ON/OFF (Steven 23/07) : les arbs detectes sur tout
        Polymarket (hors crypto) sont executes en ARGENT REEL, avec preflight
        + cap plus stricts que le paper (univers de marches bien plus varie
        en qualite/liquidite que les 5 crypto deja valides)."""
        self.state["ultrapoly_real"] = bool(enabled)
        self._save()
        self._log(f"🌐 ULTRAPOLY REEL -> {'ON' if enabled else 'OFF'}")
        return {"ok": True, "ultrapoly_real": self.state["ultrapoly_real"]}

    def set_deltaneutral(self, enabled):
        """DELTA-NEUTRE ON/OFF (Steven 23/07) : poste un bid des DEUX cotes des
        marches crypto pour capturer l'arb-au-bid (combined < 1) via le WS."""
        self.state["dn_enabled"] = bool(enabled)
        self._save()
        self._log(f"⚖️ DELTA-NEUTRE -> {'ON' if enabled else 'OFF'}")
        if not enabled:
            self._dn_cancel_all()
        return {"ok": True, "dn_enabled": self.state["dn_enabled"]}

    def set_marketmaker(self, enabled):
        """MARKET MAKER ON/OFF (Steven 23/07) : active la cotation bid/ask
        conditionnelle. Un kill switch deja arme (perte du jour / adverse
        fills) n'est PAS leve par ce toggle -> il faut le reset explicitement
        (reset_mm_kill) pour eviter de relancer aveuglement apres un arret
        automatique."""
        self.state["mm"]["enabled"] = bool(enabled)
        self._save()
        self._log(f"🎯 MARKET MAKER -> {'ON' if enabled else 'OFF'}")
        if not enabled:
            self._mm_cancel_all()
        return {
            "ok": True,
            "enabled": self.state["mm"]["enabled"],
            "killed": self.state["mm"]["killed"],
        }

    def reset_mm_kill(self):
        """Releve le kill switch MM (perte du jour ou fills adverses en serie)
        APRES revue manuelle -> jamais automatique (Steven : garde-fou explicite)."""
        self.state["mm"]["killed"] = False
        self.state["mm"]["kill_reason"] = None
        self.state["mm"]["consec_adverse"] = 0
        self._save()
        self._log("🎯 [MM] kill switch releve manuellement")
        return {"ok": True}

    # ── WATCHDOG FILL-RATE (Steven 25/07) ──
    def _check_fill_watchdog(self, sym: str) -> tuple[bool, str]:
        """Vérifie si le symbole est en cooldown à cause de trop d'échecs de 2e jambe.
        Retourne (allowed, reason).
        DESACTIVE (Steven 30/07, "retire tous les cooldown existants") : ne
        bloque plus jamais, retourne toujours (True, "")."""
        return True, ""
        wd = self._fill_watchdog.get(sym)
        if not wd:
            return True, ""
        now = time.time()
        # Nettoyer la fenêtre (garder seulement les FILL_RATE_WINDOW dernières tentatives)
        if len(wd.get("attempts", [])) > FILL_RATE_WINDOW:
            wd["attempts"] = wd["attempts"][-FILL_RATE_WINDOW:]
        # Vérifier cooldown
        if wd.get("cooldown_until", 0) > now:
            return (
                False,
                f"watchdog cooldown jusqu'à {datetime.fromtimestamp(wd['cooldown_until']).strftime('%H:%M:%S')}",
            )
        # Compter les échecs récents
        recent_fails = sum(1 for a in wd.get("attempts", []) if not a)
        if recent_fails >= FILL_RATE_MAX_FAILS:
            wd["cooldown_until"] = now + FILL_RATE_COOLDOWN_S
            self._log(
                f"🛑 [WATCHDOG] {sym} mis en cooldown {FILL_RATE_COOLDOWN_S}s : {recent_fails}/{FILL_RATE_WINDOW} échecs 2e jambe"
            )
            return (
                False,
                f"watchdog: {recent_fails}/{FILL_RATE_WINDOW} échecs -> cooldown {FILL_RATE_COOLDOWN_S}s",
            )
        return True, ""

    def _record_hedge_attempt(self, sym: str, second_leg_filled: bool):
        """Enregistre le résultat d'une tentative de hedge (2e jambe)."""
        if sym not in self._fill_watchdog:
            self._fill_watchdog[sym] = {"attempts": [], "cooldown_until": 0}
        self._fill_watchdog[sym]["attempts"].append(second_leg_filled)
        # Garder seulement la fenêtre
        if len(self._fill_watchdog[sym]["attempts"]) > FILL_RATE_WINDOW:
            self._fill_watchdog[sym]["attempts"] = self._fill_watchdog[sym]["attempts"][
                -FILL_RATE_WINDOW:
            ]

    # ── LIMITES RISQUE JOURNALIÈRES / HORAIRES (Steven 25/07) ──
    def _check_risk_limits(self, sym: str, strategy: str) -> tuple[bool, str]:
        """Vérifie les limites de risque journalières/horaires par symbole/stratégie."""
        now = time.time()
        # Initialiser si besoin
        if sym not in self._risk_limits:
            self._risk_limits[sym] = {
                "daily_pnl": 0.0,
                "daily_ts": now,
                "hourly_pnl": 0.0,
                "hourly_ts": now,
                "trades_this_hour": 0,
                "hourly_trades_ts": now,
            }
        rl = self._risk_limits[sym]

        # Reset journalier
        if now - rl["daily_ts"] >= 86400:
            rl["daily_pnl"] = 0.0
            rl["daily_ts"] = now
        # Reset horaire
        if now - rl["hourly_ts"] >= 3600:
            rl["hourly_pnl"] = 0.0
            rl["hourly_ts"] = now
            rl["trades_this_hour"] = 0
            rl["hourly_trades_ts"] = now

        # Vérifier pertes
        if rl["daily_pnl"] <= -MAX_DAILY_LOSS_PER_SYM:
            return (
                False,
                f"limite perte journalière atteinte ({rl['daily_pnl']:.2f}$ <= -{MAX_DAILY_LOSS_PER_SYM}$)",
            )
        if rl["hourly_pnl"] <= -MAX_HOURLY_LOSS_PER_SYM:
            return (
                False,
                f"limite perte horaire atteinte ({rl['hourly_pnl']:.2f}$ <= -{MAX_HOURLY_LOSS_PER_SYM}$)",
            )
        if rl["trades_this_hour"] >= MAX_TRADES_PER_HOUR_PER_SYM:
            return (
                False,
                f"limite trades/heure atteinte ({rl['trades_this_hour']} >= {MAX_TRADES_PER_HOUR_PER_SYM})",
            )

        return True, ""

    def _record_trade_pnl(self, sym: str, pnl: float):
        """Enregistre le PnL d'un trade pour les limites de risque + streaks."""
        if sym not in self._risk_limits:
            return
        now = time.time()
        rl = self._risk_limits[sym]
        rl["daily_pnl"] += pnl
        rl["hourly_pnl"] += pnl
        rl["trades_this_hour"] += 1
        # ── STREAK TRACKING V3.2 (Steven 27/07) ──
        if pnl > 0:
            rl["consec_wins"] = rl.get("consec_wins", 0) + 1
            rl["consec_losses"] = 0
        elif pnl < 0:
            rl["consec_losses"] = rl.get("consec_losses", 0) + 1
            rl["consec_wins"] = 0
        else:
            rl["consec_wins"] = 0
            rl["consec_losses"] = 0
        # STREAK GLOBALE (Steven 04/08, kill-switch) : tous symboles confondus,
        # contrairement a rl["consec_losses"] qui ne suit qu'UN symbole. Un
        # trade neutre (pnl==0) ne casse pas la streak (souvent un unwind a
        # cout nul, pas un signal de retournement).
        if self.state["modes"].get(sym) == "real":
            if pnl > 0:
                self._global_consec_losses = 0
            elif pnl < 0:
                self._global_consec_losses += 1

    # ── SIZING ADAPTATIF (Steven 25/07) ──
    def _adaptive_size(
        self, sym: str, token_id: str, base_budget_usd: float, max_entry: float
    ) -> float:
        """Ajuste la taille selon la liquidité du carnet (profondeur + spread)."""
        if self._live is None:
            return base_budget_usd
        try:
            book = self._live.get_book_sync(token_id)
            if not book or not book.get("asks") or not book.get("bids"):
                return base_budget_usd
            best_ask, ask_sz = book["asks"][0]
            best_bid, bid_sz = book["bids"][0]
            spread_bps = (best_ask - best_bid) / max(best_bid, 0.001) * 10000
            depth_ratio = min(ask_sz, bid_sz) / max(
                base_budget_usd / max(best_ask, 0.01), 1
            )

            # Réduire si spread trop large ou profondeur insuffisante
            if (
                spread_bps > SIZING_MAX_SPREAD_BPS
                or depth_ratio < SIZING_MIN_DEPTH_RATIO
            ):
                reduced = base_budget_usd * SIZING_REDUCTION_FACTOR
                self._tlog(
                    f"sizing_{sym}",
                    f"📉 [SIZING] {sym} réduit: spread={spread_bps:.0f}bps depth_ratio={depth_ratio:.2f} -> {reduced:.2f}$",
                    every=30.0,
                )
                return reduced
            # Augmenter si liquidite EXCELLENTE (spread tres serre + carnet
            # tres profond) -- symetrique a la reduction ci-dessus, jamais fait
            # avant ce soir. Reste borne par HARD_CAP_USD/MAX_FRACTION plus
            # loin dans la chaine d'appel (_budget_usd), donc sans risque de
            # depasser les gardes-fous existants.
            if (
                spread_bps < SIZING_BOOST_MAX_SPREAD_BPS
                and depth_ratio > SIZING_BOOST_MIN_DEPTH_RATIO
            ):
                boosted = base_budget_usd * SIZING_BOOST_FACTOR
                self._tlog(
                    f"sizing_{sym}",
                    f"📈 [SIZING] {sym} booste: spread={spread_bps:.0f}bps depth_ratio={depth_ratio:.2f} -> {boosted:.2f}$",
                    every=30.0,
                )
                return boosted
            # CAS NEUTRE (Steven 05/08) : loggue aussi, throttle large -- avant,
            # seuls reduce/boost etaient visibles, donc aucune donnee sur la
            # distribution reelle du carnet pour recalibrer plus tard. Sert
            # uniquement a batir un historique, n'affecte jamais le sizing.
            self._tlog(
                f"sizing_neutral_{sym}",
                f"➖ [SIZING] {sym} neutre: spread={spread_bps:.0f}bps depth_ratio={depth_ratio:.2f}",
                every=60.0,
            )
        except Exception:
            pass
        return base_budget_usd

    def _edge_based_sizing(
        self,
        sym: str,
        combined: float,
        pair: str,
        base_shares: float,
        secs_left: float = 300,
        binance_arb: bool = False,
    ) -> tuple[float, str]:
        """Tiered sizing V3.2 (Steven 27/07) — position asymetrique + dynamique.
        FRAGILE = $0.50/cote (test, pas PnL carrier)
        NORMAL  = $1.00/cote (base quotidienne)
        PREMIUM = $1.50-2.00/cote (gros gains)
        Facteurs dynamiques : momentum, serie W/L, PnL flottant.
        Retourne (target_shares, tier_label).
        Si edge < EDGE_REDUCE_THRESHOLD -> SKIP."""
        from core.btc_updown import momentum as _momentum

        edge = max(0.0, 1.0 - combined)
        if edge < EDGE_REDUCE_THRESHOLD:
            return 0.0, "SKIP"

        # ── TIER DETECTION via _detect_setup_tier ──
        depth_ok = True  # preflight rejettera si trop fin
        tier = self._detect_setup_tier(sym, edge, secs_left, binance_arb, depth_ok)

        # ── BUDGET $ PAR JAMBE SELON TIER ──
        budget_usd = self._tier_sizing(tier, combined)
        mult = 1.0

        # ── FACTEUR VITESSE MARCHÉ (Steven 27/07) ──
        mom = _momentum(pair) if pair else None
        if mom and mom.get("confirms"):
            fast = abs(mom.get("fast_pct_s", 0))
            if fast > 0.10:
                mult *= DYN_SPEED_BOOST
                tier += "+SPEED"
            elif fast > 0.05:
                mult *= BINANCE_MOMENTUM_BOOST
                tier += "+MOM"

        # ── FACTEUR SÉRIE W/L (Steven 27/07) ──
        rl = self._risk_limits.get(sym, {})
        consec_wins = rl.get("consec_wins", 0)
        consec_losses = rl.get("consec_losses", 0)
        if consec_wins >= 3:
            mult *= DYN_STREAK_BONUS
            tier += f"+W{consec_wins}"
        elif consec_losses >= 2:
            mult *= DYN_STREAK_PENALTY
            tier += f"-L{consec_losses}"

        # ── FACTEUR PNL FLOTTANT (Steven 27/07) ──
        mk = self.state.get("markets", {}).get(sym, {})
        open_poss = mk.get("open", {})
        total_float = sum(
            (p.get("filled_shares", 0) * p.get("entry_price", 0)) * -1
            + p.get("filled_shares", 0) * (p.get("entry_price", 0))
            for p in open_poss.values()
            if p.get("strat") == "bothside"
        )
        if total_float < -10:
            mult *= DYN_FLOATING_REDUCE
            tier += "-FLOAT"

        # ── DANGER SCORE ──
        try:
            from core.btc_updown import danger_score as _ds, _strike_at as _sa

            strike = _sa(pair, 0)
            d = _ds(pair, strike) if strike else 0
            if d > 50:
                mult *= BINANCE_DANGER_REDUCE
                tier += "-DNG"
        except Exception:
            pass

        # ── PLAFONNEMENT DU MULTIPLIATEUR ──
        mult = min(mult, DYN_MAX_FLOORED_MULT)
        budget_usd = round(budget_usd * mult, 2)

        # ── CONVERSION $ -> SHARES (floor: MIN_ORDER_SIZE) ──
        avg_price = combined / 2.0
        if avg_price > 0.05:
            target = round(budget_usd / avg_price, 2)
        else:
            target = base_shares
        target = max(MIN_ORDER_SIZE_SHARES, target)

        self._log(
            f"📊 [SIZING-V3.2] {sym} edge={edge * 100:.1f}% tier={tier} "
            f"mult={mult:.2f} budget=${budget_usd:.2f}/jambe -> {target:.1f} parts "
            f"(avg_px={avg_price:.3f} streak=W{consec_wins}/L{consec_losses})"
        )
        return target, tier

    # ════════════════════════════════════════════════════════════════════════
    # ── GHOST V3.1 — METHODES (Steven 26/07) ──
    # ════════════════════════════════════════════════════════════════════════

    # ── AXE 1 : PRE-FLIGHT + DEAD MARKET ──
    def _preflight_valid(self, sym, slug, quotes, outcomes, mode):
        """Verifie pre-conditions avant tout ordre. Retourne (ok, reason, comb_ask).
        ok=False -> pas d'entree. ok=True -> comb_ask retourne pour sizing."""
        # Dead market check : un cote < 0.05 = marche probablement resolu
        for side_q in outcomes:
            _, ask_q, _ = quotes.get(side_q, (None, None, None))
            if ask_q is not None and ask_q < DEAD_MARKET_THRESHOLD:
                self._log(
                    f"⛔ [DEAD-MARKET] {sym} {slug} {side_q}={ask_q:.3f} "
                    f"< {DEAD_MARKET_THRESHOLD} -> SKIP (marche mort)"
                )
                return False, "dead_market", 0.0
        # Verification des deux cotes disponibles
        asks = []
        for side_q in outcomes:
            _, ask_q, _ = quotes.get(side_q, (None, None, None))
            if ask_q is None or ask_q <= 0 or ask_q >= 1:
                return False, "no_quote", 0.0
            asks.append(ask_q)
        comb_ask = sum(asks)
        # FIX SIGNE (Steven 04/08, analyse histo Polymarket reelle) : le seuil
        # etait 1.00 + fee + margin = 1.025 -> il AUTORISAIT l'achat d'un pack
        # a 1.02 alors qu'un pack Up+Down rapporte EXACTEMENT 1.00 a la
        # resolution = perte mathematiquement garantie. Les frais sont un
        # COUT, pas un bonus : il faut comb + frais < 1.00, donc le seuil doit
        # etre SOUS 1.00. Preuve chiffree : 217 marches executes a comb>=1.00
        # => -29.26$ (WR 30%), contre 19 marches a comb<1.00 avec parts
        # equilibrees => +5.54$ (WR 100%). Trace directe cette nuit : SOL
        # 1785803700 achete 3x en 75s a comb_ask=1.020 edge=0.0% -> -0.546$.
        comb_max = 1.00 - COMB_ASK_FEE_ESTIMATE - COMB_ASK_TINY_MARGIN
        if comb_ask > comb_max:
            self._tlog(
                f"skip_comb_{sym}",
                f"📎 [PRE-FLIGHT] {sym} {slug} comb_ask={comb_ask:.3f} "
                f"> {comb_max:.3f} (1.00+fee+margin) -> SKIP",
            )
            return False, "comb_too_high", comb_ask
        return True, "ok", comb_ask

    def _dead_market_check(self, quotes, outcomes):
        """Quick check : retourne True si le marche est vivant."""
        for side_q in outcomes:
            _, ask_q, _ = quotes.get(side_q, (None, None, None))
            if ask_q is not None and ask_q < DEAD_MARKET_THRESHOLD:
                return False
        return True

    # ── AXE 2 : COOLDOWN + TIMEOUT ADAPTATIF ──
    def _in_arb_bypass_cooldown(self, sym, slug, mk):
        """Cooldown DEDIE et COURT pour l'arb garanti (Steven 29/07, "on est pas
        cense loupe d'ocasion") : le cooldown partage (_in_cooldown, 2-5min)
        est concu pour les strategies RISQUEES (protege d'un marche instable).
        Un echec d'arb garanti ne coute RIEN (fix atomique du 29/07 : jambe
        seule revendue immediatement) -> pas de raison de punir 2-5 minutes.
        Court delai (3s) juste pour eviter de re-spammer le MEME instant.
        DESACTIVE (Steven 30/07, "retire tous les cooldown existants") :
        retourne toujours (False, "") -> jamais bloque."""
        return False, ""

    def _set_arb_bypass_cooldown(self, sym, slug, mk, secs=3.0):
        mk.setdefault("arb_bypass_cooldowns", {})[slug] = time.time() + secs

    def _in_cooldown(self, sym, slug, mk):
        """Verifie si un slug ou symbole est en cooldown post-abort.
        DESACTIVE (Steven 30/07, "retire tous les cooldown existants") :
        retourne toujours (False, "") -> jamais bloque."""
        return False, ""

    def _set_slug_cooldown(self, sym, slug, mk):
        """Active le cooldown post-abort pour un slug."""
        now = time.time()
        mk.setdefault("cooldowns", {})[slug] = now + SLUG_COOLDOWN_SECS

    def _record_abort(self, sym, mk):
        """Enregistre un abort. Incremente le compteur et active cooldown symbole si >= MAX."""
        mk["consec_aborts"] = mk.get("consec_aborts", 0) + 1
        if mk["consec_aborts"] >= MAX_CONSEC_ABORTS:
            mk["symbol_cooldown_until"] = time.time() + SYMBOL_ABORT_COOLDOWN_SECS
            self._log(
                f"🛑 [COOLDOWN] {sym} {mk['consec_aborts']} aborts consec -> "
                f"pause {SYMBOL_ABORT_COOLDOWN_SECS}s"
            )

    def _reset_abort_counter(self, sym, mk):
        """Reset le compteur d'abort sur un win."""
        mk["consec_aborts"] = 0

    def _adaptive_timeout(self, mk):
        """Calcule le timeout dynamique base sur les latences recentes."""
        lats = mk.get("latence_history", [])
        if len(lats) < 3:
            return FORCE_PAIR_TIMEOUT_BASE + 1.0  # 2s par defaut
        sorted_lats = sorted(lats)
        median_lat = sorted_lats[len(sorted_lats) // 2]
        return max(FORCE_PAIR_TIMEOUT_BASE, FORCE_PAIR_TIMEOUT_MULT * median_lat)

    def _record_force_pair_latency(self, mk, seconds):
        """Enregistre une latence de force-pair pour le timeout adaptatif."""
        hist = mk.setdefault("latence_history", [])
        hist.append(seconds)
        if len(hist) > LATENCE_HISTORY_SIZE:
            hist.pop(0)

    # ── AXE 7 : TAGS DE PERTES ──
    def _classify_loss(self, pos, reason=""):
        """Determine le tag de perte pour un trade perdant."""
        legs = pos.get("legs", [])
        pnl = pos.get("pnl", 0)
        if pnl >= 0:
            return None  # pas une perte
        # ORPHAN : 1 jambe seule
        if len(legs) == 1:
            return "ORPHAN"
        # Tags lies a la raison de sortie
        if "FORCE-PAIR" in reason or "force_pair" in reason.lower():
            return "FORCE_SELL"
        if "DEAD-MARKET" in reason or "dead_market" in reason.lower():
            return "DEAD_MARKET"
        if "ABORT" in reason or "abort" in reason.lower():
            return "FORCE_ABORT"
        if "STOP" in reason or "stop" in reason.lower():
            return "STOP_HIT"
        # ARB_NEGATIVE : combine > 1.00 a l'entree
        cost = pos.get("cost", 0)
        shares = pos.get("filled_shares", 0)
        if shares > 0 and cost / (shares / 2) > 1.00:
            return "ARB_NEGATIVE"
        # FEE_DRAG : perte tres petite (entre -0.05 et 0)
        if -0.05 < pnl < 0:
            return "FEE_DRAG"
        # SLIPPAGE : perte moyenne sans raison evidente
        return "UNKNOWN"

    # ── AXE 8 : LOGGING ENRICHI ──
    def _log_trade_entry(
        self,
        sym,
        slug,
        side,
        mode,
        strat,
        tier,
        entry,
        budget,
        comb_ask,
        edge,
        d_remain,
    ):
        """Log structure d'entree."""
        self._log(
            f"📥 [ENTRY] {sym} {slug} {side} mode={mode} strat={strat} "
            f"tier={tier} entry={entry:.3f} budget={budget:.2f}$ "
            f"comb_ask={comb_ask:.3f} edge={edge * 100:.1f}% d={d_remain:.0f}s"
        )

    def _log_trade_exit(
        self,
        sym,
        slug,
        side,
        reason,
        entry,
        exit_px,
        pnl_brut,
        fees,
        slippage,
        pnl_net,
        duree_s,
        jambe1_statut,
        jambe2_statut,
        loss_tag=None,
    ):
        """Log structure de sortie."""
        tag_str = f" loss_tag={loss_tag}" if loss_tag else ""
        self._log(
            f"📤 [EXIT] {sym} {slug} {side} reason={reason} "
            f"entry={entry:.3f} exit={exit_px:.3f} "
            f"pnl_brut={pnl_brut:+.3f}$ fees={fees:.3f}$ "
            f"slip={slippage:.3f}$ pnl_net={pnl_net:+.3f}$ "
            f"duree={duree_s:.0f}s j1={jambe1_statut} j2={jambe2_statut}"
            f"{tag_str}"
        )

    def _write_trade_jsonl(self, trade_data):
        """Ecrit un trade en JSONL pour analyse."""
        try:
            jsonl_path = ROOT / "data" / "trades_v31.jsonl"
            with open(jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(trade_data, default=str) + "\n")
        except Exception:
            pass

    # ── AXE 3 : TAILLE VARIABLE SELON SIGNAL ──
    def _detect_setup_tier(self, sym, edge, d, binance_confirms, depth_ok):
        """Determine le tier du setup (fragile/normal/premium)."""
        from core.btc_updown import momentum as _momentum

        mom = _momentum(sym) if sym in ("BTC", "ETH", "SOL", "XRP", "DOGE") else None
        confirms = mom and mom.get("confirms", False) if mom else False

        if (
            edge >= TIER_EDGE_PREMIUM
            and (confirms or binance_confirms)
            and d > TIER_D_PREMIUM
            and depth_ok
        ):
            return "premium"
        elif edge >= TIER_EDGE_NORMAL and d > TIER_D_NORMAL:
            return "normal"
        else:
            return "fragile"

    def _tier_sizing(self, tier, combined):
        """Retourne le budget $ par jambe selon le tier."""
        if tier == "premium":
            edge = max(0.0, 1.0 - combined)
            if edge > 0.15:
                return TIER_SIZE_ULTRA
            return TIER_SIZE_PREMIUM
        elif tier == "normal":
            return TIER_SIZE_NORMAL
        else:
            return TIER_SIZE_FRAGILE

    # ── AXE 5 : STOPS LOGIQUES ──
    def _check_daily_stop(self, mk):
        """Verifie les stops journaliers. Retourne (should_stop, reason)."""
        daily_pnl = mk.get("daily_pnl", 0)
        if daily_pnl < 0 and abs(daily_pnl) > FLOOR_USD * STOP_DAILY_LOSS_PCT:
            return (
                True,
                f"perte journaliere {daily_pnl:+.2f}$ > {STOP_DAILY_LOSS_PCT * 100:.0f}% bankroll",
            )
        consec = mk.get("consec_losses", 0)
        if consec >= STOP_CONSEC_GLOBAL_V31:
            return True, f"{consec} pertes consecutives globales"
        return False, ""

    def _check_symbol_stop(self, mk):
        """Verifie les stops par symbole. Retourne (should_pause, reason)."""
        consec = mk.get("consec_losses", 0)
        if consec >= STOP_CONSEC_SYMBOL_V31:
            return True, f"{consec} pertes consecutives sur ce symbole"
        return False, ""

    def _should_emergency_exit(self, pos, sym, now=None):
        """Determine si une position doit etre exit immediatement."""
        if now is None:
            now = time.time()
        # Fenetre trop proche et position perdante
        secs_left = pos.get("end_ts", now) - now
        if secs_left < STOP_WINDOW_REMAIN_SECS:
            entry = pos.get("entry_price", 0)
            if entry > 0:
                try:
                    from ghost_poly.live import PolyLive as _PL

                    tid = pos.get("token_id")
                    if tid:
                        wb = self._ws.book(tid) if hasattr(self, "_ws") else None
                        if wb:
                            _, ask_now, _ = wb
                            if ask_now is not None and ask_now < entry * 0.85:
                                return True, "window_near_and_losing"
                except Exception:
                    pass
        return False, ""

    def raz(self):
        """RAZ (Steven 22/07) : sauvegarde l'etat courant dans data/backups/ puis
        remet a zero les stats paper (trades, P&L, balances, consec_losses) de
        TOUS les marches. Les positions ouvertes REELLES sont preservees. Le
        market_price_log est archive aussi (backtest possible sur la sauvegarde)."""
        bak_dir = ROOT / "data" / "backups"
        bak_dir.mkdir(parents=True, exist_ok=True)
        tag = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        bak_state = bak_dir / f"multi_state_{tag}.json"
        bak_log = bak_dir / f"ghost_v3_real_{tag}.log"
        shutil.copy2(STATE_FILE, bak_state)
        if LOG_FILE.exists():
            shutil.copy2(LOG_FILE, bak_log)
        for sym in list(SYMBOLS) + ["POLY"]:
            mk = self.state["markets"].get(sym)
            if not mk:
                continue
            mk["trades"] = []
            mk["consec_losses"] = 0
            mk["stopped"] = False
            mk["stop_reason"] = None
            mk["paper_balance"] = PAPER_START_BAL
            mk["market_price_log"] = {}
            real_open = {k: v for k, v in mk["open"].items() if v.get("mode") == "real"}
            mk["open"] = real_open
        # MM : reset des stats (fills, pnl du jour), PAS de l'inventaire reel
        # (des positions Up/Down peuvent etre encore ouvertes on-chain).
        self.state["mm"]["fills"] = []
        self.state["mm"]["daily_pnl"] = 0.0
        self.state["mm"]["daily_pnl_date"] = None
        self.state["mm"]["consec_adverse"] = 0
        self._save()
        self._log(f"🔄 RAZ effectuee — backup {bak_state.name}")
        return {"ok": True, "backup": bak_state.name}

    def fetch_real_history(self):
        """Recupere l'historique reel depuis Polymarket data-api (public, lecture
        seule) et le renvoie formate pour le dashboard.

        FIX CRITIQUE (Steven 04/08, "eth la par ex a fait bien + que 20c de
        gain hein") : les evenements type=REDEEM (paiement quand une position
        gardee jusqu'a resolution GAGNE) ont price=0 dans le flux Polymarket,
        mais un champ usdcSize = le vrai montant paye. L'ancienne version
        utilisait size*price pour TOUT (y compris REDEEM), ce qui comptait
        chaque gain resolu par redemption comme 0$ -- sous-estimant fortement
        les gains reels. Verifie : reconstruction manuelle sur 500 activites
        a trouve +144.995$ de gains reels (89 jambes) contre -195.113$ de
        pertes (134 jambes), net -50.118$ -- l'ancienne version aurait
        entierement rate les +144.995$."""
        import requests as _rq
        from collections import defaultdict

        funder = os.environ.get("POLY_FUNDER_ADDRESS", "")
        if not funder:
            return {"ok": False, "trades": [], "error": "pas de FUNDER_ADDRESS"}
        try:
            r = _rq.get(
                "https://data-api.polymarket.com/activity",
                params={"user": funder, "limit": "500"},
                timeout=12,
                headers={"User-Agent": "GHOST/3"},
            )
            raw = r.json() if r.status_code == 200 else []
            if not isinstance(raw, list):
                raw = []
        except Exception as e:
            return {"ok": False, "trades": [], "error": str(e)[:120]}

        trades = []
        groups = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "redeem": 0.0, "market": "", "outcome": ""})
        for t in raw:
            etype = t.get("type", "TRADE")
            side = t.get("side") or ("REDEEM" if etype == "REDEEM" else "?")
            size = float(t.get("size") or 0)
            price = float(t.get("price") or 0)
            # usdcSize = montant USDC reel qui a bouge on-chain -- plus precis
            # que size*price (qui vaut 0 pour un REDEEM) et evite l'arrondi
            # de `price` (souvent tronque genre 0.7599999406).
            usdc = float(t.get("usdcSize") or round(size * price, 4))
            market = t.get("title", t.get("market", "?"))
            outcome = t.get("outcome", "?")
            trades.append({
                "ts": t.get("timestamp"),
                "market": market,
                "side": side,
                "type": etype,
                "outcome": outcome,
                "size": size,
                "price": price,
                "cost": round(usdc, 4),
            })
            g = groups[(t.get("slug"), outcome)]
            g["market"], g["outcome"] = market, outcome
            if etype == "REDEEM":
                g["redeem"] += usdc
            elif side.upper() == "BUY":
                g["buy"] += usdc
            elif side.upper() == "SELL":
                g["sell"] += usdc

        by_market = []
        wins_sum = losses_sum = 0.0
        wins_n = losses_n = 0
        for (slug, outcome), g in groups.items():
            net = round(g["sell"] + g["redeem"] - g["buy"], 4)
            by_market.append({
                "slug": slug, "market": g["market"], "outcome": outcome,
                "buy": round(g["buy"], 4), "sell": round(g["sell"], 4),
                "redeem": round(g["redeem"], 4), "net_pnl": net,
            })
            if net > 0.005:
                wins_sum += net
                wins_n += 1
            elif net < -0.005:
                losses_sum += net
                losses_n += 1
        by_market.sort(key=lambda r: r["net_pnl"])

        return {
            "ok": True,
            "trades": trades,
            "count": len(trades),
            "net_pnl_by_market": by_market,
            "summary": {
                "net_total": round(wins_sum + losses_sum, 4),
                "wins_count": wins_n,
                "wins_sum": round(wins_sum, 4),
                "losses_count": losses_n,
                "losses_sum": round(losses_sum, 4),
            },
        }

    def floor(self):
        """Plancher de capital COURANT (reglable depuis le dashboard, persiste).
        FLOOR_USD ne sert plus que de valeur par defaut au premier lancement."""
        try:
            return float(self.state.get("floor_usd", FLOOR_USD))
        except Exception:
            return FLOOR_USD

    def set_floor(self, value):
        try:
            v = round(float(value), 2)
        except Exception:
            return {"ok": False, "message": "valeur invalide"}
        if not (0 <= v <= 100000):
            return {"ok": False, "message": "plancher hors bornes (0-100000$)"}
        old = self.floor()
        self.state["floor_usd"] = v
        self._save()
        self._log(f"⚙️ plancher {old}$ -> {v}$")
        try:
            self._pool.submit(self._db_save_config_state)
            self._pool.submit(self._db_log_config_event, "floor", f"{old}$ -> {v}$")
        except Exception:
            pass
        return {"ok": True, "floor": v}

    def arb_budget(self):
        """Budget ARB par jambe (reglable depuis le dashboard, persiste).
        REAL_VALIDATION_LEG_USD ne sert plus que de valeur par defaut."""
        try:
            return float(self.state.get("arb_budget_usd", REAL_VALIDATION_LEG_USD))
        except Exception:
            return REAL_VALIDATION_LEG_USD

    def set_arb_budget(self, value):
        try:
            v = round(float(value), 2)
        except Exception:
            return {"ok": False, "message": "valeur invalide"}
        if not (0.5 <= v <= 50):
            return {"ok": False, "message": "budget ARB hors bornes (0.50-50$)"}
        old = self.arb_budget()
        self.state["arb_budget_usd"] = v
        self._save()
        self._log(f"⚙️ budget ARB {old}$ -> {v}$/jambe")
        return {"ok": True, "arb_budget": v}

    def start(self):
        if self.is_running():
            return {"ok": False, "message": "deja en cours"}
        cash, msg = self._read_cash()
        # on autorise le demarrage meme si BTC pas en reel (paper ne depend pas du solde)
        if any(m == "real" for m in self.state["modes"].values()):
            if cash is None:
                return {"ok": False, "message": f"solde illisible: {msg}"}
            if cash < self.floor():
                return {
                    "ok": False,
                    "message": f"solde {cash}$ sous le plancher {self.floor()}$",
                }
        # reset des stops par-marche au demarrage manuel
        for sym in SYMBOLS:
            self.state["markets"][sym]["stopped"] = False
            self.state["markets"][sym]["stop_reason"] = None
        self._log("=" * 70)
        self._log(
            f"=== DEMARRAGE MULTI-MARCHE === modes={self.state['modes']} "
            f"| plancher={self.floor()}$ stop={STOP_CONSEC_LOSSES}_pertes_consec "
            f"| cash={cash}$"
        )
        self._running.set()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        self._fast_exit_thread = threading.Thread(target=self._fast_exit_loop, daemon=True)
        self._fast_exit_thread.start()
        # COPILOTE IA (Steven 29/07) : autonome, pilote les leviers d'admin du
        # bot (mode/budget/plancher/MM/DN) via Groq -> desactive silencieux si
        # pas de GROQ_API_KEY dans .env.
        try:
            from real_web.ai_copilot import AICopilot

            self._ai_copilot = AICopilot(self)
            self._ai_copilot.start()
        except Exception as e:
            self._log(f"⚠️ [AI-COPILOT] init impossible: {e}")
        return {"ok": True, "message": "demarre"}

    def _log_real_pnl(self):
        """Affiche le PnL REEL on-chain (source Polymarket) dans le log, en
        parallele des compteurs internes (Steven 04/08). Silencieux en cas
        d'echec reseau : c'est de l'information, jamais un bloqueur."""
        try:
            since = getattr(self, "_session_start_ts", None)
            if since is None:
                since = time.time() - 6 * 3600
            r = self._live.real_pnl_since(since)
            if not r.get("ok"):
                return
            icon = "✅" if r["net_usd"] >= 0 else "❌"
            self._log(
                f"{icon} [PNL-REEL-ONCHAIN] net={r['net_usd']:+.2f}$ "
                f"(achats -{r['buy_usd']:.2f}$ / ventes +{r['sell_usd']:.2f}$ / "
                f"redeems +{r['redeem_usd']:.2f}$) "
                f"[{r['n_buys']}B {r['n_sells']}S {r['n_redeems']}R] "
                f"-- source Polymarket, pas nos compteurs"
            )
        except Exception:
            pass

    def _prewarm_order_cache(self, token_ids):
        """Prechauffe les caches tick_size/neg_risk de la lib CLOB pour des
        tokens (Steven 04/08). Ces 2 lectures sont faites AUTOMATIQUEMENT par
        create_order() et sont cachees par token_id ; comme les marches 5min
        renouvellent leurs tokens en permanence, sans prechauffage chaque
        premier ordre d'une fenetre paie 2 allers-retours reseau par jambe.
        Idempotent, silencieux, jamais bloquant : un echec ici ne fait que
        laisser le comportement d'origine (lecture au moment de l'ordre)."""
        if not self._live:
            return
        already = self._prewarmed
        todo = [t for t in token_ids if t and t not in already]
        if not todo:
            return
        try:
            c = self._live.client()
            # VERSION aussi (Steven 04/08, mesure chrono) : create_order()
            # appelle __resolve_version() -> get_version() = encore un
            # aller-retour reseau si jamais appele. Cache global (pas par
            # token), donc une seule fois suffit pour toute la session.
            try:
                c.get_version()
            except Exception:
                pass
        except Exception:
            return
        for tid in todo:
            try:
                c.get_tick_size(tid)
                c.get_neg_risk(tid)
                already.add(tid)
            except Exception:
                pass
        if len(already) > 400:  # borne memoire : tokens anciens inutiles
            self._prewarmed = set(list(already)[-200:])

    def _reconcile_open_positions(self):
        """SYNC GLOBALE (Steven 30/07, "avant on voyait tout les pos, maintenant
        parfois on ne voit rien") : le fast-exit loop SAUTE ENTIEREMENT un
        symbole des que mk["open"] est vide -> une position reelle jamais
        enregistree dans mk["open"] (bug de tracking deja croise plusieurs
        fois ce soir : mismatch, top-up, excedent) devient invisible POUR
        TOUJOURS, meme dans l'onglet Positions du dashboard. Ce balayage
        compare le compte Polymarket REEL (position_size, verite terrain) a
        ce que mk["open"] connait, pour CHAQUE marche deja suivi par le flux
        WS -> toute position reelle orpheline est retrackee, redevient
        visible et geree (vente, stop-loss, etc.)."""
        try:
            arb_markets = self._ws.get_arb_markets()
        except Exception:
            return
        for slug, meta in arb_markets.items():
            sym = next((s for s in SYMBOLS if slug.lower().startswith(s.lower())), None)
            if sym is None or self.state["modes"].get(sym) != "real":
                continue
            mk = self.state["markets"][sym]
            outcomes = meta.get("outcomes", [])
            token_ids = meta.get("token_ids", [])
            for side, tid in zip(outcomes, token_ids):
                key = f"{slug}|{side}"
                if key in mk["open"] or not tid:
                    continue
                try:
                    real_held = self._live.position_size(tid) if self._live else -1.0
                except Exception:
                    real_held = -1.0
                if real_held >= 0.5:
                    book = None
                    try:
                        book = self._live.get_book_sync(tid)
                    except Exception:
                        pass
                    px = (book["bids"][0][0] if book and book.get("bids") else 0.5)
                    self._log(
                        f"🔎 [RECONCILIATION-GLOBALE] {sym} {slug} {side} : {real_held} "
                        f"parts reelles non trackees trouvees -> retrackees (visibles/gerables)"
                    )
                    now_ts = time.time()
                    mk["open"][key] = {
                        "symbol": sym, "slug": slug, "side": side, "mode": "real",
                        "strat": "orphan", "token_id": tid, "entry_price": px,
                        "filled_shares": round(real_held, 2),
                        "cost": round(real_held * px, 2),
                        "start_ts": now_ts, "pair": None,
                        "end_ts": now_ts + 300, "opened_ts": now_ts, "buffer": 0.0,
                    }

    def _fast_exit_loop(self):
        """SL/TP RAPIDE (Steven 28/07) : verifie le prix des positions ouvertes
        via le flux WS (pas le scan REST complet, ~7-12s/symbole) toutes les
        FAST_EXIT_POLL_S secondes. C'est le seul appelant de _manage_orphans /
        _manage_pnl_tier_exits desormais -> le -30% SL a enfin une chance
        raisonnable de se declencher avant que le marche 5min ne resolve."""
        while self._running.is_set():
            try:
                try:
                    self._reconcile_open_positions()
                except Exception as e:
                    self._tlog("fastexit_reconcile_err", f"💥 [FAST-EXIT] reconciliation erreur: {e}")
                for sym in SYMBOLS:
                    mode = self.state["modes"].get(sym)
                    # FIX (regression) : limiter au reel privait le PAPER de tout
                    # SL/TP (l'appel avait ete retire du scan lent pour les DEUX
                    # modes) -> positions paper jamais coupees, meme a -90%.
                    if mode not in ("real", "paper"):
                        continue
                    if not self.state["markets"][sym]["open"]:
                        continue
                    try:
                        self._log_position_prices(sym)
                    except Exception as e:
                        self._tlog(f"fastexit_price_err_{sym}", f"💥 [FAST-EXIT] {sym} prix erreur: {e}")
                    try:
                        self._manage_orphans(sym)
                    except Exception as e:
                        self._tlog(f"fastexit_orphan_err_{sym}", f"💥 [FAST-EXIT] {sym} orphans erreur: {e}")
                    try:
                        self._manage_pnl_tier_exits(sym)
                    except Exception as e:
                        self._tlog(f"fastexit_pnl_err_{sym}", f"💥 [FAST-EXIT] {sym} pnl-exits erreur: {e}")
            except Exception as e:
                self._log(f"💥 [FAST-EXIT] erreur boucle: {e}")
            time.sleep(FAST_EXIT_POLL_S)

    def stop(self):
        self._running.clear()
        self._log("=== ARRET MANUEL (interface) ===")
        return {"ok": True}

    def _opportunity_on(self, sym):
        return bool(self.state.get("opportunity", {}).get(sym, False))

    # ── sizing KELLY FRACTIONNE 1/4 (mise PLUS sur cheap + forte conviction, via b) ──
    def _budget_usd(self, ask, sig, investable, sym=None):
        conviction = max(1.0, min(2.5, abs(sig["buffer"]) / max(sig["margin"], 1)))
        lean = (conviction - 1) / 1.5  # 0..1 : force de l'edge au-dela du seuil
        b = (1 - ask) / max(ask, 1e-6)  # cote implicite du marche
        q = min(0.995, ask + KELLY_ASSUMED_EDGE * lean)  # proba de gain estimee
        f_star = max(0.0, (b * q - (1 - q)) / b) * KELLY_FRACTION
        budget = investable * f_star
        # MULTIPLICATEUR PAR PALIER DE PRIX (Steven 04/08, "on aurait du miser
        # plus sur la position la plus chere") : analyse de 221 jambes on-chain
        # (301$ engages) a montre un ROI qui va dans le sens INVERSE de ce que
        # ce Kelly (edge fixe suppose 6%) produit -- prix<0.30 : -24.4% ROI
        # (19% win rate, 88.59$ engages) ; 0.30-0.50 : -18.2% (34%, 213.02$,
        # la PLUS GROSSE part du capital engage) ; 0.50-0.70 : -3.8% (54%) ;
        # >0.70 : +9.1% ROI (77% win rate, SEULE tranche rentable, mais
        # seulement 63.01$ engages dessus). Ce multiplicateur EMPIRIQUE
        # resserre l'allocation vers ce qui a reellement marche, en attendant
        # un vrai recalibrage de la proba/edge suppose (KELLY_ASSUMED_EDGE
        # n'a jamais ete recalibre depuis sa valeur initiale).
        if ask < 0.30:
            tier_mult = PRICE_TIER_BUDGET_MULT["below_030"]
        elif ask < 0.50:
            tier_mult = PRICE_TIER_BUDGET_MULT["p030_050"]
        elif ask < 0.70:
            tier_mult = PRICE_TIER_BUDGET_MULT["p050_070"]
        else:
            tier_mult = PRICE_TIER_BUDGET_MULT["above_070"]
        budget *= tier_mult
        # le plancher de securite reste PRIORITAIRE : on ne depasse jamais
        # l'investissable (= solde - FLOOR_USD), ni le hard cap, ni MAX_FRACTION.
        return max(
            MIN_BUDGET_USD,
            min(budget, HARD_CAP_USD, investable * MAX_FRACTION, investable),
        )

    # ── boucle principale ──
    def _loop(self):
        scan = 0
        while self._running.is_set():
            scan += 1
            tick_t0 = time.time()
            per_sym_ms = {}
            try:
                # FAST MODE (Laguna XS 24/07) : ne fetch que les coins actifs
                _active_modes = self.state["modes"]
                _active_tags = [
                    s.lower()
                    for s in SYMBOLS
                    if _active_modes.get(s, "off") not in ("off",)
                ]
                markets = find_active_markets(_active_tags if _active_tags else None)
                by_sym = {}
                _ws_tokens = []
                for m in markets:
                    p = parse_updown_market(m)
                    if p:
                        by_sym.setdefault(p["symbol"], []).append((m, p))
                        try:
                            _ws_tokens += json.loads(m.get("clobTokenIds") or "[]")
                        except Exception:
                            pass
                # DECLARE les tokens actifs au flux WebSocket -> il s'abonne et
                # pousse leur carnet en temps reel (Steven 23/07).
                if _ws_tokens:
                    self._ws.want_tokens(_ws_tokens)
                    # PRECHAUFFAGE CACHE ORDRE (Steven 04/08, "regarde mieux") :
                    # create_order() de la lib CLOB appelle get_tick_size() ET
                    # get_neg_risk() sur le token -> caches PAR TOKEN, mais les
                    # marches Up/Down changent de token TOUTES LES 5 MIN, donc
                    # cache vide a chaque fenetre = 4 allers-retours reseau (2
                    # par jambe) AVANT de pouvoir poster. Trace mesuree : BTC
                    # 1785804000 detecte a 00:40:14, ordre poste seulement vers
                    # 00:40:20 = 6s de latence, l'arb etait mort avant l'envoi.
                    # On prechauffe donc ces 2 caches en tache de fond des que
                    # les tokens d'une fenetre sont connus -> au moment de
                    # l'arb, create_order() est purement local (0 reseau).
                    if any(mo == "real" for mo in self.state["modes"].values()):
                        self._pool.submit(self._prewarm_order_cache, list(_ws_tokens))
                # PRIORITE A L'URGENCE : traiter d'abord les fenetres les plus
                # proches de leur fin. Un marche a 8s restantes doit passer AVANT
                # un marche a 250s, sinon on perd de precieuses centaines de ms
                # (voire des secondes) sur celui qui va expirer.
                _now = synced_now()
                for _s in by_sym:
                    by_sym[_s].sort(key=lambda mp: mp[1]["end_ts"] - _now)

                # workflow d'UN marche (detection + gestion + resolution). Chaque
                # marche ne touche QUE son propre sous-etat mk -> pas de conflit
                # entre threads ; seule l'execution d'ordre reel est verrouillee.
                def _process(sym):
                    _t0 = time.time()
                    mode = self.state["modes"].get(sym, "off")
                    if mode == "off":
                        per_sym_ms[sym] = round((time.time() - _t0) * 1000, 1)
                        return
                    # DISABLED SYMBOLS (Steven 26/07) : DOGE/XRP nets negatifs -> skip tout
                    if sym in DISABLED_SYMBOLS:
                        per_sym_ms[sym] = round((time.time() - _t0) * 1000, 1)
                        return
                    mk = self.state["markets"][sym]
                    # V3.1 AXE 5 : stop journalier AVANT tentative d'entree
                    if not mk["stopped"]:
                        _stop, _reason = self._check_daily_stop(mk)
                        if _stop:
                            self._log(f"🛑 [DAILY-STOP] {sym}: {_reason}")
                            mk["stopped"] = True
                            mk["stop_reason"] = _reason
                    if not mk["stopped"]:
                        for m, p in by_sym.get(sym, []):
                            secs_left_f = p["end_ts"] - synced_now()
                            if secs_left_f < 0:
                                continue
                            self._try_market(sym, mode, m, p)
                    # MARKET MAKER (Steven 23/07) : independant de mk['stopped']
                    # (stop = arret de l'ARB par pertes consecutives, pas du MM)
                    # mais respecte son propre kill switch + toggle global.
                    # FIX 23/07 (bug de double-comptage) : le MM garde UN SEUL slot
                    # d'etat par symbole (mmst["quotes"][sym]) — si 2 fenetres actives
                    # se chevauchent brievement (bord de cycle 5min), boucler sur les
                    # DEUX aurait fait ecraser/re-traiter le meme symbole 2x dans le
                    # meme tick -> double credit de P&L. On ne traite QUE la fenetre
                    # la PLUS PROCHE de sa fin PARMI CELLES ENCORE ACTIVES.
                    # BUG CORRIGE (Steven 23/07, meme jour) : find_active_markets()
                    # renvoie TOUJOURS aussi la fenetre precedente (deja terminee,
                    # secs_left negatif) en plus de l'actuelle -> apres le tri par
                    # end_ts croissant, cette fenetre EXPIREE se retrouvait en
                    # premiere position (la plus negative), et mm_markets[0] la
                    # choisissait au lieu de la fenetre active -> secs_left<=0 a
                    # chaque tick, gate3 sortait en silence, le MM ne faisait plus
                    # RIEN depuis des minutes. On filtre desormais les fenetres deja
                    # terminees avant de prendre la plus proche.
                    mm_markets = [
                        mp for mp in by_sym.get(sym, []) if mp[1]["end_ts"] > _now
                    ]
                    if self.state.get("mm", {}).get("enabled") and mm_markets:
                        m, p = mm_markets[0]
                        try:
                            self._mm_tick(sym, mode, m, p)
                        except Exception as e:
                            self._tlog(f"mm_err_{sym}", f"💥 [MM] {sym} erreur: {e}")
                    # DELTA-NEUTRE both-side au bid (Steven 23/07)
                    if self.state.get("dn_enabled") and mm_markets:
                        m, p = mm_markets[0]
                        try:
                            self._dn_tick(sym, mode, m, p)
                        except Exception as e:
                            self._tlog(f"dn_err_{sym}", f"💥 [DN] {sym} erreur: {e}")
                    # meme arrete : on continue de gerer/resoudre les positions
                    # DEJA ouvertes -> jamais de position abandonnee sans suivi.
                    self._manage_swings(sym)
                    # FIX (Steven 28/07, "bot begaye") : _manage_orphans et
                    # _manage_pnl_tier_exits (le SL/TP) tournaient UNIQUEMENT ici,
                    # au rythme du scan complet du marche (~7-12s par symbole,
                    # per_symbol_ms mesure). Sur un marche binaire 5min qui peut
                    # passer de 0.74 a 0.01 en <2min, le SL a -30% n'avait JAMAIS
                    # le temps de se declencher : le prix depassait le seuil ET
                    # revenait (ou continuait de chuter) ENTRE deux checks -> la
                    # position partait toujours a resolution nue, jamais de VENTE
                    # (confirme : min_pnl_pct enregistre a -34% sur une position
                    # jamais vendue, 0 ligne PNL-SL/SPREAD/vente sur tout le log).
                    # Deplace vers _fast_exit_loop() (thread dedie, tick WS rapide) :
                    # meme raison que ci-dessus, le prix affiche au dash etait
                    # rafraichi au rythme du scan lent (7-12s), pas du flux WS.
                    self._resolve_market(sym, mode)
                    per_sym_ms[sym] = round((time.time() - _t0) * 1000, 1)

                # TOUS les marches en parallele -> un fill reel long ne bloque plus
                # la detection/resolution des autres. On attend la fin du tick avant
                # de sauver (une seule ecriture d'etat, pas de concurrence sur le fichier).
                futures = [self._pool.submit(_process, sym) for sym in SYMBOLS]
                for f in futures:
                    try:
                        f.result()
                    except Exception as e:
                        self._log(f"💥 erreur marche: {e}")
                # ── MM : resolution des positions archivees par un roulement de
                # fenetre, HORS thread par-symbole (pending est partage) ──
                if self.state.get("mm", {}).get("enabled"):
                    try:
                        self._mm_resolve_pending()
                    except Exception as e:
                        self._log(f"💥 [MM] erreur resolution pending: {e}")
                    try:
                        self._mm_check_markouts()
                    except Exception as e:
                        self._log(f"💥 [MM] erreur markout: {e}")
                # ── ULTRAPOLY : cycle de fond (jamais 2 en parallele, throttle) ──
                if self.state.get("ultrapoly"):
                    if (
                        time.time() - self._ultra_last_scan >= ULTRAPOLY_SCAN_INTERVAL_S
                        and (self._ultra_future is None or self._ultra_future.done())
                    ):
                        self._ultra_last_scan = time.time()
                        self._ultra_future = self._pool.submit(self._ultra_safe)
                self._save()
                self._diag = {
                    "scan_ms": round((time.time() - tick_t0) * 1000, 1),
                    "per_symbol_ms": per_sym_ms,
                    "tick_ts": time.time(),
                    "scan_count": scan,
                }
                if scan % 30 == 0:
                    parts = " | ".join(
                        f"{s}:{self.state['modes'][s]}"
                        f"({len(self.state['markets'][s]['trades'])}t,"
                        f"{self.state['markets'][s]['consec_losses']}L)"
                        for s in SYMBOLS
                    )
                    self._log(f"💓 vivant scan#{scan} | {parts}")
                    # VERITE TERRAIN (Steven 04/08) : PnL reel on-chain a cote
                    # du heartbeat -> on ne peut plus piloter a l'aveugle sur
                    # un compteur interne qui s'ecartait de ~51$ de la realite.
                    if self._live and any(
                        m == "real" for m in self.state["modes"].values()
                    ):
                        self._pool.submit(self._log_real_pnl)
            except Exception as e:
                self._log(f"💥 erreur boucle: {e}\n{traceback.format_exc()}")
            time.sleep(POLL_SECS)
        self._running.clear()

    def _try_market(self, sym, mode, m, p):
        from paper_snipe import outcome_price, size_stake
        from core.btc_updown import evaluate, momentum as _momentum

        mk = self.state["markets"][sym]
        slug = m.get("slug")
        strat = self.state.get("strategies", {}).get(sym, "hold")
        if strat == "swing":
            if slug in mk["open"] or any(t["slug"] == slug for t in mk["trades"]):
                return
            return self._try_swing(
                sym, mode, m, p
            )  # achete pas cher, revend avant reso
        if strat == "hold":
            # DISABLED SYMBOLS (Steven 26/07) : skip DOGE/XRP nets negatifs
            if sym in DISABLED_SYMBOLS:
                return
            # FIX (Steven 29/07) : _try_both_side (arb garanti + hedge favori/
            # underdog, SL/TP geres) tourne INDEPENDAMMENT de "opportunite" --
            # c'est la strategie principale, validee, qui tournait deja toute
            # la nuit. "opportunite" ne doit lever que le PLAFOND du near-certain
            # directionnel (voir plus bas), jamais bloquer l'arb/hedge lui-meme.
            if self._try_both_side(sym, mode, m, p):
                return
            # NEAR-CERTAIN COUPE (Steven 22/07, -9.71$ reels sur des favoris 96-99c
            # flippes ; re-confirme 29/07 : achat nu sans aucun SL/TP, meme
            # pattern) : bloque en DUR par NEAR_CERTAIN_ENABLED=False, ET
            # necessite "opportunite" ON en plus -> double condition,
            # inconditionnellement return si l'une des deux manque (avant, un
            # elif mal cable laissait ce chemin s'executer SANS AUCUN garde-fou
            # des que "opportunite" etait OFF, ce qui est le defaut).
            if not (NEAR_CERTAIN_ENABLED and self._opportunity_on(sym)):
                return
            # (si un jour reactive) pas d'arb -> tente le directionnel, mais
            # SEULEMENT si on ne tient/n'a pas deja trade ce slug.
            if any(k == slug or k.startswith(slug + "|") for k in mk["open"]) or any(
                t.get("slug") == slug for t in mk["trades"]
            ):
                return
            # -> tombe dans la logique directionnelle ci-dessous (near-certain)
        elif strat != "swing":
            return
        sig = evaluate(m, window_secs=ENTRY_WINDOW_SECS)
        if not sig:
            return
        mk["danger"] = sig.get("danger", 0)
        # MOMENTUM LOG-ONLY (Steven 22/07, test avant activation) : vitesse
        # courte (2.5s, le sursaut) vs longue (12s, filtre anti-bruit) autour
        # de CHAQUE signal, pour TOUS les marches. N'INFLUENCE AUCUNE decision
        # -> juste pour voir apres coup si ca aurait aide a entrer plus tot/
        # moins cher (cas DOGE du 22/07, signal arrive a 12s seulement).
        mom = _momentum(p["pair"])
        if mom:
            self._log(
                f"📐 {sym} {slug} momentum court={mom['fast_pct_s']:+.4f}%/s "
                f"long={mom['slow_pct_s']:+.4f}%/s confirme={mom['confirms']} (log-only)"
            )
        if sig.get("danger", 0) > DANGER_MAX:
            self._log(
                f"⚠️ {sym} {slug} skip (danger={sig['danger']} > {DANGER_MAX}, marche instable)"
            )
            self._reject(
                sym, slug, "danger", f"danger={sig['danger']} max={DANGER_MAX}"
            )
            return
        outcomes = json.loads(m.get("outcomes") or "[]")
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
        token_id = token_ids[outcomes.index(sig["side"])]
        t0 = time.time()

        if mode == "real":
            # precision adaptee au prix (etait :+.1f, ecrasait les petites valeurs
            # XRP/DOGE a "0.0" -> illisible pour juger la conviction du signal)
            _nd = 2 if sig["price"] >= 100 else (4 if sig["price"] >= 1 else 6)
            self._log(
                f"👁️ [REEL] {sym} {slug} signal {sig['side']} "
                f"buffer={sig['buffer']:+.{_nd}f}/{sig['margin']:.{_nd}f} "
                f"danger={sig.get('danger', 0)} "
                f"| {sig['seconds_left']}s restantes -> verif carnet…"
            )
            # lecture du carnet HORS verrou -> parallele entre marches (juste du HTTP)
            book = self._live.get_book_sync(token_id)
            ask = book["asks"][0][0] if book and book.get("asks") else None
            # NEAR-CERTAIN (position SEULE, non hedgee) : plancher DUR a 0.94
            # (Steven 22/07) -> jamais un pile-ou-face nu comme le SOL a 0.49.
            # Le cheap ne se prend qu'en PAIRE via l'arb both-side. Opportunité ne
            # leve QUE le plafond haut (achete jusqu'a 0.99 sur un favori convaincu).
            opp_on = self._opportunity_on(sym)
            min_entry = NEAR_CERTAIN_MIN_PRICE
            max_entry = MAX_ENTRY_PRICE_OPPORTUNITY if opp_on else MAX_ENTRY_PRICE
            if ask is None or ask > max_entry or ask < min_entry:
                # book.get('error') distingue panne reseau/carnet vraiment vide
                # (cf. get_book_sync) de "trop cher"/"trop bas" -> diagnostic
                # reel du "ask=None" au lieu de deviner (Steven 22/07).
                err = (book or {}).get("error")
                reason = (
                    f"reseau={err}"
                    if err
                    else "carnet vide"
                    if ask is None
                    else "hors bornes"
                )
                if ask is None:
                    rej_reason = "ask_network" if err else "ask_empty_book"
                else:
                    rej_reason = "ask_out_of_range"
                self._log(f"⚠️ {sym} {slug} skip (ask={ask}, min={min_entry}, {reason})")
                self._reject(
                    sym,
                    slug,
                    rej_reason,
                    f"ask={ask} range=[{min_entry},{max_entry}] {reason}",
                )
                return
            # ── SECTION CRITIQUE (verrou) : engagement du capital + ordre CLOB ──
            # serialisee entre marches reels pour ne jamais double-engager le solde
            # ni appeler le client CLOB en concurrence. Rapide (l'attente de fill
            # domine), donc n'annule pas le gain de parallelisme sur le reste.
            with self._order_lock:
                cash, _ = self._read_cash(
                    max_age=0
                )  # frais : le plancher doit etre exact
                if cash is None:
                    return
                investable = max(0.0, cash - self.floor())
                if investable < MIN_BUDGET_USD:
                    mk["stopped"] = True
                    mk["stop_reason"] = f"solde au plancher {self.floor()}$"
                    self._log(f"🛑 {sym} : {mk['stop_reason']}")
                    self._reject(
                        sym,
                        slug,
                        "below_floor",
                        f"cash={cash} floor={self.floor()} investable={investable:.2f}",
                    )
                    return
                budget = self._budget_usd(ask, sig, investable, sym)
                # MODE VALIDATION REEL : force la mise au minimum (5 parts x prix)
                # pour de tout petits premiers achats reels.
                if REAL_VALIDATION_MODE:
                    budget = round(REAL_VALIDATION_SHARES * ask, 2)
                # SIZING ADAPTATIF (Steven 25/07, boost symetrique 04/08) : reduit
                # la taille si liquidite faible, l'augmente si excellente.
                budget = self._adaptive_size(sym, token_id, budget, max_entry)
                # REPLAFONNEMENT POST-BOOST (Steven 04/08) : le boost peut depasser
                # ce que _budget_usd avait deja borne (HARD_CAP_USD/investable) --
                # jamais engager plus que le capital reellement disponible, meme
                # sur un carnet excellent.
                budget = min(budget, HARD_CAP_USD, investable)
                # GARDE-FOU MINIMUM VENDABLE (Steven 04/08, analyse on-chain :
                # 63 positions tenues jusqu'a resolution jamais vendues, -116.20$
                # au total, prix d'entree moyen 0.408). Cause racine trouvee :
                # l'ordre MARKET ci-dessous accepte des budgets jusqu'a 0.10$
                # (voir commentaire plus bas), ce qui produit des positions SOUS
                # le minimum vendable CLOB (5 parts) -> ni stop-loss ni take-
                # profit ne peuvent JAMAIS s'executer dessus, quoi qu'il arrive.
                # Ce n'est pas une strategie a risque assume (contrairement a la
                # petite mise underdog du hedge, volontairement jetable) -- c'est
                # une position normale qui devient un pari pile-ou-face force par
                # accident. Soit on met assez pour pouvoir sortir, soit on
                # n'entre pas du tout.
                _min_sellable_budget = round(MIN_ORDER_SIZE_SHARES * ask, 2)
                if budget < _min_sellable_budget:
                    if investable >= _min_sellable_budget:
                        budget = _min_sellable_budget
                    else:
                        self._reject(
                            sym,
                            slug,
                            "below_sellable_min",
                            f"budget={budget:.2f} < min_vendable={_min_sellable_budget:.2f} "
                            f"(investable={investable:.2f}) -> position invendable evitee",
                        )
                        return
                self._log(
                    f"🎯 [REEL] {sym} {slug} {sig['side']} ask={ask:.3f} budget={budget:.2f}$ "
                    f"buffer={sig['buffer']:+.1f}/{sig['margin']:.1f} danger={sig.get('danger', 0)} "
                    f"| {sig['seconds_left']}s"
                )
                # ORDRE MARKET partout (Steven 22/07, "je le veux") : dimensionne en
                # DOLLARS (snipe_buy_market), contourne le plancher de 5 parts des
                # ordres LIMIT/FAK -> suit fidelement le budget Kelly, meme tout petit
                # (jusqu'a 0.10$), sur TOUS les marches y compris BTC. L'ancien chemin
                # LIMIT/FAK (snipe_buy) reste dispo mais n'est plus utilise par defaut.
                res = self._live.snipe_buy_market(token_id, max_entry, budget)
                filled = res.get("filled_shares", 0.0)
                self._log(
                    f"📨 [REEL] {sym} {slug} rempli={filled} @ {res.get('ask')} "
                    f"depense~{res.get('spent_est')}$ err={res.get('error', '')}"
                )
                if filled <= 0:
                    self._reject(
                        sym,
                        slug,
                        "fill_failed",
                        f"ask={ask:.3f} budget={budget:.2f} err={res.get('error', '')}",
                    )
                    return
                avg = res.get("avg_cost") or ask
                mk["open"][slug] = {
                    "symbol": sym,
                    "slug": slug,
                    "side": sig["side"],
                    "mode": "real",
                    "token_id": token_id,
                    "entry_price": avg,
                    "filled_shares": filled,
                    "cost": round(filled * avg, 2),
                    "start_ts": p["start_ts"],
                    "pair": p["pair"],
                    "end_ts": p["end_ts"],
                    "opened_ts": t0,
                    "buffer": sig["buffer"],
                    "strat": "bothside",  # FIX (Steven 29/07) : meme filet SL/TP qu'en paper
                }
                self._log(
                    f"✅ [REEL] POSITION {sym} {slug} {filled} parts @ {avg:.3f} = {round(filled * avg, 2)}$"
                )
        else:  # paper
            # prix lu sur le CARNET REEL et non plus sur outcomePrices : depuis la
            # mise en cache des metadonnees de marche (fix des echecs d'achat), le
            # champ outcomePrices du dict cache serait FIGE. Le carnet est de toute
            # facon plus fidele a ce qu'on paierait vraiment.
            entry = self._live_price(token_id, m, sig["side"])
            # position SEULE -> plancher near-certain dur (Steven 22/07)
            min_entry = NEAR_CERTAIN_MIN_PRICE
            if entry is None or entry <= 0 or entry >= 1 or entry < min_entry:
                return
            # V3.2 : meme sizing que le reel (Kelly) au lieu de size_stake()=$1
            paper_cash = mk.get("paper_balance", 0.0)
            paper_investable = max(0.0, paper_cash - self.floor())
            if paper_investable < MIN_BUDGET_USD:
                return
            budget = min(
                self._budget_usd(entry, sig, paper_investable, sym), HARD_CAP_USD
            )
            shares = max(MIN_ORDER_SIZE_SHARES, budget / entry)
            mk["open"][slug] = {
                "symbol": sym,
                "slug": slug,
                "side": sig["side"],
                "mode": "paper",
                "token_id": token_id,
                "entry_price": entry,
                "filled_shares": round(shares, 2),
                "cost": round(shares * entry, 2),
                "start_ts": p["start_ts"],
                "pair": p["pair"],
                "end_ts": p["end_ts"],
                "opened_ts": t0,
                "buffer": sig["buffer"],
                # FIX (Steven 29/07, "PNL negatif malgre 97% WR") : ce pari
                # directionnel nu (favori seul 92-99c, R/R pourri : risque tout
                # pour gagner quelques %) n'avait AUCUN tag "strat" -> le SL/TP
                # (_manage_pnl_tier_exits, filtre strat in bothside/swing) ne le
                # voyait JAMAIS -> il roulait nu jusqu'a resolution complete.
                # C'est EXACTEMENT le pattern deja documente le 22/07 (-9.71$
                # reels, favoris flippes) que NEAR_CERTAIN_ENABLED=False est
                # cense bloquer -> ce tag est un FILET DE SECURITE en plus.
                "strat": "bothside",
            }
            self._log(
                f"🎯 [PAPER] {sym} {slug} {sig['side']} entry={entry:.3f} danger={sig.get('danger', 0)} "
                f"{round(shares, 2)} parts = {round(shares * entry, 2)}$"
            )

    def _slot_trade_count(self, mk, slug):
        return mk.setdefault("slot_trades", {}).get(slug, 0)

    def _open_leg(
        self,
        sym,
        mode,
        m,
        p,
        side,
        token_id,
        max_entry,
        tag,
        target_shares=None,
        entry_px=None,
        budget_usd=None,
        force=False,
        no_slippage=False,
    ):
        """Ouvre UNE jambe both-side (reel ou paper). Rejette les tickets de
        loterie (< BOTH_SIDE_LEG_MIN) et plafonne les fills paper irrealistes.
        `target_shares` (Steven 22/07) : si fourni, vise ce NOMBRE DE PARTS exact
        (pour l'arb en parts egales) au lieu d'un budget en $.
        `entry_px` (Steven 22/07) : prix d'entree FIGE (deja lu pour la verif du
        combine). Evite de RELIRE le prix ici -> sinon il derive entre la verif
        et l'enregistrement et MANGE la marge de l'arb (bug : paires validees a
        0.95 enregistrees a 1.00 -> +0). Fige = paper honnete + colle a une
        execution reelle rapide. `force=True` (Laguna XS 25/07) : bypass BOTH_SIDE_LEG_MIN
        pour HEDGE-NEAR-RESOLUTION (mieux vaut combined 1.05 que bet directionnel nu).
        Retourne (True, cout) si ouverte, (False, 0) sinon."""
        mk = self.state["markets"][sym]
        slug = m.get("slug")
        key = f"{slug}|{side}"
        if key in mk["open"] or any(
            t.get("slug") == slug and t.get("side") == side for t in mk["trades"]
        ):
            return False, 0.0
        base = {
            "symbol": sym,
            "slug": slug,
            "side": side,
            "strat": "bothside",
            "token_id": token_id,
            "start_ts": p["start_ts"],
            "pair": p["pair"],
            "end_ts": p["end_ts"],
            "opened_ts": time.time(),
            "buffer": 0.0,
        }
        if mode == "real":
            # FAST PATH (Laguna XS 24/07) : utiliser le WS book (TEMPS REEL)
            # au lieu de REST (lent ~300-500ms). Seul fallback REST si WS absent.
            ask = None
            try:
                wb = self._ws.book(token_id)
                if wb:
                    _, ws_ask, _ = wb
                    if ws_ask is not None:
                        ask = ws_ask
            except Exception:
                pass
            if ask is None:
                book = self._live.get_book_sync(token_id)
                ask = book["asks"][0][0] if book and book.get("asks") else None
            if (
                ask is None
                or ask > max_entry
                or (not force and ask < BOTH_SIDE_LEG_MIN)
            ):
                return False, 0.0
            with self._order_lock:
                cash, _ = self._read_cash(max_age=0)
                if cash is None:
                    return False, 0.0
                investable = max(0.0, cash - self.floor())
                if investable < MIN_BUDGET_USD:
                    return False, 0.0
                # budget en $ FIXE ($1-egal, methode Steven) > parts egales > Kelly
                if budget_usd is not None:
                    budget = min(round(budget_usd, 2), investable * 0.9)
                elif target_shares is not None:
                    budget = min(round(target_shares * ask, 2), investable * 0.9)
                else:
                    budget = self._budget_usd(
                        ask, {"buffer": 0.0, "margin": 1.0}, investable, sym
                    )
                # PLANCHER ARB (Steven 23/07) : chaque jambe au minimum arb_budget
                budget = max(self.arb_budget(), min(budget, investable * 0.9))
                # SIZING ADAPTATIF (Steven 25/07, boost symetrique 04/08) : reduit
                # la taille si liquidite faible, l'augmente si excellente.
                budget = self._adaptive_size(sym, token_id, budget, max_entry)
                # REPLAFONNEMENT POST-BOOST (Steven 04/08) : meme garde-fou que
                # plus haut -- jamais depasser le capital reellement disponible.
                budget = min(budget, HARD_CAP_USD, investable)
                if no_slippage:
                    _shares = target_shares if target_shares is not None else round(budget / ask, 2)
                    res = self._live.snipe_buy_limit_exact(token_id, ask, _shares)
                else:
                    res = self._live.snipe_buy_market(token_id, max_entry, budget)
                filled = res.get("filled_shares", 0.0)
                err_msg = str(res.get("error", ""))
                # RETRY 425 (Laguna XS 24/07) : order manager not ready apres restart
                if filled <= 0 and "425" in err_msg and not no_slippage:
                    self._log(f"🔁 [RETRY-425] {sym} {slug} {side} -> retry dans 3s")
                    time.sleep(3)
                    ask2 = None
                    try:
                        wb2 = self._ws.book(token_id)
                        if wb2:
                            _, ws_ask2, _ = wb2
                            if ws_ask2 is not None:
                                ask2 = ws_ask2
                    except Exception:
                        pass
                    if ask2 is None:
                        book2 = self._live.get_book_sync(token_id)
                        ask2 = (
                            book2["asks"][0][0] if book2 and book2.get("asks") else None
                        )
                    if (
                        ask2 is not None
                        and ask2 <= max_entry
                        and ask2 >= BOTH_SIDE_LEG_MIN
                    ):
                        res = self._live.snipe_buy_market(token_id, max_entry, budget)
                        filled = res.get("filled_shares", 0.0)
                        err_msg = str(res.get("error", ""))
                self._log(
                    f"🔀 [BOTHSIDE][REEL]{tag} {sym} {slug} {side} ask={ask:.3f} "
                    f"budget={budget:.2f}$ rempli={filled} @ {res.get('ask')} "
                    f"depense~{res.get('spent_est')}$ err={err_msg}"
                )
                if filled <= 0:
                    return False, 0.0
                avg = res.get("avg_cost") or ask
                base.update(
                    mode="real",
                    entry_price=avg,
                    filled_shares=filled,
                    cost=round(filled * avg, 2),
                )
                mk["open"][key] = base
                return True, base["cost"]
        else:  # paper
            # prix FIGE si fourni (arb : pas de relecture qui dériverait et
            # mangerait la marge) ; sinon lecture live (autres appels).
            entry = (
                entry_px
                if entry_px is not None
                else self._live_price(token_id, m, side)
            )
            if (
                entry is None
                or (not force and entry < BOTH_SIDE_LEG_MIN)
                or entry > max_entry
            ):
                return False, 0.0
            if budget_usd is not None:
                # V3.2 : plancher MIN_ORDER_SIZE_SHARES pour matcher le reel
                # (snipe_buy_market achete toujours au minimum 5 parts meme si
                # le budget est inferieur). Sans ca, le paper montre 2 parts
                # la ou le reel en a 5 -> sizing desync.
                shares = min(
                    PAPER_MAX_FILL_SHARES,
                    max(MIN_ORDER_SIZE_SHARES, budget_usd / entry),
                )
            elif target_shares is not None:
                shares = min(
                    PAPER_MAX_FILL_SHARES, max(MIN_ORDER_SIZE_SHARES, target_shares)
                )
            else:
                budget = min(HARD_CAP_USD, 5.0)
                # plafond de parts : evite les fills fantomes du paper qui gonflent le P&L
                shares = min(
                    PAPER_MAX_FILL_SHARES, max(MIN_ORDER_SIZE_SHARES, budget / entry)
                )
            base.update(
                mode="paper",
                entry_price=entry,
                filled_shares=round(shares, 2),
                cost=round(shares * entry, 2),
            )
            mk["open"][key] = base
            self._log(
                f"🔀 [BOTHSIDE][PAPER]{tag} {sym} {slug} {side} entry={entry:.3f} "
                f"{round(shares, 2)} parts = {round(shares * entry, 2)}$"
            )
            return True, base["cost"]

    def _sell_orphan(self, token_id, shares, tag="", entry_price=None, symbol=None, slug=None, side=None):
        """Revend `shares` parts au meilleur bid et VERIFIE ON-CHAIN que la vente
        est reellement passee (Steven 22/07 : plus jamais de vente supposee).
        Retourne le nombre de parts effectivement vendues. Log explicite.
        NB : le CLOB refuse les ordres < 5 parts -> l'appelant doit gerer ce cas
        (hold force + log), on ne poste pas un ordre voue au rejet.
        `entry_price`/`symbol`/`slug`/`side` (Steven 30/07, "solde a 13.04 mais
        pnl dit +4.85 ?!") : quand fournis, ENREGISTRE le trade (mode=real,
        pnl reel achat->vente) dans mk['trades'] -> sans ca, decouvert que
        les cycles achat-puis-revente-immediate (unwind d'orphelin) depensaient
        et recuperaient du vrai argent SANS JAMAIS que le delta (souvent une
        petite perte de spread) soit compte nulle part -> pnl_total_real
        mentait par omission, ecart de plusieurs dollars invisible."""
        if shares < 0.01:
            return 0.0
        if shares < MIN_ORDER_SIZE_SHARES:
            # TOP-UP (Steven 30/07, "pas de demi-mesure") : avant, un reste
            # sous le plancher CLOB (5 parts) restait INVENDABLE -> tenu de
            # force jusqu'a resolution, nue et exposee (observe : -97% de
            # marque avant un retournement in extremis qui a sauve la mise,
            # mais ca viole "jamais de jambe nue" des que la chance tourne
            # mal). Fix : on ACHETE agressivement le complement pour ATTEINDRE
            # le plancher vendable, puis on revend le tout d'un coup -> cout
            # marginal (quelques cents de plus), mais sortie garantie au lieu
            # de tenir un pari directionnel nu par accident de sizing.
            shortfall = round(MIN_ORDER_SIZE_SHARES - shares + 0.05, 2)
            book0 = self._live.get_book_sync(token_id)
            ask0 = book0["asks"][0][0] if book0 and book0.get("asks") else None
            if ask0 is not None:
                topup_usd = round(shortfall * ask0 * 1.05, 2)  # +5% buffer prix
                res = self._live.snipe_buy_market(token_id, round(ask0 + 0.05, 2), topup_usd)
                filled = res.get("filled_shares", 0.0)
                self._log(
                    f"➕ [TOP-UP]{tag} {round(shares, 2)} parts sous le plancher "
                    f"CLOB -> achat complement {shortfall} parts tente, "
                    f"{filled} parts obtenues (@ ~{ask0:.3f})"
                )
                if filled > 0:
                    shares = round(shares + filled, 2)
            if shares < MIN_ORDER_SIZE_SHARES:
                self._log(
                    f"🚫 [VENTE]{tag} {round(shares, 2)} parts toujours < minimum "
                    f"CLOB (5) apres tentative de complement -> INVENDABLE, "
                    f"position a tenir jusqu'a resolution"
                )
                return 0.0
        book = self._live.get_book_sync(token_id)
        bid = book["bids"][0][0] if book and book.get("bids") else None
        if bid is None:
            self._log(
                f"🚫 [VENTE]{tag} pas de bid (carnet vide) -> vente impossible pour l'instant"
            )
            return 0.0
        before = self._live.position_size(token_id)
        before = before if before >= 0 else shares
        with self._order_lock:
            # AGRESSIF (Steven 30/07, "orphelin evitable ?") : GTC pile au bid
            # etait un ordre MAKER, aucune garantie de croiser -> observe
            # plusieurs fois a 0/N vendues apres le delai de verif complet.
            # _sell_orphan sert TOUJOURS a sortir vite (unwind, stop-loss,
            # fin de fenetre) -> la vitesse prime sur le prix ici.
            self._live.sell_position(token_id, round(bid, 2), round(shares, 2), aggressive=True)
        # VERIFICATION on-chain du fill (jusqu'a 4s) — une vente postee n'est PAS
        # une vente executee.
        sold = 0.0
        deadline = time.time() + 4.0
        while time.time() < deadline:
            time.sleep(0.6)
            after = self._live.position_size(token_id)
            if after >= 0:
                sold = max(0.0, round(before - after, 2))
                if sold >= shares - 0.01:
                    break
        icon = "✅" if sold >= shares - 0.01 else "⚠️"
        self._log(
            f"{icon} [VENTE]{tag} {sold}/{round(shares, 2)} parts vendues @ ~{bid:.3f}"
        )
        if entry_price is not None and symbol is not None and sold > 0:
            _pnl = round(sold * (bid - entry_price), 3)
            mk_rec = self.state["markets"].get(symbol)
            if mk_rec is not None:
                _now_ts = time.time()
                mk_rec["trades"].append({
                    "symbol": symbol, "slug": slug, "side": side, "mode": "real",
                    "strat": "orphan", "entry_price": entry_price, "exit_price": bid,
                    "filled_shares": sold, "cost": round(sold * entry_price, 2),
                    "pnl": _pnl, "win": _pnl > 0, "resolved_by": "unwind",
                    # FIX (Steven 30/07, "je vois toujours pas nos dernieres
                    # trades") : le dashboard trie/filtre sur "opened_ts", pas
                    # "ts" -> mes trades UNWIND n'avaient pas ce champ, donc
                    # (champ manquant) triaient comme "aucune date" et
                    # remontaient EN PREMIER en tri desc (devant les vrais
                    # trades recents qui, eux, ont un opened_ts). Les 2 champs
                    # sont maintenant remplis, coherent avec le reste du code.
                    "opened_ts": _now_ts, "start_ts": _now_ts, "ts": _now_ts,
                })
                _icon2 = "✅ WIN " if _pnl > 0 else "❌ LOSS"
                self._log(
                    f"{_icon2} [UNWIND] {symbol} {slug} {side} entree={entry_price:.3f} "
                    f"sortie={bid:.3f} {sold} parts pnl={_pnl:+.3f}$ (enregistre)"
                )
        return sold

    # ── STOP-LOSS CONTEXTUEL (Steven 25/07) : ajuste les seuils par actif selon
    # spread et temps restant. Un SOL plus volatile a un stop plus large ; un marche
    # pres de la resolution a un stop plus serré (moins de temps pour rebondir).
    _CTX_SL_MULTIPLIER = {
        "BTC": 1.0,
        "ETH": 1.0,
        "SOL": 1.15,
        "XRP": 1.15,
        "DOGE": 1.2,
    }

    # ── REJECTION LOGGING STRUCTURE (Steven 25/07) : format machine-parseable ──
    # Chaque rejet d'entrée est loggé avec un tag standardisé pour analyse dashboard.
    # Format: [REJECT] sym=.. slug=.. reason=.. detail=.. (tags triables par grep)
    def _reject(self, sym: str, slug: str, reason: str, detail: str = ""):
        tag_map = {
            "no_signal": "SIG",
            "danger": "DNG",
            "ask_out_of_range": "ASK",
            "ask_network": "NET",
            "ask_empty_book": "ASK",
            "low_cash": "CASH",
            "below_floor": "CASH",
            "fill_failed": "FILL",
            "preflight_failed": "PF",
            "watchdog_cooldown": "WD",
            "risk_limit": "RL",
            "spread_too_wide": "LQ",
            "depth_insufficient": "LQ",
            "too_late": "TIME",
            "already_open": "DUP",
            "hedge_failed_2nd": "HEDGE",
            "emergency_close": "EMRG",
            "not_posted": "POST",
        }
        tag = tag_map.get(reason, "REJ")
        self._log(f"[REJECT][{tag}] {sym} {slug} reason={reason} {detail}")

    def _contextual_sl(
        self, sym: str, base_stop: float, entry_px: float, secs_left: float
    ) -> float:
        """Calcule le stop-loss contextuel : multiplie par actif + ajuste par temps restant."""
        mult = self._CTX_SL_MULTIPLIER.get(sym, 1.0)
        # Moins de temps = stop plus serré (pas le temps de rebondir)
        if secs_left < 30:
            mult *= 0.85
        elif secs_left < 60:
            mult *= 0.92
        # Plus de temps = on tolère un peu plus de bruit
        elif secs_left > 120:
            mult *= 1.08
        return round(base_stop * mult, 3)

    def _manage_orphans(self, sym):
        """GESTION ACTIVE DES JAMBES NUES (Steven 22/07) :
        - cote GAGNANT cote Binance -> on TIENT (payout 1$ a la resolution)
        - cote PERDANT -> vente par PALIERS au bid, chaque vente VERIFIEE on-chain
        - < 5 parts (minimum CLOB) -> invendable, hold force assume.
        Phase 2 (25/07) : SL/TP unifies via STRATEGY_RISK_PARAMS + stops contextuels
        par actif/temps/liquidite.
        Phase 3 (25/07) : vente momentum (paliers adaptes au signal Binance)."""
        from core.btc_updown import _binance_price, _strike_at, momentum as _momentum

        mk = self.state["markets"][sym]
        now = synced_now()
        orphan_params = STRATEGY_RISK_PARAMS.get("orphan", {})
        bothside_params = STRATEGY_RISK_PARAMS.get("bothside", {})
        for key, pos in list(mk["open"].items()):
            if pos.get("strat") != "orphan" or pos.get("mode") != "real":
                continue
            secs_left = pos["end_ts"] - now
            if secs_left <= 3:
                continue  # trop tard, la resolution normale prendra le relais

            # V3.1 AXE 5 : sortie d'urgence (fenetre proche + position perdante)
            _emrg, _emrg_reason = self._should_emergency_exit(pos, sym, now)
            if _emrg:
                sold = self._sell_orphan(
                    pos["token_id"],
                    pos["filled_shares"],
                    f" {sym} {pos['slug']} {pos['side']} EMERGENCY-EXIT",
                )
                if sold > 0:
                    realized = round(sold * (0.0 - pos["entry_price"]), 3)
                    pos["realized_pnl"] = round(
                        pos.get("realized_pnl", 0.0) + realized, 3
                    )
                    pos["filled_shares"] = 0.0
                    pnl = pos["realized_pnl"]
                    pos.update(
                        win=pnl > 0,
                        pnl=pnl,
                        resolved_by="emergency_exit",
                    )
                    loss_tag = self._classify_loss(pos, _emrg_reason)
                    if loss_tag:
                        pos["loss_tag"] = loss_tag
                    mk["trades"].append(pos)
                    del mk["open"][key]
                    self._record_trade_pnl(sym, pnl)
                    self._set_slug_cooldown(sym, pos.get("slug", ""), mk)
                    self._record_abort(sym, mk)
                    self._log(
                        f"🚨 [EMERGENCY] {sym} {pos['slug']} {pos['side']} "
                        f"raison={_emrg_reason} pnl={pnl:+.3f}$"
                    )
                    self._log_trade_exit(
                        sym,
                        pos.get("slug", ""),
                        pos["side"],
                        f"emergency:{_emrg_reason}",
                        pos.get("entry_price", 0),
                        0.0,
                        realized,
                        0.0,
                        0.0,
                        pnl,
                        now - pos.get("opened_ts", pos["start_ts"]),
                        "sold",
                        "never_opened",
                        loss_tag=loss_tag,
                    )
                continue

            # ── STOP-LOSS CONTEXTUEL (Phase 2) : seuil ajuste par actif/temps ──
            _shares = pos["filled_shares"]
            _hard_stop = self._contextual_sl(
                sym,
                orphan_params.get("hard_stop", ORPHAN_HARD_STOP),
                pos["entry_price"],
                secs_left,
            )
            # DYNAMIC SL BINANCE GAP (Phase 3) : si le gap spot-strike est large
            # (confirmation forte de perte), serre le stop de 20%
            if pos.get("pair"):
                _px = _binance_price(pos["pair"])
                _st = _strike_at(pos["pair"], pos["start_ts"], slug=pos.get("slug"))
                if _px is not None and _st is not None:
                    _gap = abs(_px - _st)
                    _margin = _px * 0.001  # 0.1% du prix
                    if _gap > _margin:
                        _hard_stop = round(_hard_stop * 0.8, 3)
            if _shares >= MIN_ORDER_SIZE_SHARES:
                _book = self._live.get_book_sync(pos["token_id"])
                _bid = _book["bids"][0][0] if _book and _book.get("bids") else None
                if _bid is not None and (pos["entry_price"] - _bid) >= _hard_stop:
                    sold = self._sell_orphan(
                        pos["token_id"],
                        _shares,
                        f" {sym} {pos['slug']} {pos['side']} STOP-LOSS",
                    )
                    if sold > 0:
                        realized = round(sold * (_bid - pos["entry_price"]), 3)
                        pos["realized_pnl"] = round(
                            pos.get("realized_pnl", 0.0) + realized, 3
                        )
                        pos["filled_shares"] = round(_shares - sold, 2)
                        self._log(
                            f"🛑 [ORPHAN][STOP] {sym} {pos['slug']} {pos['side']} coupe {sold} parts "
                            f"@ {_bid:.3f} (entree {pos['entry_price']:.3f}) realise={realized:+.3f}$ "
                            f"(stop_ctx={_hard_stop:.3f})"
                        )
                        if pos["filled_shares"] <= 0.01:
                            pnl = pos["realized_pnl"]
                            pos.update(win=pnl > 0, pnl=pnl, resolved_by="orphan_stop")
                            mk["trades"].append(pos)
                            del mk["open"][key]
                            self._record_trade_pnl(sym, pnl)
                            if pos.get("mm_handoff"):
                                self.state["mm"]["daily_pnl"] = round(
                                    self.state["mm"]["daily_pnl"] + pnl, 4
                                )
                                self.state["mm"]["consec_adverse"] = (
                                    self.state["mm"].get("consec_adverse", 0) + 1
                                )
                            self._log(
                                f"❌ LOSS [ORPHAN] {sym} {pos['slug']} STOP-LOSS complet pnl={pnl:+.3f}$"
                            )
                            # V3.1 AXE 8 : log sortie structure
                            _duree = now - pos.get(
                                "opened_ts", pos.get("start_ts", now)
                            )
                            self._log_trade_exit(
                                sym,
                                pos.get("slug", ""),
                                pos["side"],
                                "orphan_stoploss",
                                pos.get("entry_price", 0),
                                _bid if _bid else 0,
                                realized,
                                0.0,
                                0.0,
                                pnl,
                                _duree,
                                "sold",
                                "never_opened",
                                loss_tag=pos.get("loss_tag", "ORPHAN"),
                            )
                        continue

            winning = None
            if pos.get("pair"):
                price = _binance_price(pos["pair"])
                strike = _strike_at(pos["pair"], pos["start_ts"], slug=pos.get("slug"))
                if price is not None and strike is not None:
                    winning = (pos["side"] == "Up") == (price > strike)
            if winning and secs_left > 15:
                _tp_price = orphan_params.get("tp_price", ORPHAN_TP_PRICE)
                _tp_min_profit = orphan_params.get(
                    "tp_min_profit", ORPHAN_TP_MIN_PROFIT
                )
                _tp_fraction = orphan_params.get(
                    "tp_sell_fraction", ORPHAN_TP_SELL_FRACTION
                )
                shares = pos["filled_shares"]
                book = self._live.get_book_sync(pos["token_id"])
                bid = book["bids"][0][0] if book and book.get("bids") else None
                if (
                    bid is not None
                    and shares >= MIN_ORDER_SIZE_SHARES
                    and bid >= _tp_price
                    and (bid - pos["entry_price"]) >= _tp_min_profit
                ):
                    sell_n = round(shares * _tp_fraction, 2)
                    if shares - sell_n < MIN_ORDER_SIZE_SHARES:
                        sell_n = shares  # evite un reliquat invendable (<5 parts)
                    sold = self._sell_orphan(
                        pos["token_id"],
                        sell_n,
                        f" {sym} {pos['slug']} {pos['side']} TP",
                    )
                    if sold > 0:
                        realized = round(sold * (bid - pos["entry_price"]), 3)
                        pos["realized_pnl"] = round(
                            pos.get("realized_pnl", 0.0) + realized, 3
                        )
                        pos["filled_shares"] = round(shares - sold, 2)
                        self._log(
                            f"💰 [ORPHAN][TP] {sym} {pos['slug']} {pos['side']} pris profit "
                            f"{sold} parts @ {bid:.3f} realise={realized:+.3f}$ (reste {pos['filled_shares']})"
                        )
                        if pos["filled_shares"] <= 0.01:
                            pnl = pos["realized_pnl"]
                            pos.update(
                                win=pnl > 0, pnl=pnl, resolved_by="orphan_tp_exit"
                            )
                            mk["trades"].append(pos)
                            del mk["open"][key]
                            self._record_trade_pnl(sym, pnl)
                            if pos.get("mm_handoff"):
                                self.state["mm"]["daily_pnl"] = round(
                                    self.state["mm"]["daily_pnl"] + pnl, 4
                                )
                            icon = "✅ WIN " if pnl > 0 else "❌ LOSS"
                            self._log(
                                f"{icon} [ORPHAN] {sym} {pos['slug']} sortie complete (TP) pnl={pnl:+.3f}$"
                            )
                            # V3.1 AXE 8 : log sortie structure
                            _duree = now - pos.get(
                                "opened_ts", pos.get("start_ts", now)
                            )
                            self._log_trade_exit(
                                sym,
                                pos.get("slug", ""),
                                pos["side"],
                                "orphan_tp_exit",
                                pos.get("entry_price", 0),
                                bid if bid else 0,
                                realized,
                                0.0,
                                0.0,
                                pnl,
                                _duree,
                                "tp_sold",
                                "never_opened",
                                loss_tag=pos.get("loss_tag"),
                            )
                        continue
                # V3.1 AXE 4 : TRAILING RUNNER — si on a deja pris du profit
                # (TP1), track le pic du bid et vend le runner si le prix
                # recule de TRAILING_STOP_PCT depuis ce pic.
                if pos.get("realized_pnl", 0) > 0 and bid is not None:
                    peak = pos.get("tp_peak_bid", bid)
                    if bid > peak:
                        pos["tp_peak_bid"] = bid
                        peak = bid
                    if peak > 0 and bid < peak * (1 - SWING_STOP_PCT):
                        sold_r = self._sell_orphan(
                            pos["token_id"],
                            pos["filled_shares"],
                            f" {sym} {pos['slug']} {pos['side']} TRAILING-RUNNER",
                        )
                        if sold_r > 0:
                            realized_r = round(sold_r * (bid - pos["entry_price"]), 3)
                            pos["realized_pnl"] = round(
                                pos.get("realized_pnl", 0.0) + realized_r, 3
                            )
                            pos["filled_shares"] = 0.0
                            pnl_r = pos["realized_pnl"]
                            pos.update(
                                win=pnl_r > 0,
                                pnl=pnl_r,
                                resolved_by="orphan_trailing_exit",
                            )
                            mk["trades"].append(pos)
                            del mk["open"][key]
                            self._record_trade_pnl(sym, pnl_r)
                            self._log(
                                f"🏃 [ORPHAN][TRAILING] {sym} {pos['slug']} "
                                f"{pos['side']} runner vendu @ {bid:.3f} "
                                f"(pic={peak:.3f}) pnl={pnl_r:+.3f}$"
                            )
                            _duree = now - pos.get(
                                "opened_ts", pos.get("start_ts", now)
                            )
                            self._log_trade_exit(
                                sym,
                                pos.get("slug", ""),
                                pos["side"],
                                "orphan_trailing_exit",
                                pos.get("entry_price", 0),
                                bid,
                                realized_r,
                                0.0,
                                0.0,
                                pnl_r,
                                _duree,
                                "runner_sold",
                                "never_opened",
                                loss_tag=pos.get("loss_tag"),
                            )
                        continue
                self._tlog(
                    f"orph_{key}",
                    f"🦺 [ORPHAN] {sym} {pos['slug']} {pos['side']} cote GAGNANT "
                    f"(Binance) -> on TIENT ({secs_left:.0f}s restantes)",
                )
                continue
            # perdant (ou signal indisponible) -> vente par paliers adaptees au momentum
            shares = pos["filled_shares"]
            if shares < MIN_ORDER_SIZE_SHARES:
                self._tlog(
                    f"orph_{key}",
                    f"🦺 [ORPHAN] {sym} {pos['slug']} {pos['side']} {shares} parts "
                    f"< min CLOB -> invendable, hold force",
                )
                continue
            # VENTE MOMENTUM (Steven 25/07) : le fraction vendu depend du signal Binance
            # - momentum CONFIRME perte (fast+slow meme sens negatif) -> 70% (agressif)
            # - momentum MIXTE (pas de confirmation) -> 30% (conservateur, en cas de rebond)
            # - pas de donnees momentum -> 50% (defaut, ancien comportement)
            # Toujours tout vendre en fin de fenetre (<20s) ou trop peu de parts
            if secs_left <= 20 or shares < 2 * MIN_ORDER_SIZE_SHARES:
                sell_n = shares
                sell_tag = "fin_fenetre"
            else:
                mom = None
                if pos.get("pair"):
                    mom = _momentum(pos["pair"])
                if mom and mom["confirms"] and abs(mom["fast_pct_s"]) > 0.05:
                    # momentum confirme perte -> coupe agressive (70%)
                    sell_frac = 0.70
                    sell_tag = "mom_loss"
                elif mom and not mom["confirms"]:
                    # momentum mixte -> coupe conservatrice (30%)
                    sell_frac = 0.30
                    sell_tag = "mom_mix"
                else:
                    # pas de donnees ou pas de confirmation -> defaut 50%
                    sell_frac = 0.50
                    sell_tag = "defaut"
                sell_n = round(shares * sell_frac, 2)
            sold = self._sell_orphan(
                pos["token_id"],
                sell_n,
                f" {sym} {pos['slug']} {pos['side']} palier [{sell_tag}]",
            )
            if sold <= 0:
                continue  # pas de bid / rejet -> retente au prochain tick
            book = self._live.get_book_sync(pos["token_id"])
            bid = (
                book["bids"][0][0] if book and book.get("bids") else pos["entry_price"]
            )
            realized = round(sold * (bid - pos["entry_price"]), 3)
            pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
            pos["filled_shares"] = round(shares - sold, 2)
            self._log(
                f"🦺 [ORPHAN] {sym} {pos['slug']} {pos['side']} [{sell_tag}] "
                f"vendu {sold} parts "
                f"(reste {pos['filled_shares']}) realise={realized:+.3f}$"
            )
            if pos["filled_shares"] <= 0.01:
                pnl = pos["realized_pnl"]
                pos.update(win=pnl > 0, pnl=pnl, resolved_by="orphan_exit")
                mk["trades"].append(pos)
                del mk["open"][key]
                self._record_trade_pnl(sym, pnl)
                icon = "✅ WIN " if pnl > 0 else "❌ LOSS"
                self._log(
                    f"{icon} [ORPHAN] {sym} {pos['slug']} sortie complete pnl={pnl:+.3f}$"
                )
                # V3.1 AXE 8 : log sortie structure
                _duree = now - pos.get("opened_ts", pos.get("start_ts", now))
                self._log_trade_exit(
                    sym,
                    pos.get("slug", ""),
                    pos["side"],
                    f"orphan_momentum:{sell_tag}",
                    pos.get("entry_price", 0),
                    bid,
                    realized,
                    0.0,
                    0.0,
                    pnl,
                    _duree,
                    "sold",
                    "never_opened",
                    loss_tag=pos.get("loss_tag"),
                )
                if pos.get(
                    "mm_handoff"
                ):  # position transferee depuis le MM (Steven 23/07)
                    self.state["mm"]["daily_pnl"] = round(
                        self.state["mm"]["daily_pnl"] + pnl, 4
                    )
                    self._log(
                        f"🎯 [MM] pnl {pnl:+.3f}$ credite au P&L du jour MM (position transferee)"
                    )

        # ── STOP LOSS ARB NEAR RESOLUTION (Phase 2 : seuils via STRATEGY_RISK_PARAMS) ──
        for key, pos in list(mk["open"].items()):
            if pos.get("strat") != "bothside" or pos.get("mode") != "real":
                continue
            # RISK-FREE : NE JAMAIS COUPER UNE SEULE JAMBE (Steven 05/08, meme
            # classe de bug que celle deja trouvee le 29/07 et corrigee dans
            # _manage_pnl_tier_exits via is_risk_free -- cette fonction-ci
            # avait le meme trou, jamais colmate. Couper la jambe "perdante"
            # d'une paire VRAIMENT couplee (combined<1, profit garanti quel
            # que soit le resultat) transforme un gain garanti en resultat
            # incertain -- l'inverse de l'effet protecteur recherche ici pour
            # un hedge normal.
            if pos.get("is_risk_free"):
                continue
            secs_left = pos["end_ts"] - now
            _arb_sl_secs = bothside_params.get("arb_sl_secs_left", ARB_SL_SECS_LEFT)
            _arb_sl_bid = bothside_params.get(
                "arb_sl_bid_threshold", ARB_SL_BID_THRESHOLD
            )
            if secs_left > _arb_sl_secs or secs_left <= 3:
                continue
            shares = pos["filled_shares"]
            if shares < MIN_ORDER_SIZE_SHARES:
                continue
            _book = self._live.get_book_sync(pos["token_id"])
            _bid = _book["bids"][0][0] if _book and _book.get("bids") else None
            if _bid is None or _bid >= _arb_sl_bid:
                continue  # prix encore Correct, on tient
            # BINANCE CONFIRMATION : on ne vend QUE si le signal Binance confirme
            # que la direction est LOCKEE (l'autre cote gagne franchement).
            binance_ok = False
            if pos.get("pair"):
                _px = _binance_price(pos["pair"])
                _st = _strike_at(pos["pair"], pos["start_ts"], slug=pos.get("slug"))
                if _px is not None and _st is not None:
                    _gap = abs(_px - _st)
                    _margin = _px * 0.0008  # 0.08% du prix = minimum confirmatoire
                    binance_ok = _gap >= _margin
            if not binance_ok:
                self._tlog(
                    f"arb_sl_nobin_{key}",
                    f"🛑 [ARB][SL] {sym} {pos['slug']} {pos['side']} bid={_bid:.3f} < "
                    f"seuil {_arb_sl_bid:.3f} MAIS Binance ne confirme pas -> on tient",
                )
                continue
            # BINANCE CONFIRME + bid < seuil -> vendre la jambe perdante
            sold = self._sell_orphan(
                pos["token_id"],
                shares,
                f" {sym} {pos['slug']} {pos['side']} ARB-SL",
            )
            if sold > 0:
                realized = round(sold * (_bid - pos["entry_price"]), 3)
                pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
                pos["filled_shares"] = 0.0
                pnl = pos["realized_pnl"]
                pos.update(win=pnl > 0, pnl=pnl, resolved_by="arb_stoploss")
                mk["trades"].append(pos)
                del mk["open"][key]
                self._record_trade_pnl(sym, pnl)
                icon = "✅ WIN " if pnl > 0 else "❌ LOSS"
                self._log(
                    f"{icon} [ARB][SL] {sym} {pos['slug']} {pos['side']} coupe {sold} parts "
                    f"@ {_bid:.3f} (entree {pos['entry_price']:.3f}) realize={realized:+.3f}$ "
                    f"[Binance confirme, jambe gagnante reste ouverte]"
                )

    # ── MARKET MAKER CONDITIONNEL (Steven 23/07) ──
    def _mm_cancel_symbol(self, sym):
        """Annule les 2 quotes actives (bid+ask) d'un symbole, si elles existent."""
        q = self.state["mm"]["quotes"].pop(sym, None)
        if not q:
            return
        for oid in (q.get("bid_order_id"), q.get("ask_order_id")):
            if oid:
                self._live.cancel_order(oid)

    def _mm_cancel_all(self):
        for sym in list(self.state["mm"]["quotes"].keys()):
            self._mm_cancel_symbol(sym)

    def _mm_check_daily_reset(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if self.state["mm"]["daily_pnl_date"] != today:
            self.state["mm"]["daily_pnl"] = 0.0
            self.state["mm"]["daily_pnl_date"] = today
            self.state["mm"]["consec_adverse"] = 0

    def _mm_archive_rolled_position(self, sym, cur, new_token_id):
        """ROULEMENT DE FENETRE (Steven 23/07, correctif bug "balance 0") :
        chaque fenetre 5min est un TOKEN CTF DIFFERENT. Quand le token suivi
        pour un symbole change, la position de l'ANCIENNE fenetre (si non
        nulle) est ARCHIVEE dans 'pending' pour resolution differee via
        Polymarket (meme mecanisme que _resolve_market), au lieu de rester
        dans l'inventaire courant ou elle causait des tentatives de vente sur
        le nouveau token (rejetees : 'balance 0', on ne le detient pas)."""
        mmst = self.state["mm"]
        old_shares = cur.get("shares", 0.0) or 0.0
        # GARDE ANTI-DOUBLON (Steven 23/07, correctif double-comptage) : si ce
        # (slug, token_id) est DEJA dans pending (ex: 2 fenetres actives se
        # chevauchant brievement -> plusieurs archivages du meme token dans le
        # meme tick), ne pas re-ajouter une 2e entree pour la MEME position.
        already = any(pp["token_id"] == cur.get("token_id") for pp in mmst["pending"])
        if (
            abs(old_shares) >= 0.01
            and cur.get("token_id")
            and cur.get("slug")
            and not already
        ):
            mmst["pending"].append(
                {
                    "sym": sym,
                    "slug": cur["slug"],
                    "token_id": cur["token_id"],
                    "shares": old_shares,
                    "avg_entry": cur.get("avg_entry") or cur.get("bid") or 0.5,
                    "end_ts": cur.get("end_ts", time.time()),
                }
            )
            self._log(
                f"🎯 [MM] {sym} {cur['slug']} fenetre roulee, {old_shares} parts "
                f"archivees -> resolution differee (Polymarket)"
            )
        for oid in (cur.get("bid_order_id"), cur.get("ask_order_id")):
            if oid:
                self._live.cancel_order(oid)
        mmst["inventory"][sym] = 0.0  # repart a zero sur le NOUVEAU token

    def _mm_resolve_pending(self):
        """Regle le P&L des positions archivees par un roulement de fenetre
        (Steven 23/07), via la VRAIE resolution Polymarket (meme source que
        _resolve_market : data-api, pas Binance). Appelee UNE FOIS par tick,
        hors thread par-symbole (pending est partage entre symboles)."""
        mmst = self.state["mm"]
        if not mmst["pending"]:
            return
        now = time.time()
        still = []
        for pos in mmst["pending"]:
            if now < pos["end_ts"] + SETTLE_DELAY:
                still.append(pos)
                continue
            out = self._live.settled_outcome(pos["slug"], pos["token_id"])
            if not out["resolved"]:
                if out.get("found") is False and now > pos["end_ts"] + 180:
                    won = True  # absente + temps ecoule = gagnee et deja reclamee
                else:
                    still.append(pos)
                    continue
            else:
                won = out["won"]
            shares, entry = pos["shares"], pos["avg_entry"]
            pnl = round(shares * (1 - entry) if won else -shares * entry, 4)
            mmst["daily_pnl"] = round(mmst["daily_pnl"] + pnl, 4)
            mmst["fills"].append(
                {"ts": now, "sym": pos["sym"], "side": "RESOLUTION", "realized": pnl}
            )
            mmst["fills"] = mmst["fills"][-200:]
            icon = "✅" if pnl > 0 else "❌"
            self._log(
                f"{icon} [MM][RESOLUTION] {pos['sym']} {pos['slug']} {shares} parts "
                f"pnl={pnl:+.3f}$ (pnl_jour={mmst['daily_pnl']:+.3f}$)"
            )
        mmst["pending"] = still

    def _mm_check_markouts(self):
        """CONTROLE DE QUALITE DE FILL (Steven 23/07, "aucun controle de
        qualite de fill... il y a forcement des solutions") : 15s apres
        chaque fill BID, verifie si le prix mid a bouge CONTRE nous (achat
        suivi d'une baisse = fill adverse -> on a probablement ete pris par
        un vendeur mieux informe juste avant un mouvement defavorable) ou EN
        NOTRE FAVEUR (le marche a tenu/monte). Alimente mmst["consec_adverse"]
        -> donne des DENTS reelles au kill switch deja en place (avant, seul
        le P&L final d'une ASK/resolution y contribuait, bien trop tard pour
        reagir a un mauvais fill)."""
        mmst = self.state["mm"]
        if not mmst["markout_pending"]:
            return
        now = time.time()
        still = []
        for mo in mmst["markout_pending"]:
            if now < mo["check_at"]:
                still.append(mo)
                continue
            book = self._live.get_book_sync(mo["token_id"])
            mid = None
            if book:
                bids, asks = book.get("bids") or [], book.get("asks") or []
                if bids and asks:
                    mid = round((bids[0][0] + asks[0][0]) / 2, 4)
            if mid is None:
                continue  # carnet illisible -> retente au prochain tick (garde l'entree)
            markout = round(
                mid - mo["fill_price"], 4
            )  # positif = favorable pour un achat
            adverse = (
                markout < -0.01
            )  # tolerance 1c (bruit de cotation, pas un vrai signal)
            mmst["consec_adverse"] = mmst["consec_adverse"] + 1 if adverse else 0
            icon = "⚠️" if adverse else "✅"
            self._log(
                f"{icon} [MM][MARKOUT] {mo['sym']} fill@{mo['fill_price']:.3f} -> "
                f"mid+15s={mid:.3f} markout={markout:+.3f} "
                f"({'ADVERSE' if adverse else 'ok'}, consec_adverse={mmst['consec_adverse']})"
            )
        mmst["markout_pending"] = still

    def _mm_tick(self, sym, mode, m, p):
        """Un cycle de cotation conditionnelle pour un marche (Steven 23/07).
        Coup d'oeil rapide sur le flux :
        1) garde-fous globaux (enabled, kill switch, mode reel, data fraiche)
        2) fair value + regime -> si pas CALM, annule les quotes et sort
        3) detecte les fills depuis le dernier tick (delta de position on-chain)
           -> met a jour inventaire, P&L du jour, compteur de fills adverses
        4) recalcule bid/ask, ne recote que si l'ecart depasse le seuil (anti-churn)."""
        from core.btc_updown import (
            _binance_price,
            _strike_at,
            momentum as _momentum,
            get_price_history,
        )

        mmst = self.state["mm"]
        self._mm_check_daily_reset()
        if not mmst.get("enabled") or mmst.get("killed") or mode != "real":
            return
        if mmst["daily_pnl"] <= mm.MM_DAILY_LOSS_LIMIT_USD:
            mmst["killed"] = True
            mmst["kill_reason"] = (
                f"perte du jour {mmst['daily_pnl']:.2f}$ <= limite {mm.MM_DAILY_LOSS_LIMIT_USD}$"
            )
            self._mm_cancel_all()
            self._log(f"🛑 [MM] KILL SWITCH : {mmst['kill_reason']}")
            return
        if mmst["consec_adverse"] >= mm.MM_MAX_CONSEC_ADVERSE:
            mmst["killed"] = True
            mmst["kill_reason"] = f"{mmst['consec_adverse']} fills adverses consecutifs"
            self._mm_cancel_all()
            self._log(f"🛑 [MM] KILL SWITCH : {mmst['kill_reason']}")
            return

        slug = m.get("slug")
        outcomes = json.loads(m.get("outcomes") or "[]")
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
        if len(outcomes) != 2 or len(token_ids) != 2:
            return
        # cote UNIQUE (Steven : "poster bid ET ask sur le MEME token, ex UP") —
        # simplification V1, evite de gerer 2 inventaires symetriques par marche.
        try:
            up_i = [o.lower() for o in outcomes].index("up")
        except ValueError:
            up_i = 0
        token_id = token_ids[up_i]

        # ROULEMENT DE FENETRE (Steven 23/07) : si le token suivi pour ce
        # symbole a change depuis le dernier tick, l'ancienne position (si non
        # nulle) est ARCHIVEE pour resolution differee -> plus jamais de vente
        # tentee sur un token qu'on ne detient plus ("balance 0").
        cur_existing = mmst["quotes"].get(sym)
        if (
            cur_existing
            and cur_existing.get("token_id")
            and cur_existing["token_id"] != token_id
        ):
            cur_existing["slug"] = cur_existing.get("slug") or slug
            self._mm_archive_rolled_position(sym, cur_existing, token_id)
            mmst["quotes"].pop(sym, None)

        strike = _strike_at(p["pair"], p["start_ts"], slug=slug)
        spot = self._ws.spot_price(p["pair"]) or _binance_price(
            p["pair"]
        )  # WS temps reel, fallback REST
        now = synced_now()
        secs_left = p["end_ts"] - now
        data_age_s = 0.0  # _binance_price cache <=1.5s ; pas de flux WS en V1 (voir docstring module)
        if strike is None or spot is None or secs_left <= 0:
            return

        book = self._live.get_book_sync(token_id)
        bid, ask = (None, None)
        depth_ok = False
        if book:
            bids, asks = book.get("bids") or [], book.get("asks") or []
            bid = bids[0][0] if bids else None
            ask = asks[0][0] if asks else None
            bid_sz = bids[0][1] if bids else 0.0
            ask_sz = asks[0][1] if asks else 0.0
            depth_ok = min(bid_sz, ask_sz) >= MIN_ORDER_SIZE_SHARES
        poly_mid = (
            round((bid + ask) / 2, 4) if (bid is not None and ask is not None) else None
        )

        sigma = mm.estimate_sigma(get_price_history(p["pair"]))
        mom = _momentum(p["pair"])
        fair = mm.fair_value(
            spot, strike, secs_left, sigma, mom["fast_pct_s"] if mom else 0.0
        )
        regime = classify = mm.classify_regime(
            secs_left, mom, 0, poly_mid, fair, depth_ok, data_age_s
        )

        # ── DERIVE LENTE (Steven 23/07, "selection adverse sur cotes perimees") :
        # un marche peut glisser progressivement SANS jamais depasser les seuils
        # PANIC/MOMENTUM tick-par-tick (trop bruyants pour du 5min), mais deriver
        # quand meme de facon significative sur ~1-2 min pendant que notre bid
        # reste immobile (anti-churn 1.5c). On detecte ce glissement SOUTENU
        # separement et on le traite comme une dislocation (regime DRIFT). ──
        if regime == "CALM" and poly_mid is not None:
            hist = mmst.setdefault("mid_history", {}).setdefault(sym, [])
            hist.append((now, poly_mid))
            cutoff = now - mm.MM_DRIFT_WINDOW_S * 2
            while hist and hist[0][0] < cutoff:
                hist.pop(0)
            drift = mm.detect_drift(hist)
            if drift is not None and abs(drift) >= mm.MM_DRIFT_THRESHOLD:
                regime = "DRIFT"

        prev_regime = mmst["regime_log"].get(sym, {}).get("regime")
        if prev_regime != regime:
            self._log(
                f"🎯 [MM] {sym} {slug} regime {prev_regime or '?'} -> {regime} "
                f"(fair={fair:.3f} poly_mid={poly_mid} secs_left={secs_left:.0f})"
            )
        mmst["regime_log"][sym] = {
            "regime": regime,
            "ts": now,
            "fair": fair,
            "poly_mid": poly_mid,
        }

        # ── DETECTION DE FILL (delta de position depuis le dernier tick connu) ──
        q = mmst["quotes"].get(sym)
        pos_now = self._live.position_size(token_id)
        if q and pos_now is not None and pos_now >= 0:
            prev_shares = q.get("shares", 0.0)
            delta = round(pos_now - prev_shares, 2)
            if abs(delta) >= 0.05:
                px_ref = q.get("bid") if delta > 0 else q.get("ask")
                if delta > 0:  # fill sur notre BID -> on a achete
                    new_inv = mmst["inventory"].get(sym, 0.0) + delta * (px_ref or fair)
                    mmst["inventory"][sym] = round(new_inv, 4)
                    self._log(
                        f"🎯 [MM][FILL] {sym} {slug} BID rempli +{delta} parts @ {px_ref}"
                    )
                    # MARKOUT (Steven 23/07, "aucun controle de qualite de fill") :
                    # verifie dans 15s si le marche a bouge CONTRE nous juste apres
                    # ce fill -> alimente consec_adverse (donc le kill switch deja
                    # en place), sans attendre le P&L final de la resolution.
                    mmst["markout_pending"].append(
                        {
                            "sym": sym,
                            "side": "bid",
                            "fill_price": px_ref or fair,
                            "token_id": token_id,
                            "check_at": now + 15.0,
                        }
                    )
                else:  # fill sur notre ASK -> on a vendu, P&L realise vs entree moyenne
                    entry = q.get("avg_entry", px_ref or fair)
                    realized = round(abs(delta) * ((px_ref or fair) - entry), 4)
                    mmst["daily_pnl"] = round(mmst["daily_pnl"] + realized, 4)
                    mmst["inventory"][sym] = round(
                        mmst["inventory"].get(sym, 0.0) + delta * entry, 4
                    )
                    mmst["consec_adverse"] = (
                        0 if realized > 0 else mmst["consec_adverse"] + 1
                    )
                    mmst["fills"].append(
                        {"ts": now, "sym": sym, "side": "ASK", "realized": realized}
                    )
                    mmst["fills"] = mmst["fills"][-200:]
                    self._log(
                        f"🎯 [MM][FILL] {sym} {slug} ASK rempli {delta} parts @ {px_ref} "
                        f"realise={realized:+.3f}$ (pnl_jour={mmst['daily_pnl']:+.3f}$)"
                    )
            q["shares"] = pos_now

        time_trigger = secs_left <= mm.MM_TIME_HANDOFF_S
        if regime != "CALM" or time_trigger:
            # HANDOFF VERS L'ORPHAN MANAGER (Steven 23/07, "1/3" : des que le
            # regime quitte CALM ET qu'on tient de l'inventaire non couvert,
            # on ne l'abandonne plus passivement jusqu'a resolution -> il est
            # transfere IMMEDIATEMENT vers _manage_orphans, EXACTEMENT le meme
            # systeme deja utilise par l'arb (signal Binance : HOLD si gagnant,
            # vente par PALIERS si perdant). Corrige le cas ETH 50c qui a perdu
            # 100% du cout, jamais surveille apres l'annulation de la quote.
            #
            # CORRECTIF 2 (Steven 23/07, meme jour) : un marche peut rester
            # CALM (jamais de PANIC/MOMENTUM) sur TOUTE la duree de la fenetre
            # tout en derivant contre nous -> ETH et SOL ont perdu 100% du cout
            # ainsi, jamais transferes car le regime n'avait JAMAIS quitte
            # CALM. On ajoute donc un declencheur TEMPS : au-dela de
            # MM_TIME_HANDOFF_S restantes, le transfert a lieu MEME en CALM.
            shares_held = q.get("shares", 0.0) if q else (pos_now or 0.0)
            mk = self.state["markets"][sym]
            side_name = outcomes[up_i]
            handoff_key = f"{slug}|{side_name}"
            # GARDE ANTI-DOUBLON (Steven 23/07, correctif double-comptage) : si
            # cette position a DEJA ete transferee (l'orphan manager la gere
            # deja, peut-etre partiellement vendue en paliers), ne PAS ecraser
            # l'entree existante -> ca effacerait le realized_pnl deja
            # accumule et reinitialiserait le prix d'entree sur la fair value
            # COURANTE au lieu du vrai cout d'achat (source du double credit).
            if shares_held >= MIN_ORDER_SIZE_SHARES and handoff_key not in mk["open"]:
                avg_entry = (q or {}).get("avg_entry") or fair
                mk["open"][handoff_key] = {
                    "symbol": sym,
                    "slug": slug,
                    "side": side_name,
                    "mode": "real",
                    "strat": "orphan",
                    "token_id": token_id,
                    "entry_price": avg_entry,
                    "filled_shares": round(shares_held, 2),
                    "cost": round(shares_held * avg_entry, 2),
                    "start_ts": p["start_ts"],
                    "pair": p["pair"],
                    "end_ts": p["end_ts"],
                    "opened_ts": time.time(),
                    "buffer": 0.0,
                    "mm_handoff": True,
                }
                reason = (
                    f"regime {regime}"
                    if regime != "CALM"
                    else f"temps ({secs_left:.0f}s restantes, CALM persistant)"
                )
                self._log(
                    f"🎯 [MM] {sym} {slug} {reason} + inventaire {round(shares_held, 2)} parts "
                    f"non couvert -> TRANSFERE a l'orphan manager (plus jamais laisse a l'abandon)"
                )
                mmst["inventory"][sym] = (
                    0.0  # position desormais suivie par l'orphan manager, pas par le MM
                )
            self._mm_cancel_symbol(sym)
            return

        # ── FILTRE ZONE RENTABLE (Steven 23/07, backtest 256 fenetres reelles) :
        # on ne cote QUE quand le PRIX REEL DU MARCHE (poly_mid) est dans
        # [0.55, 0.80] — le seul intervalle a P&L positif net du spread.
        # Au-dessus de 0.80 (favori fort) : -60$ au backtest (flips ruineux,
        # exactement le 0.91->0.33 qui a fait perdre en reel). En dessous de
        # 0.55 (coin-flip) : negatif au bid reel. On se base sur poly_mid (le
        # marche), PAS sur fair (estimation Binance, ici volontairement biaisee
        # vers les extremes -> inutilisable comme filtre de zone).
        if poly_mid is None or not (mm.MM_SWEET_LOW <= poly_mid <= mm.MM_SWEET_HIGH):
            self._mm_cancel_symbol(sym)
            return

        # ── PLAFONDS D'INVENTAIRE (Steven : "jamais reserver 100% du capital, ni
        # moyenner une position perdante") ──
        net_notional = mmst["inventory"].get(sym, 0.0)
        total_notional = sum(abs(v) for v in mmst["inventory"].values())
        skew = mm.inventory_skew(net_notional, mm.MM_MAX_NOTIONAL_PER_MARKET)
        half_spread = mm.half_spread_for(
            sigma, data_age_s, 1.0 if depth_ok else 0.3, secs_left
        )
        quote = mm.compute_quote(fair, half_spread, skew)
        if quote is None:
            self._mm_cancel_symbol(sym)
            return
        des_bid, des_ask = quote
        # ── FIX 23/07 (Steven, faille reelle : ETH achete a 0.02$ malgre fair=
        # 0.16-0.20 au moment du check) : le garde-fou precedent ne verifiait
        # que `fair`/`poly_mid`, PAS le prix REELLEMENT calcule pour le bid.
        # Avec un demi-spread large (jusqu'a 0.12), bid = fair - half_spread
        # peut retomber sous 0.15 meme quand fair est au-dessus. On verifie
        # maintenant le PRIX EFFECTIF de la quote, pas seulement l'estimation
        # qui a servi a le calculer.
        if des_bid < mm.MM_LEG_MIN:
            self._mm_cancel_symbol(sym)
            return
        cur = mmst["quotes"].get(sym, {})
        # ── FIX 23/07 (Steven) : un ASK n'est postable QUE si on detient
        # REELLEMENT des parts a vendre (le CLOB Polymarket ne permet pas de
        # vendre a decouvert). L'ancienne condition `net_notional <= 0` etait
        # VRAIE des l'ouverture d'une fenetre neuve (inventaire a 0 par
        # defaut, avant meme le 1er achat) -> tentative de vente d'un token
        # qu'on ne possede pas encore, rejetee en boucle ("balance 0").
        shares_held = cur.get("shares", 0.0) if cur else (pos_now or 0.0)
        allow_bid = (
            secs_left > mm.MM_CLOSE_CUTOFF_S
            and abs(net_notional) < mm.MM_MAX_NOTIONAL_PER_MARKET
            and total_notional < mm.MM_MAX_NOTIONAL_TOTAL
            and net_notional >= 0
        )
        allow_ask = (
            shares_held >= MIN_ORDER_SIZE_SHARES and secs_left > mm.MM_CLOSE_CUTOFF_S
        )

        size_shares = max(
            MIN_ORDER_SIZE_SHARES,
            round(mm.MM_QUOTE_NOTIONAL_USD / max(des_bid, 0.02), 2),
        )

        # ── BID ──
        if allow_bid and (
            bid is None or des_bid < bid + mm.MM_TICK
        ):  # jamais traverser le book
            if (
                cur.get("bid") is None
                or abs(cur["bid"] - des_bid) >= mm.MM_REQUOTE_MIN_DELTA
            ):
                if cur.get("bid_order_id"):
                    self._live.cancel_order(cur["bid_order_id"])
                r = self._live.post_limit_buy(token_id, des_bid, size_shares)
                cur["bid_order_id"] = r.get("order_id") if r.get("success") else None
                cur["bid"] = des_bid if r.get("success") else cur.get("bid")
                if not r.get("success"):
                    self._tlog(
                        f"mm_bid_{sym}",
                        f"⚠️ [MM] {sym} bid non poste : {r.get('error')}",
                    )
        elif cur.get("bid_order_id"):
            self._live.cancel_order(cur["bid_order_id"])
            cur["bid_order_id"] = None

        # ── ASK ──
        if allow_ask and (ask is None or des_ask > ask - mm.MM_TICK):
            if (
                cur.get("ask") is None
                or abs(cur["ask"] - des_ask) >= mm.MM_REQUOTE_MIN_DELTA
            ):
                if cur.get("ask_order_id"):
                    self._live.cancel_order(cur["ask_order_id"])
                ask_size = min(
                    size_shares, shares_held
                )  # jamais vendre plus que ce qu'on detient
                r = self._live.post_limit_sell(token_id, des_ask, ask_size)
                cur["ask_order_id"] = r.get("order_id") if r.get("success") else None
                cur["ask"] = des_ask if r.get("success") else cur.get("ask")
                if r.get("success"):
                    cur.setdefault("avg_entry", des_bid)
                if not r.get("success"):
                    self._tlog(
                        f"mm_ask_{sym}",
                        f"⚠️ [MM] {sym} ask non poste : {r.get('error')}",
                    )
        elif cur.get("ask_order_id"):
            self._live.cancel_order(cur["ask_order_id"])
            cur["ask_order_id"] = None

        cur["shares"] = pos_now if pos_now is not None else cur.get("shares", 0.0)
        cur["token_id"] = token_id
        cur["slug"] = slug
        cur["end_ts"] = p["end_ts"]
        mmst["quotes"][sym] = cur

    # ── DELTA-NEUTRE both-side au bid (Steven 23/07) ──
    def _dn_cancel_symbol(self, sym):
        q = self._dn_quotes.pop(sym, None)
        if not q:
            return
        for side in ("Up", "Down"):
            oid = q.get(side, {}).get("oid")
            if oid:
                self._live.cancel_order(oid)

    def _dn_cancel_all(self):
        for sym in list(self._dn_quotes.keys()):
            self._dn_cancel_symbol(sym)

    def _dn_tick(self, sym, mode, m, p):
        """Poste un BID sur Up ET sur Down (au best bid WS) quand leur somme est
        < DN_COMBINED_TARGET -> si les 2 se remplissent, arb garanti (entree
        combined < 1). Chaque jambe remplie sans sa jumelle -> tracee 'orphan'
        (gestion active existante). Lit le carnet en TEMPS REEL (WS)."""
        if mode != "real":
            return
        dn = self.state["dn"]
        slug = m.get("slug")
        outcomes = json.loads(m.get("outcomes") or "[]")
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
        if len(outcomes) != 2 or len(token_ids) != 2:
            return
        try:
            up_i = [o.lower() for o in outcomes].index("up")
        except ValueError:
            up_i = 0
        dn_i = 1 - up_i
        tid_up, tid_dn = token_ids[up_i], token_ids[dn_i]
        now = synced_now()
        secs_left = p["end_ts"] - now

        # roulement de fenetre -> annule les quotes de l'ancienne
        q = self._dn_quotes.get(sym)
        if q and q.get("slug") != slug:
            self._dn_cancel_symbol(sym)
            q = None

        # detection de fill (delta de position on-chain sur chaque token)
        if q:
            for side, tid in (("Up", tid_up), ("Down", tid_dn)):
                leg = q.get(side, {})
                pos_now = self._live.position_size(tid)
                prev = leg.get("shares", 0.0)
                if pos_now is not None and pos_now - prev >= 0.05:
                    got = round(pos_now - prev, 2)
                    leg["shares"] = pos_now
                    self._log(
                        f"⚖️ [DN][FILL] {sym} {slug} {side} +{got} parts @ {leg.get('bid')}"
                    )
                    # jumelle remplie ? -> paire complete = arb verrouille
                    other = "Down" if side == "Up" else "Up"
                    if (
                        q.get(other, {}).get("shares", 0.0) >= MIN_ORDER_SIZE_SHARES
                        and pos_now >= MIN_ORDER_SIZE_SHARES
                    ):
                        up_px, dn_px = q["Up"].get("bid"), q["Down"].get("bid")
                        self._log(
                            f"✅ [DN] {sym} {slug} PAIRE COMPLETE -> arb verrouille "
                            f"(Up@{up_px} + Down@{dn_px})"
                        )
                        # ROUTE vers la resolution 'bothside' existante (deja testee/
                        # fiable) au lieu de reinventer le P&L -> _resolve_market
                        # combine les 2 jambes en 1 trade net au moment voulu.
                        M = min(q["Up"]["shares"], q["Down"]["shares"])
                        mk = self.state["markets"][sym]
                        for lside, lpx, ltid in (
                            ("Up", up_px, tid_up),
                            ("Down", dn_px, tid_dn),
                        ):
                            mk["open"].pop(
                                f"{slug}|dn_{lside}", None
                            )  # remplace l'entree orphan provisoire
                            mk["open"][f"{slug}|{lside}"] = {
                                "symbol": sym,
                                "slug": slug,
                                "side": lside,
                                "mode": "real",
                                "strat": "bothside",
                                "token_id": ltid,
                                "entry_price": lpx or 0.5,
                                "filled_shares": round(M, 2),
                                "cost": round(M * (lpx or 0.5), 2),
                                "start_ts": p["start_ts"],
                                "pair": p["pair"],
                                "end_ts": p["end_ts"],
                                "opened_ts": time.time(),
                                "buffer": 0.0,
                            }
                        # libere le slot -> un autre symbole peut demarrer une paire
                        del self._dn_quotes[sym]
                        for lside, tid in (("Up", tid_up), ("Down", tid_dn)):
                            if lside != side:
                                oid_leg = q.get(lside, {})
                                if oid_leg.get("oid"):
                                    self._live.cancel_order(oid_leg["oid"])
                        return
                    else:
                        # jambe orpheline -> gestion active (on GARDE l'autre bid poste
                        # pour tenter de completer la paire ; l'orphan filet si echec)
                        mk = self.state["markets"].get(sym)
                        if (
                            mk is not None
                            and f"{slug}|{outcomes[up_i if side == 'Up' else dn_i]}"
                            not in mk["open"]
                        ):
                            entry = leg.get("bid") or 0.5
                            mk["open"][f"{slug}|dn_{side}"] = {
                                "symbol": sym,
                                "slug": slug,
                                "side": outcomes[up_i if side == "Up" else dn_i],
                                "mode": "real",
                                "strat": "orphan",
                                "token_id": tid,
                                "entry_price": entry,
                                "filled_shares": round(pos_now, 2),
                                "cost": round(pos_now * entry, 2),
                                "start_ts": p["start_ts"],
                                "pair": p["pair"],
                                "end_ts": p["end_ts"],
                                "opened_ts": time.time(),
                                "buffer": 0.0,
                                "dn_leg": True,
                            }

        # ne PAS ouvrir de nouvelle paire trop tard ou si cap atteint
        open_pairs = (
            len({pp["slug"] for pp in dn["pairs"].values()}) if dn.get("pairs") else 0
        )
        if secs_left < DN_MIN_SECS_LEFT:
            return
        if q is None and open_pairs >= DN_MAX_OPEN_PAIRS:
            return
        # ── UN SEUL SYMBOLE ACTIF A LA FOIS (Steven 23/07, fix "balance
        # insuffisante en boucle") : ne demarre PAS sur un nouveau symbole si un
        # autre a deja des quotes en cours -> tout le capital libre va sur UNE
        # paire, qui a une vraie chance de se completer. ──
        if q is None and len(self._dn_quotes) >= DN_MAX_ACTIVE_SYMBOLS:
            return
        if q is None:
            cash, _ = self._read_cash(max_age=5)
            if cash is None or cash < DN_MIN_FREE_CASH:
                self._tlog(
                    "dn_nofund",
                    f"💸 [DN] solde libre {cash}$ < {DN_MIN_FREE_CASH}$ "
                    f"-> pas assez pour completer une paire, on attend",
                )
                return

        # lecture carnet TEMPS REEL (WS) AVEC PROFONDEUR (Steven 23/07, faille
        # trouvee a l'audit : on postait au best bid sans jamais verifier qu'il
        # y avait une VRAIE contrepartie en face -> risque de poster dans un
        # carnet fantome qui ne remplit jamais, ou pire, un carnet fin qui nous
        # adverse-selectionne).
        bu = self._ws.book_depth(tid_up)
        bd = self._ws.book_depth(tid_dn)
        if not bu or not bd:
            return
        up_bid, up_bid_sz, up_ask, up_ask_sz, _ = bu
        dn_bid, dn_bid_sz, dn_ask, dn_ask_sz, _ = bd
        # profondeur minimale cote ASK (preuve d'un marche 2 faces actif, pas
        # un carnet abandonne) sur les 2 tokens
        if up_ask_sz < MIN_ORDER_SIZE_SHARES or dn_ask_sz < MIN_ORDER_SIZE_SHARES:
            if q:
                self._dn_cancel_symbol(sym)
            return
        comb_bid = up_bid + dn_bid
        if comb_bid > DN_COMBINED_TARGET:
            # pas d'arb-au-bid pour l'instant -> annule d'eventuelles quotes obsoletes
            if q:
                self._dn_cancel_symbol(sym)
            return

        # ── BUDGET COMBINE VERROUILLE (Steven 23/07, FIX du bug qui a fait perdre :
        # le DN re-cotait chaque jambe au best bid du marche INDEPENDAMMENT ->
        # quand une jambe etait deja remplie a f1 et que l'autre montait, il
        # remontait son bid -> Up@0.25 rempli + Down remonte a 0.76 = combined
        # 1.01 = PERTE au lieu d'arb). Regle : chaque jambe est capee pour que
        # fill_1ere + bid_2e <= DN_COMBINED_TARGET. Si le marche exige un bid
        # au-dela de ce cap, l'arb n'existe plus -> on NE remonte PAS (on garde
        # le bid bas, quitte a ne pas etre rempli). L'arb reste GARANTI. ──
        q = self._dn_quotes.setdefault(sym, {"slug": slug, "Up": {}, "Down": {}})
        q["slug"] = slug
        up_leg, dn_leg = q["Up"], q["Down"]
        up_filled = up_leg.get("shares", 0.0) >= MIN_ORDER_SIZE_SHARES
        dn_filled = dn_leg.get("shares", 0.0) >= MIN_ORDER_SIZE_SHARES
        # prix de reference deja engage sur chaque cote (fill ou bid poste)
        up_ref = up_leg.get("bid") if (up_filled or up_leg.get("oid")) else None
        dn_ref = dn_leg.get("bid") if (dn_filled or dn_leg.get("oid")) else None
        for side, tid, mkt_bid, other_ref, leg, is_filled in (
            ("Up", tid_up, up_bid, dn_ref, up_leg, up_filled),
            ("Down", tid_dn, dn_bid, up_ref, dn_leg, dn_filled),
        ):
            leg["token"] = tid
            if is_filled:
                continue  # deja rempli sur ce cote, on ne re-poste jamais
            # cap : le budget restant apres l'autre jambe (fixee ou best bid marche)
            reserved = other_ref if other_ref is not None else mkt_bid
            cap = round(DN_COMBINED_TARGET - reserved, 2)
            des = round(min(mkt_bid, cap), 2)
            if des < 0.02:  # plus de marge d'arb sur ce cote -> on ne poste pas
                if leg.get("oid"):
                    self._live.cancel_order(leg["oid"])
                    leg["oid"] = None
                    leg["bid"] = None
                continue
            if leg.get("bid") is None or abs(leg["bid"] - des) >= DN_REQUOTE_DELTA:
                if leg.get("oid"):
                    self._live.cancel_order(leg["oid"])
                r = self._live.post_limit_buy(tid, des, DN_SHARES)
                leg["oid"] = r.get("order_id") if r.get("success") else None
                leg["bid"] = des if r.get("success") else leg.get("bid")
                if not r.get("success"):
                    self._tlog(
                        f"dn_{sym}_{side}",
                        f"⚠️ [DN] {sym} {side} bid non poste : {r.get('error')}",
                    )
        self._tlog(
            f"dn_post_{sym}",
            f"⚖️ [DN] {sym} {slug} bids Up@{up_leg.get('bid')}+Down@{dn_leg.get('bid')} "
            f"(comb_marche={comb_bid:.3f})",
            every=20.0,
        )

    def _open_pair_parallel_real(
        self, sym, m, p, legs, target_shares, combined, tier_label="", no_slippage=False
    ):
        """ARB REEL PARALLELISE, PARTS-EGALES (Steven 22/07, correctifs 23/07) :
        0) PREFLIGHT : verifie les 2 jambes (ask + profondeur) EN PARALLELE,
           SANS RIEN POSTER. Si l'une des 2 echoue, on ABANDONNE LES DEUX -> zero
           capital engage, plus jamais d'achat suivi d'une revente en catastrophe.
        1) POST des 2 jambes quasi-simultane (ordre market dollars = target*prix)
           -> on ne bloque plus 8s sur le fill de la 1re avant de poster la 2e.
        2) SYNC : apres fills, rebalance a parts EGALES = min(fill1, fill2)
           (revend l'exces) -> arb propre malgre les partials.
        3) ROLLBACK REEL : si une jambe trop vide malgre tout (fill partiel post-
           preflight), la jambe filled est TRACKEE COMME ORPHAN (gestion active
           via signal Binance dans _manage_orphans) au lieu d'etre revendue a
           l'aveugle -> ne dump plus une jambe qui serait en train de GAGNER.

        Audit 23/07 : 5/5 tentatives reelles avaient echoue (carnet SOL/XRP thin,
        bouge de 0.04-0.10$ en ~2s), et le rollback aveugle avait deja coute
        -0.35$ de spread pur sur une jambe revendue 3s apres achat. Ce correctif
        vise les 2 causes : latence/slippage (preflight) et dump reflexe (orphan)."""
        # CHRONO (Steven 04/08, "faut etre + rapide alors ?") : on arrete de
        # supposer ou passe le temps. Chaque etape du chemin critique est
        # horodatee, et le detail est logue au moment du post -> on saura
        # exactement quoi optimiser au lieu d'optimiser au hasard.
        _t = {"t0": time.time()}
        # WATCHDOG + RISK LIMITS (Steven 25/07)
        allowed, wd_reason = self._check_fill_watchdog(sym)
        if not allowed:
            self._log(f"🛑 [WATCHDOG][REEL] {sym} SKIP arb : {wd_reason}")
            return False
        rl_ok, rl_reason = self._check_risk_limits(sym, "bothside")
        if not rl_ok:
            self._log(f"🛑 [RISK][REEL] {sym} SKIP arb : {rl_reason}")
            return False
        mk = self.state["markets"][sym]
        slug = m.get("slug")
        (side1, tid1, px1), (side2, tid2, px2) = legs
        # QUALITE DE DECISION (Steven 04/08, "5 metriques prioritaires") :
        # capture SEULE de variables deja calculees + une lecture memoire
        # deja en cache (book_depth ne fait aucun appel reseau) -> aucun
        # changement de logique, aucune latence ajoutee. try/except partout,
        # jamais fatal si une source manque.
        _feed_age_ms = None
        try:
            _bd1 = self._ws.book_depth(tid1)
            _bd2 = self._ws.book_depth(tid2)
            _ages = [time.time() - bd[4] for bd in (_bd1, _bd2) if bd]
            if _ages:
                _feed_age_ms = round(max(_ages) * 1000)
        except Exception:
            pass
        _edge_pct = round((1.0 - combined) * 100, 2)
        _ev_net_fees_pct = round((1.0 - combined - COMB_ASK_FEE_ESTIMATE) * 100, 2)
        # CAP ANTI-SLIPPAGE DYNAMIQUE (Steven 23/07) : proportionnel a l'edge reel
        # (1-combined) plutot qu'un +0.02 fixe -> un arb a grosse marge tolere plus
        # de mouvement (aligne avec la volatilite REELLEMENT observee), un arb a
        # marge fine reste protege (peu de budget slippage -> plutot skip que
        # payer plus que le gain). Toujours borne [MIN, MAX] absolu.
        edge = max(0.0, 1.0 - combined)
        slip_total = min(
            REAL_SLIPPAGE_MAX * 2,
            max(REAL_SLIPPAGE_MIN * 2, edge * REAL_SLIPPAGE_EDGE_FRACTION),
        )
        # GARDE RISK-FREE (Steven 30/07, "juste milieu... rester risk free") :
        # le plancher fixe (REAL_SLIPPAGE_MIN*2) peut, sur un edge fin (ex:
        # 0.02 depuis que REAL_MAX_COMBINED=0.98), depasser l'edge lui-meme ->
        # combined execute > 1.0 = perte possible malgre le tag "risk free".
        # On borne DONC TOUJOURS slip_total a 90% de l'edge -> au moins 10% de
        # l'edge detecte survit en profit reel garanti, quoi que fassent
        # MIN/MAX ci-dessus.
        slip_total = min(slip_total, edge * 0.9)
        # EV net de fees ET slippage (Steven 04/08, "5+ metriques") : ce qui
        # resterait si TOUT le budget slippage etait consomme -- le pire cas
        # realiste, pas l'optimiste _ev_net_fees_pct seul.
        _ev_net_slippage_pct = round((edge - COMB_ASK_FEE_ESTIMATE - slip_total) * 100, 2)
        slip_each = round(slip_total / 2, 3)
        cap1 = min(BOTH_SIDE_LEG_MAX, round(px1 + slip_each, 2))
        cap2 = min(BOTH_SIDE_LEG_MAX, round(px2 + slip_each, 2))
        # PHASE 0 : PREFLIGHT PARALLELE, aucun ordre poste. Si l'une des 2 jambes
        # echoue (ask deja trop haut OU carnet trop fin), on ABANDONNE LES DEUX
        # -> zero capital engage, pas de rollback a faire.
        # WS D'ABORD (Steven 30/07, "faut pas tergiverser") : preflight_leg()
        # faisait un ROUND-TRIP REST (~200-500ms, meme via le pool) pour
        # revalider un prix deja capture par WS (<100ms) quelques lignes plus
        # haut -> sur un marche 5min qui bouge de 10-45c/s en fin de fenetre,
        # ce seul aller-retour suffisait a rendre le cap (fige au moment de
        # la detection) obsolete avant meme le check. book_depth() du WS feed
        # porte DEJA bid/ask + tailles (pas juste le prix) -> zero appel
        # reseau si les 2 jambes ont un flux frais. Fallback REST uniquement
        # si l'une des 2 est absente/perimee (>STALE_S).
        min_depth = round(target_shares * REAL_MIN_DEPTH_RATIO, 2)

        def _ws_preflight(tid, cap):
            # PROFONDEUR CUMULEE (Steven 04/08) : cf. ask_depth_upto(). On
            # additionne tous les niveaux <= cap au lieu du seul meilleur ask,
            # sinon on rejette des paires que l'ordre remplirait sans peine.
            try:
                r = self._ws.ask_depth_upto(tid, cap)
            except Exception:
                r = None
            if not r:
                return None
            ask, depth, _ts = r
            if ask is None:
                return None
            if ask > cap:
                return {"ok": False, "ask": ask, "depth": depth or 0.0,
                        "error": f"ask {ask:.3f} > max {cap:.3f}"}
            if (depth or 0.0) < min_depth:
                return {"ok": False, "ask": ask, "depth": depth or 0.0,
                        "error": f"profondeur {depth:.1f} < min {min_depth:.1f} parts (cumul jusqu'a {cap:.3f})"}
            return {"ok": True, "ask": ask, "depth": depth, "error": None}

        if PREFLIGHT_DISABLED:
            self._tlog(
                f"preflight_off_{sym}",
                f"⚠️ [PREFLIGHT-OFF] {sym} {slug} check prix/profondeur SAUTE "
                f"(PREFLIGHT_DISABLED=True) -> achat direct sans reverif",
            )
            pf = {side1: {"ok": True}, side2: {"ok": True}}
        else:
            pf = {}
            rest_needed = {}
            for sd, tid, cap in ((side1, tid1, cap1), (side2, tid2, cap2)):
                r = _ws_preflight(tid, cap)
                if r is not None:
                    pf[sd] = r
                else:
                    rest_needed[sd] = tid
            if rest_needed:
                pf_futs = {
                    sd: self._pool.submit(
                        self._live.preflight_leg, tid,
                        cap1 if sd == side1 else cap2, min_depth,
                    )
                    for sd, tid in rest_needed.items()
                }
                for sd, fut in pf_futs.items():
                    pf[sd] = fut.result()
        # REEVALUATION AUX PRIX FRAIS (Steven 04/08, "ca doit passer") :
        # defaut de logique trouve dans les rejets. On comparait le prix FRAIS
        # de chaque jambe a un plafond calcule sur le prix PERIME de la
        # detection (cap = px_fige + marge) -> des qu'une jambe bougeait on
        # rejetait, SANS JAMAIS se demander si la paire restait un arb valide
        # aux prix du moment. Exemple reel : BTC "Up ask 0.640 > max 0.460"
        # rejete, alors que si Down etait tombe a 0.330 le combine valait 0.97
        # = arb toujours bon. On teste donc le VRAI critere : combine frais des
        # 2 jambes. S'il tient sous le seuil reel avec un edge suffisant, on
        # recalcule les caps sur ces prix frais et on continue. Ce n'est PAS un
        # assouplissement : on valide le combine qu'on va reellement payer,
        # ce qui est plus strict que valider deux jambes isolement.
        if not (pf[side1]["ok"] and pf[side2]["ok"]):
            _a1 = pf[side1].get("ask")
            _a2 = pf[side2].get("ask")
            _depth_ko = any(
                "profondeur" in (pf[sd].get("error") or "") for sd in (side1, side2)
            )
            if _a1 and _a2 and not _depth_ko:
                _comb_frais = round(_a1 + _a2, 4)
                _edge_frais = round(1.0 - _comb_frais, 4)
                if _comb_frais <= REAL_MAX_COMBINED and _edge_frais >= EDGE_REDUCE_THRESHOLD:
                    _slip_f = min(
                        REAL_SLIPPAGE_MAX * 2,
                        max(REAL_SLIPPAGE_MIN * 2, _edge_frais * REAL_SLIPPAGE_EDGE_FRACTION),
                    )
                    _slip_f = min(_slip_f, _edge_frais * 0.9) / 2.0
                    cap1 = min(BOTH_SIDE_LEG_MAX, round(_a1 + _slip_f, 2))
                    cap2 = min(BOTH_SIDE_LEG_MAX, round(_a2 + _slip_f, 2))
                    px1, px2 = _a1, _a2
                    combined = _comb_frais
                    self._log(
                        f"♻️ [PREFLIGHT-REEVAL] {sym} {slug} prix bouges mais combine "
                        f"FRAIS={_comb_frais:.3f} (edge {_edge_frais*100:.1f}%) toujours "
                        f"valide -> on continue sur {side1}@{_a1:.3f}+{side2}@{_a2:.3f} "
                        f"(caps recalcules {cap1:.3f}/{cap2:.3f})"
                    )
                    pf[side1]["ok"] = pf[side2]["ok"] = True
        if not (pf[side1]["ok"] and pf[side2]["ok"]):
            for sd in (side1, side2):
                if not pf[sd]["ok"]:
                    self._log(
                        f"🚫 [ARB][REEL] {sym} {slug} {sd} PREFLIGHT echec : {pf[sd]['error']} "
                        f"-> abandon des 2 jambes, aucun ordre poste"
                    )
            return False
        # PHASE 1 : poster les 2 ordres EN PARALLELE (Steven 23/07 : le post
        # sequentiel precedent laissait la 2e jambe exposee a la latence reseau
        # complete de la 1re -> fenetre de slippage bien plus large que necessaire).
        with self._order_lock:
            if no_slippage:
                # SANS SLIPPAGE (Steven 30/07, "on achette les deux coter sans
                # slippage") : GTC exact au prix fige px1/px2 au lieu du market
                # FAK avec buffer -> soit rempli PILE au prix qui garantit
                # l'edge, soit pas du tout (jamais paye plus que valide).
                # UN SEUL ENVOI (Steven 30/07, "un seul envois demander
                # plusieurs achats") : post_limit_pair_no_slippage() poste les
                # 2 ordres signes dans UNE requete HTTP (post_orders batch de
                # la lib CLOB) au lieu de 2 requetes concurrentes -> supprime
                # l'ecart de latence CLIENT entre les 2 legs.
                # RESET TRACKING WS AVANT POST (Steven 30/07, "on a WS aussi")
                # : canal user pousse les fills en direct -> plus rapide que le
                # polling REST position_size qui attend le reglement custody
                # on-chain (source confirmee du delai detection->achat).
                try:
                    self._ws.reset_fill_tracking(tid1)
                    self._ws.reset_fill_tracking(tid2)
                except Exception:
                    pass
                # FIX (Steven 30/07, "encore bcp d'orphelins") : cap1/cap2
                # (prix fige + marge de slippage, deja borne a 90% de l'edge
                # plus haut) etaient CALCULES mais jamais utilises -> l'achat
                # partait au prix BRUT fige (px1/px2), un ordre bien plus
                # passif que prevu, qui ratait le marche des qu'il bougeait
                # un peu -> exactement la cause mecanique des mismatchs
                # frequents. On poste desormais au cap (le vrai plafond
                # tolere), toujours protege par le garde-fou edge*0.9.
                _t["avant_post"] = time.time()
                _pair_res = self._live.post_limit_pair_no_slippage(
                    tid1, cap1, target_shares, tid2, cap2, target_shares
                )
                _t["apres_post"] = time.time()
                _tim = _pair_res.get("timing") or {}
                _avant_post_ms = round((_t["avant_post"] - _t["t0"]) * 1000)
                _post_ms = round((_t["apres_post"] - _t["avant_post"]) * 1000)
                _total_ms = round((_t["apres_post"] - _t["t0"]) * 1000)
                self._log(
                    f"⏱️ [CHRONO] {sym} {slug} entree_fonction->avant_post="
                    f"{_avant_post_ms}ms | "
                    f"post_lui_meme={_post_ms}ms "
                    f"(baseline={_tim.get('baseline_ms','?')}ms signature={_tim.get('signature_ms','?')}ms "
                    f"rust_resign={_tim.get('rust_resign_ms','?')}ms[{'RUST' if _tim.get('rust_used') else 'python'}] "
                    f"post_orders={_tim.get('post_orders_ms','?')}ms) | "
                    f"TOTAL={_total_ms}ms"
                )
                # HISTORIQUE STRUCTURE (Steven 04/08, "onglet dedie latence
                # historique") : le texte de log seul ne permet pas de calculer
                # des percentiles fiables. Liste bornee en memoire + persistee,
                # exposee via /api/latency.
                self.state.setdefault("latency_history", []).append({
                    "ts": _t["t0"],
                    "symbol": sym,
                    "avant_post_ms": _avant_post_ms,
                    "post_ms": _post_ms,
                    "baseline_ms": _tim.get("baseline_ms"),
                    "signature_ms": _tim.get("signature_ms"),
                    "rust_resign_ms": _tim.get("rust_resign_ms"),
                    "rust_used": _tim.get("rust_used", False),
                    "post_orders_ms": _tim.get("post_orders_ms"),
                    "total_ms": _total_ms,
                })
                if len(self.state["latency_history"]) > 1000:
                    del self.state["latency_history"][: len(self.state["latency_history"]) - 1000]
                if _pair_res.get("success") and len(_pair_res.get("legs", [])) == 2:
                    _l1, _l2 = _pair_res["legs"]
                    h1 = {"posted": _l1["success"], "before": _l1["before"],
                          "ask": px1, "order_id": _l1.get("order_id"), "token_id": tid1,
                          "error": None if _l1["success"] else "batch: jambe non postee"}
                    h2 = {"posted": _l2["success"], "before": _l2["before"],
                          "ask": px2, "order_id": _l2.get("order_id"), "token_id": tid2,
                          "error": None if _l2["success"] else "batch: jambe non postee"}
                else:
                    err = _pair_res.get("error", "post_orders batch echec")
                    self._log(f"🚫 [ARB][REEL] {sym} {slug} envoi groupe echec : {err}")
                    h1 = {"posted": False, "before": 0.0, "ask": px1, "error": err}
                    h2 = {"posted": False, "before": 0.0, "ask": px2, "error": err}
                for sd, h in ((side1, h1), (side2, h2)):
                    if not h.get("posted"):
                        self._log(
                            f"🚫 [ARB][REEL] {sym} {slug} {sd} ordre NON POSTE : {h.get('error', '?')}"
                        )
                post_futs = None
            else:
                # CHRONO (Steven 05/08, "l'onglet latence ne se rempli pas...
                # malgre les nombreux trades") : trouve que latency_history
                # n'etait alimente QUE par la branche no_slippage (GTC prix
                # fige), jamais par CETTE branche (ordre MARKET), qui est
                # pourtant celle qui execute reellement les trades ce soir --
                # onglet vide malgre l'activite, pas un manque d'activite.
                _t["avant_post"] = time.time()
                post_futs = {
                    side1: self._pool.submit(
                        self._live.post_market_order,
                        tid1,
                        cap1,
                        max(self.arb_budget(), round(target_shares * px1, 2)),
                    ),
                    side2: self._pool.submit(
                        self._live.post_market_order,
                        tid2,
                        cap2,
                        max(self.arb_budget(), round(target_shares * px2, 2)),
                    ),
                }
            if post_futs is not None:
                h1 = post_futs[side1].result()
                h2 = post_futs[side2].result()
                if "avant_post" in _t:
                    _t["apres_post"] = time.time()
                    _avant_post_ms = round((_t["avant_post"] - _t["t0"]) * 1000)
                    _post_ms = round((_t["apres_post"] - _t["avant_post"]) * 1000)
                    _total_ms = round((_t["apres_post"] - _t["t0"]) * 1000)
                    self.state.setdefault("latency_history", []).append({
                        "ts": _t["t0"],
                        "symbol": sym,
                        "avant_post_ms": _avant_post_ms,
                        "post_ms": _post_ms,
                        "baseline_ms": None,
                        "signature_ms": None,
                        "rust_resign_ms": None,
                        "rust_used": False,
                        "post_orders_ms": _post_ms,
                        "total_ms": _total_ms,
                    })
                    if len(self.state["latency_history"]) > 1000:
                        del self.state["latency_history"][: len(self.state["latency_history"]) - 1000]
        # ECHECS DE POST EXPLICITES (Steven 22/07) : plus d'echec muet (deja
        # loggue au-dessus pour la voie no_slippage/batch -> pas de doublon)
        if not no_slippage:
            for sd, h in ((side1, h1), (side2, h2)):
                if not h.get("posted"):
                    self._log(
                        f"🚫 [ARB][REEL] {sym} {slug} {sd} ordre NON POSTE : {h.get('error', '?')}"
                    )
        # PHASE 2 : confirmer les fills EN PARALLELE
        # WS D'ABORD (Steven 30/07, "on a WS aussi", jusqu'a 2s) : le canal
        # user pousse le fill des qu'il matche, bien avant que le solde
        # custody on-chain (REST position_size) ne se mette a jour -> on
        # verifie ce chemin rapide en premier. JAMAIS la seule source pour du
        # capital reel : si rien vu apres 2s, on retombe integralement sur le
        # polling REST confirm_fill (8s), qui reste la verite terrain.
        fills = {side1: 0.0, side2: 0.0}
        _ws_t0 = time.time()
        _ws_deadline = _ws_t0 + 2.0
        _remaining = {side1: True, side2: True}
        if no_slippage:
            while time.time() < _ws_deadline and any(_remaining.values()):
                for side, tid in ((side1, tid1), (side2, tid2)):
                    if not _remaining[side]:
                        continue
                    try:
                        seen = self._ws.fill_since(tid)
                    except Exception:
                        seen = 0.0
                    if seen >= target_shares - 0.01:
                        fills[side] = seen
                        _remaining[side] = False
                        # LOG DEDIE (Steven 30/07, "dedie des log... on tente
                        # le ws avant le post") : preuve visible que le canal
                        # WS user a bien detecte le fill, et en combien de
                        # temps, au lieu d'un chemin totalement silencieux.
                        self._log(
                            f"⚡ [WS-FILL] {sym} {slug} {side} {round(seen,2)} parts "
                            f"vues via WS en {round((time.time()-_ws_t0)*1000)}ms "
                            f"(avant fallback REST)"
                        )
                if any(_remaining.values()):
                    time.sleep(0.1)
            if any(_remaining.values()):
                self._log(
                    f"⏱️ [WS-FILL] {sym} {slug} rien vu via WS apres "
                    f"{round((time.time()-_ws_t0)*1000)}ms -> fallback REST (confirm_fill)"
                )
        futs = {}
        # DELAI ETENDU (Steven 30/07, "revente trop rapide, arb loupe") : 8s
        # coupait parfois juste avant qu'un GTC en attente ne finisse par
        # matcher -> unwind premature d'une jambe qui allait completer un
        # arb entier. 15s laisse plus de marge au GTC restant, tout en
        # restant borne (pas d'attente indefinie sur une jambe nue).
        CONFIRM_FILL_TIMEOUT_S = 15.0
        for side, tid, h in ((side1, tid1, h1), (side2, tid2, h2)):
            if h.get("posted") and _remaining.get(side, True):
                futs[side] = self._pool.submit(
                    self._live.confirm_fill, tid, h["before"], CONFIRM_FILL_TIMEOUT_S
                )
        for side, fut in futs.items():
            try:
                fills[side] = fut.result()
            except Exception:
                fills[side] = 0.0
        f1, f2 = fills[side1], fills[side2]
        if no_slippage:
            # GTC non/partiellement rempli apres le timeout -> annule le reste
            # (sinon un ordre exact-price traine ouvert sur le book).
            for h, fv in ((h1, f1), (h2, f2)):
                if h.get("posted") and h.get("order_id") and fv < target_shares - 0.01:
                    self._live.cancel_order(h["order_id"])
        M = min(f1, f2)
        # SEUIL DE SUCCES en $ (Steven 23/07) : avec un cap a REAL_VALIDATION_LEG_USD
        # ($1), le nombre de PARTS remplies est structurellement < 5 (plancher CLOB
        # des ordres LIMITE, sans rapport avec les ordres MARKET utilises ici) ->
        # l'ancien seuil "M < 5 parts" aurait classe CHAQUE succes comme un echec.
        # On juge desormais sur la VALEUR remplie, pas le nombre de parts.
        min_val = min(f1 * px1, f2 * px2)
        # FALLBACK COMPLETION (Steven 30/07, "on doit pouvoir se couvrir et ne
        # faire que des vrais arb", 36 min sans arb complet) : avant
        # d'abandonner, UNE tentative d'achat agressif/marketable (FAK, pas
        # passif) sur la jambe manquante, plafonnee au MEME cap risk-free
        # deja calcule (cap1/cap2, borne a 90% de l'edge) -> soit ca complete
        # une VRAIE paire (mieux qu'un aller-retour a perte), soit le cap
        # refuse et on retombe sur l'unwind normal. Jamais de prix chasse
        # au-dela de la marge de securite deja etablie.
        if no_slippage and abs(f1 - f2) > 0.05 and max(f1, f2) >= MIN_ORDER_SIZE_SHARES * 0.5:
            _under_side, _under_tid, _under_cap = (
                (side1, tid1, cap1) if f1 < f2 else (side2, tid2, cap2)
            )
            _need = round(max(f1, f2) - min(f1, f2), 2)
            if _need >= 0.5:
                _fb_res = self._live.snipe_buy_market(
                    _under_tid, _under_cap, round(_need * _under_cap, 2)
                )
                _fb_filled = _fb_res.get("filled_shares", 0.0)
                if _fb_filled > 0:
                    if _under_side == side1:
                        f1 = round(f1 + _fb_filled, 2)
                    else:
                        f2 = round(f2 + _fb_filled, 2)
                    M = min(f1, f2)
                    min_val = min(f1 * px1, f2 * px2)
                    self._log(
                        f"🎯 [FALLBACK-COMPLETE] {sym} {slug} {_under_side} "
                        f"{_fb_filled} parts achetees en rattrapage (cap={_under_cap:.3f}) "
                        f"-> {'PAIRE COMPLETEE' if M >= min(f1,f2) - 0.01 else 'toujours partiel'}"
                    )
                else:
                    self._log(
                        f"🎯 [FALLBACK-COMPLETE] {sym} {slug} {_under_side} echec "
                        f"(prix au-dessus du cap {_under_cap:.3f} ou pas de liquidite) "
                        f"-> unwind normal"
                    )
        # 2E ESSAI EN REQUETES SEPAREES (Steven 30/07, "reactive le arb en 2
        # requetes si 2 fail a acheter les deux") : quand le POST GROUPE (1
        # requete pour les 2 jambes) n'a RIEN rempli du tout (les 2 a 0), on
        # retente une fois via 2 requetes INDIVIDUELLES (post_limit_order_handle,
        # deja existant, pas utilise depuis le passage au batch) sur prix
        # frais, toujours plafonnees par cap1/cap2 (meme garde risk-free).
        # Objectif : plus de volume de tentatives sans jamais depasser la
        # marge de securite deja etablie.
        if no_slippage and f1 <= 0.01 and f2 <= 0.01:
            _book1 = self._live.get_book_sync(tid1)
            _book2 = self._live.get_book_sync(tid2)
            _ask1 = _book1["asks"][0][0] if _book1 and _book1.get("asks") else None
            _ask2 = _book2["asks"][0][0] if _book2 and _book2.get("asks") else None
            if _ask1 is not None and _ask1 <= cap1 and _ask2 is not None and _ask2 <= cap2:
                self._log(
                    f"🔁 [2-REQUETES] {sym} {slug} post groupe vide -> retente "
                    f"en 2 requetes separees (ask1={_ask1:.3f}<=cap1={cap1:.3f}, "
                    f"ask2={_ask2:.3f}<=cap2={cap2:.3f})"
                )
                _r_futs = {
                    side1: self._pool.submit(
                        self._live.post_limit_order_handle, tid1, _ask1, target_shares
                    ),
                    side2: self._pool.submit(
                        self._live.post_limit_order_handle, tid2, _ask2, target_shares
                    ),
                }
                _r1 = _r_futs[side1].result()
                _r2 = _r_futs[side2].result()
                f1 = _r1.get("filled_shares", 0.0) if _r1.get("success") else 0.0
                f2 = _r2.get("filled_shares", 0.0) if _r2.get("success") else 0.0
                M = min(f1, f2)
                min_val = min(f1 * px1, f2 * px2) if f1 and f2 else min(f1 * _ask1, f2 * _ask2)
                self._log(
                    f"🔁 [2-REQUETES] {sym} {slug} resultat : f1={f1} f2={f2}"
                )
            else:
                self._log(
                    f"🔁 [2-REQUETES] {sym} {slug} abandon (prix deja au-dessus "
                    f"du cap sur au moins une jambe) -> pas de 2e essai"
                )
        # PHASE 3a : une jambe (quasi) vide -> l'AUTRE jambe (deja remplie et
        # payee) est REVENDUE IMMEDIATEMENT (Steven 30/07, "JAMAIS D'ACHAT
        # D'1 SEULE JAMBE en mode risk free" - explicite et sans exception).
        # Avant (23/07) on la trackait comme "orphan" geree par signal Binance
        # (garder si gagnant) -> mais ca VIOLE la garantie risk-free elle-meme :
        # une jambe seule est un pari directionnel nu, quel que soit le signal.
        # On desenroule TOUJOURS, meme au prix d'un peu de spread perdu -
        # c'est le cout accepte pour ne JAMAIS tenir de position non couverte.
        if min_val < self.arb_budget() * 0.5:
            # GARDE PAR SLUG (Steven 30/07, "encore subit perte" - XRP et SOL
            # ont perdu 2 fois de suite sur le MEME slug a des ticks
            # differents, ~30s d'ecart, apres le retrait complet des
            # cooldowns) : rien n'empechait de retenter la seconde d'apres
            # sur un marche qui vient de prouver que sa jambe manquante ne
            # matche pas -> perte de spread composee. Un mismatch reel sur ce
            # slug bloque desormais TOUTE nouvelle tentative sur CE MEME slug
            # (pas les autres marches, pas le symbole entier) pour le reste
            # de sa fenetre -> plus de perte en cascade sur un marche deja
            # prouve mauvais, sans reintroduire de cooldown general.
            # LISTE, pas set() (Steven 30/07, trouve en creusant "erreur
            # boucle: Object of type set is not JSON serializable" en boucle
            # depuis ce fix -> self._save() plante a CHAQUE tick, cassait le
            # scan de TOUS les symboles, pas juste celui-ci).
            _ms = mk.setdefault("mismatch_slugs", [])
            if slug not in _ms:
                _ms.append(slug)
            for side, tid, px, fv in ((side1, tid1, px1, f1), (side2, tid2, px2, f2)):
                if fv <= 0.01:
                    continue
                sold = self._sell_orphan(
                    tid, fv, f" {sym} {slug} {side} ARB-PARALLEL-UNWIND",
                    entry_price=px, symbol=sym, slug=slug, side=side,
                )
                self._log(
                    f"🔓 [ARB-PARALLEL-UNWIND] {sym} {slug} {side} {round(fv, 2)} parts "
                    f"revendues immediatement ({round(sold, 2)} confirmees) -> "
                    f"jamais de jambe nue en risk-free"
                )
                # FILET DE SECURITE (Steven 30/07, trouve en creusant la latence) :
                # si la vente n'a PAS entierement confirme dans les 4s de
                # _sell_orphan (GTC au bid = pas garanti de croiser tout de
                # suite, meme delai de reglement que les achats), le reste
                # DOIT rester TRACKE quelque part -> sinon un capital reel est
                # invisible pour toute gestion future (jamais l'intention,
                # juste un residu du unwind qui n'a pas fini de confirmer).
                _leftover = round(fv - sold, 2)
                if _leftover >= 0.01:
                    mk["open"][f"{slug}|{side}"] = {
                        "symbol": sym, "slug": slug, "side": side, "mode": "real",
                        "strat": "orphan", "token_id": tid, "entry_price": px,
                        "filled_shares": _leftover, "cost": round(_leftover * px, 2),
                        "start_ts": p["start_ts"], "pair": p["pair"],
                        "end_ts": p["end_ts"], "opened_ts": time.time(), "buffer": 0.0,
                    }
                    self._log(
                        f"🦺 [ORPHAN] {sym} {slug} {side} {_leftover} parts residuelles "
                        f"(vente non confirmee a temps) -> trackees pour gestion/retry"
                    )
            self._log(f"↩️ [BOTHSIDE][REEL] {sym} {slug} pair KO (f1={f1} f2={f2})")
            _fill_pct = round(min(f1, f2) / target_shares * 100, 1) if target_shares else 0.0
            self._record_execution_quality(
                sym, slug, _edge_pct, _ev_net_fees_pct, _feed_age_ms, filled=False,
                ev_net_slippage_pct=_ev_net_slippage_pct, fill_pct=_fill_pct,
            )
            self._reject(
                sym,
                slug,
                "hedge_failed_2nd",
                f"f1={f1:.2f} f2={f2:.2f} comb={combined:.3f}",
            )
            # Watchdog: enregistrer l'échec (si une jambe a rempli et pas l'autre)
            if (f1 > 0.01 and f2 <= 0.01) or (f2 > 0.01 and f1 <= 0.01):
                self._record_hedge_attempt(sym, False)
            return False
        # PHASE 3b : rebalance a parts EGALES = M (revend l'exces, fill verifie)
        # FIX (Steven 30/07, "on a garde Up qui a perdu... solde a baisse ?") :
        # decouvert via un ecart REEL entre notre etat interne (3.0/3.0 trackees,
        # is_risk_free) et le compte Polymarket reel (3.0 Up / 6.3 Down) -> le
        # commentaire d'origine ("sera redeem a la resolution, pas un risque")
        # etait FAUX : un exces non revendu N'EST PAS couvert par l'autre jambe,
        # c'est une exposition directionnelle nue, et l'ancien code ne le
        # trackait NULLE PART si la vente ratait -> invisible pour toujours,
        # jamais gere, jamais compte dans le pnl. Desormais : trackee comme
        # orphelin actif si non revendue.
        _residual_excess = {}
        for side, tid, fv, _px in ((side1, tid1, f1, px1), (side2, tid2, f2, px2)):
            excess = round(fv - M, 2)
            if excess >= 0.01:
                sold = self._sell_orphan(
                    tid, excess, f" {sym} {slug} {side} exces",
                    entry_price=_px, symbol=sym, slug=slug, side=side,
                )
                if sold < excess - 0.01:
                    _leftover_excess = round(excess - sold, 2)
                    self._log(
                        f"⚠️ [ARB][REEL] {sym} {slug} {side} exces {_leftover_excess} parts "
                        f"non revendu -> trackee comme orphelin actif (PAS ignoree)"
                    )
                    _residual_excess[side] = _leftover_excess
        for side, tid, px in ((side1, tid1, px1), (side2, tid2, px2)):
            mk["open"][f"{slug}|{side}"] = {
                "symbol": sym,
                "slug": slug,
                "side": side,
                "mode": "real",
                "strat": "bothside",
                "tier": tier_label.split("+")[0].split("-")[0] if tier_label else "",
                "token_id": tid,
                "entry_price": px,
                "filled_shares": round(M, 2),
                "cost": round(M * px, 2),
                "start_ts": p["start_ts"],
                "pair": p["pair"],
                "end_ts": p["end_ts"],
                "opened_ts": time.time(),
                "buffer": 0.0,
                # TAG RISK-FREE (Steven 30/07) : meme fix que la voie paper -
                # sans ca, cette paire reelle est geree comme un hedge
                # directionnel ordinaire par _manage_pnl_tier_exits (SL/TP
                # individuel) au lieu de rider a la resolution garantie.
                "is_risk_free": True,
                "arb_combined": round(combined, 4),
                "arb_edge": round(1 - combined, 4),
            }
            if side in _residual_excess:
                mk["open"][f"{slug}|{side}|excess"] = {
                    "symbol": sym, "slug": slug, "side": side, "mode": "real",
                    "strat": "orphan", "token_id": tid, "entry_price": px,
                    "filled_shares": _residual_excess[side],
                    "cost": round(_residual_excess[side] * px, 2),
                    "start_ts": p["start_ts"], "pair": p["pair"],
                    "end_ts": p["end_ts"], "opened_ts": time.time(), "buffer": 0.0,
                }
        # RECONCILIATION (Steven 30/07, "fetch mon histo poly reel") : verifie
        # le solde REEL on-chain (position_size, verite terrain Polymarket)
        # contre ce qu'on vient d'ecrire (M) -> si notre comptage de fill (f1/
        # f2, base sur WS/REST potentiellement en retard ou en course) derive
        # de la realite, on le detecte ICI plutot que de laisser un ecart
        # invisible et non gere trainer indefiniment.
        for side, tid in ((side1, tid1), (side2, tid2)):
            real_held = self._live.position_size(tid)
            if real_held >= 0 and round(real_held - M, 2) > 0.05:
                _drift = round(real_held - M, 2)
                self._log(
                    f"🔎 [RECONCILIATION] {sym} {slug} {side} : compte reel="
                    f"{real_held} vs trackee={M} -> ecart {_drift} parts non "
                    f"comptabilise, ajoute comme orphelin"
                )
                _px = px1 if side == side1 else px2
                mk["open"][f"{slug}|{side}|drift"] = {
                    "symbol": sym, "slug": slug, "side": side, "mode": "real",
                    "strat": "orphan", "token_id": tid, "entry_price": _px,
                    "filled_shares": _drift, "cost": round(_drift * _px, 2),
                    "start_ts": p["start_ts"], "pair": p["pair"],
                    "end_ts": p["end_ts"], "opened_ts": time.time(), "buffer": 0.0,
                }
        self._log(
            f"✅ [BOTHSIDE][REEL] {sym} {slug} PAIRE parallele [{tier_label}] {round(M, 2)} parts/cote "
            f"(f1={f1} f2={f2}) comb={combined:.3f} -> arb +{M * (1 - combined):.2f}$"
        )
        _fill_pct = round(M / target_shares * 100, 1) if target_shares else 100.0
        self._record_execution_quality(
            sym, slug, _edge_pct, _ev_net_fees_pct, _feed_age_ms, filled=True,
            ev_net_slippage_pct=_ev_net_slippage_pct, fill_pct=_fill_pct,
        )
        return True

    def _open_hedge_pair(self, sym, mode, m, p, legs, combined):
        """Wrapper (Steven 23/07) : serialise un HEDGE COMPLET (lecture cash ->
        favori -> underdog) via _hedge_lock -> plus de course entre marches
        paralleles sur le meme solde partage (voir _open_hedge_pair_impl)."""
        if mode == "real":
            with self._hedge_lock:
                return self._open_hedge_pair_impl(sym, mode, m, p, legs, combined)
        return self._open_hedge_pair_impl(sym, mode, m, p, legs, combined)

    def _open_hedge_pair_impl(self, sym, mode, m, p, legs, combined):
        """FAVORI + UNDERDOG (Steven 22/07 : "20cts sur perdant ET 1$+ sur favori").
        On met une mise DYNAMIQUE (visant +FAV_TARGET_NET_USD net) sur le FAVORI ET
        UNDERDOG_BET_USD ($0.20) sur l'UNDERDOG (le cote cheap). Le favori gagne
        usuellement (petit gain) ; l'underdog assure le flip (paie huge si ca se
        retourne au strike). Les 2 jambes = 'bothside' -> combinees en 1 trade net.
        Budgets par jambe selon fav (prix haut) / dog (prix bas)."""
        # WATCHDOG CHECK (Steven 25/07) : vérifier si ce symbole est en cooldown
        if mode == "real":
            allowed, wd_reason = self._check_fill_watchdog(sym)
            if not allowed:
                self._log(f"🛑 [WATCHDOG][REEL] {sym} SKIP hedge : {wd_reason}")
                return False
            # RISK LIMITS CHECK (Steven 25/07)
            rl_ok, rl_reason = self._check_risk_limits(sym, "hedge")
            if not rl_ok:
                self._log(f"🛑 [RISK][REEL] {sym} SKIP hedge : {rl_reason}")
                return False
        mk = self.state["markets"][sym]
        slug = m.get("slug")
        # trie : le prix le plus haut = FAVORI, le plus bas = UNDERDOG
        fav_side, fav_tid, fav_px = max(legs, key=lambda leg: leg[2])
        dog_side, dog_tid, dog_px = min(legs, key=lambda leg: leg[2])
        if fav_px > FAV_MAX_PRICE:
            return False  # mise requise explosive pour 0.30$ de gain -> pas rentable
        # mise favori DYNAMIQUE : net = +FAV_TARGET quand le favori gagne, dog deduit
        # KELLY BRANCHE (Steven 23/07, "hedge fonctionne bien +1.77, on peut brancher
        # le kelly pour gagner + par trade") : au lieu du plafond fixe 1.6$, le cap
        # est calcule par Kelly fractionne 1/4 (meme formule/constantes que le
        # sizing directionnel existant) sur le capital investissable -> mise plus
        # grosse quand l'edge/le capital le justifient, toujours borne par les
        # memes garde-fous (HARD_CAP_USD, MAX_FRACTION, solde disponible).
        cash, _ = self._read_cash(max_age=5)
        # RESERVE 1$ POUR L'UNDERDOG (Steven 23/07, "toujours garder 1$ de cote
        # au cas ou on aurait besoin de underdog") : le Kelly ne doit PAS pouvoir
        # engager tout le cash sur le favori et laisser 0$ pour l'assurance au
        # moment ou le danger se declenche.
        investable = max(0.0, (cash if cash is not None else 0.0) - self.floor() - 1.0)
        b_odds = (1 - fav_px) / max(fav_px, 1e-6)
        # PROBABILITE CALIBREE (Steven 29/07, "arrete de deviner, calcule") :
        # remplace l'edge FIXE devine (KELLY_ASSUMED_EDGE=6%, jamais recalibre)
        # par la vraie probabilite issue du mouvement brownien (volatilite
        # MESUREE par actif, prix/strike/temps restant reels). Le Kelly sizing
        # utilise alors l'edge REEL de ce trade precis, pas une moyenne
        # supposee universelle. Fallback sur l'ancienne constante si la
        # volatilite n'est pas encore mesuree (demarrage a froid).
        from core.btc_updown import probability_above_strike, _strike_at as _strike_at_early

        _strike_early = (
            _strike_at_early(p["pair"], p["start_ts"], slug=slug) if p.get("pair") else None
        )
        _secs_left_early = p.get("end_ts", 0) - synced_now()
        _p_calc = None
        if p.get("pair") and _strike_early:
            _live_px = self._ws.spot_price(p["pair"]) if hasattr(self, "_ws") else None
            if _live_px is not None:
                _p_calc = probability_above_strike(
                    p["pair"], _live_px, _strike_early, _secs_left_early
                )
                if fav_side == "Down" and _p_calc is not None:
                    _p_calc = 1.0 - _p_calc  # p_calc est P(Up) par defaut
        if _p_calc is not None and _p_calc < MIN_CALIBRATED_PROB:
            # FILTRE QUALITE (Steven 29/07) : le modele dit que ce "favori"
            # n'est en realite pas assez favori (proba calculee < 85%) ->
            # on ne force pas le trade, on attend une meilleure fenetre.
            self._tlog(
                f"proba_reject_{sym}",
                f"🧮❌ [PROBA-REJET] {sym} {slug} {fav_side} P_calc={_p_calc:.3f} "
                f"< {MIN_CALIBRATED_PROB} -> pas assez sur, on saute ce cycle",
                every=10.0,
            )
            return False
        if _p_calc is not None:
            real_edge = _p_calc - fav_px
            q_est = min(0.995, max(fav_px, fav_px + real_edge))
            self._tlog(
                f"proba_calc_{sym}",
                f"🧮 [PROBA] {sym} {slug} {fav_side} P_calc={_p_calc:.3f} "
                f"vs prix={fav_px:.3f} edge_reel={real_edge:+.3f} (vs suppose {KELLY_ASSUMED_EDGE:+.3f})",
                every=15.0,
            )
        else:
            q_est = min(0.995, fav_px + KELLY_ASSUMED_EDGE)
        f_star = max(0.0, (b_odds * q_est - (1 - q_est)) / b_odds) * KELLY_FRACTION
        # AUTO-ADAPTATION (Steven 23/07, "si il a besoin de 4$ mais 3$ dispo -1$
        # secu alors on prend le reste") : avant, max(MIN_BUDGET_USD, ...) forcait
        # une mise minimale MEME quand moins etait reellement disponible -> l'ordre
        # partait quand meme sur ce montant force, rejete "solde insuffisant", et
        # plus RIEN ne se passait. Desormais on prend le MINIMUM entre ce que Kelly
        # voudrait et ce qui est VRAIMENT disponible -> jamais de blocage total,
        # juste une mise plus petite si le capital est serre. Skip uniquement si
        # il ne reste litteralement rien (investable <= 0.05$).
        # FIX 23/07 (Steven, log "invalid amount ... min size: 1") : le favori
        # part en ordre MARKET, qui a le MEME minimum de 1$ que l'underdog. Un
        # plancher de 0.05$ (comme avant) faisait poster un ordre voue au rejet.
        # On saute proprement si l'investissable ne couvre pas ce minimum,
        # au lieu de tenter un montant illegal.
        if investable < 1.0:
            self._tlog(
                f"hedge_nofund_{sym}",
                f"💸 [HEDGE] {sym} {slug} solde insuffisant "
                f"apres reserve underdog ({investable:.2f}$ dispo < 1$ min market) -> saute",
                every=15.0,
            )
            return False
        fav_cap = max(
            1.0,
            min(
                investable * f_star, HARD_CAP_USD, investable * MAX_FRACTION, investable
            ),
        )
        dog_bet = (
            min(UNDERDOG_BET_USD, HEDGE_DOG_MAX_USD_VALIDATION)
            if HEDGE_VALIDATION_MODE
            else UNDERDOG_BET_USD
        )
        fav_bet = round(
            min(
                fav_cap, (FAV_TARGET_NET_USD + dog_bet) * fav_px / max(1e-6, 1 - fav_px)
            ),
            2,
        )
        # ── DECISION UNDERDOG (Steven 23/07, "si underdog avait mis 50c ou 1$
        # on aurait recupere plusieurs dizaines de $ sur le flip -> fait ce que
        # je dit") : le cap "cout <= gain favori" est RETIRE (l'underdog paie
        # ENORME sur un vrai flip, largement de quoi couvrir la perte du favori
        # -> le plafonner au petit gain favori (2-12c) le rendait quasi inutile).
        # On garde uniquement le signal danger_score (retournement REEL signale,
        # pas juste "au cas ou") ; l'underdog passe en ORDRE LIMITE (plancher 5
        # parts, pas 1$ comme le market qui echouait sur 0.20$), mise fixe visee.
        from core.btc_updown import danger_score, _strike_at, _binance_price

        fav_gain = round(fav_bet * (1 - fav_px) / fav_px, 4)
        strike = (
            _strike_at(p["pair"], p["start_ts"], slug=slug) if p.get("pair") else None
        )
        danger = (
            danger_score(p["pair"], strike)
            if (p.get("pair") and strike is not None)
            else 0
        )
        # SURVEILLANCE PASSIVE GROS ORDRES (Steven 29/07, etude Stanford/SMU
        # confirmee sur nos propres marches : "price at the end" pas TWAP ->
        # manipulation de derniere seconde toujours possible). USAGE DEFENSIF
        # UNIQUEMENT : si un pari massif apparait contre notre favori dans les
        # dernieres secondes, on renforce le danger -> assurance forcee, ou
        # skip total si trop extreme. On ne suit JAMAIS le pic pour en profiter.
        try:
            from core.whale_watch import detect_late_spike
            from core.btc_updown import momentum as _momentum_whale

            _cond_id = m.get("conditionId")
            _mom_whale = _momentum_whale(p["pair"]) if p.get("pair") else None
            _fast_pct = _mom_whale.get("fast_pct_s") if _mom_whale else None
            _spike = detect_late_spike(
                _cond_id, p.get("end_ts", 0), fav_side=fav_side,
                binance_fast_pct_s=_fast_pct,
            )
            if _spike.get("suspect") and _spike.get("against_fav"):
                # CORRELE (Steven 29/07, "art de la guerre" : 2 signaux qui se
                # confirment > 1 signal isole) : mise a fort levier CONTRE notre
                # favori ET vitesse Binance qui va dans le meme sens au meme
                # instant -> signal beaucoup plus fiable qu'un pari suspect
                # isole. On skip carrement plutot que de juste assurer.
                if _spike.get("correlated"):
                    danger = max(danger, NEAR_CERTAIN_SKIP_DANGER_MIN)
                    self._log(
                        f"🐋🚨 [WHALE-CORRELE] {sym} {slug} pari a fort levier "
                        f"(payout visé {_spike['payout']:.0f}$/{_spike['notional']:.0f}$ engages) "
                        f"CONTRE favori {fav_side} a {_spike['secs_left']:.0f}s "
                        f"+ vitesse Binance confirme le sens (wallet ...{_spike['wallet'][-6:]}) "
                        f"-> danger={danger}, skip probable"
                    )
                else:
                    danger = max(danger, NEAR_CERTAIN_DANGER_MIN_FOR_HEDGE + 5)
                    self._log(
                        f"🐋 [WHALE] {sym} {slug} pari suspect "
                        f"(payout visé {_spike['payout']:.0f}$/{_spike['notional']:.0f}$ engages) "
                        f"CONTRE favori {fav_side} a {_spike['secs_left']:.0f}s de la fin "
                        f"(wallet ...{_spike['wallet'][-6:]}) -> danger force a {danger}"
                    )
        except Exception:
            pass
        # COUVERTURE PROPORTIONNELLE (Steven 29/07) : shares underdog visent
        # DOG_COVERAGE_MULT x fav_bet de payout potentiel, pas un montant fixe.
        # Plancher HEDGE_DOG_STAKE_USD (l'assurance ne doit jamais couter moins
        # que le minimum deja valide) et plafond HEDGE_DOG_MAX_USD_VALIDATION.
        _dog_target_usd = max(
            HEDGE_DOG_STAKE_USD,
            min(fav_bet * DOG_COVERAGE_MULT * dog_px, HEDGE_DOG_MAX_USD_VALIDATION),
        )
        dog_shares_target = max(
            MIN_ORDER_SIZE_SHARES, round(_dog_target_usd / max(dog_px, 0.01), 2)
        )
        dog_cost_min = round(dog_shares_target * dog_px, 3)
        # ASSURANCE SYSTEMATIQUE (Steven 29/07, "pour chaque loss on doit avoir
        # un bothside qui gagne enormement + que la perte") : avant, l'underdog
        # n'etait achete QUE si danger>=8 -> vu en reel, un flip peut arriver
        # avec danger=2 (le score sous-estime, ce n'est qu'une heuristique) et
        # la perte n'etait alors PAS couverte du tout. L'assurance est desormais
        # TOUJOURS prise (peu couteuse vu le sizing proportionnel ci-dessus) ;
        # seul le vrai coinflip (prix pile sur le strike, gap check ci-dessous)
        # la saute, car il n'y a alors pas de "favori" a assurer.
        buy_dog = True
        # BINANCE CONTEXT CHECK : l'underdog n'a de sens que si le prix Binance
        # est assez loin du strike Poly -> il y a un vrai ecart directionnel qui
        # justifie la couverture. Sans ca, on achetait l'underdog quand le prix
        # etait pile sur le strike (= pile-ou-face pur).
        binance_min_gap_pct = 0.0003  # 0.03% minimum ecart prix/strike
        if buy_dog and p.get("pair") and strike and strike > 0:
            binance_px = _binance_price(p["pair"])
            if binance_px is not None:
                gap_pct = abs(binance_px - strike) / strike
                if gap_pct < binance_min_gap_pct:
                    buy_dog = False
                    self._tlog(
                        f"hedge_nodog_nogap_{sym}",
                        f"🛡️ [HEDGE] {sym} {slug} PAS d'underdog "
                        f"(gap Binance={gap_pct:.5f} < {binance_min_gap_pct}, "
                        f"prix trop proche du strike {strike}, pas un vrai favori) "
                        f"-> favori seul",
                        every=10.0,
                    )
        if not buy_dog:
            self._tlog(
                f"hedge_nodog_{sym}",
                f"🛡️ [HEDGE] {sym} {slug} PAS d'underdog -> favori seul, stop-loss actif",
                every=10.0,
            )

        # SKIP TOTAL (Steven 29/07) : marche trop retourne (flips frequents +
        # vitesse elevee) -> meme avec l'underdog, le risque n'en vaut pas la
        # chandelle -> on ne prend PAS le favori du tout ce cycle (retente au
        # prochain, le marche peut se calmer).
        if danger >= NEAR_CERTAIN_SKIP_DANGER_MIN:
            self._tlog(
                f"hedge_skip_danger_{sym}",
                f"⛔ [HEDGE-SKIP] {sym} {slug} danger={danger} >= "
                f"{NEAR_CERTAIN_SKIP_DANGER_MIN} -> marche trop instable, "
                f"pas de near-certain ce cycle (retournement trop probable)",
                every=10.0,
            )
            return False

        # ── EDGE MINIMUM (Zero-Lag Cut) : ne pas entrer si edge trop faible ──
        edge = max(0.0, 1.0 - combined)
        if edge < MIN_EDGE_FOR_SEQUENTIAL:
            self._log(
                f"🚫 [HEDGE][EDGE] {sym} {slug} edge={edge:.3f} < min {MIN_EDGE_FOR_SEQUENTIAL} "
                f"(combined={combined:.3f}) -> skip (risque orphan > gain attendu)"
            )
            return False

        # ── MICRO-SIZING (Zero-Lag Cut) : cap la mise par jambe si solde petit ──
        if cash is not None and cash < 10.0:
            fav_bet = min(fav_bet, MICRO_SIZING_MAX_LEG_USD)
            dog_bet = min(dog_bet, MICRO_SIZING_MAX_LEG_USD)

        if mode == "real":
            # ── PREFLIGHT PARALLÈLE (Steven 25/07) : vérifie les 2 jambes AVANT d'engager du capital
            fav_slip = round(
                max(
                    REAL_SLIPPAGE_MIN,
                    min(REAL_SLIPPAGE_MAX, (1 - fav_px) * REAL_SLIPPAGE_EDGE_FRACTION),
                ),
                2,
            )
            dog_slip = round(
                max(
                    REAL_SLIPPAGE_MIN,
                    min(REAL_SLIPPAGE_MAX, (1 - dog_px) * REAL_SLIPPAGE_EDGE_FRACTION),
                ),
                2,
            )
            fav_cap1 = min(0.99, round(fav_px + fav_slip, 2))
            dog_cap1 = min(0.99, round(dog_px + dog_slip, 2))
            min_depth = round(MIN_ORDER_SIZE_SHARES * REAL_MIN_DEPTH_RATIO, 2)

            pf_futs = {
                fav_side: self._pool.submit(
                    self._live.preflight_leg, fav_tid, fav_cap1, min_depth
                ),
                dog_side: self._pool.submit(
                    self._live.preflight_leg, dog_tid, dog_cap1, min_depth
                )
                if buy_dog
                else None,
            }
            pf = {}
            for sd, fut in pf_futs.items():
                if fut is not None:
                    pf[sd] = fut.result()

            if not pf[fav_side]["ok"]:
                self._log(
                    f"🚫 [HEDGE][REEL] {sym} {slug} {fav_side} PREFLIGHT echec : {pf[fav_side]['error']}"
                )
                self._reject(
                    sym,
                    slug,
                    "preflight_failed",
                    f"leg=fav err={pf[fav_side]['error']}",
                )
                return False
            if buy_dog and not pf[dog_side]["ok"]:
                self._log(
                    f"🚫 [HEDGE][REEL] {sym} {slug} {dog_side} PREFLIGHT echec : {pf[dog_side]['error']} -> favori seul"
                )
                self._reject(
                    sym,
                    slug,
                    "preflight_failed",
                    f"leg=dog err={pf[dog_side]['error']} fav_only",
                )
                buy_dog = False  # on continue favori seul si underdog indisponible

            # ── POST PARALLÈLE DES 2 JAMBES (Steven 25/07) : quasi-simultané pour réduire la fenêtre de slippage
            fav_before = self._live.position_size(fav_tid)
            dog_before = self._live.position_size(dog_tid) if buy_dog else 0.0

            post_futs = {}
            with self._order_lock:
                post_futs[fav_side] = self._pool.submit(
                    self._live.post_market_order, fav_tid, 0.99, fav_bet
                )
                if buy_dog:
                    dog_limit_px = round(min(0.99, dog_px + 0.01), 2)
                    # garantir notional >= 1$
                    if dog_shares_target * dog_limit_px < 1.0:
                        dog_shares_target = max(
                            dog_shares_target, math.ceil(1.0 / max(dog_limit_px, 0.01))
                        )
                    post_futs[dog_side] = self._pool.submit(
                        self._live.post_limit_buy,
                        dog_tid,
                        dog_limit_px,
                        dog_shares_target,
                    )

            h_fav = post_futs[fav_side].result()
            h_dog = (
                post_futs[dog_side].result()
                if buy_dog
                else {"success": False, "error": "not_attempted"}
            )

            if not h_fav.get("posted"):
                self._log(
                    f"🚫 [HEDGE][REEL] {sym} {slug} {fav_side} ordre NON POSTE : {h_fav.get('error', '?')}"
                )
                return False
            if buy_dog and not h_dog.get("success"):
                self._log(
                    f"🚫 [HEDGE][REEL] {sym} {slug} {dog_side} (limite) ordre NON POSTE : {h_dog.get('error', '?')}"
                )
                buy_dog = False

            # ── CONFIRMATION DES FILLS EN PARALLÈLE avec timeout adaptatif + hedge forcé
            start_time = time.time()
            fills = {fav_side: 0.0, dog_side: 0.0}
            force_triggered = False

            def confirm_with_timeout(tid, before, timeout_s, side_name):
                return self._live.confirm_fill(tid, before, timeout_s)

            # Premier passage : confirmation normale (8s max)
            confirm_futs = {}
            if h_fav.get("posted"):
                confirm_futs[fav_side] = self._pool.submit(
                    confirm_with_timeout, fav_tid, h_fav["before"], 8.0, fav_side
                )
            if buy_dog and h_dog.get("success"):
                confirm_futs[dog_side] = self._pool.submit(
                    confirm_with_timeout,
                    dog_tid,
                    h_dog.get("before", dog_before),
                    8.0,
                    dog_side,
                )

            for side, fut in confirm_futs.items():
                try:
                    fills[side] = fut.result()
                except Exception:
                    fills[side] = 0.0

            ff, fd = fills[fav_side], fills[dog_side] if buy_dog else 0.0

            # ── HEDGE FORCÉ (Steven 25/07) : si jambe 1 remplie mais jambe 2 non confirmée après timeout
            elapsed_ms = (time.time() - start_time) * 1000
            liquidity_ratio = 1.0
            try:
                book_fav = self._live.get_book_sync(fav_tid)
                book_dog = self._live.get_book_sync(dog_tid) if buy_dog else None
                fav_ask_sz = (
                    book_fav["asks"][0][1] if book_fav and book_fav.get("asks") else 0
                )
                dog_ask_sz = (
                    book_dog["asks"][0][1] if book_dog and book_dog.get("asks") else 0
                )
                if fav_ask_sz > 0:
                    liquidity_ratio = min(
                        fav_ask_sz, dog_ask_sz if dog_ask_sz > 0 else fav_ask_sz
                    ) / max(fav_ask_sz, 1)
            except Exception:
                pass

            is_liquid = liquidity_ratio >= HEDGE_LIQUID_THRESHOLD_DEPTH_RATIO
            force_timeout = (
                HEDGE_LIQUID_FORCE_TIMEOUT_MS
                if is_liquid
                else HEDGE_THIN_FORCE_TIMEOUT_MS
            )
            aggr_slip_pct = 0.005 if is_liquid else HEDGE_THIN_AGGR_MAX_SLIP_PCT_OF_EDGE

            if (
                buy_dog
                and ff > 0.01
                and fd <= 0.01
                and elapsed_ms >= force_timeout
                and not force_triggered
            ):
                force_triggered = True
                self._log(
                    f"⚡ [HEDGE-FORCE] {sym} {slug} {dog_side} non rempli après {elapsed_ms:.0f}ms -> mode agressif (liquidite={liquidity_ratio:.2f})"
                )

                # Annuler l'ordre limite existant et poster un ordre market agressif
                if h_dog.get("order_id"):
                    self._live.cancel_order(h_dog["order_id"])

                # Prix agressif : best ask + 1-2 ticks ou basé sur l'edge
                edge = max(0.0, 1.0 - (fav_px + dog_px))
                max_aggr_slip = min(HEDGE_AGGR_MAX_TICKS * 0.01, edge * aggr_slip_pct)
                dog_aggr_px = round(min(0.99, dog_px + max_aggr_slip), 2)

                with self._order_lock:
                    h_dog_force = self._live.post_market_order(
                        dog_tid, dog_aggr_px, dog_bet
                    )

                if h_dog_force.get("posted"):
                    # Attendre le fill avec timeout réduit
                    remaining_emergency_ms = (
                        HEDGE_EMERGENCY_TIMEOUT_MS - (time.time() - start_time) * 1000
                    )
                    force_timeout_s = max(0.5, remaining_emergency_ms / 1000.0)
                    fd = self._live.confirm_fill(dog_tid, dog_before, force_timeout_s)
                    self._log(
                        f"⚡ [HEDGE-FORCE] {sym} {slug} {dog_side} fill force={fd} @ {dog_aggr_px:.2f}"
                    )
                else:
                    self._log(
                        f"🚫 [HEDGE-FORCE] {sym} {slug} {dog_side} ordre force NON POSTE : {h_dog_force.get('error', '?')}"
                    )

            # ── FERMETURE D'URGENCE / ZERO-LAG CUT : échec total -> vente IMMÉDIATE au marché
            total_elapsed_ms = (time.time() - start_time) * 1000
            if (
                buy_dog
                and ff > 0.01
                and fd <= 0.01
                and total_elapsed_ms >= HEDGE_EMERGENCY_TIMEOUT_MS
            ):
                self._log(
                    f"🛑 [ZERO-LAG-CUT] {sym} {slug} {dog_side} echec {total_elapsed_ms:.0f}ms "
                    f"-> VENTE IMMEDIATE favori {ff:.2f} parts (slippage max {EMERGENCY_SELL_SLIPPAGE_PCT * 100:.0f}%)"
                )
                self._reject(
                    sym,
                    slug,
                    "zero_lag_cut",
                    f"elapsed={total_elapsed_ms:.0f}ms fav={ff:.2f} dog={fd:.2f}",
                )
                # Vente IMMÉDIATE au marché (pas de orphan manager, pas d'attente)
                emergency_px = round(max(0.01, fav_px - EMERGENCY_SELL_SLIPPAGE_PCT), 2)
                try:
                    # FIX CRITIQUE (Steven 04/08) : ordre (token, PRIX, parts) --
                    # ff/emergency_px etaient inverses (ff=parts, emergency_px=prix
                    # dans sell_position(token_id, price, size)), ce qui envoyait un
                    # "prix" de plusieurs parts (invalide, >1$) -> vente TOUJOURS
                    # rejetee silencieusement sur ce chemin d'urgence.
                    sell_result = self._live.sell_position(fav_tid, emergency_px, ff)
                    sell_shares = (
                        sell_result.get("filled_shares", 0)
                        if isinstance(sell_result, dict)
                        else 0
                    )
                    emergency_pnl = sell_shares * (emergency_px - fav_px)
                    self._log(
                        f"🛑 [ZERO-LAG-CUT] {sym} {slug} vendu {sell_shares:.2f}/{ff:.2f} parts "
                        f"@ {emergency_px:.2f} pnl={emergency_pnl:+.3f}$"
                    )
                except Exception as e:
                    self._log(f"🛑 [ZERO-LAG-CUT] {sym} {slug} erreur vente: {e}")
                    # Fallback: vente classique
                    self._sell_orphan(
                        fav_tid, ff, f" {sym} {slug} {fav_side} emergency-fallback"
                    )
                # Record for watchdog
                self._record_hedge_attempt(sym, False)
                return False

            if ff <= 0.01:
                return False
            if not buy_dog or fd <= 0.01:
                # favori seul -> orphan, PAS de dump aveugle
                mk["open"][f"{slug}|{fav_side}"] = {
                    "symbol": sym,
                    "slug": slug,
                    "side": fav_side,
                    "mode": "real",
                    "strat": "orphan",
                    "token_id": fav_tid,
                    "entry_price": fav_px,
                    "filled_shares": round(ff, 2),
                    "cost": round(ff * fav_px, 2),
                    "start_ts": p["start_ts"],
                    "pair": p["pair"],
                    "end_ts": p["end_ts"],
                    "opened_ts": time.time(),
                    "buffer": 0.0,
                }
                self._log(
                    f"🛡️ [HEDGE][REEL] {sym} {slug} favori {fav_side}@{fav_px:.2f} {fav_bet}$ seul -> orphan, stop-loss actif"
                )
                self._reject(
                    sym,
                    slug,
                    "hedge_failed_2nd",
                    f"fav={ff:.2f} dog={fd:.2f} elapsed={elapsed_ms:.0f}ms",
                )
                # Watchdog: enregistrer l'échec de la 2e jambe
                self._record_hedge_attempt(sym, False)
                return True
            for sd, tid, px, fv in (
                (fav_side, fav_tid, fav_px, ff),
                (dog_side, dog_tid, dog_px, fd),
            ):
                mk["open"][f"{slug}|{sd}"] = {
                    "symbol": sym,
                    "slug": slug,
                    "side": sd,
                    "mode": "real",
                    "strat": "bothside",
                    "token_id": tid,
                    "entry_price": px,
                    "filled_shares": round(fv, 2),
                    "cost": round(fv * px, 2),
                    "start_ts": p["start_ts"],
                    "pair": p["pair"],
                    "end_ts": p["end_ts"],
                    "opened_ts": time.time(),
                    "buffer": 0.0,
                }
            self._log(
                f"🛡️ [HEDGE][REEL] {sym} {slug} favori {fav_side}@{fav_px:.2f} {fav_bet}$ "
                f"+ underdog {dog_side}@{dog_px:.2f} {round(fd * dog_px, 3)}$ (danger={danger}) "
                f"(fav={ff} dog={fd})"
            )
            self._record_hedge_attempt(sym, True)
            return True
        else:  # paper
            shares_fav = min(500.0, fav_bet / fav_px)
            mk["open"][f"{slug}|{fav_side}"] = {
                "symbol": sym,
                "slug": slug,
                "side": fav_side,
                "mode": "paper",
                "strat": "bothside" if buy_dog else "orphan",
                "token_id": fav_tid,
                "entry_price": fav_px,
                "filled_shares": round(shares_fav, 2),
                "cost": round(shares_fav * fav_px, 2),
                "start_ts": p["start_ts"],
                "pair": p["pair"],
                "end_ts": p["end_ts"],
                "opened_ts": time.time(),
                "buffer": 0.0,
            }
            dog_cost_final = 0.0
            if buy_dog:
                dog_shares = dog_shares_target
                dog_cost_final = round(dog_shares * dog_px, 3)
                mk["open"][f"{slug}|{dog_side}"] = {
                    "symbol": sym,
                    "slug": slug,
                    "side": dog_side,
                    "mode": "paper",
                    "strat": "bothside",
                    "token_id": dog_tid,
                    "entry_price": dog_px,
                    "filled_shares": round(dog_shares, 2),
                    "cost": dog_cost_final,
                    "start_ts": p["start_ts"],
                    "pair": p["pair"],
                    "end_ts": p["end_ts"],
                    "opened_ts": time.time(),
                    "buffer": 0.0,
                }
            win_net = round(fav_gain - dog_cost_final, 3)
            self._log(
                f"🛡️ [HEDGE][PAPER] {sym} {slug} favori {fav_side}@{fav_px:.2f} {fav_bet}$ "
                f"+ underdog {'oui ' + str(dog_cost_final) + '$' if buy_dog else 'NON (danger/cout)'} "
                f"(favori gagne: {win_net:+}$)"
            )
            return True

    def _bet_underdog(self, sym, mode, m, p, leg):
        """PARI UNDERDOG (Steven 22/07) : petite mise (UNDERDOG_BET_USD) sur le cote
        CHEAP d'un near-certain. Perte max = la mise (qq centimes) ; gros gain si
        ca flippe. Position SEULE marquee 'underdog' -> exemptee du compteur de
        pertes consecutives (elle perd souvent par design, c'est voulu)."""
        side, tid, px = leg
        mk = self.state["markets"][sym]
        slug = m.get("slug")
        key = f"{slug}|{side}"
        if key in mk["open"] or any(
            t.get("slug") == slug and t.get("side") == side for t in mk["trades"]
        ):
            return False
        base = {
            "symbol": sym,
            "slug": slug,
            "side": side,
            "strat": "underdog",
            "token_id": tid,
            "start_ts": p["start_ts"],
            "pair": p["pair"],
            "end_ts": p["end_ts"],
            "opened_ts": time.time(),
            "buffer": 0.0,
        }
        if mode == "real":
            with self._order_lock:
                res = self._live.snipe_buy_market(tid, 0.99, UNDERDOG_BET_USD)
                filled = res.get("filled_shares", 0.0)
                if filled <= 0:
                    return False
                avg = res.get("avg_cost") or px
                base.update(
                    mode="real",
                    entry_price=avg,
                    filled_shares=filled,
                    cost=round(filled * avg, 2),
                )
                mk["open"][key] = base
        else:  # paper
            shares = min(500.0, UNDERDOG_BET_USD / px)
            base.update(
                mode="paper",
                entry_price=px,
                filled_shares=round(shares, 2),
                cost=round(shares * px, 2),
            )
            mk["open"][key] = base
        sh = base["filled_shares"]
        self._log(
            f"🎰 [UNDERDOG] {sym} {slug} {side} {UNDERDOG_BET_USD}$ @ {px:.3f} "
            f"-> {round(sh, 1)} parts (paie {round(sh, 1)}$ si flip, perte max {UNDERDOG_BET_USD}$)"
        )
        return True

    def _try_both_side(self, sym, mode, m, p):
        """SCALP BOTH-SIDE (Steven 22/07). Deux modes selon BOTH_SIDE_SIMULTANEOUS :

        - SIMULTANE (defaut, test paper) : "both side = both side" -> quand le
          marche bouge (danger >= MIN_DANGER) et qu'on ne tient RIEN sur ce slug,
          on achete les DEUX jambes en meme temps (chacune dans [LEG_MIN, LEG_MAX],
          coût combine <= COMBINED_MAX pour ne pas verrouiller une grosse perte).
          Puis on scalpe le bruit : TP la jambe qui monte, SL celle qui plonge.
          Jusqu'a MAX_TRADES_PER_SLOT cycles par tranche de 5 min.

        - INDEPENDANT (BOTH_SIDE_SIMULTANEOUS=False) : ancien comportement, achete
          chaque cote quand il devient cheap independamment, + couverture forcee
          en fin de fenetre."""
        from core.btc_updown import danger_score, _strike_at

        mk = self.state["markets"][sym]
        slug = m.get("slug")

        # GARDE PAR SLUG (Steven 30/07) : un mismatch reel deja survenu sur ce
        # slug precis bloque toute nouvelle tentative dessus (voir
        # _open_pair_parallel_real) -> stoppe les pertes en cascade tick apres
        # tick sans reintroduire de cooldown general sur tout le symbole.
        if mode == "real" and slug in mk.get("mismatch_slugs", ()):
            return False

        # ── V8.0 PENDING BOTHSIDE : completer la 2e jambe si fonds dispo ──
        pending = mk.get("pending_bothside", {})
        if slug in pending:
            pp = pending[slug]
            now = synced_now()
            if pp.get("end_ts", 0) - now < 30:
                del pending[slug]
            else:
                cash2, _ = self._read_cash(max_age=3)
                if cash2 is not None and cash2 >= round(pp["target_shares"] * pp["px"] + 0.1, 2):
                    self._log(
                        f"🛒 [ARB][SEQ] {sym} {slug} fonds dispo ({cash2}$) -> "
                        f"achat jambe 2 ({pp['side']}@{pp['px']:.3f})"
                    )
                    ok2, _ = self._open_leg(
                        sym, pp["mode"], m, p, pp["side"], pp["token_id"],
                        BOTH_SIDE_LEG_MAX, "[SEQ 2/2]",
                        target_shares=pp["target_shares"], entry_px=pp["px"],
                    )
                    if ok2:
                        _edge_val = max(0.0, 1.0 - pp["combined"])
                        self._log_trade_entry(
                            sym, slug, pp["side"], pp["mode"], "bothside",
                            pp["tier_label"], pp["px"], pp["target_shares"] * pp["px"],
                            pp["combined"], _edge_val, pp["end_ts"] - now,
                        )
                        self._log(
                            f"✅ [ARB][SEQ] {sym} {slug} paire completee! "
                            f"{pp['target_shares']} parts/cote comb={pp['combined']:.3f}"
                        )
                    del pending[slug]
                    return True

        strike = _strike_at(p["pair"], p["start_ts"], slug=slug)
        if strike is None:
            return
        d = danger_score(p["pair"], strike)
        mk["danger"] = d
        outcomes = json.loads(m.get("outcomes") or "[]")
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
        if len(outcomes) != 2 or len(token_ids) != 2:
            return
        # ── LECTURE UNIQUE des carnets (fix 22/07) : une seule lecture par jambe,
        # partagee entre la capture (📊) et la DECISION d'arb. Avant : 2e lecture
        # ~0.7s apres la capture -> le prix avait bouge -> 3 arbs reels rates en
        # silence a 18:03. Desormais la decision = exactement les prix captures.
        quotes = {}
        for side, token_id in zip(outcomes, token_ids):
            quotes[side] = self._book_quote(token_id)
        self._log_market_prices(sym, slug, outcomes, quotes)
        now = synced_now()
        secs_left = p["end_ts"] - now
        legs_held = sum(1 for side in outcomes if f"{slug}|{side}" in mk["open"])

        # ── COMBINED HISTORY (Steven 26/07) :跟踪每个slug的combined历史 ──
        # Le combined oscille pendant la fenêtre 5min. Un snapshot à T peut
        # dire "combined=1.01" alors qu'il était 0.96 il y a 3 secondes.
        # On garde un buffer de 30 secondes pour décider intelligemment.
        COMBINED_HISTORY_WINDOW = 30  # secondes d'historique conservées
        _ask_vals_hist = []
        for _sq in outcomes:
            _, _aq, _ = quotes.get(_sq, (None, None, None))
            if _aq is not None and _aq > 0:
                _ask_vals_hist.append(_aq)
        if len(_ask_vals_hist) == 2:
            _comb_now = sum(_ask_vals_hist)
            _hist = mk.setdefault("combined_history", {})
            _slug_hist = _hist.setdefault(slug, [])
            _slug_hist.append((now, _comb_now))
            # Purge les vieilles entrées
            while _slug_hist and _slug_hist[0][0] < now - COMBINED_HISTORY_WINDOW:
                _slug_hist.pop(0)
            # Stats sur l'historique récent
            _comb_values = [v for _, v in _slug_hist]
            _comb_best = min(_comb_values)  # meilleur combined observé
            _comb_avg = sum(_comb_values) / len(_comb_values)
            _comb_recent_trend = (
                _comb_values[-1] - _comb_values[0] if len(_comb_values) > 1 else 0
            )
        else:
            _comb_now = 1.0
            _comb_best = 1.0
            _comb_avg = 1.0
            _comb_recent_trend = 0

        if BOTH_SIDE_SIMULTANEOUS:
            if legs_held > 0:
                return True  # on tient deja une paire sur ce slug
            if secs_left < BOTH_SIDE_SL_MIN_SECS_LEFT:
                return False
            if self._slot_trade_count(mk, slug) >= MAX_TRADES_PER_SLOT:
                return False
            # prix de decision : ASK en reel (executable), MID en paper
            legs = []
            for side, token_id in zip(outcomes, token_ids):
                bid, ask, mid = quotes.get(side, (None, None, None))
                px = ask if mode == "real" else mid
                if px is None or px <= 0 or px >= 1:
                    self._tlog(
                        f"noquote_{sym}",
                        f"⚠️ [ARB] {sym} {slug} carnet illisible cote {side} "
                        f"(bid={bid} ask={ask}) -> skip",
                    )
                    return False
                legs.append((side, token_id, px))
            combined = sum(px for _, _, px in legs)
            side1, tid1, px1 = legs[0]
            side2, tid2, px2 = legs[1]
            # CAS 1 : ARB GARANTI (parts egales) si les 2 jambes pas cheres + combine<0.95
            is_arb = combined <= BOTH_SIDE_COMBINED_MAX and all(
                BOTH_SIDE_LEG_MIN <= px <= BOTH_SIDE_LEG_MAX for _, _, px in legs
            )
            if is_arb:
                target_shares, tier_label = self._edge_based_sizing(
                    sym,
                    combined,
                    p["pair"],
                    MIN_ORDER_SIZE_SHARES,
                    secs_left=secs_left,
                    binance_arb=True,
                )
                if target_shares <= 0:
                    self._tlog(
                        f"skip_edge_{sym}",
                        f"📎 [ARB] {sym} {slug} comb={combined:.3f} edge "
                        f"{(1 - combined) * 100:.1f}% < {EDGE_REDUCE_THRESHOLD * 100:.0f}% -> SKIP",
                    )
                    return False
                if mode == "real":
                    # V3.1 AXE 1 : dead market check
                    if not self._dead_market_check(quotes, outcomes):
                        return False
                    if combined > REAL_MAX_COMBINED:
                        self._tlog(
                            f"thinarb_{sym}",
                            f"📎 [ARB][REEL] {sym} {slug} comb={combined:.3f} > "
                            f"{REAL_MAX_COMBINED} (seuil reel) -> capte en paper seulement",
                        )
                        return False
                    # PRE-CHECK FONDS EXPLICITE (Steven 22/07) : plus d'echec muet.
                    # V8.0 : si pas assez pour les 2 jambes, acheter la 1ere seule
                    # et laisser la 2eme en pending (achat differe quand funds dispo).
                    need = round(target_shares * combined + 0.2, 2)
                    cash, _ = self._read_cash(max_age=3)
                    if cash is None or cash < need:
                        # V8.0 SEQUENTIAL : achete la jambe 1 seule si fonds suffisants
                        single_need = round(target_shares * px1 + 0.1, 2)
                        if cash is not None and cash >= single_need and secs_left > 60:
                            self._log(
                                f"🛒 [ARB][SEQ] {sym} {slug} fonds insuffisants pour paire "
                                f"({cash}$ < {need}$) -> achat jambe 1 seule ({side1}@{px1:.3f})"
                            )
                            ok1, _ = self._open_leg(
                                sym, mode, m, p, side1, tid1,
                                BOTH_SIDE_LEG_MAX, "[SEQ 1/2]",
                                target_shares=target_shares, entry_px=px1,
                            )
                            if ok1:
                                pending = mk.setdefault("pending_bothside", {})
                                pending[slug] = {
                                    "sym": sym, "mode": mode,
                                    "side": side2, "token_id": tid2,
                                    "px": px2, "target_shares": target_shares,
                                    "combined": combined, "tier_label": tier_label,
                                    "slug": slug, "end_ts": p["end_ts"],
                                }
                                self._log(
                                    f"⏳ [ARB][SEQ] {sym} {slug} jambe 2 ({side2}@{px2:.3f}) "
                                    f"mise en attente -> achat au prochain cycle si fonds dispo"
                                )
                                _edge_val = max(0.0, 1.0 - combined)
                                self._log_trade_entry(
                                    sym, slug, side1, mode, "bothside",
                                    tier_label, px1, target_shares * px1,
                                    combined, _edge_val, secs_left,
                                )
                                return True
                        self._tlog(
                            f"nofund_{sym}",
                            f"💸 [ARB][REEL] {sym} {slug} ARB DETECTE comb={combined:.3f} "
                            f"(+{target_shares * (1 - combined):.2f}$ dispo) mais SOLDE INSUFFISANT "
                            f"({cash}$ < ~{need}$ requis) -> rate",
                        )
                        return False
                    self._log(
                        f"🛒 [ARB][REEL] TENTATIVE {sym} {slug} "
                        f"{side1}@{px1:.3f}+{side2}@{px2:.3f} comb={combined:.3f} "
                        f"{target_shares} parts/cote [{tier_label}] (cash {cash}$)"
                    )
                    ok = self._open_pair_parallel_real(
                        sym, m, p, legs, target_shares, combined, tier_label, no_slippage=True
                    )
                    # V3.1 AXE 8 : log entree structure pour chaque jambe ARB
                    if ok:
                        _edge_val = max(0.0, 1.0 - combined)
                        for _ls, _lt, _lp in legs:
                            self._log_trade_entry(
                                sym,
                                slug,
                                _ls,
                                mode,
                                "bothside",
                                tier_label,
                                _lp,
                                target_shares * _lp,
                                combined,
                                _edge_val,
                                secs_left,
                            )
                else:
                    ok1, _ = self._open_leg(
                        sym,
                        mode,
                        m,
                        p,
                        side1,
                        tid1,
                        BOTH_SIDE_LEG_MAX,
                        "[SIMUL 1/2]",
                        target_shares=target_shares,
                        entry_px=px1,
                    )
                    if not ok1:
                        return False
                    ok2, _ = self._open_leg(
                        sym,
                        mode,
                        m,
                        p,
                        side2,
                        tid2,
                        BOTH_SIDE_LEG_MAX,
                        "[SIMUL 2/2]",
                        target_shares=target_shares,
                        entry_px=px2,
                    )
                    if not ok2:
                        mk["open"].pop(f"{slug}|{side1}", None)
                        self._log(
                            f"↩️ [BOTHSIDE] {sym} {slug} 2e jambe KO -> annulation 1re"
                        )
                        return False
                    self._log(
                        f"✅ [BOTHSIDE] {sym} {slug} ARB {target_shares} parts/cote "
                        f"@ combine {combined:.3f} -> +{target_shares * (1 - combined):.2f}$"
                    )
                    # V3.1 AXE 8 : log entree structure pour paper sequential
                    _edge_val = max(0.0, 1.0 - combined)
                    for _ls, _lt, _lp in legs:
                        self._log_trade_entry(
                            sym,
                            slug,
                            _ls,
                            mode,
                            "bothside",
                            tier_label,
                            _lp,
                            target_shares * _lp,
                            combined,
                            _edge_val,
                            secs_left,
                        )
                    ok = True
            elif (not ARB_ONLY) and max(px1, px2) >= NEAR_CERTAIN_MIN_PRICE:
                # CAS 2 (COUPE si ARB_ONLY) : FAVORI + UNDERDOG.
                ok = self._open_hedge_pair(sym, mode, m, p, legs, combined)
            else:
                # mid-prix (0.5/0.5) sans arb ni near-certain : rien (pas de churn).
                return False
            if ok:
                slot = mk.setdefault("slot_trades", {})
                slot[slug] = slot.get(slug, 0) + 1
                if len(slot) > 20:
                    for old in list(slot.keys())[: len(slot) - 20]:
                        del slot[old]
            return bool(ok)

        # ── FAVORITE DETECTION (Steven 28/07, refonte 05/08) : Binance N'ETAIT
        # utilise QUE comme signal principal, Polymarket seulement en repli si
        # Binance echouait -> Steven : "on regarde les prix poly autant que
        # binance ... bien les 2 en meme temps". Desormais les DEUX signaux
        # sont TOUJOURS calcules, et le favori n'est retenu QUE s'ils sont
        # D'ACCORD (meme cote favori). S'ils se contredisent (Binance dit Up
        # mais Polymarket cote Down plus cher, ou l'inverse), on ne privilegie
        # AUCUN cote -> pas de FAVORITE_BUDGET_MULT applique ce cycle, plutot
        # que de trancher a l'aveugle entre 2 signaux qui divergent. Utilise
        # pour : FIRST-LEG (achat favori en 1er), FAVORITE-BUDGET (mise 2.5x),
        # max_entry (favori achetable a tout prix).
        from core.btc_updown import _binance_price as _bp
        _fav_binance = None
        try:
            spot = self._ws.spot_price(p["pair"]) or _bp(p["pair"])
            if spot is not None and strike is not None:
                _fav_binance = "Up" if spot > strike else "Down"
        except Exception:
            pass
        _fav_poly = None
        _fav_prices = [(s, a) for s, (_, a, _) in zip(outcomes, [quotes.get(s, (None, None, None)) for s in outcomes]) if a is not None]
        if len(_fav_prices) == 2:
            _fav_poly = max(_fav_prices, key=lambda x: x[1])[0]
        if _fav_binance is not None and _fav_poly is not None:
            fav_side = _fav_binance if _fav_binance == _fav_poly else None
            if fav_side is None:
                self._tlog(
                    f"fav_disagree_{sym}",
                    f"⚖️ [FAVORI-DESACCORD] {sym} {slug} Binance={_fav_binance} "
                    f"vs Polymarket={_fav_poly} -> pas de favori ce cycle, "
                    f"budget neutre sur les 2 jambes",
                    every=10.0,
                )
        else:
            # un seul signal dispo (l'autre API/donnee indisponible) -> on le
            # prend quand meme plutot que de rester totalement aveugle.
            fav_side = _fav_binance if _fav_binance is not None else _fav_poly
        # ── mode INDEPENDANT (legacy) ──
        combined = None  # V3.1 : init pour eviter UnboundLocalError
        force_hedge = legs_held == 1 and secs_left <= BOTH_SIDE_FORCE_HEDGE_SECS_LEFT
        # ARB GATE (Laguna XS 24/07) : pour l'arb, le danger score est INUTILE.
        # Ce qui compte c'est le combined. On bypass le gate danger si les 2 asks
        # sont dispo et le combined < 0.98 (profit potentiel).
        arb_bypass_danger = False
        if not force_hedge and d < BOTH_SIDE_MIN_DANGER:
            ask_vals = []
            leg_data_immediate = []
            first_target = None
            for side_q, tid_q in zip(outcomes, token_ids):
                _, ask_q, _ = quotes.get(side_q, (None, None, None))
                if ask_q is not None:
                    ask_vals.append(ask_q)
                    leg_data_immediate.append((side_q, tid_q, ask_q))
                    is_fav = side_q == fav_side
                    if is_fav:
                        # FAVORITE-FIRST (Steven 28/07) : la favorite est achetee
                        # en 1er, meme si >0.52. max_entry=1.0 pour favorite.
                        first_target = (side_q, ask_q, True)
                    elif first_target is None and ask_q < BOTH_SIDE_MAX_ENTRY:
                        first_target = (side_q, ask_q, False)
            # FAVORI D'ABORD (Steven 04/08, "en achetant favori d'abord") : les
            # 2 jambes partent ensemble, mais l'ordre dans lequel on les envoie
            # decide laquelle a le plus de chances d'etre servie en premier si
            # le carnet ne peut pas tout absorber. Mesure de cette nuit : quand
            # une seule jambe se remplit, on garde un orphelin -- autant que ce
            # soit le FAVORI (donne gagnant par le marche) plutot qu'un cote au
            # hasard. Un orphelin favori gagne plus souvent qu'il ne perd ; un
            # orphelin underdog est perdant par construction.
            if fav_side and len(leg_data_immediate) == 2:
                leg_data_immediate.sort(key=lambda L: 0 if L[0] == fav_side else 1)
            # GENUINE ARB SEULEMENT (Steven 29/07) : combined<0.98 est trivialement
            # vrai des qu'un marche est quasi-resolu (ex: Up=0.86/Down=0.015,
            # comb=0.875) -> ce n'est PAS un arb, la jambe a 1.5c est un billet
            # de loterie deja rejete par BOTH_SIDE_LEG_MIN plus bas -> tentative
            # gaspillee + FAIL log. On exige les 2 jambes au-dessus du plancher
            # AVANT de declarer une opportunite -> vrai edge liquide seulement.
            if (
                len(ask_vals) == 2
                and sum(ask_vals) < 0.98
                and legs_held == 0
                and all(v >= BOTH_SIDE_LEG_MIN for v in ask_vals)
            ):
                arb_bypass_danger = True
                combined_now = round(sum(ask_vals), 3)
                self._log(
                    f"⚡ [ARB-BYPASS] {sym} {slug} d={d} < {BOTH_SIDE_MIN_DANGER} "
                    f"MAIS combined={combined_now:.3f} < 0.98 -> bypass danger"
                )
                # EXECUTION IMMEDIATE (Steven 29/07, fix "l'arb s'evapore") : avant,
                # ce flag ne faisait QUE logger -> l'execution retombait plus loin
                # sur le chemin sequentiel (2 achats espaces de 1-2s, prix deja
                # remontes = edge perdu, ex. 12% detecte -> 0% realise). On achete
                # ICI, TOUT DE SUITE, prix FIGES (entry_px), avec le scale-up
                # capital (INSTANT-ARB) si combined < INSTANT_ARB_MAX_COMBINED.
                _target_shares, _tier_lbl = self._edge_based_sizing(
                    sym, combined_now, p["pair"], MIN_ORDER_SIZE_SHARES,
                    secs_left=secs_left, binance_arb=True,
                )
                # VERROU ARB (Steven 29/07) : acquis AVANT la lecture du solde,
                # relache APRES l'execution -> aucun autre symbole ne peut lire
                # le meme capital "disponible" entre-temps et le sur-engager.
                self._arb_lock.acquire()
                _arb_lock_held = True
                if _target_shares <= 0:
                    # LOG MANQUANT (Steven 30/07, "comb a 0.92 aurait du acheter")
                    # : avant, ce cas (edge sous EDGE_REDUCE_THRESHOLD) sortait
                    # de la fonction sans AUCUNE trace -> "pourquoi rien ne s'est
                    # passe" invisible dans les logs malgre une detection reelle.
                    self._log(
                        f"⏭️ [ARB-BYPASS-SKIP] {sym} {slug} comb={combined_now:.3f} "
                        f"edge={(1-combined_now)*100:.1f}% < {EDGE_REDUCE_THRESHOLD*100:.0f}% "
                        f"requis -> sizing refuse, aucun achat tente"
                    )
                    self._arb_lock.release()
                    return legs_held > 0
                if _target_shares > 0 and combined_now < INSTANT_ARB_MAX_COMBINED:
                    if mode == "real":
                        _cash_i, _ = self._read_cash(max_age=0)
                        _investable_i = max(0.0, (_cash_i or 0.0) - self.floor())
                    else:
                        _investable_i = mk.get("paper_balance", 20.0)
                    _scaled_i = round(
                        (_investable_i * INSTANT_ARB_CAPITAL_FRACTION) / max(combined_now, 0.01), 2
                    )
                    _cap_shares_i = min(
                        INSTANT_ARB_MAX_SHARES, INSTANT_ARB_MAX_USD / max(combined_now, 0.01)
                    )
                    if _scaled_i > _target_shares:
                        _target_shares = min(_scaled_i, _cap_shares_i)
                        _tier_lbl += "+SCALED"
                if _target_shares > 0:
                    in_cd_i, cd_reason_i = self._in_arb_bypass_cooldown(sym, slug, mk)
                    if in_cd_i:
                        self._log(f"⏸️ [ARB-BYPASS-COOLDOWN] {sym} {slug} -> {cd_reason_i}")
                        self._arb_lock.release()
                        return legs_held > 0
                    self._set_arb_bypass_cooldown(sym, slug, mk)
                    if mode == "real":
                        if combined_now > REAL_MAX_COMBINED:
                            self._tlog(
                                f"thinarb_bypass_{sym}",
                                f"📎 [ARB-BYPASS][REEL] {sym} {slug} comb={combined_now:.3f} "
                                f"> {REAL_MAX_COMBINED} -> paper only",
                            )
                            self._arb_lock.release()
                            return False
                        _ok_bp = self._open_pair_parallel_real(
                            sym, m, p, leg_data_immediate, _target_shares, combined_now, _tier_lbl,
                            no_slippage=True,
                        )
                    else:
                        # FIX (Steven 29/07, occasions manquees) : BOTH_SIDE_MAX_ENTRY
                        # (0.52) est le plafond de l'ANCIENNE strategie hedge (achat
                        # cote pas cher) -> reutilise ici, il rejetait a tort toute
                        # jambe d'arb genuine au-dessus de 0.52 (ex: 0.60+0.35=0.95,
                        # un arb parfaitement valide et deja valide par le check
                        # combined<0.98). Le prix EST DEJA connu et fige
                        # (leg_data_immediate[i][2]) -> on l'utilise directement
                        # +buffer au lieu d'un plafond fixe sans rapport.
                        _max1 = round(leg_data_immediate[0][2] + 0.02, 2)
                        _max2 = round(leg_data_immediate[1][2] + 0.02, 2)
                        _ok1, _ = self._open_leg(
                            sym, mode, m, p, leg_data_immediate[0][0], leg_data_immediate[0][1],
                            _max1, f"[ARB-BYPASS d={d}]",
                            target_shares=_target_shares, entry_px=leg_data_immediate[0][2],
                        )
                        _ok2, _ = self._open_leg(
                            sym, mode, m, p, leg_data_immediate[1][0], leg_data_immediate[1][1],
                            _max2, f"[ARB-BYPASS d={d}]",
                            target_shares=_target_shares, entry_px=leg_data_immediate[1][2],
                        )
                        # TAG RISK-FREE (Steven 29/07, "l'historique ne montre pas
                        # correctement les risk free") : les 2 jambes d'un arb
                        # garanti se géraient EXACTEMENT comme un hedge directionnel
                        # ordinaire dans l'historique -> aucune distinction visible.
                        # On tague chaque jambe avec is_risk_free + le combined
                        # (edge garanti) au moment de l'execution -> visible dans
                        # le detail du trade, meme apres resolution/SL/TP.
                        if _ok1:
                            _p1 = mk["open"].get(f"{slug}|{leg_data_immediate[0][0]}")
                            if _p1:
                                _p1["is_risk_free"] = True
                                _p1["arb_combined"] = combined_now
                                _p1["arb_edge"] = round(1 - combined_now, 4)
                        if _ok2:
                            _p2 = mk["open"].get(f"{slug}|{leg_data_immediate[1][0]}")
                            if _p2:
                                _p2["is_risk_free"] = True
                                _p2["arb_combined"] = combined_now
                                _p2["arb_edge"] = round(1 - combined_now, 4)
                        # ATOMICITE (Steven 29/07, fail meconnu trouve en analysant
                        # les 3 vrais echecs de cette nuit) : si UNE seule jambe se
                        # remplit, l'ancien code laissait _ok_bp=False sans rien
                        # faire -> la jambe seule restait ouverte dans mk['open'],
                        # puis un fallback SEQUENTIEL (hors risk-free) rachetait
                        # l'autre cote PLUS TARD, a un prix DIFFERENT -> plus un
                        # arb du tout, un pari directionnel deguise (observe : les
                        # 3 echecs ont chacun fini en TP+SL disjoints, PAS en
                        # arb -0.85$ a -1.55$ de perte reelle sur la jambe SL).
                        # On desenroule IMMEDIATEMENT la jambe seule au lieu de la
                        # laisser courir -> zero exposition directionnelle, meme
                        # en echec partiel.
                        if _ok1 and not _ok2:
                            _key1 = f"{slug}|{leg_data_immediate[0][0]}"
                            _pos1 = mk["open"].get(_key1)
                            if _pos1:
                                if mode == "real":
                                    self._sell_orphan(
                                        _pos1["token_id"], _pos1["filled_shares"],
                                        f" {sym} {slug} {_pos1['side']} ARB-BYPASS-UNWIND",
                                        entry_price=_pos1["entry_price"], symbol=sym,
                                        slug=slug, side=_pos1["side"],
                                    )
                                    # FIX (Steven 30/07) : le rameau real ne
                                    # supprimait jamais l'entree mk["open"]
                                    # apres l'avoir revendue -> position
                                    # "fantome" qui restait affichee comme
                                    # ouverte alors qu'elle etait deja soldee.
                                    del mk["open"][_key1]
                                else:
                                    _exit_px = self._live_price(_pos1["token_id"], m, _pos1["side"]) or _pos1["entry_price"]
                                    _pnl1 = round(_pos1["filled_shares"] * (_exit_px - _pos1["entry_price"]), 3)
                                    _pos1.update(win=_pnl1 > 0, pnl=_pnl1, exit_price=_exit_px, resolved_by="arb_atomic_unwind")
                                    mk["trades"].append(_pos1)
                                    del mk["open"][_key1]
                                self._log(
                                    f"🔓 [ARB-BYPASS-UNWIND] {sym} {slug} jambe {leg_data_immediate[0][0]} "
                                    f"seule remplie -> revendue immediatement (pas de pari directionnel deguise)"
                                )
                        _ok_bp = bool(_ok1 and _ok2)
                    self._arb_lock.release()
                    _arb_lock_held = False
                    if _ok_bp:
                        self._log(
                            f"🛒 [ARB-BYPASS-FILL] {sym} {slug} "
                            f"{leg_data_immediate[0][0]}@{leg_data_immediate[0][2]:.3f}+"
                            f"{leg_data_immediate[1][0]}@{leg_data_immediate[1][2]:.3f} "
                            f"comb={combined_now:.3f} {_target_shares} parts [{_tier_lbl}]"
                        )
                        slot = mk.setdefault("slot_trades", {})
                        slot[slug] = slot.get(slug, 0) + 1
                        return True
                    else:
                        self._tlog(
                            f"arb_bypass_fail_{sym}",
                            f"⚠️ [ARB-BYPASS-FAIL] {sym} {slug} comb={combined_now:.3f} "
                            f"shares={_target_shares} -> echec ouverture (voir BOTHSIDE juste au-dessus)",
                        )
                if _arb_lock_held:
                    self._arb_lock.release()  # _target_shares<=0 apres scaling -> jamais entre dans le if ci-dessus
            elif first_target and legs_held == 0 and d > 0 and not self._risk_free_on(sym):
                # RISK-FREE (Steven 29/07) : bouton dashboard -> si actif sur ce
                # symbole, on ne prend QUE l'arb garanti (ci-dessus), jamais ce
                # fallback hedge/favori (qui porte un vrai risque directionnel).
                # FIRST-LEG FALLBACK (Steven 28/07) : achete la FAVORITE en 1er
                # (pas la moins chere comme avant). La FORCE-PAIR completera l'autre.
                ok_pf, reason_pf, comb_pf = self._preflight_valid(
                    sym, slug, quotes, outcomes, mode
                )
                if not ok_pf:
                    self._log(
                        f"⛔ [FIRST-LEG-BLOCKED] {sym} {slug} {first_target[0]} "
                        f"@ {first_target[1]:.3f} -> {reason_pf} (V3.1 pre-flight)"
                    )
                    return legs_held > 0
                in_cd, cd_reason = self._in_cooldown(sym, slug, mk)
                if in_cd:
                    self._log(f"⏸️ [FIRST-LEG-COOLDOWN] {sym} {slug} -> {cd_reason}")
                    return legs_held > 0
                _tag = "FAVORITE-LEG" if first_target[2] else "FIRST-LEG"
                self._log(
                    f"🎯 [{_tag}] {sym} {slug} d={d} {first_target[0]} "
                    f"@ {first_target[1]:.3f} -> "
                    f"fallback (arb simultane indispo) comb={comb_pf:.3f}"
                )
            elif legs_held == 0:
                # STOP : aucune jambe tenue et pas d'opportunite FIRST-LEG
                return False
            # si legs_held > 0 : on a deja 1 jambe, on continue vers sequential bothside
        max_entry = (
            BOTH_SIDE_FORCE_HEDGE_MAX_PRICE if force_hedge else BOTH_SIDE_MAX_ENTRY
        )
        # PARALLEL PATH SPAMMÉ (Laguna XS 24/07) : quand legs_held==0 et les 2 asks
        # passent le max_entry, on POSTE LES 2 ORDRES EN MEME TEMPS.
        # On RETENTE jusqu'a 3 fois avant de tomber sur l'achat sequentiel
        # (une jambe a la fois = risque de solo bet si FORCE-PAIR echoue).
        # V3.1 AXE 2 : cooldown check avant tout parallel attempt
        in_cd_par, cd_reason_par = self._in_cooldown(sym, slug, mk)
        if in_cd_par:
            self._log(f"⏸️ [PARALLEL-COOLDOWN] {sym} {slug} -> {cd_reason_par}")
            return legs_held > 0
        if legs_held == 0 and not force_hedge:
            PARALLEL_MAX_RETRIES = 3
            PARALLEL_RETRY_SLEEP = 0.5
            for _ptry in range(PARALLEL_MAX_RETRIES):
                # (Steven 30/07 : plus de guard "stop si jambe ouverte" ici -
                # inutile maintenant que _open_pair_parallel_real revend TOUJOURS
                # la jambe seule immediatement en cas de pair KO (voir plus bas) ->
                # au moment ou ce retry demarre, aucune jambe ne reste ouverte,
                # chaque essai est un POST COMBINE up+down propre, jamais un rachat
                # sur une jambe deja tenue.)
                leg_data = []
                both_pass = True
                for side, token_id in zip(outcomes, token_ids):
                    bid_q, ask_q, _ = quotes.get(side, (None, None, None))
                    px = (
                        ask_q
                        if mode == "real"
                        else ((ask_q + (bid_q or 0)) / 2 if ask_q else None)
                    )
                    if px is None or px < BOTH_SIDE_LEG_MIN or px > max_entry:
                        both_pass = False
                        break
                    leg_data.append((side, token_id, px))
                if not (both_pass and len(leg_data) == 2):
                    break
                combined = sum(px for _, _, px in leg_data)
                real_max = (
                    REAL_MAX_COMBINED if mode == "real" else BOTH_SIDE_COMBINED_MAX
                )
                # COMBINED HISTORY GATE (Steven 26/07) : si le combined actuel
                # est au-dessus du seuil MAIS le meilleur récent était en dessous,
                # on sait que le prix oscille -> on autorise l'entrée.
                if combined > real_max and _comb_best <= real_max:
                    self._tlog(
                        f"osc_allow_{sym}",
                        f"📊 [OSC-GATE] {sym} {slug} comb={combined:.3f} > {real_max:.3f} "
                        f"MAIS best_recent={_comb_best:.3f} <= {real_max:.3f} -> OSCILLATION",
                    )
                    combined = _comb_best  # utilise le meilleur observed pour sizing
                if combined > real_max:
                    break
                target_shares, tier_label = self._edge_based_sizing(
                    sym,
                    combined,
                    p["pair"],
                    MIN_ORDER_SIZE_SHARES,
                    secs_left=secs_left,
                    binance_arb=True,
                )
                if target_shares <= 0:
                    self._tlog(
                        f"skip_edge_{sym}",
                        f"📎 [ARB-PARALLEL] {sym} {slug} comb={combined:.3f} edge "
                        f"{(1 - combined) * 100:.1f}% < {EDGE_REDUCE_THRESHOLD * 100:.0f}% -> SKIP",
                    )
                    break
                # INSTANT-ARB SCALE-UP (Steven 29/07) : profit garanti (2 jambes
                # simultanees) -> augmente la taille selon le capital dispo au
                # lieu de rester colle aux tiers fixes minuscules, tant que
                # combined < INSTANT_ARB_MAX_COMBINED (marge garantie suffisante).
                if combined < INSTANT_ARB_MAX_COMBINED:
                    if mode == "real":
                        _cash_scale, _ = self._read_cash(max_age=3)
                        _investable_scale = max(0.0, (_cash_scale or 0.0) - self.floor())
                    else:
                        _investable_scale = mk.get("paper_balance", 20.0)
                    _scaled_shares = round(
                        (_investable_scale * INSTANT_ARB_CAPITAL_FRACTION) / max(combined, 0.01), 2
                    )
                    _cap_shares = min(
                        INSTANT_ARB_MAX_SHARES, INSTANT_ARB_MAX_USD / max(combined, 0.01)
                    )
                    if _scaled_shares > target_shares:
                        target_shares = min(_scaled_shares, _cap_shares)
                        tier_label += "+SCALED"
                        self._tlog(
                            f"instant_arb_scale_{sym}",
                            f"📈 [INSTANT-ARB] {sym} {slug} comb={combined:.3f} "
                            f"-> scale {target_shares} parts/jambe (investable={_investable_scale:.2f}$)",
                        )
                if mode == "real":
                    if combined > REAL_MAX_COMBINED:
                        self._tlog(
                            f"thinarb_{sym}",
                            f"📎 [ARB][REEL] {sym} {slug} comb={combined:.3f} > "
                            f"{REAL_MAX_COMBINED} -> paper only",
                        )
                        return False
                    need = round(target_shares * combined + 0.2, 2)
                    cash, _ = self._read_cash(max_age=3)
                    if cash is None or cash < need:
                        self._tlog(
                            f"nofund_{sym}",
                            f"💸 [ARB][REEL] {sym} {slug} SOLDE INSUFFISANT "
                            f"({cash}$ < ~{need}$) -> rate",
                        )
                        return False
                    self._log(
                        f"🛒 [ARB-PARALLEL] {sym} {slug} "
                        f"{leg_data[0][0]}@{leg_data[0][2]:.3f}+"
                        f"{leg_data[1][0]}@{leg_data[1][2]:.3f} "
                        f"comb={combined:.3f} {target_shares} parts [{tier_label}] (cash {cash}$) "
                        f"[try {_ptry + 1}/{PARALLEL_MAX_RETRIES}]"
                    )
                    ok = self._open_pair_parallel_real(
                        sym, m, p, leg_data, target_shares, combined, tier_label, no_slippage=True
                    )
                    if ok:
                        slot = mk.setdefault("slot_trades", {})
                        slot[slug] = slot.get(slug, 0) + 1
                        return True
                    # STOP APRES 1 MISMATCH REEL (Steven 30/07, "ca rachete la
                    # meme jambe et revend celle qui va bien" - trouve sur
                    # XRP 1785781200 : Down n'a JAMAIS matche sur 3 essais
                    # identiques, chacun rachetant Up en pure perte -0.363$
                    # puis -0.675$ avant de rester bloque sous le plancher
                    # vendable). Un echec ICI = du capital reel deja depense
                    # et desenroule (pas un simple rejet de gate) -> retenter
                    # la MEME combinaison de prix sur le MEME carnet fin dans
                    # les 0.5s qui suivent ne resout rien, ca compose juste
                    # la perte de spread. On abandonne ce slug pour ce tour
                    # au lieu d'insister.
                    self._log(
                        f"🛑 [ARB-PARALLEL] {sym} {slug} echec try {_ptry + 1} "
                        f"(capital reel deja depense/desenroule) -> abandon du slug, "
                        f"pas de retry aveugle"
                    )
                    break
                    # fin for _ptry -> tous les try parallel ont echoue, tombe au sequentiel
                else:
                    # PAPER (Steven 29/07) : le mode paper ne simulait JAMAIS l'arb
                    # simultane (branche precedente = "if mode=='real'" only) ->
                    # il tombait toujours au sequentiel favori+underdog (pas un
                    # arb garanti). On simule les 2 jambes ICI, EN MEME TEMPS,
                    # prix FIGES (entry_px), pour que le paper reflete la meme
                    # strategie instant-arb que le reel (et le scale-up ci-dessus).
                    ok1, _ = self._open_leg(
                        sym, mode, m, p, leg_data[0][0], leg_data[0][1], max_entry,
                        f"[ARB-PARALLEL d={d}]", target_shares=target_shares,
                        entry_px=leg_data[0][2],
                    )
                    ok2, _ = self._open_leg(
                        sym, mode, m, p, leg_data[1][0], leg_data[1][1], max_entry,
                        f"[ARB-PARALLEL d={d}]", target_shares=target_shares,
                        entry_px=leg_data[1][2],
                    )
                    # TAG RISK-FREE (Steven 30/07, "je vois encore des lignes loss")
                    # : cette 2e voie d'arb simultane (parallele a ARB-BYPASS) ne
                    # taguait jamais is_risk_free -> le SL/TP pouvait encore casser
                    # la paire individuellement (meme bug ARB_NEGATIVE que celui
                    # deja corrige sur ARB-BYPASS, juste sur ce chemin-ci).
                    if ok1:
                        _pp1 = mk["open"].get(f"{slug}|{leg_data[0][0]}")
                        if _pp1:
                            _pp1["is_risk_free"] = True
                            _pp1["arb_combined"] = combined
                            _pp1["arb_edge"] = round(1 - combined, 4)
                    if ok2:
                        _pp2 = mk["open"].get(f"{slug}|{leg_data[1][0]}")
                        if _pp2:
                            _pp2["is_risk_free"] = True
                            _pp2["arb_combined"] = combined
                            _pp2["arb_edge"] = round(1 - combined, 4)
                    if ok1 and not ok2:
                        # ATOMICITE (meme fix que ARB-BYPASS) : jambe seule -> revente immediate
                        _key1p = f"{slug}|{leg_data[0][0]}"
                        _pos1p = mk["open"].get(_key1p)
                        if _pos1p:
                            if mode == "real":
                                self._sell_orphan(
                                    _pos1p["token_id"], _pos1p["filled_shares"],
                                    f" {sym} {slug} {_pos1p['side']} ARB-PARALLEL-UNWIND",
                                    entry_price=_pos1p["entry_price"], symbol=sym,
                                    slug=slug, side=_pos1p["side"],
                                )
                                del mk["open"][_key1p]
                            else:
                                _exp1 = self._live_price(_pos1p["token_id"], m, _pos1p["side"]) or _pos1p["entry_price"]
                                _pn1 = round(_pos1p["filled_shares"] * (_exp1 - _pos1p["entry_price"]), 3)
                                _pos1p.update(win=_pn1 > 0, pnl=_pn1, exit_price=_exp1, resolved_by="arb_atomic_unwind")
                                mk["trades"].append(_pos1p)
                                del mk["open"][_key1p]
                            self._log(f"🔓 [ARB-PARALLEL-UNWIND] {sym} {slug} jambe seule -> revendue")
                    if ok1 and ok2:
                        self._log(
                            f"🛒 [ARB-PARALLEL][PAPER] {sym} {slug} "
                            f"{leg_data[0][0]}@{leg_data[0][2]:.3f}+"
                            f"{leg_data[1][0]}@{leg_data[1][2]:.3f} "
                            f"comb={combined:.3f} {target_shares} parts [{tier_label}]"
                        )
                        slot = mk.setdefault("slot_trades", {})
                        slot[slug] = slot.get(slug, 0) + 1
                        return True
                    break
        # RISK-FREE (Steven 29/07) : au-dela de ce point, tout est le hedge
        # favori/underdog SEQUENTIEL (risque directionnel reel) -> si le
        # bouton est actif sur ce symbole, on s'arrete ICI. L'arb garanti
        # (ARB-BYPASS + PARALLEL ci-dessus) a deja eu sa chance ce cycle.
        if self._risk_free_on(sym):
            return legs_held > 0
        max_entry = (
            BOTH_SIDE_FORCE_HEDGE_MAX_PRICE if force_hedge else BOTH_SIDE_MAX_ENTRY
        )
        # V3.1 AXE 3 : BUDGET VARIABLE SELON TIER — remplace arb_budget() fixe
        _ask_vals = []
        for _sq in outcomes:
            _, _aq, _ = quotes.get(_sq, (None, None, None))
            if _aq is not None:
                _ask_vals.append(_aq)
        _comb_sequential = sum(_ask_vals) if len(_ask_vals) == 2 else 1.0
        # COMBINED HISTORY (Steven 26/07) : utilise le meilleur combined observé
        # pour décider du tier/budget. Si le prix oscille, le meilleur recent
        # est plus fiable que le snapshot instantané.
        _comb_eff = (
            min(_comb_sequential, _comb_best) if _comb_best < 1.0 else _comb_sequential
        )
        _edge_seq = max(0.0, 1.0 - _comb_eff)
        _tier_seq = self._detect_setup_tier(sym, _edge_seq, secs_left, False, True)
        leg_budget = self._tier_sizing(_tier_seq, _comb_sequential)
        # plancher : au moins arb_budget()
        leg_budget = max(self.arb_budget(), leg_budget)
        # FAVORITE FIRST (Steven 28/07) : reordonne outcomes pour que la favorite
        # soit achetee en 1er (fav_side deja detectee plus haut).
        # FAVORITE FIRST (Steven 28/07) + ATOMIC ARB GUARD : la favorite est
        # achetee en 1er avec 1.6x budget. Si elle ne remplit PAS, on saute
        # l'underdog (ATOMIC ARB GUARD) pour eviter les ORPHELINS.
        # FAVORITE BUDGET OVERWEIGHT (Steven 28/07) : la jambe favorite
        # recoit FAVORITE_BUDGET_MULT * leg_budget, l'underdog garde leg_budget.
        fav_budget = round(leg_budget * FAVORITE_BUDGET_MULT, 2) if fav_side else leg_budget
        acted = False
        filled_legs = []
        failed_legs = []
        # PRE-FILL (Steven 28/07) : les jambes deja en portefeuille comptent
        # comme "remplies" pour l'ATOMIC ARB GUARD.
        for side, token_id in zip(outcomes, token_ids):
            key = f"{slug}|{side}"
            if key in mk["open"]:
                filled_legs.append((side, token_id))
        for i, (side, token_id) in enumerate(zip(outcomes, token_ids)):
            key = f"{slug}|{side}"
            # SKIP : jambe deja tenue (pas de rachat)
            if key in mk["open"]:
                acted = True
                continue
            # ATOMIC ARB GUARD (Steven 28/07) : jamais d'orphelin.
            # Si la 1ere jambe NEUVE ne se remplit PAS, on saute la 2e.
            if i > 0 and not filled_legs:
                self._log(
                    f"⏳ [ARB-ATOMIC] {sym} {slug} leg1={outcomes[0]} PAS remplie "
                    f"-> skip leg2={side} (prochain cycle)"
                )
                failed_legs.append((side, token_id))
                continue
            tag = f"[HEDGE {secs_left:.0f}s]" if force_hedge else f"[d={d}]"
            # FAVORITE MAX_ENTRY (fix : 1.0 est REJETE par Polymarket, bornes
            # reelles [0.01, 0.99] -> la jambe favorite echouait a 100%, TOUJOURS,
            # empechant tout le mecanisme FAVORITE_BUDGET_MULT de jamais s'executer).
            _max_entry = 0.99 if side == fav_side else max_entry
            ok, _ = self._open_leg(
                sym,
                mode,
                m,
                p,
                side,
                token_id,
                _max_entry,
                tag,
                budget_usd=fav_budget if side == fav_side else leg_budget,
            )
            if ok:
                filled_legs.append((side, token_id))
                # V3.1 AXE 8 : log entree structure avec tier reel
                _leg_info = mk["open"].get(f"{slug}|{side}", {})
                _edge_val = max(0.0, 1.0 - _comb_sequential)
                _tier_label = (
                    _tier_seq.upper() if _tier_seq else f"edge={_edge_val * 100:.1f}%"
                )
                _used_budget = fav_budget if side == fav_side else leg_budget
                self._log_trade_entry(
                    sym,
                    slug,
                    side,
                    mode,
                    "bothside",
                    _tier_label,
                    _leg_info.get("entry_price", 0),
                    _used_budget,
                    _comb_sequential,
                    _edge_val,
                    secs_left,
                )
            else:
                failed_legs.append((side, token_id))
            acted = acted or ok
        # FIX 23/07 : si jambe 1 remplie mais jambe 2 ratee, FORCER jambe 2
        # MACHINE A POGNON (Laguna XS 25/07) : on attend que Leg2 baisse assez
        # pour que combined <= 1.02. Si ca echoue -> ON VEND PAS, on attend.
        # La fenetre de 5min oscille: Leg2 finit par baisser a un moment.
        # HEDGE-NEAR-RESOLUTION rattrape tout a <30s si Leg2 n'est jamais
        # devenue assez pas chere. max_payable = 1.02 - fill_price.
        if filled_legs and failed_legs and secs_left > 5:
            for side, token_id in failed_legs:
                fill_price = None
                filled_side = None
                for fs, _ in filled_legs:
                    leg_info = mk["open"].get(f"{slug}|{fs}")
                    if leg_info:
                        fill_price = leg_info.get("entry_price")
                        filled_side = fs
                        break
                if fill_price is not None:
                    max_payable = round(max(0.05, 1.02 - fill_price), 3)
                else:
                    max_payable = 0.50
                tag_force = f"[FORCE-PAIR {secs_left:.0f}s]"
                # RETRY BORNE (Steven 05/08, "il aurait du persister sur Down") :
                # avant, un seul essai -> le moindre echec (liquidite eclair, ordre
                # concurrent) faisait abandonner tout de suite et revendre la jambe
                # deja tenue. On retente maintenant jusqu'a FORCE_PAIR_MAX_RETRIES
                # fois, TOUJOURS au meme plafond de prix max_payable (jamais
                # surpaye pour forcer la paire -> une paire trop chere garantit
                # une perte, ce n'est pas le but). Court delai entre essais pour
                # laisser une chance a un carnet qui bouge vite.
                ok2 = False
                for _fp_try in range(FORCE_PAIR_MAX_RETRIES):
                    ok2, _ = self._open_leg(
                        sym,
                        mode,
                        m,
                        p,
                        side,
                        token_id,
                        max_payable,
                        tag_force,
                        budget_usd=fav_budget if side == fav_side else leg_budget,
                    )
                    if ok2:
                        break
                    if _fp_try < FORCE_PAIR_MAX_RETRIES - 1:
                        time.sleep(FORCE_PAIR_RETRY_SLEEP_S)
                if ok2:
                    self._log(
                        f"🔗 [BOTHSIDE] {sym} {slug} {side} FORCE-PAIR apres echec "
                        f"(jambe1={fill_price:.3f} max_leg2={max_payable:.3f} "
                        f"combined<{fill_price + max_payable:.3f})"
                    )
                else:
                    # OSCILLATION GATE (Steven 26/07) : si le combined recent
                    # oscillait dans la zone, on ATTEND au lieu de vendre.
                    # Le prix va probablement revenir.
                    _osc_ready = _comb_best <= 1.02 and _comb_recent_trend < 0
                    _osc_wait_secs = 15  # max 15s d'attente si oscille
                    if (
                        _osc_ready
                        and secs_left > _osc_wait_secs
                        and fill_price is not None
                        and fill_price < 0.85
                    ):
                        self._log(
                            f"📊 [OSC-WAIT] {sym} {slug} FORCE-PAIR echec "
                            f"MAIS combined_best={_comb_best:.3f} trend={_comb_recent_trend:+.3f} "
                            f"-> ATTENTE {_osc_wait_secs}s (prix oscille)"
                        )
                    else:
                        # FORCE SELL (Laguna XS 24/07) : si FORCE-PAIR echoue
                        # ET pas d'oscillation -> on vend la jambe tenue.
                        sell_side = filled_legs[0][0] if filled_legs else None
                        sell_token = filled_legs[0][1] if filled_legs else None
                        if sell_side and sell_token:
                            sell_info = mk["open"].get(f"{slug}|{sell_side}")
                            if sell_info:
                                sell_shares = sell_info.get("filled_shares", 0)
                                if sell_shares > 0:
                                    bid_s, _, _ = quotes.get(
                                        sell_side, (None, None, None)
                                    )
                                    sell_price = (
                                        bid_s
                                        if bid_s
                                        else sell_info.get("entry_price", 0)
                                    )
                                    self._log(
                                        f"🚨 [FORCE-PAIR] {sym} {slug} {side} echec "
                                        f"-> VENTE jambe {sell_side}@{sell_price:.3f} "
                                        f"(perte ~{(sell_info.get('entry_price', 0) - sell_price) * sell_shares:.2f}$)"
                                    )
                                    try:
                                        # FIX CRITIQUE (Steven 04/08, trouve via le
                                        # screenshot "31.9 positions" jamais liquidees) :
                                        # ordre (token, PRIX, parts) -- sell_shares et
                                        # sell_price etaient inverses, envoyant un
                                        # "prix" de plusieurs parts (invalide, >1$) ->
                                        # cette vente d'urgence echouait TOUJOURS
                                        # silencieusement, expliquant l'accumulation.
                                        self._live.sell_position(
                                            sell_token, sell_price, sell_shares
                                        )
                                    except Exception as e:
                                        self._log(f"⚠️ [FORCE-PAIR] vente echouee: {e}")
                                    # V3.1 AXE 2 : cooldown slug + abort tracking
                                    self._set_slug_cooldown(sym, slug, mk)
                                    self._record_abort(sym, mk)
                                    # V3.1 AXE 7 : loss tag
                                    loss_tag = self._classify_loss(
                                        {
                                            "legs": [sell_side],
                                            "pnl": -abs(
                                                (
                                                    sell_info.get("entry_price", 0)
                                                    - sell_price
                                                )
                                                * sell_shares
                                            ),
                                        },
                                        "FORCE-PAIR echec",
                                    )
                                    # V3.1 AXE 8 : log structure
                                    self._log_trade_exit(
                                        sym,
                                        slug,
                                        sell_side,
                                        "FORCE_SELL",
                                        sell_info.get("entry_price", 0),
                                        sell_price,
                                        -abs(
                                            (
                                                sell_info.get("entry_price", 0)
                                                - sell_price
                                            )
                                            * sell_shares
                                        ),
                                        0.0,
                                        0.0,
                                        -abs(
                                            (
                                                sell_info.get("entry_price", 0)
                                                - sell_price
                                            )
                                            * sell_shares
                                        ),
                                        0.0,
                                        "filled",
                                        "rejected",
                                        loss_tag,
                                    )
                                    mk["open"].pop(f"{slug}|{sell_side}", None)
        # ORPHAN FIX (Laguna XS 25/07) : quand legs_held==1 depuis un tick
        # precedent, filled_legs est VIDE (la jambe existante est skippee par
        # _open_leg). On retente l'achat de la 2e jambe chaque tick avec prix
        # frais (WS temps reel). ON VEND PAS — on attend que Leg2 baisse.
        # max_payable = 1.02 - fill_price. Si ca echoue, le prochain tick
        # retentera. HEDGE-NEAR-RESOLUTION rattrape a <30s.
        if not filled_legs and legs_held == 1 and secs_left > 5:
            for side_o, token_id_o in zip(outcomes, token_ids):
                key_o = f"{slug}|{side_o}"
                if key_o not in mk["open"]:
                    owned_side = [s for s in outcomes if f"{slug}|{s}" in mk["open"]]
                    if owned_side:
                        owned_info = mk["open"].get(f"{slug}|{owned_side[0]}")
                        fill_price = (
                            owned_info.get("entry_price") if owned_info else None
                        )
                        if fill_price is not None:
                            max_payable = round(max(0.05, 1.02 - fill_price), 3)
                        else:
                            max_payable = 0.50
                        tag_orphan = f"[ORPHAN-FIX {secs_left:.0f}s]"
                        ok3, _ = self._open_leg(
                            sym,
                            mode,
                            m,
                            p,
                            side_o,
                            token_id_o,
                            max_payable,
                            tag_orphan,
                            budget_usd=leg_budget,
                        )
                        if ok3:
                            self._log(
                                f"🔗 [ORPHAN] {sym} {slug} {side_o} FORCE-PAIR "
                                f"(owned={owned_side[0]}@{fill_price:.3f} "
                                f"max={max_payable:.3f} combined<{fill_price + max_payable:.3f})"
                            )
                            acted = True
                        else:
                            # FORCE SELL ORPHAN (Laguna XS 24/07) : si ORPHAN-FIX
                            # echoue, on vend la jambe tenue plutot que solo bet.
                            sell_info_orph = mk["open"].get(f"{slug}|{owned_side[0]}")
                            if sell_info_orph:
                                sell_shares_orph = sell_info_orph.get(
                                    "filled_shares", 0
                                )
                                if sell_shares_orph > 0:
                                    bid_orph, _, _ = quotes.get(
                                        owned_side[0], (None, None, None)
                                    )
                                    sell_price_orph = (
                                        bid_orph
                                        if bid_orph
                                        else sell_info_orph.get("entry_price", 0)
                                    )
                                    self._log(
                                        f"🚨 [ORPHAN] {sym} {slug} {side_o} echec "
                                        f"-> VENTE jambe {owned_side[0]}@{sell_price_orph:.3f} "
                                        f"(perte ~{(sell_info_orph.get('entry_price', 0) - sell_price_orph) * sell_shares_orph:.2f}$)"
                                    )
                                    try:
                                        token_own = (
                                            mk["open"]
                                            .get(f"{slug}|{owned_side[0]}", {})
                                            .get("token_id")
                                        )
                                        if token_own:
                                            # FIX CRITIQUE (Steven 04/08) : ordre
                                            # (token, PRIX, parts) -- meme inversion
                                            # que les 2 autres chemins d'urgence,
                                            # meme consequence (vente jamais executee).
                                            self._live.sell_position(
                                                token_own,
                                                sell_price_orph,
                                                sell_shares_orph,
                                            )
                                    except Exception as e:
                                        self._log(f"⚠️ [ORPHAN] vente echouee: {e}")
                                    mk["open"].pop(f"{slug}|{owned_side[0]}", None)
        # HEDGE NEAR-RESOLUTION (Laguna XS 24/07) : si < 30s et on tient 1 jambe,
        # on achete l'autre AU MARCHE quel que soit le prix pour completer la paire.
        # Meme a combined 1.05, c'est mieux que de perdre 1$ sur un bet directionnel.
        # Transforme un bet directionnel en hedge (perte limitee a combined - 1.00).
        HEDGE_NEAR_SECS = 30
        if legs_held == 1 and secs_left < HEDGE_NEAR_SECS and secs_left > 3:
            for side_h, token_h in zip(outcomes, token_ids):
                key_h = f"{slug}|{side_h}"
                if key_h not in mk["open"]:
                    bid_h, ask_h, _ = quotes.get(side_h, (None, None, None))
                    if ask_h is not None:
                        owned_side_h = [
                            s for s in outcomes if f"{slug}|{s}" in mk["open"]
                        ]
                        if owned_side_h:
                            owned_info_h = mk["open"].get(f"{slug}|{owned_side_h[0]}")
                            entry_h = (
                                owned_info_h.get("entry_price")
                                if owned_info_h
                                else 0.50
                            )
                            combined_h = entry_h + ask_h
                            self._log(
                                f"🛡️ [HEDGE-NEAR] {sym} {slug} {side_h} "
                                f"@ {ask_h:.3f} (owned={owned_side_h[0]}@{entry_h:.3f} "
                                f"comb={combined_h:.3f} secs={secs_left:.0f})"
                            )
                            ok_h, _ = self._open_leg(
                                sym,
                                mode,
                                m,
                                p,
                                side_h,
                                token_h,
                                ask_h + 0.02,
                                "[HEDGE-NEAR]",
                                force=True,
                            )
                            if ok_h:
                                self._log(
                                    f"🛡️ [HEDGE-NEAR] {sym} {slug} PAIRE COMPLETEE "
                                    f"comb={combined_h:.3f} -> hold to resolution"
                                )
                                acted = True
                            else:
                                self._log(
                                    f"⚠️ [HEDGE-NEAR] {sym} {slug} {side_h} "
                                    f"echec -> hold jambe nue"
                                )
        return acted or legs_held > 0

    def _book_quote(self, token_id):
        """(bid, ask, mid) — d'abord via le flux WebSocket TEMPS REEL (Steven
        23/07 : <100ms de latence, best bid/ask pousses en direct), fallback
        REST si le flux est absent/stale. C'EST le fix racine : l'arb crypto
        s'evaporait faute de voir le carnet en direct (~1s de retard REST), et
        le 'spread 14c' qui tuait le MM etait en grande partie de la staleness."""
        try:
            wb = self._ws.book(token_id)  # (best_bid, best_ask, ts) ou None
            if wb:
                bid, ask, _ = wb
                if bid is not None and ask is not None:
                    return bid, ask, round((bid + ask) / 2, 4)
                if ask is not None:
                    return None, ask, ask
                if bid is not None:
                    return bid, None, bid
        except Exception:
            pass
        # fallback REST
        try:
            book = self._live.get_book_sync(token_id)
            if book:
                bids = book.get("bids") or []
                asks = book.get("asks") or []
                bid = bids[0][0] if bids else None
                ask = asks[0][0] if asks else None
                if bid is not None and ask is not None:
                    return bid, ask, round((bid + ask) / 2, 4)
                if ask is not None:
                    return None, ask, ask
                if bid is not None:
                    return bid, None, bid
        except Exception:
            pass
        return None, None, None

    def _log_market_prices(self, sym, slug, outcomes, quotes):
        """Capture prix Up/Down du marche EVALUE (Steven 22/07). REFACTOR 22/07 :
        ne LIT PLUS les carnets — recoit les quotes {side: (bid, ask, mid)} deja
        lues par _try_both_side -> une SEULE lecture par jambe et par tick, et la
        DECISION d'arb utilise exactement les prix captures (avant : 2e lecture
        ~0.7s plus tard -> le prix avait bouge, arbs rates en silence)."""
        mk = self.state["markets"][sym]
        throttle = mk.setdefault("market_price_ts", {})
        now = time.time()
        if now - throttle.get(slug, 0) < PRICE_LOG_INTERVAL_S:
            return
        point = {"ts": round(now, 1), "danger": mk.get("danger", 0)}
        got = 0
        for side in outcomes:
            bid, ask, mid = quotes.get(side, (None, None, None))
            if mid is None:
                continue
            got += 1
            point[side] = round(mid, 4)  # compat backtest existant (mid)
            if ask is not None:
                point[f"{side}_ask"] = round(ask, 4)
            if bid is not None:
                point[f"{side}_bid"] = round(bid, 4)
        if got < 2:
            return
        # combine a l'ASK (le vrai coût d'un arb reel) si les 2 asks dispo
        asks = [point.get(f"{s}_ask") for s in outcomes]
        if all(a is not None for a in asks):
            point["comb_ask"] = round(sum(asks), 4)
        throttle[slug] = now
        all_logs = mk.setdefault("market_price_log", {})
        hist = all_logs.setdefault(slug, [])
        hist.append(point)
        if len(hist) > PRICE_LOG_MAX_POINTS:
            del hist[: len(hist) - PRICE_LOG_MAX_POINTS]
        if (
            len(all_logs) > 20
        ):  # garde-fou : ne suit que les 20 marches les plus recents
            for old_slug in list(all_logs.keys())[: len(all_logs) - 20]:
                del all_logs[old_slug]
        parts = " / ".join(f"{s}={point[s]:.3f}" for s in outcomes if s in point)
        comb = f" | comb_ask={point['comb_ask']:.3f}" if "comb_ask" in point else ""
        self._log(f"📊 [MARCHE] {sym} {slug} {parts}{comb}")

    def _log_position_prices(self, sym):
        """Journalise le prix COURANT de chaque position ouverte, a intervalle
        regulier (Steven 22/07) : permet de revivre un trade apres coup et de
        mieux comprendre les decisions prises en cours de route. Deux formats :
        - texte dans le log (relisible directement)
        - structure dans pos['price_log'] (releve par le dashboard pour une
          mini-courbe par position)."""
        mk = self.state["markets"][sym]
        now = time.time()
        # list(...values()) (Steven 04/08) : cette boucle tourne dans
        # _fast_exit_loop (thread de fond) pendant que d'autres threads
        # ouvrent/ferment des positions sur le meme mk["open"] -> iterer
        # directement sur .values() plantait par intermittence avec
        # "dictionary changed size during iteration" (vu sur SOL en pleine
        # ouverture de paire). Copier la liste des positions au moment du
        # snapshot coute rien et rend la boucle immune aux mutations concurrentes.
        for pos in list(mk["open"].values()):
            last = pos.get("last_price_log_ts", 0)
            if now - last < PRICE_LOG_INTERVAL_S:
                continue
            cur = self._live_price(pos.get("token_id"), None, pos.get("side"))
            if cur is None:
                continue
            pos["last_price_log_ts"] = now
            entry = pos.get("entry_price") or cur
            pnl_pct = ((cur - entry) / entry * 100) if entry else 0.0
            hist = pos.setdefault("price_log", [])
            hist.append({"ts": round(now, 1), "price": round(cur, 4)})
            if len(hist) > PRICE_LOG_MAX_POINTS:
                del hist[: len(hist) - PRICE_LOG_MAX_POINTS]
            tag = f"[{pos['strat'].upper()}]" if pos.get("strat") else ""
            self._log(
                f"💹 [PRIX]{tag} {sym} {pos['slug']} {pos['side']} {cur:.3f} "
                f"(entree {entry:.3f}, {pnl_pct:+.1f}%)"
            )

    def _guard_both_side(self, sym):
        """SL PAR JAMBE (Steven 22/07, scalp simultane "en sl et gard pos
        gagnante") : coupe TOUTE jambe both-side qui plonge sous BOTH_SIDE_SL_PRICE
        SI le momentum confirme la baisse (fail-OPEN : on protege par defaut, on
        n'annule QUE si un vrai rebond est en cours). La jambe gagnante, elle,
        reste geree par le TP -> on coupe le perdant, on garde le gagnant.

        En mode SIMULTANE, s'applique aux 2 jambes independamment (plus la
        restriction 'solo uniquement' de l'ancien gardien : ici on tient les 2
        des le depart, donc couper le perdant est justement le but)."""
        from core.btc_updown import momentum as _momentum

        mk = self.state["markets"][sym]
        now = synced_now()
        for key, pos in list(mk["open"].items()):
            if pos.get("strat") != "bothside":
                continue
            # ARB CHECK : si les 2 jambes du meme slug sont tenues,
            # on HOLD jusqu'a resolution (profit garanti, pas de SL).
            # La jambe "perdante" est compensee par la gagnante a $1.
            slug_check = pos.get("slug", "")
            both_legs_held = sum(
                1
                for k in mk["open"]
                if mk["open"][k].get("slug") == slug_check
                and mk["open"][k].get("strat") == "bothside"
            )
            if both_legs_held >= 2:
                continue  # ARB complet -> hold to resolution, pas de SL
            secs_left = pos["end_ts"] - now
            if secs_left < BOTH_SIDE_SL_MIN_SECS_LEFT:
                continue  # trop peu de temps pour qu'un bail serve a quelque chose
            slug = pos["slug"]
            cur = self._live_price(pos["token_id"], None, pos["side"])
            if cur is None or cur > BOTH_SIDE_SL_PRICE:
                continue
            mom = _momentum(pos["pair"])
            # BUG CORRIGE (22/07, trouvaille Steven) : l'ancienne condition exigeait
            # un momentum CALCULABLE ET confirme -> si l'historique de prix etait trop
            # court (mom=None, frequent en debut de vie d'une position), le gardien ne
            # se declenchait JAMAIS (fail-closed), meme a un prix deja tres bas (0.02-
            # 0.03 vu en reel). Desormais : on protege par DEFAUT des que le prix a
            # atteint le seuil ; on n'ANNULE la protection QUE si le momentum est a la
            # fois calculable ET montre un vrai rebond en cours (pas juste "pas de
            # confirmation") -> fail-OPEN sur la protection, pas fail-closed.
            if mom and not mom["confirms"] and mom["fast_pct_s"] > 0:
                continue  # rebond net en cours -> on laisse une chance avant de vendre
            if pos["mode"] == "real":
                book = self._live.get_book_sync(pos["token_id"])
                bid = book["bids"][0][0] if book and book.get("bids") else None
                if bid is None:
                    continue
                with self._order_lock:
                    res = self._live.sell_position(
                        pos["token_id"], round(bid, 2), pos["filled_shares"]
                    )
                ok = bool(res) and res.get("success", True) is not False
                mom_txt = f"{mom['fast_pct_s']:+.4f}%/s" if mom else "n/a"
                self._log(
                    f"🩹 [SL][REEL] {sym} {slug} {pos['side']} coupe @ {bid:.3f} "
                    f"(entree {pos['entry_price']:.3f}) mom={mom_txt} ok={ok}"
                )
                if not ok:
                    continue
                exit_price = bid
            else:  # paper
                exit_price = cur
                mom_txt = f"{mom['fast_pct_s']:+.4f}%/s" if mom else "n/a"
                self._log(
                    f"🩹 [SL][PAPER] {sym} {slug} {pos['side']} coupe @ {exit_price:.3f} "
                    f"(entree {pos['entry_price']:.3f}) mom={mom_txt}"
                )
            # + les paliers de TP deja realises sur cette jambe (le cas echeant)
            pnl = round(
                pos["filled_shares"] * (exit_price - pos["entry_price"])
                + pos.get("realized_pnl", 0.0),
                3,
            )
            pos.update(
                win=pnl > 0,
                pnl=pnl,
                exit_price=round(exit_price, 3),
                resolved_by="stop_loss",
            )
            if pos["mode"] == "paper":
                mk["paper_balance"] = round(mk["paper_balance"] + pnl, 3)
            mk["trades"].append(pos)
            del mk["open"][key]

    # 3 paliers de TP : (prix_declencheur, fraction_du_restant_a_vendre)
    _TP_TIERS = (
        (BOTH_SIDE_TP1_PRICE, BOTH_SIDE_TP1_FRACTION),
        (BOTH_SIDE_TP2_PRICE, BOTH_SIDE_TP2_FRACTION),
        (BOTH_SIDE_TP3_PRICE, 1.0),  # dernier palier : vend tout ce qui reste
    )

    def _take_profit_both_side(self, sym):
        """PRISE DE PROFIT PAR PALIERS (Steven 22/07) : une jambe both-side qui
        s'envole avant resolution est vendue PROGRESSIVEMENT sur 3 niveaux.
        SKIP si les 2 jambes du meme slug sont tenues (= ARB complet) :
        profit garanti a la resolution, pas de TP intermediaire.
        Laguna XS 24/07 : le TP vendait des jambes ARB a 0.88 alors que la
        resolution paie $1.00 -> on rateait le gain."""
        mk = self.state["markets"][sym]
        for key, pos in list(mk["open"].items()):
            if pos.get("strat") != "bothside":
                continue
            # ARB CHECK : si les 2 jambes du meme slug sont tenues,
            # on HOLD jusqu'a resolution (profit garanti, pas de TP).
            slug = pos.get("slug", "")
            both_legs_held = sum(
                1
                for k in mk["open"]
                if mk["open"][k].get("slug") == slug
                and mk["open"][k].get("strat") == "bothside"
            )
            if both_legs_held >= 2:
                continue  # ARB complet -> hold to resolution
            stage = pos.get("tp_stage", 0)
            if stage >= len(self._TP_TIERS):
                continue
            cur = self._live_price(pos.get("token_id"), None, pos.get("side"))
            if cur is None:
                continue
            target_price, frac = self._TP_TIERS[stage]
            if cur < target_price:
                continue
            remaining = pos["filled_shares"]
            sell_shares = round(remaining * frac, 2)
            if pos["mode"] == "real" and sell_shares < MIN_ORDER_SIZE_SHARES:
                # trop petit pour un ordre LIMIT reel -> vend tout d'un coup
                # plutot que de forcer un palier impossible a executer
                sell_shares = remaining
                frac = 1.0
            if pos["mode"] == "real":
                book = self._live.get_book_sync(pos["token_id"])
                bid = book["bids"][0][0] if book and book.get("bids") else None
                if bid is None or bid < target_price * 0.9:  # tampon anti-slippage
                    continue
                with self._order_lock:
                    res = self._live.sell_position(
                        pos["token_id"], round(bid, 2), sell_shares
                    )
                ok = bool(res) and res.get("success", True) is not False
                if not ok:
                    continue
                exit_price = bid
            else:
                exit_price = cur

            realized = round(sell_shares * (exit_price - pos["entry_price"]), 3)
            pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
            pos["filled_shares"] = round(remaining - sell_shares, 3)
            pos["tp_stage"] = stage + 1
            if pos["mode"] == "paper":
                mk["paper_balance"] = round(mk["paper_balance"] + realized, 3)
            self._log(
                f"💰 [TP{stage + 1}] {sym} {pos['slug']} {pos['side']} vend {sell_shares} @ {exit_price:.3f} "
                f"(palier {target_price}) realise={realized:+.3f}$ cumul={pos['realized_pnl']:+.3f}$"
            )
            if pos["filled_shares"] <= 0.01:
                pos.update(
                    win=pos["realized_pnl"] > 0,
                    pnl=pos["realized_pnl"],
                    resolved_by="take_profit",
                )
                mk["trades"].append(pos)
                del mk["open"][key]

    # ══════════════════════════════════════════════════════════════════════════
    # ── PNL-BASED TIERED TP/SL V3.2 (Steven 27/07) ──
    # ══════════════════════════════════════════════════════════════════════════

    def _manage_pnl_tier_exits(self, sym):
        """TP/SL PALIERS PNL-BASED V3.2 : sort 25% a +25%, 25% a +50%,
        25% a +75%, laisse 25% runner avec trailing stop.
        Stop loss a -30% du prix d'entree.
        S'applique aux positions bothside/swing (pas orphan, pas ARB pur).
        Les orphans ont leur propre gestion dans _manage_orphans."""
        mk = self.state["markets"][sym]
        now = synced_now()
        for key, pos in list(mk["open"].items()):
            if pos.get("strat") not in ("bothside", "swing"):
                continue
            # RISK-FREE : NE JAMAIS GERER INDIVIDUELLEMENT (Steven 29/07, bug
            # trouve en prod : une paire d'arb garanti a comb=0.93 (edge 7%,
            # profit garanti +2.8$ quel que soit le resultat) a ete cassee par
            # ce systeme -> jambe Up stop-lossee a -27.4$, jambe Down vendue en
            # trailing a +20.6$, NET -6.8$ au lieu du +2.8$ garanti. Le tag
            # is_risk_free existait deja (affichage) mais rien ne l'utilisait
            # ICI pour bloquer le SL/TP/SPREAD-EXIT individuel. Une paire
            # risk-free doit etre tenue ENTIERE jusqu'a resolution -> c'est
            # la SEULE facon de preserver la garantie ; toucher une seule
            # jambe casse l'equation edge = payout - cout.
            if pos.get("is_risk_free"):
                continue
            # ARB CHECK : si les 2 jambes du meme slug sont tenues -> SPREAD EXIT
            slug = pos.get("slug", "")
            both_legs_held = sum(
                1
                for k in mk["open"]
                if mk["open"][k].get("slug") == slug
                and mk["open"][k].get("strat") == "bothside"
            )
            if both_legs_held >= 2:
                # ── SPREAD-BASED EXIT V8.0 : coupe la jambe perdante tôt ──
                # Quand l'autre jambe performe mieux de >10%, cette jambe est probablement
                # le loser -> on la sort pour récupérer du capital au lieu de tout perdre.
                SPREAD_EXIT_THRESHOLD = 0.10
                SPREAD_MIN_SECS_LEFT = 30
                _entry = pos.get("entry_price", 0)
                if _entry > 0:
                    _secs_left = pos.get("end_ts", now) - now
                    if _secs_left > SPREAD_MIN_SECS_LEFT:
                        _cur = self._live_price(pos.get("token_id"), None, pos.get("side"))
                        if _cur is not None:
                            _own_pnl = (_cur - _entry) / _entry
                            _other_pos = None
                            for _k in mk["open"]:
                                if _k != key and mk["open"][_k].get("slug") == slug and mk["open"][_k].get("strat") == "bothside":
                                    _other_pos = mk["open"][_k]
                                    break
                            if _other_pos:
                                _other_entry = _other_pos.get("entry_price", 0.5)
                                _other_cur = self._live_price(_other_pos.get("token_id"), None, _other_pos.get("side"))
                                if _other_cur is not None and _other_entry > 0:
                                    _other_pnl = (_other_cur - _other_entry) / _other_entry
                                    _spread = _other_pnl - _own_pnl
                                    if _spread > SPREAD_EXIT_THRESHOLD:
                                        _shares = pos.get("filled_shares", 0)
                                        if _shares >= MIN_ORDER_SIZE_SHARES:
                                            _bid = self._get_bid(pos)
                                            if _bid is None:
                                                continue
                                            if pos["mode"] == "real":
                                                sold = self._sell_orphan(
                                                    pos["token_id"], _shares,
                                                    f" {sym} {slug} {pos['side']} SPREAD-EXIT spread={_spread:.3f}"
                                                )
                                            else:
                                                sold = _shares
                                            if sold > 0:
                                                realized = round(sold * (_bid - _entry), 3)
                                                pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
                                                pos["filled_shares"] = 0.0
                                                pnl = pos["realized_pnl"]
                                                pos.update(win=pnl > 0, pnl=pnl, resolved_by="spread_exit")
                                                mk["trades"].append(pos)
                                                del mk["open"][key]
                                                self._record_trade_pnl(sym, pnl)
                                                icon = "✅ WIN " if pnl > 0 else "❌ LOSS"
                                                self._log(
                                                    f"{icon} [SPREAD] {sym} {slug} {pos['side']} coupe {sold} parts "
                                                    f"@ {_bid:.3f} (entree {_entry:.3f}) realize={realized:+.3f}$ "
                                                    f"spread={_spread:.3f} (other={_other_pnl:.2%} own={_own_pnl:.2%})"
                                                )
                                        continue
                    # Spread below threshold -> fall through to RL exit manager
                # No other leg found -> fall through to RL exit manager

            entry = pos.get("entry_price", 0)
            if entry <= 0:
                continue
            secs_left = pos.get("end_ts", now) - now
            if secs_left < PNL_SL_MIN_SECS_LEFT:
                continue

            # Prix courant
            cur = self._live_price(pos.get("token_id"), None, pos.get("side"))
            if cur is None:
                continue

            # PnL% du contrat
            pnl_pct = (cur - entry) / entry

            # ── TRACK MIN DRAWDOWN pour reversal stats ──
            min_pct = pos.get("min_pnl_pct", 0)
            if pnl_pct < min_pct:
                pos["min_pnl_pct"] = round(pnl_pct, 4)
                min_pct = pnl_pct

            # ── TRACK PEAK PnL pour trailing ──
            peak_pct = pos.get("peak_pnl_pct", 0)
            if pnl_pct > peak_pct:
                pos["peak_pnl_pct"] = round(pnl_pct, 4)
                peak_pct = pnl_pct

            shares = pos.get("filled_shares", 0)
            if shares < MIN_ORDER_SIZE_SHARES:
                continue

            # Auto-init pour les positions ouvertes avant V3.2
            if "init_shares" not in pos:
                pos["init_shares"] = shares
            if "pnl_tp_stage" not in pos:
                pos["pnl_tp_stage"] = 0
            init_shares = pos.get("init_shares", shares)

            # ── RL EXIT MANAGER V4.0 : proposition d'action ──
            # Gere BOTH les positions bothside ET non-bothside
            is_bothside = pos.get("strat") == "bothside"
            if (
                self._rl is not None
                and self._rl.enabled
                and secs_left > RL_EXIT_MIN_SECS_LEFT
                and now - self._rl_last_proposal.get(sym, 0) >= RL_EXIT_INTERVAL_S
            ):
                # Injecter les donnees live dans la position pour le RL
                pos["_current_price"] = cur

                # Find other leg for bothside positions
                _other_pnl_pct = 0.0
                _mom_other = 0.0
                _combined_now = cur
                if is_bothside:
                    for _k in mk["open"]:
                        if _k != key and mk["open"][_k].get("slug") == slug and mk["open"][_k].get("strat") == "bothside":
                            _opp = mk["open"][_k]
                            _opp_entry = _opp.get("entry_price", 0.5)
                            _opp_cur = self._live_price(_opp.get("token_id"), None, _opp.get("side"))
                            if _opp_cur is not None and _opp_entry > 0:
                                _other_pnl_pct = (_opp_cur - _opp_entry) / _opp_entry
                                _mom_other = 0.0
                                _combined_now = cur + _opp_cur
                            break

                sym_state = {
                    "_momentum_fast": 0.0,
                    "_volatility": 0.5,
                    "_mom_other": _mom_other,
                    "_other_pnl_pct": _other_pnl_pct,
                    "_combined_now": _combined_now,
                    "_risk_limits": self._risk_limits.get(sym, {}),
                    "_floating_pnl_pct": sum(
                        p.get("filled_shares", 0)
                        * (
                            p.get("_current_price", p.get("entry_price", 0.5))
                            - p.get("entry_price", 0.5)
                        )
                        for p in mk["open"].values()
                        if p.get("strat") == "bothside"
                    )
                    / max(1.0, init_shares * entry),
                    "_reversal_stats": self.state.get("reversal_stats", {}),
                }
                rl_action, rl_params, rl_name, rl_reason = self._rl.propose_action(
                    pos, sym_state, now
                )
                self._rl_last_proposal[sym] = now
                # Log RL decision
                _f = self._rl.extract_features(pos, sym_state, now)
                _sp = _f[5]  # spread feature
                _pnl = _f[0]  # own pnl%
                _mom = _f[3]  # momentum
                _mom_o = _f[4]  # other momentum
                if rl_name != "HOLD":
                    self._log(
                        f"🧠 [RL-{rl_name}] {sym} {slug} {pos['side']} "
                        f"pnl={_pnl:+.2%} spread={_sp:+.3f} mom={_mom:+.2f} "
                        f"mom_o={_mom_o:+.2f} reason={rl_reason}"
                    )
                else:
                    # Log HOLD periodically (every 60s)
                    last_rl_log = pos.get("_rl_last_log_ts", 0)
                    if now - last_rl_log >= 60:
                        pos["_rl_last_log_ts"] = now
                        self._log(
                            f"🧠 [RL-HOLD] {sym} {slug} {pos['side']} "
                            f"pnl={_pnl:+.2%} spread={_sp:+.3f} mom={_mom:+.2f} "
                            f"mom_o={_mom_o:+.2f} shares={shares:.1f}"
                        )
                # Shadow mode : log only, pas d'execution
                if self._rl.shadow:
                    self._rl.log_shadow(
                        sym,
                        pos,
                        self._rl.extract_features(pos, sym_state, now),
                        rl_action,
                    )
                else:
                    # LIVE : appliquer l'action RL
                    self._apply_rl_action(
                        sym,
                        pos,
                        key,
                        mk,
                        rl_action,
                        rl_params,
                        rl_name,
                        rl_reason,
                        cur,
                        entry,
                        shares,
                        init_shares,
                        slug,
                        now,
                    )
                    # FIX (Steven 29/07, "doublon dans historique") : si le RL
                    # vient de FERMER la position (ACTION_EXIT_100 -> del mk['open']
                    # + append trades), le code plus bas (STOP LOSS / TP paliers)
                    # continuait sur ce MEME `pos` avec des `shares` PERIMEES (lues
                    # avant l'action RL) -> re-declenchait le SL sur une position
                    # deja fermee -> 2e append identique dans mk['trades'] (le
                    # dashboard montrait 2 lignes identiques pour un seul trade).
                    if key not in mk["open"]:
                        continue

            stage = pos.get("pnl_tp_stage", 0)

            # ── STOP LOSS : -30% du prix d'entree (ou override RL) ──
            effective_sl_pct = pos.get("rl_stop_pct", PNL_SL_PCT)
            if pnl_pct <= -effective_sl_pct and stage < len(PNL_TP_TARGETS):
                exit_price = self._get_bid(pos) if pos["mode"] == "real" else cur
                if exit_price is None:
                    continue
                sold = shares
                if pos["mode"] == "real":
                    sold = self._sell_orphan(
                        pos["token_id"], shares, f" {sym} {slug} {pos['side']} PNL-SL"
                    )
                    if sold <= 0:
                        continue
                realized = round(sold * (exit_price - entry), 3)
                pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
                pos["filled_shares"] = 0.0
                pnl = pos["realized_pnl"]
                pos.update(
                    win=pnl > 0,
                    pnl=pnl,
                    resolved_by="pnl_stoploss",
                    exit_price=round(exit_price, 3),
                )
                self._record_reversal(sym, pos, min_pct, pnl)
                if pos["mode"] == "paper":
                    mk["paper_balance"] = round(mk["paper_balance"] + pnl, 3)
                mk["trades"].append(pos)
                del mk["open"][key]
                self._record_trade_pnl(sym, pnl)
                self._log(
                    f"🛑 [PNL-SL] {sym} {slug} {pos['side']} -{abs(pnl_pct) * 100:.1f}% "
                    f"-> coupe @ {exit_price:.3f} pnl={pnl:+.3f}$ [{pos.get('tier', '?')}]"
                )
                self._log_trade_exit(
                    sym,
                    slug,
                    pos["side"],
                    "pnl_stoploss",
                    entry,
                    exit_price,
                    realized,
                    0,
                    0,
                    pnl,
                    now - pos.get("opened_ts", pos["start_ts"]),
                    "sold",
                    "open",
                )
                continue

            # ── TP PAR PALIERS (25%/50%/75%) ──
            if stage < len(PNL_TP_TARGETS):
                target_pct = PNL_TP_TARGETS[stage]
                if pnl_pct >= target_pct:
                    # ESCALADE APRES ECHECS REPETES (Steven 05/08, "on voit
                    # l'argent filer entre nos doigts") : avant, un fill rate
                    # entre pos["filled_shares"] et le palier vise reessayait
                    # la MEME petite fraction chaque cycle, indefiniment, si le
                    # carnet ne suivait pas (observe : 6 echecs consecutifs sur
                    # 2.5min, 0 part vendue, pendant que le marche continuait
                    # de bouger). Des PNL_TP_ESCALATE_AFTER echecs de suite sur
                    # CETTE position -> on vend TOUT ce qui reste au lieu de
                    # ne retenter que la fraction du palier -- sortir en
                    # entier vaut mieux que continuer a esperer un fill
                    # partiel qui ne vient pas.
                    _fail_streak = pos.get("tp_fail_streak", 0)
                    _escalate = _fail_streak >= PNL_TP_ESCALATE_AFTER
                    # Vend 25% de la taille INITIALE (ou tout, si escalade)
                    sell_target = shares if _escalate else round(init_shares * PNL_TP_FRACTIONS[stage], 2)
                    sell_shares = min(sell_target, shares)
                    if sell_shares < MIN_ORDER_SIZE_SHARES:
                        sell_shares = shares  # vend tout si trop petit
                    exit_price = self._get_bid(pos) if pos["mode"] == "real" else cur
                    if exit_price is None:
                        continue
                    if pos["mode"] == "real":
                        _tag_tp = f" {sym} {slug} {pos['side']} PNL-TP{stage + 1}"
                        if _escalate:
                            _tag_tp += f" ESCALADE(x{_fail_streak})"
                        sold = self._sell_orphan(
                            pos["token_id"],
                            sell_shares,
                            _tag_tp,
                        )
                        if sold <= 0:
                            pos["tp_fail_streak"] = _fail_streak + 1
                            continue
                        pos["tp_fail_streak"] = 0
                        sell_shares = sold
                    realized = round(sell_shares * (exit_price - entry), 3)
                    pos["realized_pnl"] = round(
                        pos.get("realized_pnl", 0.0) + realized, 3
                    )
                    pos["filled_shares"] = round(shares - sell_shares, 3)
                    pos["pnl_tp_stage"] = stage + 1
                    if pos["mode"] == "paper":
                        mk["paper_balance"] = round(mk["paper_balance"] + realized, 3)
                    self._log(
                        f"💰 [PNL-TP{stage + 1}] {sym} {slug} {pos['side']} "
                        f"+{pnl_pct * 100:.1f}% vend {sell_shares} @ {exit_price:.3f} "
                        f"realise={realized:+.3f}$ cumul={pos['realized_pnl']:+.3f}$ "
                        f"[{pos.get('tier', '?')}]"
                    )
                    # TOUT VENDU -> close
                    if pos["filled_shares"] <= 0.01:
                        pnl = pos["realized_pnl"]
                        pos.update(
                            win=pnl > 0,
                            pnl=pnl,
                            resolved_by="pnl_take_profit",
                            exit_price=round(exit_price, 3),
                        )
                        self._record_reversal(sym, pos, min_pct, pnl)
                        mk["trades"].append(pos)
                        del mk["open"][key]
                        self._record_trade_pnl(sym, pnl)
                    continue

            # ── TRAILING RUNNER : apres TP3, garde 25% avec stop trailing ──
            if stage >= len(PNL_TP_TARGETS) and shares > 0:
                # Stop trailing : giveback depuis le pic (override RL si actif)
                effective_giveback = pos.get("rl_trail_giveback", PNL_TRAIL_GIVEBACK)
                trail_floor_pct = peak_pct - effective_giveback
                if pnl_pct <= trail_floor_pct or pnl_pct <= 0:
                    exit_price = self._get_bid(pos) if pos["mode"] == "real" else cur
                    if exit_price is None:
                        continue
                    if pos["mode"] == "real":
                        sold = self._sell_orphan(
                            pos["token_id"],
                            shares,
                            f" {sym} {slug} {pos['side']} PNL-RUNNER",
                        )
                        if sold <= 0:
                            continue
                        shares = sold
                    realized = round(shares * (exit_price - entry), 3)
                    pos["realized_pnl"] = round(
                        pos.get("realized_pnl", 0.0) + realized, 3
                    )
                    pos["filled_shares"] = 0.0
                    pnl = pos["realized_pnl"]
                    pos.update(
                        win=pnl > 0,
                        pnl=pnl,
                        resolved_by="pnl_runner_trail",
                        exit_price=round(exit_price, 3),
                    )
                    self._record_reversal(sym, pos, min_pct, pnl)
                    if pos["mode"] == "paper":
                        mk["paper_balance"] = round(mk["paper_balance"] + pnl, 3)
                    mk["trades"].append(pos)
                    del mk["open"][key]
                    self._record_trade_pnl(sym, pnl)
                    self._log(
                        f"🏃 [PNL-RUNNER] {sym} {slug} {pos['side']} "
                        f"trail giveback @ {exit_price:.3f} (pic={peak_pct * 100:.1f}%) "
                        f"pnl={pnl:+.3f}$ [{pos.get('tier', '?')}]"
                    )

    def _apply_rl_action(
        self,
        sym,
        pos,
        key,
        mk,
        action,
        params,
        action_name,
        reason,
        cur,
        entry,
        shares,
        init_shares,
        slug,
        now,
    ):
        """Applique une action RL non-shadow : EXIT partiel/total ou HOLD."""
        from rl_exit import (
            ACTION_HOLD,
            ACTION_EXIT_25,
            ACTION_EXIT_50,
            ACTION_EXIT_100,
        )

        if action == ACTION_HOLD:
            return

        # ── EXIT partiel / total ──
        if action in (ACTION_EXIT_25, ACTION_EXIT_50, ACTION_EXIT_100):
            frac_map = {
                ACTION_EXIT_25: 0.25,
                ACTION_EXIT_50: 0.50,
                ACTION_EXIT_100: 1.0,
            }
            frac = frac_map[action]
            sell_shares = max(RL_EXIT_MIN_SHARES, min(shares, shares * frac))
            if sell_shares < MIN_ORDER_SIZE_SHARES:
                sell_shares = shares  # vend tout si trop petit
            exit_price = self._get_bid(pos) if pos["mode"] == "real" else cur
            if exit_price is None:
                return
            if pos["mode"] == "real":
                sold = self._sell_orphan(
                    pos["token_id"],
                    sell_shares,
                    f" {sym} {slug} {pos['side']} RL-{action_name}",
                )
                if sold <= 0:
                    return
                sell_shares = sold
            realized = round(sell_shares * (exit_price - entry), 3)
            pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
            pos["filled_shares"] = round(shares - sell_shares, 3)
            if pos["mode"] == "paper":
                mk["paper_balance"] = round(mk["paper_balance"] + realized, 3)
            self._log(
                f"🤖 [RL-{action_name}] {sym} {slug} {pos['side']} "
                f"vend {sell_shares:.1f}/{shares:.1f} @ {exit_price:.3f} "
                f"realise={realized:+.3f}$ [{pos.get('tier', '?')}] reason={reason}"
            )
            # Tout vendu -> close
            if pos["filled_shares"] <= 0.01:
                pnl = pos["realized_pnl"]
                pos.update(
                    win=pnl > 0,
                    pnl=pnl,
                    resolved_by=f"rl_{action_name.lower()}",
                    exit_price=round(exit_price, 3),
                )
                mk["trades"].append(pos)
                del mk["open"][key]
                self._record_trade_pnl(sym, pnl)
                self._record_reversal(sym, pos, pos.get("min_pnl_pct", 0), pnl)
            return

    def _get_bid(self, pos):
        """Recupere le meilleur bid d'un token (REAL uniquement)."""
        if pos.get("mode") != "real":
            return None
        try:
            book = self._live.get_book_sync(pos.get("token_id"))
            if book and book.get("bids"):
                return book["bids"][0][0]
        except Exception:
            pass
        return None

    def _record_reversal(self, sym, pos, min_pnl_pct, final_pnl):
        """Track les retournements tardifs (bonus statistique, V3.2).
        Si une position a fait au minimum -10% puis termine gagnante,
        on enregistre le retournement pour calibrer les tiers."""
        tier = pos.get("tier", "fragile").split("+")[0].split("-")[0].lower()
        if tier not in ("fragile", "normal", "premium"):
            tier = "fragile"
        rs = self.state.setdefault("reversal_stats", {})
        bucket = rs.setdefault(tier, [])
        is_reversal = min_pnl_pct <= REVERSAL_MIN_DRAWDOWN and final_pnl > 0
        bucket.append(
            {
                "reversal": is_reversal,
                "min_pct": round(min_pnl_pct, 4),
                "final_pnl": round(final_pnl, 4),
                "ts": time.time(),
            }
        )
        # Garde les N dernieres
        while len(bucket) > REVERSAL_STATS_WINDOW:
            bucket.pop(0)
        if is_reversal:
            self._log(
                f"🔄 [REVERSAL] {sym} {pos.get('slug', '')} {pos.get('side', '')} "
                f"tier={tier} drawdown={min_pnl_pct * 100:+.1f}% -> "
                f"gagnant pnl={final_pnl:+.3f}$ (bonus statistique)"
            )

    def _get_reversal_factor(self, tier):
        """Calcule le facteur d'ajustement du tier base sur les retournements.
        Si >40% des fragiles se retournent -> upgrade temporaire vers normal."""
        rs = self.state.get("reversal_stats", {})
        bucket = rs.get(tier, [])
        if len(bucket) < 10:
            return 1.0  # pas assez de donnees
        reversals = sum(1 for r in bucket if r.get("reversal"))
        ratio = reversals / len(bucket)
        if tier == "fragile" and ratio > REVERSAL_UPGRADE_THRESHOLD:
            self._tlog(
                f"reversal_upgrade_{tier}",
                f"🔄 [REVERSAL-CALIB] {ratio * 100:.0f}% des fragiles se retournent "
                f"-> upgrade vers NORMAL (ratio > {REVERSAL_UPGRADE_THRESHOLD * 100:.0f}%)",
                every=600.0,
            )
            return TIER_SIZE_NORMAL / TIER_SIZE_FRAGILE
        return 1.0

    def _live_price(self, token_id, fallback_market=None, side=None):
        """Prix REEL du contrat via le CARNET CLOB (mid entre meilleur bid et ask).

        BUG CORRIGE (21/07) : le swing lisait `outcomePrices` (champ Gamma), qui est
        lent et souvent FIGE -> 5 des 10 premiers swings ETH sont sortis exactement
        a leur prix d'entree (pnl 0.000), preuve que le prix ne bougeait jamais.
        Le swing pilotait a l'aveugle. On lit desormais le vrai carnet, comme le
        mode reel. Repli sur outcomePrices seulement si le carnet est injoignable."""
        try:
            book = self._live.get_book_sync(token_id)
            if book:
                bids, asks = book.get("bids") or [], book.get("asks") or []
                if bids and asks:
                    return round((bids[0][0] + asks[0][0]) / 2, 3)
                if asks:
                    return round(asks[0][0], 3)
                if bids:
                    return round(bids[0][0], 3)
        except Exception:
            pass
        if fallback_market is not None and side:
            from paper_snipe import outcome_price

            return outcome_price(fallback_market, side)
        return None

    def _try_momentum_fallback(
        self, sym, mode, m, p, outcomes, token_ids, quotes, secs_left
    ):
        """MOMENTUM FALLBACK (Steven 26/07) : quand l'ARB est bloquee (combined > 0.95
        ou edge < 4%), si le momentum Binance confirme un cote et que le contrat
        est encore pas cher (< 0.50), on achete DIRECTIONNELLEMENT ce cote.
        Geree par _manage_swings (trailing/stop) comme un swing classique."""
        if not MOMENTUM_FALLBACK_ENABLED:
            return False
        if secs_left < MOMENTUM_FALLBACK_MIN_SECS:
            return False
        from core.btc_updown import (
            _binance_price,
            _strike_at,
            danger_score,
            momentum as _momentum,
        )

        mk = self.state["markets"][sym]
        slug = m.get("slug")
        strike = _strike_at(p["pair"], p["start_ts"], slug=slug)
        spot = _binance_price(p["pair"])
        if strike is None or spot is None:
            return False
        d = danger_score(p["pair"], strike)
        if d > DANGER_MAX:
            return False
        mom = _momentum(p["pair"])
        if not mom:
            self._tlog(
                f"mom_skip_{sym}",
                f"📎 [MOM-SKIP] {sym} {slug} pas de donnees momentum",
            )
            return False
        fast_pct = mom.get("fast_pct_s", 0)
        slow_pct = mom.get("slow_pct_s", 0)
        # PAS BESOIN de confirms (fast+slow meme sens) pour le momentum fallback.
        # On veut juste un fast fort -> capter les sursauts directionnels courts.
        # Le scalpe est court (trailing a 0.72, stop a 0.25), pas besoin de confirmation lente.
        if abs(fast_pct) < MOMENTUM_FALLBACK_MIN_FAST_PCT:
            self._tlog(
                f"mom_tooweak_{sym}",
                f"📎 [MOM-SKIP] {sym} {slug} mom trop faible "
                f"fast={fast_pct:+.4f}%/s < {MOMENTUM_FALLBACK_MIN_FAST_PCT}",
            )
            return False
        direction = "Up" if spot > strike else "Down"
        if abs(spot - strike) < strike * SWING_MIN_EDGE_PCT:
            return False
        in_cd, cd_reason = self._in_cooldown(sym, slug, mk)
        if in_cd:
            return False
        idx = None
        for i, (side, tid) in enumerate(zip(outcomes, token_ids)):
            if side == direction:
                idx = i
                break
        if idx is None:
            return False
        entry = self._live_price(token_ids[idx], m, direction)
        if entry is None or entry <= 0.03 or entry > MOMENTUM_FALLBACK_MAX_ENTRY:
            self._log(
                f"⚡ [MOM-SKIP] {sym} {slug} {direction} entry={entry} "
                f"(max={MOMENTUM_FALLBACK_MAX_ENTRY})"
            )
            return False
        budget = min(MOMENTUM_FALLBACK_BUDGET, HARD_CAP_USD)
        shares = max(MIN_ORDER_SIZE_SHARES, budget / entry)
        pos = {
            "symbol": sym,
            "slug": slug,
            "side": direction,
            "mode": "paper",
            "strat": "swing",
            "token_id": token_ids[idx],
            "entry_price": entry,
            "filled_shares": round(shares, 2),
            "cost": round(shares * entry, 2),
            "target": SWING_TARGET,
            "stop": SWING_STOP,
            "pair": p["pair"],
            "start_ts": p["start_ts"],
            "end_ts": p["end_ts"],
            "opened_ts": time.time(),
        }
        mk["open"][slug] = pos
        self._log(
            f"⚡ [MOMENTUM] {sym} {slug} {direction} achat @ {entry:.3f} "
            f"({round(shares, 2)} parts) mom={fast_pct:+.4f}%/s d={d} "
            f"-> trail {SWING_TARGET} / stop {SWING_STOP} | {round(secs_left)}s"
        )
        slot = mk.setdefault("slot_trades", {})
        slot[slug] = slot.get(slug, 0) + 1
        return True

    def _try_swing(self, sym, mode, m, p):
        """SWING (paper) : achete le contrat du cote favori par Binance QUAND il
        est encore pas cher (bruit), pour le REVENDRE plus haut avant resolution.
        Profite des oscillations du prix du contrat -> adapte a ETH (bouge peu).
        Direction = signe de l'ecart Binance ; entree si prix <= SWING_MAX_ENTRY."""
        from paper_snipe import outcome_price
        from core.btc_updown import _binance_price, _strike_at, danger_score

        mk = self.state["markets"][sym]
        slug = m.get("slug")
        secs = p["end_ts"] - synced_now()
        if secs < SWING_MIN_SECS or secs > SWING_ENTER_MAX_SECS:
            return
        strike = _strike_at(p["pair"], p["start_ts"], slug=slug)
        price = _binance_price(p["pair"])
        if strike is None or price is None:
            return
        mk["danger"] = danger_score(p["pair"], strike)
        if mk["danger"] > DANGER_MAX:
            self._log(
                f"⚠️ {sym} {slug} swing skip (danger={mk['danger']} > {DANGER_MAX})"
            )
            return
        # CONVICTION MINIMALE (21/07) : le seuil precedent (0.003% du prix) etait si
        # bas que TOUTES les entrees se faisaient a 0.46-0.52, soit du pile-ou-face
        # sur du bruit -> d'ou le 2W/7L. On exige desormais un vrai ecart directionnel.
        if abs(price - strike) < strike * SWING_MIN_EDGE_PCT:
            return  # pas de direction nette : on s'abstient plutot que de jouer a pile ou face
        side = "Up" if price > strike else "Down"
        outcomes = json.loads(m.get("outcomes") or "[]")
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
        try:
            tid_for_side = token_ids[outcomes.index(side)]
        except Exception:
            return
        entry = self._live_price(
            tid_for_side, m, side
        )  # carnet reel, plus outcomePrices fige
        if entry is None or entry <= 0.03 or entry > MAX_ENTRY_PRICE:
            return  # deja tranche (>0.97) : plus d'upside, comme pour BTC reel
        token_id = token_ids[outcomes.index(side)]
        budget = min(HARD_CAP_USD, 5.0)
        shares = max(MIN_ORDER_SIZE_SHARES, budget / entry)
        # DEMANDE STEVEN (21/07) : le swing rejetait TOUT achat > 0.55 -> ETH ne
        # tradait jamais des que le marche etait deja tranche avant que le signal
        # Binance passe le seuil. Desormais on achete quand meme, mais on adapte
        # la strategie : cher (>SWING_MAX_ENTRY) -> pas de vente anticipee, on
        # TIENT jusqu'a la vraie resolution (comme BTC hold, paiement plein 1$/0$
        # au lieu d'une vente a prix de marche degradee par le spread). Pas cher
        # (<=SWING_MAX_ENTRY) -> logique swing habituelle (trailing/stop).
        is_cheap = entry <= SWING_MAX_ENTRY
        if not is_cheap:
            # ACHAT CHER = on TIENDRA jusqu'a resolution, sans stop pour limiter la
            # casse : c'est un pari binaire, exactement comme la strategie de BTC.
            # Il doit donc passer la MEME barre : marge proportionnelle au temps
            # restant (via evaluate), et pas le seuil swing de 0.04% qui n'a de sens
            # que pour une position protegee par un stop.
            # (sans ca : Up achete a 0.805 avec 153s restantes = quasi pile-ou-face
            #  tenu sans filet -> la perte de -5.00$ du 21/07)
            from core.btc_updown import evaluate as _strict_eval

            strict = _strict_eval(m, window_secs=ENTRY_WINDOW_SECS)
            if not strict or strict["side"] != side:
                return
        pos = {
            "symbol": sym,
            "slug": slug,
            "side": side,
            "mode": "paper",
            "token_id": token_id,
            "entry_price": entry,
            "filled_shares": round(shares, 2),
            "cost": round(shares * entry, 2),
            "target": SWING_TARGET,
            "stop": SWING_STOP,
            "pair": p["pair"],
            "start_ts": p["start_ts"],
            "end_ts": p["end_ts"],
            "opened_ts": time.time(),
        }
        if is_cheap:
            pos["strat"] = "swing"  # geree par _manage_swings (trailing/target/stop)
            self._log(
                f"🎯 [SWING] {sym} {slug} {side} achat @ {entry:.3f} ({round(shares, 2)} parts) "
                f"-> objectif {SWING_TARGET} / stop {SWING_STOP} | {round(secs)}s"
            )
        else:
            # pas de "strat" -> _resolve_market s'en charge normalement (vraie
            # resolution Polymarket/paper, paiement plein comme un hold BTC)
            self._log(
                f"🎯 [SWING->HOLD] {sym} {slug} {side} achat cher @ {entry:.3f} "
                f"({round(shares, 2)} parts) -> tenu jusqu'a resolution | {round(secs)}s"
            )
        mk["open"][slug] = pos

    def _manage_swings(self, sym):
        """Sortie des positions swing : revend des que le prix atteint l'objectif
        ou tombe sous le stop, ou juste avant la fin (au prix courant)."""
        from core.btc_updown import find_active_markets, parse_updown_market
        from paper_snipe import outcome_price

        mk = self.state["markets"][sym]
        swings = {
            s: pos for s, pos in mk["open"].items() if pos.get("strat") == "swing"
        }
        if not swings:
            return
        # retrouve les marches actifs pour lire le prix courant du contrat
        active = {}
        for m in find_active_markets():
            pp = parse_updown_market(m)
            if pp and pp["symbol"] == sym:
                active[m.get("slug")] = m
        now = synced_now()
        for slug, pos in list(swings.items()):
            m = active.get(slug)
            # prix de sortie lu sur le CARNET REEL (cf. _live_price) : outcomePrices
            # restait fige et faisait sortir a prix d'entree (pnl 0.000) une fois sur deux.
            cur = self._live_price(pos.get("token_id"), m, pos["side"])
            secs = pos["end_ts"] - now
            reason = None
            if cur is not None:
                # TRAILING take-profit (demande Steven 21/07) : une fois l'objectif
                # de base atteint, on NE VEND PLUS a prix fixe -> on suit le prix et
                # ne sort que s'il redescend de TRAIL_GIVEBACK depuis son plus haut,
                # pour laisser courir les gros gagnants (ex: le +5,05$ initial aurait
                # pu aller bien plus loin avant de vendre a 0,995 tot).
                pos["peak"] = max(pos.get("peak", pos["entry_price"]), cur)
                armed = pos["peak"] >= pos["target"]
                if armed and cur <= pos["peak"] - SWING_TRAIL_GIVEBACK:
                    reason = f"trailing depuis pic {pos['peak']:.3f}"
                elif cur <= pos["stop"]:
                    reason = f"stop {pos['stop']}"
            if reason is None and secs <= SWING_MIN_SECS:
                reason = "fin de fenetre"  # on revend au prix courant avant reso
            if reason is None:
                continue
            exit_price = cur if cur is not None else pos["entry_price"]
            pnl = round(pos["filled_shares"] * (exit_price - pos["entry_price"]), 3)
            win = pnl > 0
            pos.update(
                win=win,
                pnl=pnl,
                exit_price=round(exit_price, 3),
                resolved_by=f"swing:{reason}",
            )
            mk["paper_balance"] = round(mk["paper_balance"] + pnl, 3)
            mk["consec_losses"] = 0 if win else mk["consec_losses"] + 1
            mk["trades"].append(pos)
            del mk["open"][slug]
            icon = "✅ WIN " if win else "❌ LOSS"
            self._log(
                f"{icon} [SWING] {sym} {slug} {pos['side']} {pos['entry_price']:.3f}->{exit_price:.3f} "
                f"pnl={pnl:+.3f}$ ({reason}) | pertes_consec={mk['consec_losses']}"
            )
            # BUG CORRIGE (21/07) : le stop n'etait verifie que dans _resolve_market,
            # que les positions swing contournent -> ETH continuait a trader au-dela
            # de STOP_CONSEC_LOSSES sans jamais s'arreter.
            if mk["consec_losses"] >= STOP_CONSEC_LOSSES:
                mk["stopped"] = True
                mk["stop_reason"] = f"{STOP_CONSEC_LOSSES} pertes consecutives (swing)"
                self._log(f"🛑 {sym} ARRET : {mk['stop_reason']}")

    def _resolve_market(self, sym, mode):
        mk = self.state["markets"][sym]
        now = time.time()
        still = {}
        pending_bs = {}  # jambes both-side resolues, a COMBINER en 1 trade net/slug
        for slug, pos in mk["open"].items():
            if pos.get("strat") == "swing":
                still[slug] = pos  # gere par _manage_swings, pas de reso binaire
                continue
            if now < pos["end_ts"] + SETTLE_DELAY:
                still[slug] = pos
                continue
            if pos["mode"] == "real":
                # pos["slug"] (pas la cle de boucle) : les positions BOTH-SIDE sont
                # stockees sous une cle composee "slug|cote" pour permettre 2 jambes
                # simultanees -> il faut le VRAI slug pour interroger la resolution.
                out = self._live.settled_outcome(pos["slug"], pos["token_id"])
                if not out["resolved"]:
                    # BUG FIX (25/07) : distinguer "API a renvoye une liste
                    # vide" (rate-limit, timeout) vs "liste non-vide mais notre
                    # position manque" (redeemee = gagnee). Avant : les 2 cas
                    # declenchaient won=True -> les 2 jambes d'une paire ARB
                    # etaient creditees gagnantes = double-credit (~14$/session).
                    api_empty = out.get("api_empty", True)
                    if (
                        out.get("found") is False
                        and not api_empty
                        and now > pos["end_ts"] + 180
                    ):
                        # Liste NON-VIDE + position absente = redeemee = gagnee
                        won = True
                    elif (
                        out.get("found") is False
                        and api_empty
                        and now > pos["end_ts"] + 600
                    ):
                        # Liste VIDE + >10min apres fin : API ne repond pas,
                        # mais c'est trop vieux pour rester en attente -> marquer
                        # comme perdu pour degager le capital (prudent)
                        won = False
                    else:
                        still[slug] = pos
                        continue
                else:
                    won = out["won"]
                # P&L reel : gain = parts*1 - cout si gagne, sinon -cout, sur le
                # RESTANT (filled_shares peut avoir ete reduit par une prise de
                # profit par palier -> cout recalcule dynamiquement, pas fige),
                # PLUS le gain deja realise par les paliers eventuels.
                remaining_cost = round(pos["filled_shares"] * pos["entry_price"], 3)
                pnl = round(
                    (pos["filled_shares"] - remaining_cost) if won else -remaining_cost,
                    3,
                )
                pnl = round(pnl + pos.get("realized_pnl", 0.0), 3)
                pos.update(
                    win=won,
                    pnl=pnl,
                    resolved_by="polymarket",
                    cur_value=out["cur_value"],
                )
                if won:
                    try:
                        with (
                            self._order_lock
                        ):  # meme verrou que les ordres (client CLOB non concurrent)
                            n = self._live.redeem_resolved()
                        if n:
                            self._log(f"💰 {sym} {n} gain(s) reclame(s)")
                    except Exception:
                        pass
            else:  # paper : resolution via prix Polymarket public (a defaut Binance)
                won = self._paper_resolve(pos)
                remaining_cost = round(pos["filled_shares"] * pos["entry_price"], 3)
                pnl = round(
                    (pos["filled_shares"] - remaining_cost) if won else -remaining_cost,
                    3,
                )
                pnl = round(pnl + pos.get("realized_pnl", 0.0), 3)
                pos.update(win=won, pnl=pnl, resolved_by="paper")
                mk["paper_balance"] = round(mk["paper_balance"] + pnl, 3)

            # BOTH-SIDE : on NE compte PAS chaque jambe comme un trade separe
            # (Steven 22/07 : "les loss d'arb ne doivent pas compter comme perte").
            # On accumule les 2 jambes du slug et on les COMBINE apres la boucle en
            # UN SEUL trade au P&L NET -> le dash montre 1 arb gagnant, pas 1 win +
            # 1 loss. Le solde paper est deja mis a jour par jambe (somme = net).
            if pos.get("strat") == "bothside":
                pending_bs.setdefault(pos["slug"], []).append(pos)
                continue
            # UNDERDOG et ORPHAN exemptes du compteur de pertes consecutives
            # (underdog perd par design ; orphan = accident d'execution, pas un
            # signal rate -> ne doit pas declencher le stop du marche).
            if pos.get("strat") not in ("underdog", "orphan"):
                mk["consec_losses"] = 0 if won else mk["consec_losses"] + 1
            # V3.1 AXE 7 : loss tag sur les trades individuels
            if not won:
                loss_tag = self._classify_loss(pos, pos.get("resolved_by", ""))
                pos["loss_tag"] = loss_tag
            # V3.1 AXE 2 : reset abort counter sur un win
            if won:
                self._reset_abort_counter(sym, mk)
            mk["trades"].append(pos)
            self._record_trade_pnl(sym, pnl)
            if pos.get(
                "mm_handoff"
            ):  # position transferee depuis le MM (Steven 23/07),
                self.state["mm"]["daily_pnl"] = round(
                    self.state["mm"]["daily_pnl"] + pnl, 4
                )
                self._log(
                    f"🎯 [MM] pnl {pnl:+.3f}$ credite au P&L du jour MM (resolue via settlement)"
                )
            icon = "✅ WIN " if won else "❌ LOSS"
            lt_str = f" tag={pos.get('loss_tag')}" if pos.get("loss_tag") else ""
            self._log(
                f"{icon} [{pos['mode'].upper()}] {sym} {pos['slug']} pnl={pnl:+.3f}$ "
                f"| pertes_consec={mk['consec_losses']}{lt_str}"
            )
            # V3.1 AXE 8 : log sortie structure pour resolution normale
            _duree = now - pos.get("opened_ts", pos.get("start_ts", now))
            _exit_px = 1.0 if won else 0.0
            self._log_trade_exit(
                sym,
                pos.get("slug", ""),
                pos.get("side", "?"),
                pos.get("resolved_by", "resolution"),
                pos.get("entry_price", 0),
                _exit_px,
                pnl,
                0.0,
                0.0,
                pnl,
                _duree,
                "resolved",
                "resolved",
                loss_tag=pos.get("loss_tag"),
            )
            self._write_trade_jsonl(
                {
                    "ts": now,
                    "sym": sym,
                    "slug": pos.get("slug"),
                    "side": pos.get("side"),
                    "mode": pos["mode"],
                    "strat": pos.get("strat"),
                    "reason": pos.get("resolved_by"),
                    "entry": pos.get("entry_price"),
                    "exit": _exit_px,
                    "pnl": pnl,
                    "win": won,
                    "duree_s": _duree,
                    "loss_tag": pos.get("loss_tag"),
                }
            )
            if mk["consec_losses"] >= STOP_CONSEC_LOSSES:
                mk["stopped"] = True
                mk["stop_reason"] = f"{STOP_CONSEC_LOSSES} pertes consecutives"
                self._log(f"🛑 {sym} ARRET : {mk['stop_reason']}")

        # ── COMBINE les paires both-side en 1 trade net chacune ──
        for bslug, legs in pending_bs.items():
            # SAFETY CHECK (25/07) : dans un marche binaire, exactement UNE jambe
            # peut gagner. Si les 2 sont won=True, c'est un double-credit bug
            # (l'ancien found=False -> won=True). Corriger en ne gardant que la
            # jambe la plus probablement gagnante (celle avec le plus de shares
            # ou le prix d'entree le plus bas = le meilleur deal).
            winners = [l for l in legs if l.get("win")]
            if len(winners) > 1 and len(legs) == 2:
                # Double-credit detecte -> garder uniquement la jambe la plus
                # logiquement gagnante (prix le plus bas = meilleure valeur)
                best = min(winners, key=lambda l: l.get("entry_price", 0.5) or 0.5)
                losers = [l for l in legs if l is not best]
                for loser in losers:
                    loser["win"] = False
                    loser["pnl"] = -abs(loser.get("cost", 0.0))
                self._log(
                    f"⚠️ [ARB][FIX] {sym} {bslug} DOUBLE-CREDIT CORRIGE : "
                    f"{len(winners)} jambes gagnantes -> garder {best.get('side')} "
                    f"(prix={best.get('entry_price', '?')})"
                )
            net = round(sum(l.get("pnl", 0.0) for l in legs), 3)
            win = net > 0
            merged = dict(legs[0])
            merged.update(
                side="ARB",
                pnl=net,
                win=win,
                filled_shares=round(sum(l.get("filled_shares", 0.0) for l in legs), 2),
                cost=round(sum(l.get("cost", 0.0) for l in legs), 2),
                entry_price=None,
                resolved_by="arb_pair",
                legs=[l.get("side") for l in legs],
            )
            # V3.1 AXE 7 : loss tag sur les ARB
            if not win:
                loss_tag = self._classify_loss(merged, "ARB resolution")
                merged["loss_tag"] = loss_tag
            mk["trades"].append(merged)
            self._record_trade_pnl(sym, net)
            # V3.1 AXE 2 : reset abort counter sur un win
            if win:
                self._reset_abort_counter(sym, mk)
            icon = "✅ WIN " if win else "❌ LOSS"
            lt_str = f" tag={merged.get('loss_tag')}" if merged.get("loss_tag") else ""
            self._log(
                f"{icon} [ARB] {sym} {bslug} net={net:+.3f}$ "
                f"({len(legs)} jambes combinees){lt_str}"
            )
            # V3.1 AXE 8 : log sortie structure pour ARB combine
            _duree = now - merged.get("opened_ts", merged.get("start_ts", now))
            self._log_trade_exit(
                sym,
                bslug,
                "ARB",
                "arb_resolution",
                merged.get("cost", 0) / max(1, merged.get("filled_shares", 1)),
                1.0 if win else 0.0,
                net,
                0.0,
                0.0,
                net,
                _duree,
                "arb_won" if win else "arb_lost",
                "arb_won" if win else "arb_lost",
                loss_tag=merged.get("loss_tag"),
            )
            self._write_trade_jsonl(
                {
                    "ts": now,
                    "sym": sym,
                    "slug": bslug,
                    "side": "ARB",
                    "mode": merged.get("mode", "real"),
                    "strat": "bothside",
                    "reason": "arb_resolution",
                    "entry": None,
                    "exit": 1.0 if win else 0.0,
                    "pnl": net,
                    "win": win,
                    "duree_s": _duree,
                    "loss_tag": merged.get("loss_tag"),
                    "legs": merged.get("legs"),
                }
            )
        mk["open"] = still

    # ── ULTRAPOLY : arb sur TOUT Polymarket (Steven 22/07, paper-only) ──
    @staticmethod
    def _clob_best_ask(token_id):
        """Meilleur ask d'un token via l'API CLOB publique (sans auth, sans
        dependre de PolyLive). None si carnet vide/injoignable."""
        import requests as _rq

        try:
            r = _rq.get(
                "https://clob.polymarket.com/book",
                params={"token_id": token_id},
                timeout=5,
            )
            asks = r.json().get("asks") or []
            prices = [float(a["price"]) for a in asks]
            return min(prices) if prices else None
        except Exception:
            return None

    def _ultra_safe(self):
        """Wrapper du cycle ULTRAPOLY (resolution puis scan), erreurs isolees."""
        try:
            self._ultrapoly_resolve()
            self._ultrapoly_scan()
        except Exception as e:
            self._log(f"💥 [ULTRA] erreur cycle: {e}")

    def _arb_stream_callback(self, slug, outcomes, tids, a0, a1, combined, meta):
        """ARB STREAM CALLBACK (Steven 26/07) : appele par le WS feed quand
        combined <= threshold en temps reel. Ouvre une paire parts-egales REEL
        ou PAPER selon la configuration. Appele HORS du thread trader principal,
        doit etre thread-safe. V3.1 : sizing asymetrique par tier."""
        mk = self.state["markets"]["POLY"]
        now = time.time()

        # Verifications rapides
        open_slugs = {p["slug"] for p in mk["open"].values()}
        if slug in open_slugs or slug in self._arb_stream_opened:
            return
        if len(open_slugs) >= ULTRAPOLY_MAX_OPEN_PAIRS:
            return
        if now - self._ultra_cooldown.get(slug, 0) < ULTRAPOLY_COOLDOWN_S:
            return

        self._arb_stream_opened.add(slug)

        # ── TIER SIZING V3.1 ──
        edge = max(0.0, 1.0 - combined)
        # ULTRAPOLY : pas de Binance, pas de secs_left fiable -> estimation conservatrice
        tier = self._detect_setup_tier(slug, edge, 300, False, True)
        budget_usd = self._tier_sizing(tier, combined)
        avg_price = combined / 2.0
        tier_shares = (
            max(ULTRAPOLY_SHARES, round(budget_usd / avg_price, 2))
            if avg_price > 0.05
            else ULTRAPOLY_SHARES
        )

        # TENTATIVE REEL d'abord
        if self.state.get("ultrapoly_real") and combined <= ULTRAPOLY_REAL_MAX_COMBINED:
            real_open = sum(1 for pp in mk["open"].values() if pp.get("mode") == "real")
            if real_open < ULTRAPOLY_REAL_MAX_OPEN_PAIRS:
                if self._ultrapoly_open_real(
                    mk, slug, outcomes, tids, a0, a1, combined, meta, now
                ):
                    self._ultra_cooldown[slug] = now
                    self._log(
                        f"⚡ [ARB-STREAM][REEL] {slug[:40]} comb={combined:.3f} "
                        f"({outcomes[0]}@{a0:.3f}+{outcomes[1]}@{a1:.3f}) "
                        f"[{tier} {tier_shares:.1f}p ${budget_usd:.2f}]"
                    )
                    return

        # FALLBACK PAPER
        self._ultra_cooldown[slug] = now
        for side, tid, px in (
            (outcomes[0], tids[0], a0),
            (outcomes[1], tids[1], a1),
        ):
            mk["open"][f"{slug}|{side}"] = {
                "symbol": "POLY",
                "slug": slug,
                "side": side,
                "mode": "paper",
                "strat": "bothside",
                "tier": tier,
                "token_id": tid,
                "entry_price": px,
                "filled_shares": tier_shares,
                "cost": round(tier_shares * px, 2),
                "start_ts": now,
                "pair": None,
                "end_ts": now + 90 * 86400,
                "opened_ts": now,
                "buffer": 0.0,
                "question": (meta.get("question") or "")[:80],
            }
        self._log(
            f"⚡ [ARB-STREAM][PAPER] {slug[:40]} {outcomes[0]}@{a0:.3f}+"
            f"{outcomes[1]}@{a1:.3f} comb={combined:.3f} [{tier} {tier_shares:.1f}p] "
            f"-> +{tier_shares * (1 - combined):.2f}$ garanti"
        )

    def _ultrapoly_open_real(self, mk, slug, outcomes, tids, a0, a1, comb, m, now):
        """ARB REEL sur un marche Polymarket GENERIQUE (Steven 23/07) : meme
        philosophie que l'arb crypto (preflight parallele avant tout ordre,
        cap slippage proportionnel a l'edge, jamais d'achat suivi d'une vente
        aveugle) mais SANS le signal Binance (inapplicable hors crypto) — si
        une jambe echoue, l'autre est revendue IMMEDIATEMENT (pas d'attente
        de signal, ces marches ne sont pas sur une horloge de 5 minutes donc
        moins urgent, mais on ne garde pas non plus une jambe nue sans aucun
        moyen de juger si elle est gagnante ou perdante).
        V3.1 : sizing asymetrique par tier (fragile/normal/premium)."""
        # ── TIER SIZING V3.1 ──
        edge = max(0.0, 1.0 - comb)
        tier = self._detect_setup_tier(slug, edge, 300, False, True)
        budget_usd = self._tier_sizing(tier, comb)
        avg_price = comb / 2.0
        tier_shares = (
            max(ULTRAPOLY_SHARES, round(budget_usd / avg_price, 2))
            if avg_price > 0.05
            else ULTRAPOLY_SHARES
        )

        need = round(tier_shares * comb + 0.2, 2)
        cash, _ = self._read_cash(max_age=3)
        if cash is None or cash < need:
            self._tlog(
                "ultra_nofund",
                f"💸 [ULTRA][REEL] {slug[:30]} comb={comb:.3f} "
                f"mais solde insuffisant ({cash}$ < ~{need}$) -> saute",
            )
            return False
        slip_total = min(
            REAL_SLIPPAGE_MAX * 2,
            max(REAL_SLIPPAGE_MIN * 2, edge * REAL_SLIPPAGE_EDGE_FRACTION),
        )
        slip_each = round(slip_total / 2, 3)
        cap0 = min(0.97, round(a0 + slip_each, 2))
        cap1 = min(0.97, round(a1 + slip_each, 2))
        min_depth = round(tier_shares * ULTRAPOLY_REAL_MIN_DEPTH_RATIO, 2)
        pf_futs = {
            0: self._pool.submit(self._live.preflight_leg, tids[0], cap0, min_depth),
            1: self._pool.submit(self._live.preflight_leg, tids[1], cap1, min_depth),
        }
        pf = {i: f.result() for i, f in pf_futs.items()}
        if not (pf[0]["ok"] and pf[1]["ok"]):
            for i in (0, 1):
                if not pf[i]["ok"]:
                    self._log(
                        f"🚫 [ULTRA][REEL] {slug[:30]} {outcomes[i]} PREFLIGHT echec : "
                        f"{pf[i]['error']} -> abandon des 2 jambes"
                    )
            return False
        with self._order_lock:
            post_futs = {
                0: self._pool.submit(
                    self._live.post_market_order,
                    tids[0],
                    cap0,
                    min(budget_usd, round(tier_shares * a0, 2)),
                ),
                1: self._pool.submit(
                    self._live.post_market_order,
                    tids[1],
                    cap1,
                    min(budget_usd, round(tier_shares * a1, 2)),
                ),
            }
            h0, h1 = post_futs[0].result(), post_futs[1].result()
        for i, h in ((0, h0), (1, h1)):
            if not h.get("posted"):
                self._log(
                    f"🚫 [ULTRA][REEL] {slug[:30]} {outcomes[i]} ordre NON POSTE : {h.get('error', '?')}"
                )
        futs = {}
        fills = {0: 0.0, 1: 0.0}
        for i, tid, h in ((0, tids[0], h0), (1, tids[1], h1)):
            if h.get("posted"):
                futs[i] = self._pool.submit(
                    self._live.confirm_fill, tid, h["before"], 8.0
                )
        for i, fut in futs.items():
            try:
                fills[i] = fut.result()
            except Exception:
                fills[i] = 0.0
        M = min(fills[0], fills[1])
        min_val = min(fills[0] * a0, fills[1] * a1)
        if min_val < budget_usd * 0.5:
            for i, tid, px in ((0, tids[0], a0), (1, tids[1], a1)):
                if fills[i] >= MIN_ORDER_SIZE_SHARES:
                    self._sell_orphan(
                        tid, fills[i], f" [ULTRA] {slug[:30]} {outcomes[i]}"
                    )
            self._log(
                f"↩️ [ULTRA][REEL] {slug[:30]} pair KO [{tier}] (f0={fills[0]} f1={fills[1]})"
            )
            return False
        for i, tid, px in ((0, tids[0], a0), (1, tids[1], a1)):
            excess = round(fills[i] - M, 2)
            if excess >= MIN_ORDER_SIZE_SHARES:
                self._sell_orphan(
                    tid, excess, f" [ULTRA] {slug[:30]} {outcomes[i]} exces"
                )
        for i, tid, px in ((0, tids[0], a0), (1, tids[1], a1)):
            mk["open"][f"{slug}|{outcomes[i]}"] = {
                "symbol": "POLY",
                "slug": slug,
                "side": outcomes[i],
                "mode": "real",
                "strat": "bothside",
                "tier": tier,
                "token_id": tid,
                "entry_price": px,
                "filled_shares": round(M, 2),
                "cost": round(M * px, 2),
                "start_ts": now,
                "pair": None,
                "end_ts": now + 90 * 86400,
                "opened_ts": now,
                "buffer": 0.0,
                "question": (m.get("question") or "")[:80],
            }
        self._log(
            f"✅ [ULTRA][REEL] {slug[:30]} PAIRE [{tier}] {round(M, 2)} parts/cote "
            f"comb={comb:.3f} -> +{M * (1 - comb):.2f}$ garanti "
            f"({(m.get('question') or '')[:50]})"
        )
        return True

    def _ultrapoly_scan(self):
        """Scanne le top Polymarket par volume : marches binaires dont la SOMME
        DES ASKS <= ULTRAPOLY_COMB_MAX -> ouvre une paire parts-egales (paper).
        Les up/down crypto sont exclus (deja geres par les 5 marches)."""
        import requests as _rq

        mk = self.state["markets"]["POLY"]
        now = time.time()
        open_slugs = {p["slug"] for p in mk["open"].values()}
        if len(open_slugs) >= ULTRAPOLY_MAX_OPEN_PAIRS:
            return
        try:
            r = _rq.get(
                "https://gamma-api.polymarket.com/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": 100,
                    "order": "volume24hr",
                    "ascending": "false",
                },
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            data = r.json()
        except Exception:
            return
        if not isinstance(data, list):
            return
        checked = 0
        for m in data:
            if (
                checked >= ULTRAPOLY_TOP_N
                or len(open_slugs) >= ULTRAPOLY_MAX_OPEN_PAIRS
            ):
                break
            slug = m.get("slug") or ""
            if "updown" in slug or slug in open_slugs:
                continue
            if now - self._ultra_cooldown.get(slug, 0) < ULTRAPOLY_COOLDOWN_S:
                continue
            try:
                outcomes = json.loads(m.get("outcomes") or "[]")
                tids = json.loads(m.get("clobTokenIds") or "[]")
            except Exception:
                continue
            if len(outcomes) != 2 or len(tids) != 2:
                continue
            if float(m.get("volume24hr") or 0) < ULTRAPOLY_MIN_VOL24:
                continue
            checked += 1
            # ARB STREAM : register pour suivi temps reel (callback push)
            # Pas besoin de REST _clob_best_ask ici : le WS stream detecte
            # le combined en temps reel via _arb_stream_callback
            self._ws.register_market(
                slug,
                outcomes,
                tids,
                question=(m.get("question") or "")[:80],
                volume24hr=float(m.get("volume24hr") or 0),
            )

    def _ultrapoly_resolve(self):
        """Resout les paires POLY via gamma (outcomePrices extremes). Combine la
        paire en UN trade net. Throttle 120s par slug (marches long-terme)."""
        import requests as _rq

        mk = self.state["markets"]["POLY"]
        if not mk["open"]:
            return
        now = time.time()
        by_slug = {}
        for k, pos in mk["open"].items():
            by_slug.setdefault(pos["slug"], []).append((k, pos))
        for slug, legs in by_slug.items():
            if now - legs[0][1].get("last_reso_check", 0) < 120:
                continue
            for _, pos in legs:
                pos["last_reso_check"] = now
            try:
                r = _rq.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"slug": slug},
                    timeout=8,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                d = r.json()
                mdata = (d[0] if isinstance(d, list) and d else d) or {}
                prices = json.loads(mdata.get("outcomePrices") or "[]")
                outcomes = json.loads(mdata.get("outcomes") or "[]")
            except Exception:
                continue
            if not prices or not outcomes or max(float(p) for p in prices) < 0.98:
                continue  # pas encore resolu
            winner = outcomes[max(range(len(prices)), key=lambda i: float(prices[i]))]
            net = 0.0
            total_cost = 0.0
            for key, pos in legs:
                won = pos["side"] == winner
                pnl = round(
                    (pos["filled_shares"] - pos["cost"]) if won else -pos["cost"], 3
                )
                net += pnl
                total_cost += pos.get("cost", 0.0)
                del mk["open"][key]
            self._arb_stream_opened.discard(slug)
            self._ws.unregister_market(slug)
            net = round(net, 3)
            mk["paper_balance"] = round(mk["paper_balance"] + net, 3)
            merged = dict(legs[0][1])
            merged.update(
                side="ARB",
                pnl=net,
                win=net > 0,
                cost=round(total_cost, 2),
                resolved_by="ultrapoly",
                legs=[p["side"] for _, p in legs],
            )
            mk["trades"].append(merged)
            icon = "✅ WIN " if net > 0 else "❌ LOSS"
            self._log(f"{icon} [ULTRA] {slug[:40]} net={net:+.3f}$ (gagnant={winner})")

    def _paper_resolve(self, pos):
        """Resolution paper : prix Polymarket public a defaut, sinon Binance."""
        import requests

        try:
            r = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"slug": pos["slug"]},
                timeout=8,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            d = r.json()
            m = (d[0] if isinstance(d, list) and d else d) or {}
            prices = json.loads(m.get("outcomePrices") or "[]")
            outcomes = json.loads(m.get("outcomes") or "[]")
            if prices and outcomes and pos["side"] in outcomes:
                p = float(prices[outcomes.index(pos["side"])])
                if p >= 0.98:
                    return True
                if p <= 0.02:
                    return False
        except Exception:
            pass
        # fallback Binance (imparfait mais c'est du paper)
        # BUG CORRIGE (21/07) : pos.get("pair", ...) retombait TOUJOURS sur le
        # defaut car "pair" n'etait jamais stocke a l'ouverture -> tout marche
        # non-BTC (SOL/XRP/DOGE) etait resolu contre le prix d'ETHUSDT par erreur
        # des que la resolution Gamma primaire echouait. C'est probablement ce qui
        # a fait perdre le trade SOL signale par Steven. On stocke desormais la
        # vraie paire a l'ouverture, et on refuse de trancher si elle manque
        # (plutot que de deviner faux).
        from core.btc_updown import _strike_at

        pair = pos.get("pair")
        if not pair:
            return False  # ne jamais deviner une paire -> pas de faux resultat
        s = _strike_at(pair, pos["start_ts"], slug=pos.get("slug"))
        e = _strike_at(pair, pos["end_ts"], slug=pos.get("slug"))
        if s is None or e is None:
            return False
        actual = "Up" if e > s else "Down"
        return actual == pos["side"]

    # ── projections (indicatives) ──
    @staticmethod
    def _projection(trades):
        """Extrapolation LINEAIRE du P&L a 1h / 7j / 1 mois a partir du rythme
        REELLEMENT observe (P&L total / duree d'activite mesuree entre le premier
        et le dernier trade).

        C'est une INDICATION, pas une prevision : elle suppose que le rythme de
        trades ET le taux de reussite restent identiques. Sur un petit echantillon
        a 100% de reussite, c'est mecaniquement trop optimiste (la premiere perte
        fera chuter la courbe). `confiance` reflete cette incertitude.
        """
        done = [t for t in trades if t.get("pnl") is not None and t.get("opened_ts")]
        if len(done) < 2:
            return {"ready": False, "trades": len(done)}
        ts = sorted(t["opened_ts"] for t in done)
        span_h = (ts[-1] - ts[0]) / 3600.0
        if span_h < 0.02:  # < ~1 min de recul : rythme non mesurable
            return {"ready": False, "trades": len(done)}
        pnl = sum(t.get("pnl", 0.0) for t in done)
        wins = sum(1 for t in done if t.get("win"))
        wr = wins / len(done)
        # DECOMPOSITION (demande Steven 22/07) : la projection repose sur le NOMBRE
        # DE TRADES EFFECTIFS PAR HEURE, pas sur une simple division du P&L par le
        # temps. On mesure la cadence reelle observee, plafonnee au maximum PHYSIQUE
        # (un marche = une fenetre de 5 min = 12 trades/h max ; on ne peut pas en
        # faire plus meme en rafale). Puis P&L/h = cadence x gain_moyen_par_trade.
        MAX_TRADES_PER_H = 12.0
        raw_tph = len(done) / span_h
        trades_per_h = min(raw_tph, MAX_TRADES_PER_H)  # honnete : jamais > 12/h
        avg_pnl = pnl / len(done)
        per_h = trades_per_h * avg_pnl
        # confiance : croit avec le nombre de trades ET la duree observee.
        # Volontairement severe : 100% de reussite sur 15 trades reste fragile.
        if len(done) >= 100 and span_h >= 12:
            conf = "bonne"
        elif len(done) >= 40 and span_h >= 4:
            conf = "moyenne"
        elif len(done) >= 15:
            conf = "faible"
        else:
            conf = "tres faible"
        return {
            "ready": True,
            "trades": len(done),
            "span_h": round(span_h, 2),
            "per_hour": round(per_h, 2),
            "win_rate": round(wr * 100, 1),
            "trades_per_h": round(trades_per_h, 1),
            "avg_pnl": round(avg_pnl, 3),
            "capped": raw_tph > MAX_TRADES_PER_H,
            "h1": round(per_h, 2),
            "d7": round(per_h * 24 * 7, 2),
            "m1": round(per_h * 24 * 30, 2),
            "confiance": conf,
        }

    # ── etat pour l'UI ──
    def snapshot(self):
        cash, _ = self._read_cash() if self._live or True else (None, "")
        out = {
            "running": self.is_running(),
            "modes": self.state["modes"],
            "cash_usdc": self._last_good_cash,
            "floor": self.floor(),
            "arb_budget": self.arb_budget(),
            "stop_consec": STOP_CONSEC_LOSSES,
            "markets": {},
            "diag": self._diag,
        }
        for sym in SYMBOLS:
            mk = self.state["markets"][sym]
            trades = mk["trades"]
            wins = [t for t in trades if t.get("win")]
            # SEPARATION STRICTE paper/reel (Steven 23/07 : "plus modifier
            # resulta paper au resulta reel", "on veut des prevision paper &
            # reel") : les deux etaient melanges dans pnl_total/projection,
            # ce qui pouvait donner une fausse impression de performance reelle
            # (un gain paper genereux masquant un reel neutre/negatif).
            real_trades = [t for t in trades if t.get("mode") == "real"]
            paper_trades = [t for t in trades if t.get("mode") != "real"]
            real_wins = [t for t in real_trades if t.get("win")]
            paper_wins = [t for t in paper_trades if t.get("win")]
            rl_sym = self._risk_limits.get(sym, {})
            out["markets"][sym] = {
                "mode": self.state["modes"][sym],
                "strategy": self.state.get("strategies", {}).get(sym, "hold"),
                "trades_done": len(trades),
                "wins": len(wins),
                "losses": len(trades) - len(wins),
                "consec_losses": mk["consec_losses"],
                "consec_wins": rl_sym.get("consec_wins", 0),
                "streak_losses": rl_sym.get("consec_losses", 0),
                "danger": mk.get("danger", 0),
                "opportunity": self._opportunity_on(sym),
                "risk_free": self._risk_free_on(sym),
                "stopped": mk["stopped"],
                "stop_reason": mk["stop_reason"],
                "pnl_total_real": round(sum(t.get("pnl", 0.0) for t in real_trades), 3),
                "pnl_total_paper": round(
                    sum(t.get("pnl", 0.0) for t in paper_trades), 3
                ),
                "trades_done_real": len(real_trades),
                "trades_done_paper": len(paper_trades),
                "wins_real": len(real_wins),
                "wins_paper": len(paper_wins),
                "paper_balance": mk.get("paper_balance"),
                "open": list(mk["open"].values()),
                "trades": trades[-40:],
                "projection_real": self._projection(real_trades),
                "projection_paper": self._projection(paper_trades),
                "price_log": {
                    slug: pts[-60:]
                    for slug, pts in mk.get("market_price_log", {}).items()
                },
            }
        # ── ULTRAPOLY : flag + resume du bucket POLY (hors cartes crypto) ──
        out["ultrapoly"] = bool(self.state.get("ultrapoly"))
        out["ultrapoly_real"] = bool(self.state.get("ultrapoly_real"))
        # ── REVERSAL STATS V3.2 (Steven 27/07) ──
        rs = self.state.get("reversal_stats", {})
        out["reversal_stats"] = {}
        for tier_name, bucket in rs.items():
            rev_count = sum(1 for r in bucket if r.get("reversal"))
            out["reversal_stats"][tier_name] = {
                "total": len(bucket),
                "reversals": rev_count,
                "ratio": round(rev_count / max(len(bucket), 1), 3),
            }
        # ── DELTA-NEUTRE + sante du flux WebSocket ──
        dn = self.state.get("dn", {})
        out["dn"] = {
            "enabled": bool(self.state.get("dn_enabled")),
            "open_pairs": len({p["slug"] for p in dn.get("pairs", {}).values()})
            if dn.get("pairs")
            else 0,
            "trades": len(dn.get("trades", [])),
            "pnl": round(dn.get("pnl", 0.0), 3),
            "quotes": {
                sym: {
                    "slug": q.get("slug"),
                    "up_bid": q.get("Up", {}).get("bid"),
                    "dn_bid": q.get("Down", {}).get("bid"),
                }
                for sym, q in self._dn_quotes.items()
            },
        }
        try:
            out["ws"] = self._ws.stats()
        except Exception:
            out["ws"] = None
        pm = self.state["markets"].get("POLY")
        if pm:
            ptr = pm["trades"]
            ptr_real = [t for t in ptr if t.get("mode") == "real"]
            ptr_paper = [t for t in ptr if t.get("mode") != "real"]
            out["poly"] = {
                "open_pairs": len({p["slug"] for p in pm["open"].values()}),
                "open_pairs_real": len(
                    {p["slug"] for p in pm["open"].values() if p.get("mode") == "real"}
                ),
                "open": list(pm["open"].values()),
                "trades_done_real": len(ptr_real),
                "trades_done_paper": len(ptr_paper),
                "wins_real": sum(1 for t in ptr_real if t.get("win")),
                "wins_paper": sum(1 for t in ptr_paper if t.get("win")),
                "pnl_total_real": round(sum(t.get("pnl", 0.0) for t in ptr_real), 3),
                "pnl_total_paper": round(sum(t.get("pnl", 0.0) for t in ptr_paper), 3),
                "paper_balance": pm.get("paper_balance"),
                "trades": ptr[-20:],
            }
        # ── MARKET MAKER : etat pour le dashboard ──
        mmst = self.state.get("mm", {})
        out["mm"] = {
            "enabled": bool(mmst.get("enabled")),
            "killed": bool(mmst.get("killed")),
            "kill_reason": mmst.get("kill_reason"),
            "daily_pnl": round(mmst.get("daily_pnl", 0.0), 3),
            "consec_adverse": mmst.get("consec_adverse", 0),
            "inventory": mmst.get("inventory", {}),
            "quotes": {
                sym: {
                    k: v
                    for k, v in q.items()
                    if k not in ("bid_order_id", "ask_order_id")
                }
                for sym, q in mmst.get("quotes", {}).items()
            },
            "regime": mmst.get("regime_log", {}),
            "fills": mmst.get("fills", [])[-30:],
        }
        return out
