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
from real_web import rl_shadow
from real_web.ws_feed import get_feed


ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")
LOG_FILE = ROOT / "data" / "ghost_v3_real.log"
STATE_FILE = ROOT / "data" / "multi_state.json"
# JOURNAL DE MARCHE SEPARE (Steven 11/08, 6 marches en dataset) : garder ces
# releves dans multi_state.json etait tenable a 2 symboles (tampon 8000 =
# ~17 h) mais pas a 6 -- 12 lignes toutes les 30 s saturent le tampon en 5 h,
# et surtout _save() reecrit TOUT l'etat a chaque action de trading : y
# trainer plusieurs Mo de donnees ML ralentirait le chemin critique. Fichier
# a part, en ajout seul (O(1) par ligne), lu a la demande par l'export et
# par l'entrainement.
MARKET_DATA_FILE = ROOT / "data" / "market_snapshots.jsonl"
MARKET_DATA_MAX_MB = 400

# ── garde-fous & sizing (valeurs validees par Steven) ──
FLOOR_USD = 0.0  # Steven 19/08 -- ne jamais engager de capital sous ce plancher (protege le capital,
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
# ── PLANCHER DE VENTE (Steven 05/08, "je peux vendre 25/50/75/100% quand je
# veux") : MIN_ORDER_SIZE_SHARES (5) est un plancher d'ACHAT, pas de vente --
# verifie on-chain sur ce wallet, 8 ventes sous 5 parts sont passees, la plus
# petite a 1.37 part. Avant ce constat, tout palier de TP calcule sous 5 parts
# etait converti en "vend TOUT", ce qui supprimait purement et simplement la
# sortie en paliers sur les positions normales (une position de 6 parts vendait
# 100% des le premier palier au lieu de 25%). On garde juste un plancher
# anti-poussiere, cale sur la plus petite vente reellement observee.
MIN_SELL_SHARES = 1.0
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
# FIX (Steven 02/09, enquete sur FORCE_SELL 100% perdant) : le plafond de
# prix de la 2e jambe utilisait PAIR_COMPLETION_MAX_COMBINED, desactive a
# 99.0 depuis le 19/08 (meme bug deja trouve et corrige ce soir sur
# HEDGE-NEAR -- pas re-applique ici). "jamais surpaye pour forcer la
# paire" etait donc un plafond FICTIF (99.0 - prix jambe1 ~= 98$, aucune
# limite reelle). Vrai plafond restaure : combine final <= 1.03 (perte
# max ~3% si la paire se complete, au lieu d'un plafond inoperant).
FORCE_PAIR_MAX_COMBINED = 1.03
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
# ── PLAFOND D'EXPOSITION PAR MARCHE (Steven 05/08, idee reprise du spec
# ENGINEBTB3 section 10 "max exposure par marche"). HARD_CAP_USD plafonne UN
# ordre, mais RIEN ne plafonnait le CUMUL sur une meme fenetre de 5 min : le
# bot pouvait rentrer 10 fois de suite sur la meme jambe. Mesure on-chain sur
# btc-updown-5m-1785879900 : 10 achats consecutifs, 10.47$ engages, 0 vente,
# prix moyenne a la baisse de 0.41 a 0.29. Ce plafond est un filet STRUCTUREL :
# il coupe ce type de derive quelle qu'en soit la cause (bug de garde, boucle
# de re-entree, strategie qui s'emballe), sans dependre d'un diagnostic exact.
# Une paire d'arb normale coute ~5$ (5 parts x combined ~1.0) -> 8$ laisse la
# marge d'une paire complete + un ajustement, mais jamais un doublement.
MAX_MARKET_EXPOSURE_USD = 8.0
# ── PLAFONDS PROPORTIONNELS AU CAPITAL (Steven 06/08, "on peut retirer les
# plafonds de nos strategies gagnantes ? ou pas ?").
# Reponse mesuree : ne PAS les retirer, mais les faire GRANDIR avec le
# capital -- un plafond fixe en dollars devient absurde dans les deux sens
# (8$ sur un compte de 20$ = 40% du capital sur un seul marche ; 8$ sur un
# compte de 500$ = plus rien du tout).
#
# Les deux strategies gagnantes ne se valent PAS pour autant :
#
#  - ARB VERROUILLE : quand min(parts) > cout, le profit est garanti par
#    ARITHMETIQUE, pas par statistique -- aucune variance. La seule vraie
#    limite est la profondeur du carnet et le risque d'ECHEC D'EXECUTION
#    (c'est lui qui a coute -10$ le 05/08, pas l'arb). Donc plafond genereux.
#
#  - NEAR-CERTAIN : edge mince (+1.6% de ROI mesure sur 182 jambes, WR 95%).
#    Kelly plein dirait 23% du capital, MAIS avec n=182 le WR reel est a
#    +/-3.2 points pres, et a 92% le Kelly devient NEGATIF (-25%). L'edge
#    n'est pas assez certain pour miser gros : on applique un quart de Kelly
#    (~6%), ce qui reste robuste meme si le vrai WR est plus bas que mesure.
MAX_MARKET_EXPOSURE_FRAC = 0.25   # arb : 25% du capital investissable par marche
NEARCERT_BUDGET_FRAC = 0.06       # near-certain : ~1/4 de Kelly, prudent car edge mince
MAX_MARKET_EXPOSURE_CEIL = 60.0   # garde-fou absolu tant que la strategie n'est pas
NEARCERT_BUDGET_CEIL = 20.0       # validee sur gros volume -- a relever plus tard
MAX_FRACTION = 0.40  # ... et au plus 40% du capital investissable (releve 0.30->0.40)
MIN_BUDGET_USD = 0.10  # Steven 19/08 -- exploiter meme un cash tres bas (0.55$)
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
MIN_STAKE_FRACTION = 0.17  # Steven 02/09 -- releve 10->17%, plancher de mise = 17% du capital investissable
# (voir _budget_usd) ; borne quand meme par HARD_CAP_USD/MAX_FRACTION/investable, jamais
# depasse le budget reellement disponible, juste evite les mises Kelly ridiculement petites.
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
# SEUIL RAMENE SOUS LE POINT MORT (Steven 06/08, apres backtest).
#
# 0.99 etait la MEME erreur que STAGGER_COMPLETE_MAX : un combine de 0.99 laisse
# 1.0% de marge brute, alors que les frais taker des deux jambes en coutent
# ~4.4% (0.0455 x min(p,1-p) sur chaque jambe, soit ~0.044 par part a 0.48/0.51).
# Le point mort est a 0.956 -- tout arb conclu au-dessus perd de l'argent en
# etant pourtant un vrai arb. Verifie sur nos executions reelles :
#     combine 0.96-0.98 -> ROI -20.12%   |   combine >= 1.00 -> ROI -4.43%
#
# COUT DE CE RESSERRAGE, mesure sur 694 fenetres (occasion comptee seulement si
# le PIRE prix d'achat de chaque cote dans la seconde suffit deja) :
#     seuil 0.99  -> 11.0% des fenetres, 6.6 arbs/h, marge moyenne +3.6% (perdant)
#     seuil 0.956 ->  2.7% des fenetres, 1.6 arbs/h, marge moyenne +7.7%
#     seuil 0.95  ->  2.2% des fenetres, 1.3 arbs/h, marge moyenne +9.2%
# Cette estimation est corroboree par nos executions reelles : 1.3 arb/h mesure
# sur 63 heures, contre 1.6 predit. Les 8 arbs/h demandes n'existent pas au-dela
# du point mort ; on les "obtient" uniquement en achetant des perdants.
INSTANT_ARB_MAX_COMBINED = 0.95
# ── PLAFOND DE COMPLETION D'UNE PAIRE (Steven 05/08) ────────────────────
# S'applique a TOUS les chemins qui completent une paire deja entamee
# (FORCE-PAIR, ORPHAN-PAIR, HEDGE-NEAR). Avant, ces chemins toleraient un
# combined de 1.02 -- voire AUCUN plafond du tout pour HEDGE-NEAR, qui
# calculait combined_h, le loggait, et ne s'en servait jamais.
# Or completer a combined >= 1.00, c'est payer plus de 1$ pour recevoir
# exactement 1$ : une perte GARANTIE, pas une couverture.
# Mesure on-chain sur 27.9h (67 paires) :
#   - combined NOMINAL median (somme des 2 prix)      : 1.032
#   - combined EFFECTIF median (paye / payout garanti): 1.325
#   - 55 paires sur 67 achetees a perte garantie, dont 36 au-dessus de 1.20
# La justification historique ("mieux vaut combined 1.05 qu'un pari nu")
# ne tient pas a l'examen : quand l'autre cote coute 0.95, completer et
# solder la jambe coutent EXACTEMENT la meme chose (marche efficient), mais
# completer immobilise en plus le prix de la 2e jambe. Sur un bankroll de
# quelques dollars, ce capital immobilise est ce qui empeche de prendre le
# vrai arb suivant. On complete donc UNIQUEMENT si ca verrouille vraiment ;
# sinon la jambe part en must_close (cf. "zero jambe nue").
# ── FRAIS POLYMARKET (Steven 06/08, decouverts en analysant un "verrou"
# qui n'en etait pas). Le bot n'a JAMAIS compte les frais : il loggait
# "gain garanti +0.09$" sur une paire qui, on-chain, coutait 4.72$ pour un
# payout de 4.65$ -- soit une PERTE de 0.07$.
#
# Barème officiel du marche (champ feeSchedule de l'API Gamma) :
#   {'rate': 0.07, 'takerOnly': True, 'rebateRate': 0.2, 'exponent': 1}
# Formule : frais = rate * min(p, 1-p) * parts  (maximum au milieu, nul aux
# extremes -- c'est pour ca que le near-certain a 0.96 paie tres peu).
#
# TAUX REEL DE CE COMPTE : 0.042 mesure sur 338 achats a prix central
# (badge Polymarket = -40% du bareme public, stable sur 24h). On garde une
# petite marge de securite car le taux varie legerement (p25-p75 :
# 0.036-0.048) et un badge peut changer de palier.
POLY_FEE_RATE = 0.048           # taux prudent (p75 mesure), pas la mediane
POLY_FEE_SAFETY = 0.003         # marge : mieux vaut rater un arb que le perdre
# ── TAUX MESURES SUR LE COMPTE REEL (13/08) ───────────────────────────────
# Reconciliation du champ `cost` on-chain contre prix x parts, trades
# manuels de Steven exclus. Voir le docstring de _poly_fee.
#   ACHATS : 317 trades, 94% a ecart RIGOUREUSEMENT nul, median 0.00%
#   VENTES :  70 trades, median -4.27%, taux implicite median 0.0595
# On garde une petite marge au-dessus de la mediane mesuree sur les ventes,
# dans l'esprit de POLY_FEE_SAFETY : mieux vaut renoncer a une sortie que
# la croire plus rentable qu'elle n'est.
POLY_FEE_RATE_ACHAT = 0.0       # gratuit au palier actuel (bronze)
POLY_FEE_RATE_VENTE = 0.062     # mediane 0.0595 + marge de securite

# Seuil de verrou AVANT frais. Avec 2 jambes sous 0.50 les frais valent
# rate*C, donc le vrai seuil de rentabilite est 1/(1+rate) = 0.954 a 0.048.
# On ne s'en sert plus comme critere principal (cf. _pair_net_after_fees qui
# calcule le cout EXACT jambe par jambe), mais comme premier filtre grossier.
# Ecart de parts tolere entre les 2 jambes d'une paire. Au-dela, l'excedent de
# la grosse jambe n'est couvert par rien et doit etre solde : le gagnant paie 1$
# par PART, donc seul min(parts) est un arb. 1.05 laisse passer un arrondi sans
# laisser passer un vrai desequilibre. Mesure sur 80 arbs reels : equilibres
# +0.62% de ROI, au-dela de 1.30x -14.03%.
# ── ARB MAKER EN FENETRE OUVERTE (Steven 06/08) ─────────────────────────
# Voir _manage_maker_open pour le raisonnement complet. Calibration issue du
# backtest sur 694 fenetres reelles (566k transactions publiques) : en posant
# a P des DEUX cotes, part des fenetres ou une vente a P ou moins survient de
# chaque cote (taille >= 4 parts), donc ou nos deux ordres pouvaient etre
# servis -- 5 marches a 12 fenetres/heure = 60 fenetres/heure :
#     pose 0.49+0.49 = 0.98 -> 64.6% des fenetres, marge  +2.0%
#     pose 0.48+0.48 = 0.96 -> 62.5% des fenetres, marge  +4.0%
#     pose 0.46+0.46 = 0.92 -> 58.5% des fenetres, marge  +8.0%
#     pose 0.44+0.44 = 0.88 -> 53.5% des fenetres, marge +12.0%
# On retient 0.46 : descendre de 0.49 a 0.46 quadruple la marge en ne coutant
# que 6 points de taux de service. En dessous, le taux chute plus vite.
#
# CE CHIFFRE ETAIT UN MAJORANT (une vente a 0.46 prouve que des ordres a 0.46
# ont ete servis, pas que ce serait le notre -- il peut y avoir 40 parts devant
# nous dans la file). Le journal makeropen_hist a mesure le taux REEL sur la
# nuit du 06/08 : 80.6% (29/36), tres proche du majorant "any" du backtest
# (79.8%) -- validation que le modele de remplissage n'etait pas delirant.
#
# PRIX 0.46 -> 0.35 (Steven 06/08, apres le vrai probleme trouve cette nuit).
# Le maker BTC a 0.46 tournait a -10.6% de ROI une fois les jambes seules
# comptees (elles perdent -95.6% en moyenne, la vente se fait trop tard, quand
# le prix est deja proche de zero). Deux corrections testees (couper plus tot
# dans le temps, stop-loss sur le prix) ont ECHOUE : les deux cassent plus de
# verrous reussis qu'elles ne sauvent de pertes -- une position qui va
# verrouiller traverse souvent le meme creux de prix qu'une position perdante,
# rien ne les distingue a l'avance. Ce qui MARCHE, mesure sur 519 fenetres
# BTC : baisser le prix de pose. A prix egal la marge par verrou ET la perte
# par echec varient dans le meme sens favorable :
#     prix 0.46 -> ROI -8.75% (conservateur) / -3.14% (optimiste)
#     prix 0.35 -> ROI -0.71% (conservateur) / +2.61% (optimiste)
#     prix 0.33 -> ROI -1.22% (conservateur) / +4.35% (optimiste, le meilleur)
# 0.35 retenu plutot que l'optimum 0.33 : marge de securite, l'ecart entre les
# deux est mince et non significatif sur cet echantillon.
#
# ETH AJOUTE (meme jour). Meme mecanisme rejoue sur les fenetres ETH
# historiques, meme sens d'amelioration en baissant le prix :
#     ETH prix 0.46 -> ROI -14.67% (conservateur) / +0.91% (optimiste)
#     ETH prix 0.35 -> ROI  -8.46% (conservateur) / +8.79% (optimiste)
# ATTENTION, echantillon ETH = 65 fenetres contre 519 pour BTC (8x moins) :
# le SENS du resultat est coherent avec BTC, la MAGNITUDE est peu fiable.
#
# RISQUE DE DEPLETION (a garder en tete, non corrige ici) : une simulation
# conjointe BTC+ETH partageant un solde de 20$ montre le solde tomber sous
# MAKER_OPEN_BUDGET_MIN (4.7$) en 2 a 4 jours dans TOUS les scenarios testes,
# apres quoi le bot ne peut plus dimensionner une tentative normale et reste
# bloque -- le dimensionnement proportionnel (35% de l'investissable) amplifie
# les series, bonnes et mauvaises. Pas de fix demande sur ce point pour
# l'instant, juste un fait mesure.
MAKER_OPEN_ENABLED = False  # Steven 01/09 -- "on ne parie plus sur le perdant,
# on parie sur le gagnant desormais" : coupe l'achat passif cote pas cher
# (perdant par construction), tout passe par _try_favorite (achat du cote
# cher/gagnant identifie par Binance).
# EXTENSION AUX 6 MARCHES (Steven 12/08). MSF n'etait autorise que sur BTC et
# ETH alors que la collecte tourne sur 6 marches depuis le 11/08. Mesure sur
# 24h de carnet reel, 289 fenetres 5m par symbole, meme critere partout
# (ask_top <= 0.35 sur un cote = notre achat passif peut y etre servi) :
#     symbole   2 cotes touches   profondeur med   spread
#       BTC          31.5%              687        0.010
#       ETH          27.3%              138        0.010
#       XRP          33.2%               75        0.020
#       SOL          30.4%               94        0.010
#       DOGE         28.4%              112        0.030
#       BNB          27.3%               67        0.030
# Le taux de croisement -- seule source de profit du systeme -- est
# equivalent sur les six : XRP fait meme mieux que BTC. Les 4 marches non
# exploites representent 1156 fenetres/24h contre 289 pour BTC seul.
# RISQUE BORNE : un ordre passif non servi ne coute RIEN (verifie on-chain,
# l'historique ne contient aucun evenement ORDER ni CANCEL, seulement TRADE
# et REDEEM). Le pire cas d'un nouveau symbole est de ne pas etre rempli.
# RESERVE REELLE : ces carnets sont 6 a 10x plus fins que BTC et le spread
# est 2 a 3x plus large sur DOGE et BNB -- les SORTIES y seront plus dures
# (c'est ce qui a produit 4 echecs de vente consecutifs sur ETH le 12/08,
# alors qu'ETH est deja 5x plus profond que BNB). Le correctif du meme jour
# -- prix de vente qui descend le carnet jusqu'au niveau absorbant -- adresse
# precisement ce point.
# NB : ceci n'ACTIVE rien tout seul. Le second garde-fou reste en place :
#      self.state["modes"][sym] doit valoir "real" pour que MSF pose quoi que
#      ce soit. Steven garde donc la main, symbole par symbole, depuis le
#      dashboard.
MAKER_OPEN_SYMBOLS = ("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB")
# PRIX 0.35 -> 0.10 (Steven 19/08, prolonge la meme piste que 0.46->0.35 du
# 06/08 ci-dessus -- meme mecanisme, teste plus loin). Simulation d'ordre
# resident causale (achat au 1er ask<=P, jambe seule tenue a resolution, P&L
# COMPLET pas seulement les verrous) sur 2 periodes disjointes (12-14/08 et
# 16-18/08, 6 jours calendaires, jamais chevauchees), grille P=0.30 a 0.01 :
# amelioration MONOTONE a chaque palier plus bas, jamais d'inversion, jamais
# de signe different par jour ou par symbole (0.05 : 6/6 symboles
# significatifs individuellement sur les deux jeux). A 0.05 -- l'optimum
# mesure -- \$/h = +26.5 (frais) / +14.7 (ancien) contre -38.9 / -30.7 a
# 0.35 (le prix actuel). RETENU A 0.10 PLUTOT QUE L'OPTIMUM 0.05, avec la
# meme philosophie de marge de securite que le choix 0.35 (vs 0.33) du
# 06/08 : 0.05 = exactement DEAD_MARKET_THRESHOLD, le plancher deja etabli
# ailleurs dans ce fichier pour dire "ce cote est quasi resolu, ne pas y
# toucher" -- coller pile dessus pour la pose initiale est plus risque que
# ce que le backtest (execution idealisee, pas de modele de profondeur
# reelle a des prix aussi extremes) peut garantir. A 0.10, deja tres
# largement valide (\$/h = +13.4 / +5.9) et loin de ce plancher.
# ATTENTION OPERATIONNELLE (non testee ici) : le modele RL deploye
# (rl_qnet_weights.json) a ete entraine sur des entrees pres de 0.35 --
# des entrees systematiquement pres de 0.10 sont hors de la distribution
# vue a l'entrainement. A surveiller en reel ; un reentrainement (v6) sur
# des donnees a ce nouveau prix serait la suite logique si ca se confirme.
MAKER_OPEN_PRICE = 0.10           # prix de pose, PLAFOND (jamais depasse)
# PRIX ADAPTATIF (Steven 09/08, "il pourrait pas s'adapter au marche ?") :
# au lieu d'abandonner toute la fenetre quand l'ask est deja sous 0.35
# (PRENEUR), on pose EN DESSOUS de l'ask actuel -- jamais au-dessus de
# MAKER_OPEN_PRICE (jamais plus cher que ce qu'on fait deja aujourd'hui).
# Backteste (586 fenetres, walk-forward train/test, frais de sortie inclus) :
# plafonner au-DESSUS de 0.35 (poursuivre le marche quand il est cher) perd
# systematiquement de l'argent (jusqu'a -3.7$/j) -- ca transforme l'arb en
# pari directionnel sur un cote deja tranche. Adapter uniquement vers le BAS,
# plafonne a 0.35, ne peut jamais faire pire que le fixe actuel (c'est un
# sous-ensemble strict des memes prix ou moins chers) : train +0.71->+1.44$/j
# (sell), test directionnellement coherent (jamais pire que le fixe).
# EN PAUSE (Steven 09/08, "le calm mode va nous permettre de faire plus de
# trade/h") : le mode CALME est desormais la voie choisie pour augmenter la
# frequence en marche non-croise -- l'adaptatif croisement (descendre sous
# 0.35) est mis en pause plutot que retire (reactivable en repassant a True).
MAKER_OPEN_ADAPT_ENABLED = False
MAKER_OPEN_ADAPT_DISCOUNT = 0.12  # marge sous l'ask quand il est deja < MAKER_OPEN_PRICE
# PLANCHER = DEAD_MARKET_THRESHOLD (Steven 09/08, "miser sur le perdant ne
# paye que si le marche est dangereux") : corrige apres coup -- le plancher
# etait a 0.02, sous le seuil que le reste du bot utilise deja pour dire
# "ce cote est quasi resolu, on n'y touche pas" (DEAD_MARKET_THRESHOLD=0.05).
# Sans ca, dans un marche directionnel SANS croisement, le cote perdant est
# facile a remplir (vendeurs qui bradent) alors que le cote gagnant (notre
# autre jambe, encore au prix fixe) ne se remplit quasiment jamais -> on
# finit avec SEULEMENT la jambe perdante, l'inverse d'un verrou.
MAKER_OPEN_ADAPT_FLOOR = DEAD_MARKET_THRESHOLD  # jamais sous le seuil "marche mort"
MAKER_OPEN_MAX_COMBINED = 0.94    # garde-fou : au-dela on ne pose pas
# ── COMPLETION ACTIVE DE LA JAMBE SEULE (Steven 11/08) ─────────────────
# La jambe seule coute -0.249 $/part en esperance : c'est LE poste de perte
# de MSF. Des que l'autre cote redevient assez bon marche pour que le verrou
# reste rentable, on l'achete au marche -> perte esperee transformee en gain
# garanti. Backteste (vrais prix imprimes, frais taker sur la 2e jambe) :
#   MSF actuel            TRAIN +1.59 $/j | TEST -1.67 $/j
#   + completion <= 0.97  TRAIN +6.79 $/j | TEST +24.97 $/j
# 185 completions sur 586 fenetres, ZERO perdante, mediane +0.027 $/part.
# Seuil a 0.97 et non 1.00 : au-dela la marge brute ne couvre plus les frais
# taker de la 2e jambe. MIN_GAIN garde une marge de securite absolue.
MAKER_OPEN_COMPLETION_ENABLED = True
# PLAFOND RELEVE A 0.99 (Steven 11/08, "on devrait tout le temps en avoir").
# Le 0.97 avait ete cale sur le chemin AU MARCHE, qui paie ~5% de frais. Or
# 59 completions sur 63 partent en APPORTEUR, et l'apporteur ne paie RIEN :
# il reste rentable jusqu'a un combine proche de 1.00. Le plafond bridait
# donc le bon chemin pour proteger le mauvais.
# Backteste (train ET test, remplissage conservateur) :
#   0.97 -> TRAIN 12.59% | TEST  9.79% (27.45 $/j) | 59 apporteur, 53 cutoffs
#   0.99 -> TRAIN 13.20% | TEST 10.05% (29.00 $/j) | 62 apporteur, 48 cutoffs
#   1.00 -> TRAIN 12.96% | TEST  9.84% -- au-dela on repasse sous 0.99
# Aucun risque d'ouvrir des completions perdantes AU MARCHE : le controle de
# gain APRES frais (MIN_GAIN) coupe ce chemin de lui-meme des 0.98
# (combine 0.98 -> net +0.011$ sur 10 parts, sous le seuil de 0.02$).
# PLAFOND DE COMPLETION > 1.00 (Steven 11/08, "perdant rempli mais jamais
# gagnant"). Tant qu'il valait 0.99, la completion n'existait QUE dans les
# fenetres ou l'on ne perdait pas -- or notre jambe n'est servie que lorsque
# le marche traverse notre prix, donc on est structurellement rempli du cote
# perdant, et l'autre cote est alors DEJA trop cher : combine > 1, completion
# refusee, jambe seule subie. On refusait donc de payer 0.05 de perte
# GARANTIE pour continuer a encaisser -0.249/part en esperance.
# Backtest chronologique TRAIN/TEST (retrait T=90s actif) :
#   plafond 0.99  TRAIN +30.94$  TEST +15.46$  (31 jambes seules subies)
#   plafond 1.02  TRAIN +31.55$  TEST +15.79$  (23)
#   plafond 1.05  TRAIN +33.39$  TEST +16.88$  (14)  <- retenu
#   plafond 1.08  TRAIN +34.49$  TEST +16.68$  (11)  TEST retombe
# A 1.05 on paie 1.05 pour recevoir 1.00, frais compris ~-0.065$/part : moins
# cher que couper (-0.0735) et bien moins que subir (-0.249).
MAKER_OPEN_COMPLETION_MAX = 1.05
# ATTENTE AVANT COMPLETION SUPPRIMEE (Steven 12/08). Elle valait 5s pour
# "laisser le carnet se stabiliser". Mesure on-chain sur 29 paires reelles :
# AUCUNE n'a ete faite par deux remplissages passifs a 0.35 -- toutes viennent
# d'un fill passif PUIS d'un achat au marche. La completion n'est donc pas un
# filet de secours, c'est LE mecanisme qui fabrique la paire. Chaque seconde
# d'attente est une seconde ou l'autre cote peut fuir : les 3 orphelines du
# 11/08 sont des marches partis droit ou le combine a franchi le plafond
# pendant l'attente. Backtest, jambes seules subies TRAIN / TEST :
#   attente 10s : 30 / 15   net TRAIN +33.51$
#   attente  5s : 24 / 14   net TRAIN +34.21$   (avant)
#   attente  0s : 16 / 12   net TRAIN +35.66$   <- retenu, -33% d'orphelines
# ATTENTE RETABLIE (Steven 12/08). Je l'avais mise a 0 dans c387048 en
# raisonnant "attendre, c'est laisser l'autre cote fuir". Faux : ces 5
# secondes servaient a laisser le SECOND ORDRE PASSIF se remplir a 0.35
# (verrou gratuit) au lieu d'acheter l'autre cote au marche. Corrolation
# mesuree sur le PnL reel du bot, trades manuels exclus :
#   11/08 22:00  +0.240$/fenetre   jambes restees seules  7/16 (44%)
#   12/08 00:00  +0.372$/fenetre                          4/19 (21%)
#   12/08 02:00  -0.135$/fenetre                         14/34 (41%)   <- c387048 a 01:17
#   12/08 04:00  -0.543$/fenetre                         11/17 (65%)
#   12/08 06:00  -0.536$/fenetre                         15/19 (79%)
# Le taux de jambes seules a quadruple, c'est-a-dire l'inverse exact de ce
# que le changement etait cense produire.
MAKER_OPEN_COMPLETION_MIN_HOLD_S = 5
MAKER_OPEN_COMPLETION_MIN_GAIN = 0.02   # $ : en dessous ca ne vaut pas le risque
# APPORTEUR D'ABORD (Steven 11/08). Cinq idees d'optimisation backtestees
# isolement ET combinees ; c'est la SEULE qui ameliore, et elle ameliore
# partout (train et test, les 2 modes de remplissage) :
#   completion au marche (deployee ce matin) : TEST 8.91% | 24.97 $/j
#   completion apporteur d'abord, 60s        : TEST 9.79% | 27.45 $/j
# Sur 63 completions, 59 finissent servies en apporteur -> zero frais.
# Les 4 autres idees ont ete REJETEES par la mesure : seuil adaptatif au
# temps restant (8.81% max), completion partielle (degrade de facon
# monotone : 40% -> 3.81%, 80% -> 7.40%, 100% -> 8.91%, donc le verrou
# garanti vaut mieux que l'esperance du TP), prix adaptatif (7.71%), et
# TOUTES les combinaisons degradent I1 seule (I1+I2 9.46% < I1 9.79%).
MAKER_OPEN_COMPLETION_MAKER_S = 60      # 0 = desactive, achat direct au marche
MAKER_OPEN_MIN_REMAIN_S = 120     # sous 2 min restantes, trop tard pour etre servi
MAKER_OPEN_CANCEL_BEFORE_S = 45   # a T-45s : on annule et on solde une jambe seule
# MARGE ELARGIE PAR SYMBOLE (Steven 13/08, "35% des jambes seules XRP ne sont
# jamais gerees"). Diagnostic : le solde force EXISTE et vend bien avant
# resolution ("une jambe nue portee a resolution vaut -100% quand elle perd,
# mesure sur 51 cas sur 51") -- mais sur un carnet fin (XRP : profondeur
# mediane 75 parts contre 687 pour BTC, mesure sur 24h), une premiere
# tentative de vente peut echouer (carnet trop mince pour absorber la
# taille), et il ne reste alors plus assez de cycles avant la resolution
# pour que les tentatives suivantes (voir _prix_vente_absorbant, deja
# escaladees) aboutissent. On donne donc plus de MARGE AVANT LE CUTOFF aux
# symboles a carnet fin -- plus de cycles de retry, pas un changement de
# logique de vente.
MAKER_OPEN_CANCEL_BEFORE_S_PAR_SYMBOLE = {
    "BTC": 45, "ETH": 45,          # carnet profond : la marge de base suffit
    "SOL": 75, "XRP": 75, "DOGE": 90, "BNB": 90,   # carnet fin : plus de retries
}


def _cancel_before_s(sym):
    return MAKER_OPEN_CANCEL_BEFORE_S_PAR_SYMBOLE.get(sym, MAKER_OPEN_CANCEL_BEFORE_S)
# ANTI-SELECTION ADVERSE (Steven 11/08) : si AUCUNE des deux jambes n'est
# servie apres N secondes de fenetre, on retire les deux ordres. Un ordre
# passif n'est servi que lorsque le marche vient de traverser notre prix --
# donc plus il est servi tard, plus il est certain qu'on achete le perdant.
# Mesure sur 583 fenetres, proba que la 2e jambe suive selon l'instant du 1er
# remplissage : 0-30s 47.9% | 30-60s 46.9% | 60-90s 45.6% | 90-120s 33.3% |
# 120-150s 25.7%. Le seuil d'indifference tenir/couper est 31.97% -> au-dela
# de ~90s la pose est perdante en esperance. Test chronologique, TRAIN/TEST :
#   jamais (avant)  TRAIN +3.48$  TEST -3.90$
#   T=120s          TRAIN +2.96$  TEST -1.41$
#   T=90s           TRAIN +3.94$  TEST +0.65$   <- retenu
# On sacrifie 7 verrous sur 86 pour eviter 36 remplissages tardifs.
MAKER_OPEN_NOFILL_CANCEL_S = 90
# Delai avant de retenter une pose MSF refusee pour cause TRANSITOIRE
# ("order manager not ready, please retry"). Court : la fenetre ne dure que
# 300s et une pose tardive perd son interet (cf. MAKER_OPEN_NOFILL_CANCEL_S).
MSF_RETRY_POST_S = 2.0
# TP SUR JAMBE SEULE (Steven 07/08, "utiliser signal quand on tient 1 leg et
# qu'on voit que le marche ne bouge pas assez vite de l'autre cote -> TP
# maximal et se retirer sans poser l'autre").
#
# Sans ca, une jambe seule attend passivement jusqu'a MAKER_OPEN_CANCEL_BEFORE_S
# quoi qu'il arrive -- et si elle est en train de PERDRE, son prix s'effondre
# bien avant cette echeance (mesure la nuit du 06/08 : deja pres de zero vers
# t=150-200s alors qu'on ne vendait qu'a t=255s). Si elle est en train de
# GAGNER en revanche, la garder est la bonne decision : le cote oppose descend
# en meme temps, ce qui remplit naturellement notre 2e ordre -> c'est ce qui
# fabrique les verrous a zero frais. D'ou un TP, jamais un stop-loss : on ne
# coupe jamais une jambe qui baisse (teste et rejete, casse plus de verrous
# qu'il n'evite de pertes), on prend seulement un profit sur celle qui monte
# suffisamment, AVANT que le cutoff n'attende pour rien.
#
# Backteste sur 519 fenetres BTC + 65 ETH (566k transactions publiques),
# ordre chronologique strict (un verrou qui arriverait APRES le declenchement
# du TP compte comme sacrifie, pas comme un bonus gratuit) :
#     sans TP   -> BTC ROI -1.15% a +3.63% | ETH ROI -7.22% a +9.63%
#     TP x1.8   -> BTC ROI +2.66% a +6.73% | ETH ROI -2.26% a +11.26%
# Ameliore dans les 8 decoupages testes (par symbole ET par moitie temporelle
# ancienne/recente), jamais degrade. Le TP se declenche en moyenne 30-36s
# apres le remplissage (P10 15s, P90 130s) -- un vrai mouvement, pas du bruit
# de carnet en quelques millisecondes, largement dans la capacite de reaction
# du bot (verification toutes les 2-3s). x1.8 retenu : sous x1.5 la strategie
# devient franchement mauvaise (sacrifie trop de vrais verrous pour du bruit
# qui n'aurait jamais tenu), au-dela de x2.2 le TP ne se declenche presque
# jamais et on retombe sur le comportement actuel.
#
# RESTE UN RISQUE CONNU : sur ETH seul, en hypothese de remplissage
# conservatrice, le pire cas mesure reste negatif (-2.26%, contre -7.22% sans
# TP -- ameliore mais pas gagnant). Echantillon ETH 8x plus petit que BTC (65
# vs 519 fenetres) : le signe de son edge reste incertain, contrairement a BTC.
MAKER_OPEN_TP_MULT = 1.8          # conserve pour le mode calme / historique
# TP PLUS PRECOCE, PAR SYMBOLE (Steven 13/08, "des TP comme avant, 35c to
# 55c"). x1.8 exige 0.63 sur une entree a 0.35 -- mesure : 0 declenchement
# sur 694 fenetres de backtest. Raison structurelle : notre jambe qui monte
# de X = l'autre cote qui baisse de X = le combine qui baisse de X -> soit la
# completion se declenche avant 0.63 (bon, on gagne quand meme), soit combien
# grimpe a la place -> l'abandon prend le relais avant que 0.63 soit atteint
# (mauvais, aucune sortie gagnante n'a eu la chance de s'armer). Un seuil
# PLUS BAS (x1.5 = 0.525, dans la fourchette "35c to 55c" demandee) donne au
# TP une vraie chance d'agir dans la ZONE MORTE entre completion et abandon --
# exactement la zone ou 35% des jambes seules XRP ne recevaient aucune
# gestion (mesure du 13/08). Poser un ask passif qui n'est jamais servi ne
# coute rien (verifie on-chain : aucun evenement ORDER ni CANCEL) -- risque
# asymetrique favorable, contrairement a l'abandon ou chaque seconde
# d'attente est une seconde de perte potentielle.
# BTC/ETH gardent 1.8 (mesure et validee ailleurs cette session). Les 4
# symboles a carnet fin recoivent 1.5 -- pose par raisonnement (la fourchette
# que Steven demande), PAS mesuree par backtest fiable.
MAKER_OPEN_TP_MULT_PAR_SYMBOLE = {
    "BTC": 1.8, "ETH": 1.8,
    "SOL": 1.5, "XRP": 1.5, "DOGE": 1.5, "BNB": 1.5,
}
# SEUIL DE TP EN PRIX ABSOLU (Steven 13/08 : "regle le tp a 0.42").
# En ABSOLU et non en multiple : MAKER_OPEN_PRICE (0.35) est un PLAFOND, le
# prix de pose adaptatif peut etre plus bas, et un multiple donnerait alors
# autre chose que 0.42. Un seuil absolu vaut 0.42 quel que soit le prix
# d'entree reel. BTC/ETH restent sur le multiplicatif x1.8, inchanges.
MAKER_OPEN_TP_PRIX_PAR_SYMBOLE = {
    "SOL": 0.42, "XRP": 0.42, "DOGE": 0.42, "BNB": 0.42,
}


def _tp_mult(sym):
    return MAKER_OPEN_TP_MULT_PAR_SYMBOLE.get(sym, MAKER_OPEN_TP_MULT)


def _tp_seuil_prix(sym, prix_entree):
    """Prix de declenchement du TP sur une jambe seule.

    Absolu (0.42) sur les symboles listes dans
    MAKER_OPEN_TP_PRIX_PAR_SYMBOLE, multiplicatif ailleurs.
    """
    abs_px = MAKER_OPEN_TP_PRIX_PAR_SYMBOLE.get(sym)
    if abs_px is not None:
        return abs_px
    return prix_entree * _tp_mult(sym)
# SCALP APPORTEUR DES L'ENTREE (Steven 12/08, "si on TP tres rapidement il y a
# de la marge ici aussi"). Le TP multiplicatif exigeait 0.35 x 1.8 = 0.63 : il
# ne s'est declenche ZERO fois sur 694 fenetres de backtest, TRAIN et TEST
# confondus. Raison structurelle : notre jambe qui monte de X = l'autre cote
# qui baisse de X = le combine qui baisse de X -> la completion se declenche
# toujours AVANT que 0.63 soit atteint. Les deux sont le meme evenement vu des
# deux cotes.
# A +0.02 en revanche on encaisse dans les premieres secondes, dans la fenetre
# ou le marche bouge a 21.6 millemes/s (mesure : 10x la vitesse du reste de la
# fenetre). Et l'arithmetique est nette, a gain EGAL :
#   vendre a 0.37 en APPORTEUR      -> +0.020$/part, zero frais
#   completer a un combine de 0.96  -> +0.040 - frais preneur = +0.020$/part
# ...sauf que le scalp REND le capital la ou la completion en immobilise le
# double. Backtest (vente servie uniquement par un vrai print acheteur) :
#   0.37 : TRAIN +2.20$  TEST +1.17$  rendement 12.24% -> 13.88%
#   0.38 : TRAIN +1.84$  TEST +0.81$
#   0.40 : TRAIN +1.59$  TEST +0.72$
#   0.43 : TRAIN +1.09$  TEST +0.27$
#   0.50 : TRAIN +0.80$  TEST  0.00$
# Gradient monotone : plus le TP est serre, plus il rapporte.
# Risque nul en face : un ask non servi ne coute rien, la jambe poursuit sa
# vie normale et la completion reprend la main. Le pire cas est le
# comportement actuel.
# ══ SCALP DESACTIVE EN URGENCE (Steven 12/08, solde 20$ -> 3$) ══════════
# LE SCALP TUAIT LE VERROU. Erreur de backtest de ma part, grave : mon
# simulateur decidait "verrou" AVANT de considerer le scalp, en supposant
# que si les deux cotes touchent 0.35 dans la fenetre, le verrou a lieu.
# FAUX EN REEL : poster un ask a 0.37 des le 1er remplissage fait VENDRE la
# jambe en quelques secondes ; quand l'autre cote touche 0.35 ensuite, on ne
# detient plus la premiere jambe -> pas de verrou, juste une nouvelle jambe
# seule. On echangeait donc des opportunites a +2.02$ (verrou, 0.30/part x
# 6.72) contre des sorties a +0.13$ (scalp, 0.02/part).
# Constate on-chain le 12/08 entre 06:56 et 07:43 : 8 cycles
# BUY@0.350 -> SELL@0.370 en 4 a 40 secondes, ZERO verrou, et deux abandons
# a -2.10$ et -2.29$ qui effacent 16 scalps chacun.
# Le taux de verrou historique est de 47.5% et le verrou est la SEULE source
# de profit reelle (mesure : 79 verrous = 23.7$ sur 18.06$ de net total).
# On revient donc au TP multiplicatif d'avant, qui ne se declenchait
# jamais -- inoffensif -- en attendant une version qui n'arme le scalp
# QU'APRES la disparition de toute chance de verrou.
MAKER_OPEN_TP_OFFSET = 0.02       # conserve pour reference, plus utilise
MAKER_OPEN_TP_MIN_HOLD_S = 15
# ATTENTE PLUS COURTE AVANT D'ARMER LE TP, PAR SYMBOLE (Steven 13/08). Sur un
# carnet fin qui bouge vite (mesure : XRP Up 0.35 -> 0.18 en 12s, cas du
# 13/08), un rallye favorable peut monter PUIS retomber avant que les 15s
# d'attente n'expirent -- le TP n'a alors jamais eu la moindre chance de
# s'armer. Poser l'ask plus tot ne coute rien s'il n'est pas servi (meme
# argument que le seuil ci-dessus) ; le seul risque est d'armer a un niveau
# legerement bas si le prix continue de monter -- mineur compare a rater le
# rallye entier. BTC/ETH gardent 15s (mesure et validee). Les 4 symboles a
# carnet fin passent a 5s, symetrique au comp_hold_s deja utilise pour la
# completion (meme raisonnement, meme ordre de grandeur).
MAKER_OPEN_TP_MIN_HOLD_S_PAR_SYMBOLE = {
    "BTC": 15, "ETH": 15,
    "SOL": 5, "XRP": 5, "DOGE": 5, "BNB": 5,
}


def _tp_min_hold_s(sym):
    return MAKER_OPEN_TP_MIN_HOLD_S_PAR_SYMBOLE.get(sym, MAKER_OPEN_TP_MIN_HOLD_S)
# ABANDON : la paire est devenue hors d'atteinte (Steven 12/08). Au-dessus de
# ce combine, ni la completion (plafond 1.05) ni le scalp n'ont de chance : la
# jambe n'est plus un demi-arbitrage, c'est un pari directionnel perdant.
# Mesure on-chain du 11/08 sur les 3 orphelines : vendue tot -> 40% recupere,
# vendue tard -> 5%, jamais vendue -> 0%. L'abandon se declenche a 44s en
# mediane contre 255s pour le solde force actuel, soit 211s plus tot.
# Le seuil 1.25 (plutot que 1.05) laisse vivre la jambe assez longtemps pour
# que le scalp apporteur ait lieu : a 1.05 le combine est soit dessous soit
# dessus, la decision tombe des le 1er tick et aucun TP ne peut exister.
# BUG TROUVE EN AUDIT (Steven 19/08, avant depot reel) : ce seuil etait un
# NOMBRE ABSOLU (combine = notre_entree + ask_de_l_autre_cote), derive a
# l'epoque pour une entree a MAKER_OPEN_PRICE=0.35 ("abandon si l'autre cote
# devient plus cher que 0.90"). Le 19/08 MAKER_OPEN_PRICE est passe a 0.10,
# et PERSONNE n'avait recalcule ce seuil -- a 0.10, il faudrait un ask de
# l'autre cote > 1.25-0.10 = 1.15 (ou 1.05 pour les 4 symboles carnet fin),
# alors qu'un ask ne depasse jamais ~0.99-1.00 : L'ABANDON NE POUVAIT PLUS
# JAMAIS SE DECLENCHER. Corrige en exprimant le seuil comme une TOLERANCE
# sur l'ask de l'autre cote (independante du prix d'entree), pour que ce
# genre de bug ne revienne pas silencieusement la prochaine fois que
# MAKER_OPEN_PRICE change. Tolerances (= anciens seuils - 0.35, l'entree en
# vigueur quand ces valeurs ont ete mesurees) inchangees, donc AUCUN
# changement de comportement pour l'entree actuelle a 0.35 -- seulement
# corrige pour continuer a fonctionner correctement quel que soit
# MAKER_OPEN_PRICE.
MAKER_OPEN_ABANDON_TOLERANCE = 0.90   # BTC/ETH : abandon si l'autre ask > cette valeur
MAKER_OPEN_ABANDON_TOLERANCE_PAR_SYMBOLE = {
    "BTC": 0.90, "ETH": 0.90,
    "SOL": 0.80, "XRP": 0.80, "DOGE": 0.80, "BNB": 0.80,
}


def _abandon_max(sym):
    tol = MAKER_OPEN_ABANDON_TOLERANCE_PAR_SYMBOLE.get(sym, MAKER_OPEN_ABANDON_TOLERANCE)
    return MAKER_OPEN_PRICE + tol


# INTERDICTION D'ABANDONNER JUSTE APRES LE REMPLISSAGE (Steven 13/08,
# "surtout ca doit favoriser TP, interdit de faire SL si on vient de poser
# il y a moins de xx sec"). Ni la completion (comp_hold_s) ni le TP
# (tp_min_hold_s) n'avaient d'equivalent cote abandon -- rien n'empechait de
# couper une jambe remplie depuis quelques secondes a peine, avant meme que
# le TP ait eu la moindre chance de s'armer. Fixe volontairement AU-DESSUS
# de _tp_min_hold_s(sym) (5s pour ces symboles) : le TP a systematiquement
# le temps d'essayer avant que l'abandon ne devienne seulement ELIGIBLE
# (il faut ensuite encore satisfaire la persistance ET, desormais, le score
# de risque -- ce minimum est un plancher supplementaire, pas un remplacement
# des deux autres gardes). BTC/ETH : 0, comportement mesure inchange.
MAKER_OPEN_ABANDON_MIN_HOLD_S_PAR_SYMBOLE = {
    "BTC": 0, "ETH": 0,
    "SOL": 10, "XRP": 10, "DOGE": 10, "BNB": 10,
}


# ── AUCUN SL AVANT 1min40 (Steven 13/08, consigne explicite) ──────────────
# "INTERDIT SL si ordre pose il y a moins de 1min40 ! sa nous a coute 6$ sur
# la derniere fenetre (manque a gagner) ! il a coupe trop tot !"
#
# CAS DECLENCHEUR, fenetre XRP 1:05-1:10AM ET : servi a t+13s pour 10.76
# parts a 0.35, ABANDONNE a t+64s a 0.15. Cinquante-et-une secondes de
# detention. Le min_hold XRP valait 10s et la persistance 12s : les deux
# etaient largement satisfaits, l'abandon etait donc "legitime" au sens du
# code -- et pourtant destructeur, parce qu'il restait encore 236 secondes
# de fenetre pour que le prix revienne, ce qu'il a fait.
#
# CE QUE CE SEUIL NE BLOQUE PAS, et c'est essentiel : la fermeture forcee au
# cutoff (T-75s sur XRP) reste intacte -- elle n'est pas un SL de confort
# mais le filet qui evite de rester coince dans un carnet qui se vide. Le TP
# n'est pas concerne non plus : on n'interdit que les sorties EN PERTE.
# Consequence a connaitre : une jambe servie apres T-175s ne pourra jamais
# etre abandonnee volontairement (100s de detention depasseraient le cutoff),
# elle ira au cutoff. C'est voulu.
MSF_SL_MIN_HOLD_S = 100.0


def _abandon_min_hold_s(sym):
    """Detention minimale avant toute coupe volontaire.

    Le plancher global MSF_SL_MIN_HOLD_S prime : la table par symbole ne peut
    que l'ALLONGER, jamais le raccourcir.
    """
    return max(MSF_SL_MIN_HOLD_S,
               MAKER_OPEN_ABANDON_MIN_HOLD_S_PAR_SYMBOLE.get(sym, 0))


# ── MSF-TPNOW : PRIORITE AU TP SUR UNE COMPLETION MARGINALE (Steven 13/08) ──
# Question de Steven, verifiee : la completion ACHETE (depense du cash
# MAINTENANT, gain encaisse seulement a la resolution, minutes plus tard) ;
# le TP VEND ce qu'on detient deja (encaisse du cash MAINTENANT, position
# fermee, zero capital immobilise). Exemple reel a 6.72 parts, entree 0.35 :
#   completion @0.60 -> DEPENSE 4.03$ maintenant, gain +0.30$ encaisse a la
#                        resolution (delai median mesure 2.8min, jusqu'a 22min)
#   TP @0.525         -> ENCAISSE 3.53$ MAINTENANT, gain +1.18$ deja realise
# Deux raisons independantes de preferer le TP quand la completion n'est que
# marginalement rentable : (1) potentiellement plus gros, (2) liquidite
# immediate au lieu de capital bloque -- important sur un petit bankroll ou
# on a deja vu des refus de position faute de budget.
#
# LES DEUX PREMIERS GARDE-FOUS ETAIENT MUTUELLEMENT EXCLUSIFS -- CORRIGE LE
# 13/08. L'ancienne porte exigeait a la fois :
#   (A) gain de completion entre MIN_GAIN (0.02$) et 0.06$
#   (B) notre bid >= 0.35 + 0.70 x (0.525 - 0.35) = 0.4725
# Or (A) et (B) portent sur LE MEME prix vu des deux cotes du carnet, qui se
# somment a ~1.01 : (A) demande l'autre cote a ~0.62-0.63, (B) le demande a
# ~0.54. Jamais vrais ensemble. Pire, (A) seul est deja vide sur une grille
# au centime : a 0.62 le gain vaut +0.071$ (>0.06, on complete), a 0.63 il
# vaut +0.008$ (<0.02, MIN_GAIN bloque). Il n'existe aucun prix entre les
# deux. MSF-TPNOW ne s'est donc JAMAIS declenche depuis son deploiement --
# exactement le meme piege que l'ancien TP a x1.8 (zero fois sur 694
# fenetres) : deux seuils qui decrivent le meme evenement vu des deux cotes.
#
# LA REGLE CORRECTE N'A PAS DE SEUIL A CALER : a l'instant ou la completion
# se declencherait, on calcule les DEUX gains sur le meme carnet et on prend
# le plus grand. Rien a surajuster, et il est impossible de choisir la moins
# bonne option.
#
# BACKTEST (36 h de carnet, 1212 completions reelles sur les 4 symboles) :
#   toujours completer          -> reference
#   prendre le meilleur des 2   -> +18.13$, et +11.13$ meme en degradant le
#                                  prix de vente de 2 centimes (test a charge)
#   walk-forward 18h/18h        -> positif sur les deux moities, 4 symboles
#                                  sur 4, sans exception
#   profondeur                  -> mediane 131 parts aux 3 meilleurs bids
#                                  contre 6.72 requises : vendre est realiste
#
# POURQUOI LA MARGE MINIMUM CI-DESSOUS. Sur les 278 cas ou le TP l'emportait,
# la mediane de l'ecart valait 0.0000$ : 172 etaient des egalites a la
# fraction de centime. Les 106 cas a ecart >= 0.05$ portent a eux seuls 100%
# du gain. Exiger cette marge capture donc tout le benefice en supprimant les
# 172 changements de comportement qui ne rapportaient rien -- et evite de
# troquer un gain GARANTI (la paire paie 1.00$ a coup sur) contre un gain
# REALISE pour un ecart qui n'est que du bruit d'arrondi.
MAKER_OPEN_TPNOW_SYMBOLES = ("SOL", "XRP", "DOGE", "BNB")   # BTC/ETH inchanges
MAKER_OPEN_TPNOW_MARGE_MIN = 0.05   # $ : le TP doit battre la completion d'au
                                     # moins ca pour qu'on change d'action


# ── RETRAIT D'UN COTE QUAND LE SPOT LUI EST DEJA CONTRAIRE (Steven 13/08) ──
# TROUVE SUR LES TRADES REELS, pas en simulation. Sur 82 fenetres reelles
# etiquetables (47 gagnantes / 35 perdantes), en ne regardant QUE ce qui est
# connu a l'instant du fill, le mouvement du spot Binance depuis l'ouverture
# de la fenetre, oriente dans le sens de NOTRE cote, separe les deux groupes :
#     gagnantes  -1.95 bp   |   perdantes  -3.97 bp   |   AUC 0.714
# Les deux sont negatifs -- on est toujours servi quand ca va un peu contre
# nous, c'est la definition meme de l'adverse selection -- mais les perdantes
# sont DEUX FOIS plus contraires.
#
# POURQUOI CA MARCHE, mecaniquement : notre achat passif a 0.35 n'est servi
# que si le prix descend jusqu'a nous, donc seulement si le marche s'eloigne
# de notre cote. Tant que le spot n'a que legerement bouge, la fenetre reste
# indecise, les deux cotes restent proches et la SECONDE JAMBE reste
# accessible -- or la completion est tout ce qui separe le gain de la perte :
#     completee    52 fenetres | 83% gagnantes | +0.251$/fenetre
#     jambe seule  30 fenetres | 13% gagnantes | -1.064$/fenetre
# Passe un certain mouvement, le marche a tranche : on ne se fait plus servir
# que le perdant, et la seconde jambe devient inatteignable.
#
# VALIDATION WALK-FORWARD (seuil calibre sur les 41 premieres fenetres, puis
# applique EN AVEUGLE sur les 41 suivantes -- jamais regardees pour le choix) :
#     2e moitie sans filtre : 41 fenetres, -11.64$ (-0.284$/fenetre), 54% gagnantes
#     2e moitie avec filtre : 22 fenetres,  +7.92$ (+0.360$/fenetre), 64% gagnantes
# Changement de SIGNE hors echantillon. Et la courbe est monotone sur cette
# 2e moitie (-6bp -0.174$ ; -4bp +0.002$ ; -3bp +0.116$ ; -2bp +0.360$) : ce
# n'est pas un pic isole choisi par chance.
#
# CE QUE CE FILTRE N'EST PAS : un simple frein. Les tentatives precedentes
# (annuler si le fill tarde, ne poser que d'un cote) ne faisaient que baisser
# le VOLUME, l'esperance par jambe restant collee a -0.36$. Ici le taux de
# reussite passe de 54% a 64% et le taux de completion de 63% a 70% : c'est
# l'edge qui bouge, pas seulement le nombre de coups.
#
# L'ACTION est un RETRAIT D'ORDRE, jamais une vente. Mesure de la soiree :
# sortir une position a l'instant du fill est catastrophique (-772$ sur 36 h
# simulees) parce que le carnet est alors au plus defavorable. Ici on ne vend
# rien -- on retire un ordre qui n'a pas encore ete servi, ce qui coute
# exactement zero. Un cote deja servi n'est jamais touche.
MAKER_OPEN_SPOT_GUARD_ENABLED = True
MAKER_OPEN_SPOT_GUARD_SYMBOLES = ("SOL", "XRP", "DOGE", "BNB")
MAKER_OPEN_SPOT_GUARD_BP = -2.0     # bp de mouvement du spot CONTRE un cote
                                     # au-dela duquel on retire son ordre


def _mouvement_spot_bp(sym, slug, debut_ts, side):
    """Mouvement du spot depuis l'ouverture, ORIENTE dans le sens de `side`.

    Negatif = le spot est alle CONTRE ce cote. Rend None si le spot ou le
    strike est indisponible -- l'appelant doit alors ne rien bloquer
    (fail-open : ce garde-fou ne doit jamais empecher de trader sur une
    simple panne de lecture de prix).
    """
    pair = _PAIR_PAR_SYMBOLE_RISQUE.get(sym)
    if not pair:
        return None
    try:
        from core.btc_updown import _binance_price, _strike_at
        strike = _strike_at(pair, debut_ts, slug=slug)
        spot = _binance_price(pair)
    except Exception:
        return None
    if not strike or not spot:
        return None
    bp = (spot - strike) / strike * 10000.0
    return bp if side == "Up" else -bp


# PERSISTANCE AVANT ABANDON (Steven 13/08, "le SL plombe le peu qui a a
# plombe"). Constat sur XRP 20:55-21:00 ET : remplie a 0.35, ABANDONNEE 12
# SECONDES plus tard a 0.18 (-49%), alors qu'il restait 4min48s dans la
# fenetre. Le declencheur etait une lecture INSTANTANEE du combine -- sur un
# carnet 9x plus fin que BTC, un pic de quelques secondes n'est pas forcement
# un mouvement soutenu, c'est souvent du bruit de microstructure qui se
# resorbe. On exige donc que le combine RESTE au-dessus du seuil pendant
# quelques secondes avant d'agir -- un filtre anti-bruit, pas une suppression
# de l'abandon : un vrai decrochage soutenu reste coupe, seul un pic isole
# est desormais ignore. BTC/ETH gardent 0 (comportement mesure et valide,
# inchange) ; les carnets fins recoivent une fenetre de tolerance -- valeur
# posee par raisonnement, PAS mesuree (aucun backtest fiable actuellement
# disponible pour la calibrer, cf. les 3 simulateurs qui ont echoue
# aujourd'hui) -- A RE-EVALUER sur trades reels une fois quelques jours
# accumules.
MAKER_OPEN_ABANDON_PERSIST_S_PAR_SYMBOLE = {
    "BTC": 0, "ETH": 0,
    "SOL": 12, "XRP": 12, "DOGE": 15, "BNB": 15,
}


def _abandon_persist_s(sym):
    return MAKER_OPEN_ABANDON_PERSIST_S_PAR_SYMBOLE.get(sym, 0)


# ── CONFIRMATION SPOT AVANT ABANDON (Steven 13/08) ──────────────────────
# Cas reel qui a motive ceci : XRP 21:45-21:50 ET. Combine hors d'atteinte
# pendant 18s (> les 12s de persistance) -> abandon a 0.19$. 3 SECONDES apres
# la vente, Up rebondit et grimpe a 0.99 en moins de 3 minutes. La persistance
# temporelle seule ne peut pas voir cette difference : ces marches paient sur
# "spot au-dessus ou en dessous d'un strike", et quand le spot REEL oscille
# tout pres du strike, la probabilite implicite peut faire des allers-retours
# violents pour un mouvement minuscule du sous-jacent -- ce n'est pas du bruit
# de carnet, c'est la nature de la fonction de prix pres de la frontiere.
# core.btc_updown.danger_score() existe deja (flips autour du strike +
# velocite, 0-100, LECTURE SEULE de l'historique Binance deja collecte en
# memoire -- aucun appel reseau ici), mais n'etait branche sur aucune decision
# MSF. On le combine avec le TEMPS RESTANT : un marche agite pres du strike
# avec beaucoup de temps encore devant lui a plus d'occasions de se retourner
# ENCORE qu'un marche agite a 10s de la resolution -- le risque qu'un abandon
# soit premature doit donc etre pondere par les deux, pas par le danger seul.
MAKER_OPEN_ABANDON_RISK_MAX = 50   # 0-100 : au-dessus, on retient l'abandon
MAKER_OPEN_ABANDON_RISK_TEMPS_PLEIN_S = 180  # a ce temps restant ou plus, le
                                              # facteur temps est a son maximum

_PAIR_PAR_SYMBOLE_RISQUE = {
    "SOL": "SOLUSDT", "XRP": "XRPUSDT", "DOGE": "DOGEUSDT", "BNB": "BNBUSDT",
}


def _score_risque_retournement(sym, slug, reste):
    """0-100, ou None si l'information n'est pas disponible (FAIL-OPEN : dans
    ce cas l'abandon se comporte exactement comme avant cette fonction, on ne
    bloque jamais faute de donnee). Seuls les 4 symboles a carnet fin sont
    concernes -- BTC/ETH gardent leur comportement mesure, inchange."""
    pair = _PAIR_PAR_SYMBOLE_RISQUE.get(sym)
    if pair is None:
        return None
    try:
        from core.btc_updown import danger_score, _poly_strike_cache
        strike = _poly_strike_cache.get(slug)
        if not strike or strike <= 0:
            return None
        d = danger_score(pair, strike)  # lecture memoire seule, pas de reseau
    except Exception:
        return None
    facteur_temps = min(1.0, max(0.0, reste) / MAKER_OPEN_ABANDON_RISK_TEMPS_PLEIN_S)
    return round(d * facteur_temps)


MAKER_OPEN_TOTAL_FRAC = 0.95      # TOTAL des 2 jambes, en part de l'investissable (Steven 19/08)
MAKER_OPEN_BUDGET_MIN = 0.20  # Steven 19/08 -- exploiter meme un cash tres bas
MAKER_OPEN_BUDGET_MAX = 500.0
# ── PLAFOND D'EXPOSITION PROPRE A MSF (Steven 13/08, "au lieu d'un budget
# fixe, un pourcentage ? comme ca ca suit l'evolution sans palier stricte").
#
# LE PALIER, mesure : le budget MSF etait borne par _max_market_exposure(),
# qui vaut max(8$, investissable x 0.25). Or le budget vise investissable
# x 0.35. Consequence en deux temps :
#   - entre ~23$ et 32$ d'investissable, le plafond reste colle a 8$ pendant
#     que le budget voudrait deja 8.05$ puis 11.20$ -> la machine cesse de
#     grandir alors que le compte grandit ;
#   - au-dessus de 32$, 0.25 < 0.35 TOUJOURS, donc le budget est plafonne a
#     0.25 pour l'eternite : la fraction 0.35 est purement morte. Ce n'etait
#     pas un palier temporaire, c'etait un plafond definitif.
#
# POURQUOI ON NE PASSE PAS EN POURCENTAGE PUR : Polymarket impose 5 parts
# minimum par ordre limite, soit 3.50$ pour une paire a 0.35+0.35. Un
# pourcentage sans plancher bloquerait tout compte sous ~10$ d'investissable
# -- le plancher n'est pas une erreur, il compense cette contrainte. On garde
# donc le plancher et on supprime le PALIER.
#
# MSF a desormais son propre plafond, avec une fraction volontairement au
# DESSUS de MAKER_OPEN_TOTAL_FRAC pour qu'il ne borne jamais le budget (la
# taille en parts est arrondie au-dessus, le besoin depasse donc le budget de
# quelques centimes ; sans cette marge le plafond redeviendrait mordant).
# Les autres strategies gardent MAX_MARKET_EXPOSURE_FRAC = 0.25 inchange.
#
# CE QUE CA COUTE, dit franchement : l'exposition MSF par fenetre passe de
# 25% a 35% de l'investissable au-dela de 23$. Une paire COMPLETEE est
# couverte (0.70$/part pour un paiement de 1.00$), mais une JAMBE SEULE qui
# meurt est directionnelle et peut tout perdre. A 50$ d'investissable le
# risque sur une fenetre passe de 12.50$ a 17.50$. C'est un choix assume,
# pas un effet de bord.
MAKER_OPEN_EXPO_FRAC = 0.40
MAKER_OPEN_EXPO_MIN = 8.0

# ── MODE CALME MSF (Steven 09/08, "miser sur le perdant ca marchait quand ca
# croisait, mais quand c'est calme il faut faire l'inverse") ────────────────
# Le mode CROISEMENT (danger haut) mise sur le perdant a 0.35 : marche qui
# retombe vite -> TP x1.8. En mode CALME (danger bas), le marche prend une
# direction SANS croiser : le perdant ne revient JAMAIS et on finit avec la
# seule jambe perdante (le commentaire MAKER_OPEN_ADAPT_FLOOR le decrit deja).
# L'inverse : poser des ordres a CALM_MSF_PRICE (0.65, le prix du GAGNANT) des
# 2 cotes, accepter une couverture jusqu'a CALM_MSF_MAX_COMBINED (1.05, ce
# n'est plus un arb verrouille mais une paire directionnelle), et COUPER la
# jambe qui decroche (CALM_MSF_SL_PRICE) au lieu d'attendre le cutoff. Toggle
# ACTIVE PAR DEFAUT au push (Steven 09/08, "le msf calm mode doit etre active
# au push sur git") + auto-stop apres 20 trades si ROI < 0.
# COUPE APRES MESURE (Steven 09/08). Le mecanisme reste entierement en place
# et rallumable en repassant ce drapeau a True, mais le backtest le refute
# dans TOUTES les configurations testees (190 fenetres BTC calmes, proxy
# danger<30, frais de sortie inclus) :
#   entree 0.65 maker (deploye)      : ROI -10.2%   (24 TP contre 25 SL)
#   entree au bid du favori          : ROI -13.6%
#   entree taker au prix marche      : ROI  -8.1%
#   5 autres couples TP/SL           : ROI -9.3% a -15.8%
#   TP "intelligent" (trailing x9)   : ROI -12.4% a -15.4%
#   trailing + "pres du strike"      : ROI -9.8% a -13.0%
# CAUSE RACINE : le marche est efficient sur ces fenetres. Acheter le favori
# et tenir donne un taux de reussite qui COLLE au prix paye a moins de 2
# points, a tous les seuils (0.55 -> 65.0% reussi pour 66.1% requis ; 0.65 ->
# 71.6% pour 72.0% requis). Il n'y a pas d'edge directionnel a capter, et les
# frais de sortie font basculer le reste en negatif.
# DEUX CONSTATS UTILES AU PASSAGE :
#   - le SL fait plus de mal que de bien ici : SL 0.40 -> -13.6%, aucun SL
#     -> -5.6% (il verrouille des creux qui se seraient repris) ;
#   - le filtre danger PROTEGE mais ne rapporte pas : en marche agite le
#     favori ne gagne que 51.6% du temps pour 64.6% requis (-20%), en calme
#     il revient juste a l'equilibre-moins-les-frais.
# Sur EXACTEMENT les memes fenetres calmes, le MSF de base (0.35 des 2 cotes)
# fait -3.3% (conservateur) a +1.3% (optimiste) -- mieux que chaque variante
# calme, et sans dependre d'avoir raison sur la direction.
CALM_MSF_ENABLED = False
CALM_MSF_DANGER_MAX = 30           # danger_score < ce seuil -> mode calme
CALM_MSF_PRICE = 0.65              # prix de pose en mode calme, PLAFOND (le gagnant)
CALM_MSF_ADAPT_FLOOR = 0.35        # plancher adaptatif : jamais plus bas, sinon on rachete le perdant bradé
CALM_MSF_MAX_COMBINED = 1.05       # couverture 2 jambes acceptee (pas un arb)
CALM_MSF_SL_PRICE = 0.40           # coupe la jambe seule quand son bid passe sous ce prix
# TP ABSOLU (pas x1.8) : en mode calme l'entree est a ~0.65, et 0.65 x 1.8 =
# 1.17 > 1.0 -> le seuil serait INATTEIGNABLE et le TP ne se declencherait
# jamais. On prend donc un profit a prix fixe CALM_MSF_TP_PRICE.
CALM_MSF_TP_PRICE = 0.85
CALM_MSF_AUTOSTOP_N = 20           # apres 20 trades calme, on coupe le mode si ROI < 0

PAIR_MAX_IMBALANCE = 1.05
PAIR_COMPLETION_MAX_COMBINED = 99.0  # Steven 19/08 -- desactive (TP instantane gere le risque, plus besoin du gate combine)
# ── ZONE DE COUVERTURE TOLEREE (Steven 05/08, decision explicite) ──────
# Cas reel qui a motive ce palier : 13:20:47 achat Up 4.976 @ 0.410, puis
# 6 SECONDES plus tard achat Down 4.919 @ 0.620 -> combine 1.030. Le prix
# de la 2e jambe avait derive entre les deux ordres, et son plafond etait
# fixe (0.99 pour la jambe favorite) au lieu de dependre de ce qu'avait
# reellement coute la jambe 1.
# Politique retenue, en deux temps :
#   combine < 0.99  -> VRAI verrou, profit garanti (cible)
#   0.99 a 1.03     -> pas un arb, mais une couverture : perte bornee a
#                      ~3%, et surtout le TP/SL reste ACTIF dessus (la
#                      paire n'est pas taggee risk_free) -- verifie en
#                      production : sur cette paire a 1.030, le palier a
#                      vendu 25% (1.230 parts) a 0.780 ce qui avait ete
#                      achete a 0.620, soit +26% sur la tranche.
#   > 1.03          -> refus, et la jambe 1 part en must_close.
# Ce seuil coupe la queue catastrophique mesuree la veille (36 paires
# perdantes au-dessus de 1.20) tout en gardant la bande que le TP/SL sait
# reellement gerer (seulement 6 des 55 paires perdantes etaient en 1.00-1.05,
# et elles l'ont ete SANS TP/SL fonctionnel, faussement taggees risk-free).
PAIR_COMPLETION_HEDGE_MAX = 99.0  # Steven 19/08 -- desactive (idem)

# ── RENFORT DE LA JAMBE GAGNANTE (Steven 05/08) ────────────────────────
# Idee de Steven : "des qu'on a fait un SL, meme de 25%, ca devient
# directionnel -- rien n'empeche d'ajouter sur le gagnant (en verifiant
# Binance)". C'est juste : toute coupe reduit min(parts_up, parts_down),
# donc le payout du pire cas -- le verrou est deja entame, il n'y a plus
# rien a preserver. Et une paire REELLEMENT verrouillee est taggee
# is_risk_free donc exempte de SL : si un SL s'est declenche, c'est par
# construction qu'il n'y avait pas de verrou.
#
# La question devient alors purement une question d'esperance. Sur un
# marche binaire, acheter a un prix p ne gagne en esperance que si le taux
# de reussite reel depasse p. Mesure sur l'historique on-chain (500
# evenements, taux de reussite par tranche de prix d'achat) :
#     0.40-0.50 :  25 jambes, 44% -> EV -0.010  (zone pile-ou-face)
#     0.50-0.60 :  37 jambes, 78% -> EV +0.234, ROI reel +39%
#     0.60-0.70 :  23 jambes, 78% -> EV +0.133, ROI reel +18%
#     0.70-0.80 :  13 jambes, 85% -> EV +0.096, ROI reel +25%
#     0.80-0.90 :   8 jambes, 62% -> EV -0.225, ROI reel -10%
# La bande 0.50-0.80 est nettement positive sur 73 jambes ; au-dela de
# 0.80 ca bascule (on paie plus cher que la probabilite reelle). D'ou les
# bornes ci-dessous. RESERVE : echantillons petits (8 a 37 par tranche) et
# la detection "jambe gagnante" est heuristique (rapprochement du montant
# du redeem au nombre de parts) -- tendance nette sur 3 tranches
# consecutives, pas une preuve definitive.
# DESACTIVE (Steven 05/08, meme raison que FAV_ENABLED). Ce mecanisme a ete
# construit sur la table d'esperance BIAISEE (elle excluait 29% de
# l'echantillon -- les jambes revendues avant resolution, donc les
# perdantes). Une fois le biais corrige, la bande 0.50-0.80 n'est plus
# gagnante mais a l'equilibre (+1% / -3% / -2%). La regle de declenchement
# de Steven reste juste (apres un SL on est bien directionnel, le verrou est
# deja entame) -- c'est la rentabilite du renfort qui n'est pas etablie.
# Code conserve intact, reactivable en passant ce drapeau a True.
REINFORCE_ENABLED = False
REINFORCE_MIN_PRICE = 0.50   # sous ce prix : pas de confirmation du marche
REINFORCE_MAX_PRICE = 0.80   # au-dela : esperance negative (mesure)
REINFORCE_MIN_SECS = 20      # trop tard pour ressortir si ca tourne
REINFORCE_MAX_SECS = 180     # trop tot : le marche peut encore se retourner
REINFORCE_MAX_MULT = 1.0     # renfort plafonne aux parts deja detenues (x2 max)
REINFORCE_BINANCE_MARGIN = 0.001  # ecart spot/strike mini (0.1%) = hors bruit

# ── PRIX PLANCHER POUR GARDER UNE JAMBE ORPHELINE (Steven 05/08) ───────
# _manage_orphans tenait toute jambe que Binance donnait gagnante, quel que
# soit son PRIX. Sur un outsider a 0.13, "Binance dit que ca monte" ne vaut
# rien : il faut un retournement complet pour que ca paie, et le marche le
# price deja correctement. Constate en direct sur la fenetre 10:00-10:05 :
#   ETH  Up @ 0.138 tenu jusqu'a resolution -> -1.49$ (-100%)
#   DOGE Up @ 0.242 tenu jusqu'a resolution -> -1.48$ (-100%)
#   SOL  Up @ 0.461 coupe au SL             -> -1.26$ (-27%)
#   SOL  Down @ 0.660 gardee et geree       -> +1.58$ sur la paire (+46%)
# Et ca colle a l'historique corrige (sans biais de survie, 215 jambes) :
#   0.20-0.30 -> ROI -28%   |   0.50-0.60 -> ROI  +1%
#   0.30-0.40 -> ROI -18%   |   0.60-0.70 -> ROI  -3%
#   0.40-0.50 -> ROI -17%   |   0.70-0.80 -> ROI  -2%
# Sous 0.50 c'est franchement perdant ; au-dessus c'est l'equilibre, donc
# gerable au TP/SL. On ferme donc les orphelines bon marche au lieu de les
# tenir en esperant un retournement, et on garde/gere les cheres.
ORPHAN_KEEP_MIN_PRICE = 0.50

# ── MARGE DE CONFIRMATION BINANCE (Steven 05/08, "faut pas l'inventer") ──
# Question de Steven : a-t-on le VRAI target de Polymarket ? Verifie : NON.
# Le champ officiel n'existe pas -- eventMetadata est null sur tous les
# marches 5min, aucun priceToBeat, aucun champ de strike dans l'objet Gamma.
# Le chemin "strike officiel Polymarket" de _strike_at est donc du code MORT
# en pratique : on retombe toujours sur le fallback, l'ouverture de la bougie
# 1 minute Binance. Polymarket, lui, resout sur le flux Chainlink.
# Mesure de la fiabilite de ce proxy sur 74 resolutions CERTAINES (celles
# remboursees on-chain, donc gagnant indiscutable) : 69 accords / 5 erreurs
# = 93.2%. Et les 5 erreurs sont TOUTES dans des fenetres bougeant de moins
# de 0.07% :
#     -0.002%  -0.014%  -0.041%  +0.059%  -0.066%
# Autrement dit : quand le marche bouge franchement, Binance et Chainlink
# sont d'accord ; quand ca se joue a quelques dollars, notre proxy est un
# tirage au sort. Il faut donc un ecart MINIMUM au strike avant de traiter
# le signal comme une confirmation. 0.1% couvre les 5 erreurs mesurees avec
# de la marge. Deux autres endroits du code appliquaient deja une marge
# (0.1% et 0.08%) ; la decision de TENIR une jambe nue, elle, n'en avait
# aucune -- un seul tick au-dessus du strike suffisait.
BINANCE_CONFIRM_MARGIN = 0.001

# ── PARI DIRECTIONNEL SUR LE FAVORI (Steven 05/08) ─────────────────────
# Demande explicite : "je veux voir dans le journal une ligne FAV qui mise
# sur une position a +70c en directionnel, declenchee par prix > 70c +
# signal Binance clair, puis geree par SL/TP".
# Ce que disent les donnees, honnetement : sur l'historique corrige (sans
# le biais de survie), la zone 0.70-0.80 fait -2% de ROI (n=15) et la zone
# 0.80-0.90 fait -20% (n=10). Le marche price deja correctement le fait que
# le favori est devant -- il n'y a pas d'edge gratuit a acheter cher.
# CE QUI EST NOUVEAU ici, et jamais teste : le filtre Binance STRICT a
# l'entree. L'historique ne contient aucune entree filtree ainsi. C'est donc
# une hypothese a mesurer, pas un edge etabli. D'ou :
#   - une marge Binance nettement plus exigeante que la marge de simple
#     confirmation (0.25% contre 0.1%) : on ne veut pas "le favori est
#     devant", on veut "il est devant NETTEMENT" ;
#   - un plafond a 0.85, parce que la tranche 0.80-0.90 est franchement
#     perdante dans les donnees et qu'au-dela on risque 85c pour en gagner
#     15 ;
#   - une taille volontairement modeste (pas de FAVORITE_BUDGET_MULT ici) ;
#   - strat="fav" -> JAMAIS is_risk_free, donc toujours gere en TP/SL.
# DESACTIVE (Steven 05/08, apres mesure). Le mecanisme reste entierement en
# place et reactivable en passant ce drapeau a True, mais rien ne le justifie
# aujourd'hui : la decomposition des 554$ engages montre que nos ENTREES sont
# deja neutres (+0.2% de ROI sur les marches resolus, 48% de reussite) et que
# 98% de la perte totale (-89$ sur -91$) vient des marches ou l'on finissait
# par ne detenir QUE des perdants. Le probleme n'a jamais ete le choix du
# cote -- l'inversion complete a d'ailleurs ete testee et donne -6.9% contre
# +0.2%. Ajouter un pari directionnel de plus, sur une tranche de prix qui
# affiche -2% (0.70-0.80) a -20% (0.80-0.90) de ROI historique, ne ferait que
# consommer du capital utile aux arbs verrouilles, seuls +EV par arithmetique.
# A reactiver si (et seulement si) une mesure montre un edge du filtre
# Binance strict a l'entree -- ce qui reste non teste a ce jour.
# ── NEAR-CERTAIN (Steven 05/08) : la SEULE strategie directionnelle que
# l'historique valide. Verifie sur 24.6 jours / 1718 jambes / 4545$ engages,
# en comptant TOUS les achats (y compris les marches sans redeem, donc sans
# le biais de survie qui avait fausse mes analyses precedentes) :
#     prix 0.90-0.95 :  80 jambes, WR 72%, ROI -16.6%
#     prix 0.95-0.98 : 182 jambes, WR 95%, ROI  +1.6%   <-- la seule positive
#     prix 0.98-1.00 :  31 jambes, WR 84%, ROI  -4.7%
# La regle qui explique tout : sur un marche binaire, acheter a un prix p
# n'est gagnant que si le taux de reussite depasse p. A 0.96 il faut plus de
# 96% -- c'est atteint (95% mesure, plus le filtre Binance ci-dessous). A
# 0.925 il faudrait 92.5% et on n'en fait que 72% : d'ou le -16.6%.
# La bande est donc VOLONTAIREMENT etroite. L'elargir vers le bas detruit
# l'edge, l'elargir vers le haut fait payer 99c pour en gagner 1.
# ── COPY-TRADING AUTOMATIQUE (Steven 05/08, "je te laisse faire la
# discussion seul" -- decision deleguee, documentee ici pour que le
# raisonnement reste inspectable meme sans avoir suivi la conversation).
#
# CE QUE CA FAIT : suit un petit nombre de wallets choisis via
# /api/copy-discover (ou ajoutes a la main), detecte leurs NOUVEAUX achats
# sur les marches Up/Down 5min par polling de leur activite on-chain
# publique, et repond avec une PETITE mise fixe -- jamais un montant
# proportionnel au leur (ils peuvent avoir 100x notre capital).
#
# POURQUOI DESACTIVE PAR DEFAUT (comme FAV_ENABLED/REINFORCE_ENABLED avant
# lui) : contrairement au near-certain, l'edge d'un trader source est
# mesure -- mais le mecanisme de COPIE lui-meme ne l'est pas. Entre le
# moment ou le trader source achete et le moment ou on le detecte (poll +
# latence reseau) le prix a deja bouge ; copier un favori a 0.96 quand le
# marche est passe a 0.99 pendant la latence, ce n'est plus le meme trade.
# C'est pour ca que COPY_TRADE_MAX_STALE_SECS et COPY_TRADE_MAX_PRICE_DRIFT
# existent : on prefere rater une copie que copier un prix perime.
#
# GARDE-FOUS (tous cumulatifs, comme le reste des mecanismes de ce soir) :
#  - liste de wallets EXPLICITEMENT suivie par Steven (jamais auto-ajoutee) ;
#  - mise fixe petite, jamais proportionnelle au trade source ;
#  - jamais si l'evenement source a plus de COPY_TRADE_MAX_STALE_SECS ;
#  - jamais si notre prix d'entree deriverait de plus de
#    COPY_TRADE_MAX_PRICE_DRIFT par rapport au prix source ;
#  - une seule copie par (wallet, slug) -- pas de sur-copie si le trader
#    source fait plusieurs achats sur la meme fenetre ;
#  - strat="copy", jamais is_risk_free -> TOUJOURS gere par le meme TP/SL
#    que toutes les autres positions (ajoute au filtre de
#    _manage_pnl_tier_exits, meme piege deja rencontre avec "fav"/"nearcert") ;
#  - plafonds habituels : exposition/marche, plancher de cash, MIN_BUDGET_USD.
#
# LEVE (Steven 05/08, "bah le mieux c'est tester en reel hein") : garde
# module retiree. Deuxieme verrou toujours en place et volontaire : le
# mecanisme reste inerte tant que Steven n'a pas suivi au moins un wallet et
# active le toggle depuis le dashboard (self.state["copy_trade"]["enabled"]) --
# je ne choisis pas le wallet ni ne clique le toggle a sa place.
COPY_TRADE_ENABLED = True
COPY_TRADE_POLL_S = 5              # frequence de sondage de l'activite source
COPY_TRADE_BUDGET_USD = 1.5        # mise FIXE, jamais proportionnelle au trade source
COPY_TRADE_MAX_STALE_SECS = 15     # au-dela : le prix a trop bouge, on ignore
COPY_TRADE_MAX_PRICE_DRIFT = 0.05  # notre ask ne doit pas depasser prix_source + 5c
COPY_TRADE_MIN_SECS_LEFT = 20      # pas assez de temps pour qu'un SL serve a qqch
COPY_TRADE_MAX_WALLETS = 5         # petit nombre, meme en selection automatique
COPY_TRADE_SEEN_CAP = 400          # purge simple de la dedup, pas de fuite memoire

# ── SELECTION AUTOMATIQUE DES WALLETS A SUIVRE (Steven 05/08, "selection
# doit etre automatique reflechis ; on a bien fait engine btb txt qui
# expliquait sa et faut aussi identifier ceux a ne pas suivre").
#
# Le spec ENGINEBTB3 (section 14 "SYSTEME DE CLASSEMENT") demande de classer
# les traders par edge reel, robustesse, recence, stabilite, volume utile,
# specialisation, fake-volume risk. Ce qui suit en est une implementation
# PRATIQUE et mesurable sur les seules donnees on-chain publiques
# disponibles (pas de "consensus with others" ni "force sur meteo" -- ces
# dimensions du spec demanderaient des donnees qu'on n'a pas).
#
# EXCLUSION (n'importe laquelle disqualifie, cf. _score_trader) :
#   - echantillon trop petit (< 20$ engages) -> impossible a juger ;
#   - pas assez recent (< 1 jour d'activite) -> pas de robustesse prouvee ;
#   - ROI global <= 0 ;
#   - edge CONCENTRE sur une seule bande de prix avec trop peu de trades
#     dedans (>60% du capital dans une bande a n<5) -> pas un edge repetable,
#     un coup de chance qui domine la moyenne ;
#   - pattern "billet de loterie" : plus de 40% du capital sur des jambes
#     sous 0.30 (la bande la plus perdante mesuree sur NOTRE propre
#     historique, -28% de ROI) -> le trader source prend des risques qu'on
#     ne veut pas copier, meme si son ROI global est positif par ailleurs.
#
# SCORE (traders eligibles seulement) : ROI pondere par la confiance dans
# l'echantillon (plus de trades = plus de confiance, plafonne a 1.0 au-dela
# de 60 evenements) + un bonus pour l'usage de l'arb (proxy de discipline :
# un trader qui verrouille des paires plutot que parier a plus de chances
# d'avoir un edge repetable qu'un directionnel chanceux).
COPY_AUTOSELECT_ENABLED = True
COPY_AUTOSELECT_INTERVAL_S = 1800   # 30min : le classement bouge lentement
COPY_AUTOSELECT_MIN_COST_USD = 20
COPY_AUTOSELECT_MIN_DAYS = 1.0
COPY_AUTOSELECT_MAX_LOTTERY_SHARE = 0.40
COPY_AUTOSELECT_MAX_CONCENTRATION = 0.60
COPY_AUTOSELECT_MIN_BAND_N_FOR_CONCENTRATION = 5

# ── ARB DECALE (Steven 06/08, "on fabrique notre propre combined ask") ──
# Idee de Steven, validee par SES donnees : on achete la jambe BASSE des
# l'ouverture de la fenetre, puis on attend que le cote oppose descende
# assez pour verrouiller. "Acheter a 45c et attendre l'autre a descendre,
# c'est comme si on fabriquait notre comb ask."
#
# CE QUE DISENT LES DONNEES (tout l'historique, 227 paires) :
#   ecart entre jambes | bons PRIX (nominal<1) | REELLEMENT verrouille | desequilibre
#   simultane <=2s     |        43%            |         43%           |    1.00x
#   decale 2-15s       |        32%            |         11%           |    1.42x
#   decale 15-60s      |        39%            |         16%           |    1.60x
#   decale >60s        |        47%  <-- MEILLEUR |       6%           |    2.51x
# L'arb decale TROUVE les meilleurs prix (47%, mieux que le simultane) mais
# n'en verrouillait que 6%. TOUT l'ecart vient du desequilibre de parts
# (2.51x) : la 2e jambe etait dimensionnee en DOLLARS, or le prix a bouge
# entre les deux achats. Ce n'est donc pas la methode qui etait mauvaise,
# c'est son execution.
#
# POURQUOI CA VAUT LE COUP MAINTENANT : le combine ASK ne descend jamais
# sous 1.00 (mesure : minimum 1.000 sur 172 releves) -> l'arb simultane en
# prise directe est quasi impossible. Mais le combine BID est a 0.979 en
# moyenne (jusqu'a 0.930 sur DOGE). L'argent est sous 1.00, il faut juste
# ne pas payer le spread deux fois.
#
# LES DEUX PROTECTIONS QUI MANQUAIENT (causes des pertes d'hier) :
#  1. parts EGALES sur la 2e jambe (target_shares, plus de budget en $) ;
#  2. jambe 1 FERMABLE : si le combine ne redescend pas, on solde au lieu de
#     tenir jusqu'a zero (-81% de ROI mesure sur les jambes nues gardees).
# Et on ne part JAMAIS d'une jambe chere : a 0.79 il faudrait que l'autre
# tombe sous 0.21 pour verrouiller. La bande d'entree est donc centree
# autour de 0.45, la ou etre nu est proche d'un pile-ou-face et non d'un
# billet de loterie (les jambes a 0.13-0.24 ont fait -100%).
# ACTIF. Note de mesure (Steven 06/08) -- premiere heure d'activite reelle,
# gardee ici comme point de reference, PAS comme argument d'arret : la
# decision d'activer ou non appartient a Steven.
#   5 tentatives : 2 completees (+0.27$) / 3 non completees (-6.23$)
#   NET -5.96$, taux de completion 40% -- echantillon beaucoup trop petit
#   pour conclure quoi que ce soit.
# Ce qu'il faut surveiller quand l'echantillon grandira : le gain moyen d'une
# completion (+0.135$ observe) est structurellement petit face au risque de
# la jambe 1. A suivre sur plusieurs dizaines de tentatives avant de trancher.
# Point technique constate : les 3 jambes perdantes n'ont enregistre AUCUNE
# vente on-chain malgre le stop-loss declenche -- carnet trop mince sur une
# jambe qui s'effondre. C'est la vraie fragilite a corriger en priorite si on
# veut que cette strategie tienne.
# COUPE SUR DECISION DE STEVEN (06/08), apres backtest sur 694 fenetres reelles
# (566k transactions publiques, BTC/ETH/XRP/SOL).
#
# L'arb decale perd de l'argent en TAKER, et ce resultat est solide : negatif
# dans 15 cellules de parametres sur 15, IC95 [-5.97% ; -2.71%], t = -5.35, et
# il survit a un rejeu qui n'utilise PAS la reconstruction 1-p mais les vrais
# prints des deux carnets.
#
# La raison n'est pas les frais, contrairement a ce que j'avais d'abord conclu.
# Quand une entree expire sans verrou elle vaut EXACTEMENT -100%, sur 51 cas
# sur 51 : si notre jambe est gagnante son prix doit TRAVERSER le seuil de
# verrouillage pour monter vers 1, donc on verrouille toujours avant --  seuls
# les perdants atteignent l'expiration. Le pari reel est donc :
#     89% du temps +6.7%   |   11% du temps -100%   ->  -5% d'esperance
# Aucun reglage de frais, de seuil ou de timing ne corrige une asymetrie pareille.
#
# La version maker n'est PAS une alternative : mesuree a -0.37% sur donnees
# propres, IC95 [-2.07% ; +1.09%], indiscernable de zero. Le +1.36% que j'avais
# annonce etait un optimum in-sample sur 384 combinaisons essayees.
STAGGER_ENABLED = False

# ── ARB PRE-OUVERTURE EN MAKER (Steven 06/08) ──────────────────────────
# On POSE les deux jambes (ordres GTC passifs) sur une fenetre 5min PAS
# ENCORE OUVERTE, a un prix dont la somme verrouille l'arb. Si les deux se
# remplissent -> profit garanti. Si une seule -> on annule l'autre et on a
# plusieurs minutes pour solder, la fenetre n'ayant pas commence.
#
# CE QUI REND CA POSSIBLE -- deux mesures independantes :
#
# 1) LES ORDRES MAKER NE PAIENT AUCUN FRAIS. Le bareme dit
#    takerOnly: True, et l'historique le confirme sans ambiguite : sur 851
#    achats a base de frais fiable, la distribution est parfaitement
#    BIMODALE -- 90 trades a 0.000 de frais, 761 a ~0.042, et ZERO trade
#    entre 0.005 et 0.030. Ce n'est ni de l'arrondi ni un gradient : soit on
#    prend (et on paie 4.2%), soit on pose (et on ne paie rien). Le bot
#    prenait dans 89% des cas.
#
# 2) LE CARNET PRE-OUVERTURE EST LARGE SUR LES MARCHES PEU LIQUIDES.
#    Mesure sur 32 releves des fenetres +5min et +10min :
#      DOGE : combine bid 0.940 (stable 0.940-0.970), 84% des releves
#             sous 0.95, profondeur mediane 66 parts
#      XRP  : combine bid 0.980, JAMAIS sous 0.95 (0/32)
#      BTC  : combine bid 0.990, fige
#    Avant ouverture, les market makers tiennent 0.51/0.50 sur les gros
#    marches, mais laissent un spread de 6 centimes par cote sur DOGE.
#
# ECONOMIE : poser a 0.47/0.47 = combine 0.94, zero frais -> +6.4% garanti.
# A comparer avec ce que fait le bot aujourd'hui : prendre a l'ask a 1.010
# PLUS 4.2% de frais = -5.2%.
#
# L'INCONNUE ASSUMEE : le taux de remplissage. Poser a 0.47 quand 66 parts
# y sont deja, c'est entrer dans une file d'attente -- on ne se remplit que
# si un vendeur descend jusqu'a nous. Aucune donnee existante ne permet de
# l'estimer : c'est precisement ce que ce test doit mesurer. D'ou les
# petites tailles et le perimetre restreint.
PREOPEN_ENABLED = True
PREOPEN_SYMBOLS = ("DOGE",)       # perimetre du test ; ETH/BTC gardent l'arb a l'ouverture
# EXCLUSIVITE (Steven 06/08, "protection pour que DOGE n'ouvre pas de
# position et se concentre sur la pre-ouverture") : sur ces symboles, AUCUNE
# autre strategie n'a le droit d'ouvrir. Mesure qui l'a motive : sur la
# fenetre doge-1785993000, la pre-ouverture avait fait son travail
# proprement (une seule jambe remplie -> soldee avant l'ouverture, -0.08$ de
# spread), puis l'arb decale a ouvert PAR-DESSUS 5s apres l'ouverture et a
# perdu -0.91$. Les deux mecanismes se marchaient dessus.
# N'affecte QUE la prise de position : la gestion des positions existantes
# (TP/SL, fermeture des jambes nues) reste entiere.
PREOPEN_EXCLUSIVE = True
# AMELIORATION DU BID (Steven 06/08, apres 6 fenetres a zero remplissage) :
# poses PILE au meilleur bid, on entrait dans une file de 30 a 67 parts et on
# ne se remplissait jamais -- pre-ouverture personne ne traverse le carnet,
# il n'y a que des market makers qui posent. En ajoutant 1 tick (0.01) on
# devient MEILLEUR bid, donc PREMIER servi des qu'un vendeur se presente.
# Cout : le combine passe de ~0.95 a ~0.97, soit +3% au lieu de +5%. Ca
# reste tres superieur a la prise directe (-5.2% apres frais), et c'est le
# seul moyen de trancher : si on ne se remplit toujours pas en tete de file,
# c'est qu'il n'y a AUCUN flux vendeur avant l'ouverture, et aucun prix ne
# nous remplira.
PREOPEN_IMPROVE_TICK = 0.01       # 0 = poser au bid ; 0.01 = prendre la tete de file
PREOPEN_MAX_COMBINED = 0.98       # releve de 0.96 : le bid+1 remonte mecaniquement le combine
PREOPEN_MIN_LEAD_S = 60           # ne pas poser a moins d'1min de l'ouverture
PREOPEN_MAX_LEAD_S = 900          # ni plus de 15min avant (carnet pas encore forme)
# TAILLE (Steven 06/08, "si capital a 20$+ on peut deja mettre plus").
# Il avait raison, et il y avait en plus une ambiguite : l'ancienne fraction
# s'appliquait PAR JAMBE, donc le total engage valait le double -- et avec un
# petit capital elle passait sous le plancher, si bien que c'etait toujours le
# minimum de 5 parts qui decidait. La taille ne bougeait donc jamais, que le
# compte soit a 12$ ou a 50$.
# Ces constantes designent desormais le TOTAL engage sur la fenetre (les deux
# jambes ensemble), ce qui est la grandeur qu'on veut reellement piloter.
# Justification du niveau : le mode d'echec de la pre-ouverture est benin --
# une seule jambe remplie coute le spread (~0.09$ mesure sur 4.71$ engages,
# soit ~2%), position soldee AVANT l'ouverture, jamais de pari subi. C'est
# tres different de l'arb decale ou l'echec coute 100%. On peut donc engager
# une part nettement plus grande du capital.
PREOPEN_TOTAL_FRAC = 0.35         # 35% du capital investissable, les 2 jambes
PREOPEN_BUDGET_MIN = 4.7          # ~5 parts a 0.47 x2 : le vrai plancher CLOB
PREOPEN_BUDGET_MAX = 40.0
PREOPEN_CANCEL_BEFORE_S = 30      # a T-30s : on annule ce qui n'est pas rempli
# Au-dela de cet ecart entre les 2 jambes on ne verrouille PAS tout de suite :
# on laisse la petite jambe se remplir, et a T-30s on solde l'excedent de la
# grosse. Mesure (Steven 06/08) : sur 5 fenetres pre-ouverture, les 4 a 1.00x
# gagnent toutes (+0.90$ / 19.14$), la seule a 2.03x perd -0.71$. 1.05 tolere
# un arrondi de part sans tolerer un vrai desequilibre.
PREOPEN_MAX_IMBALANCE = 1.05
# BANDE D'ENTREE RESSERREE (Steven 06/08). Mesure sur 279 fenetres du type
# "arb decale" (jambe 1 entre 0.30 et 0.60), taux de completion et resultat
# net par tranche de prix d'entree :
#     0.30-0.35 :  34 fenetres, 44% completees, net -22.65$
#     0.35-0.40 :  41 fenetres, 51% completees, net -25.22$
#     0.40-0.45 :  73 fenetres, 70% completees, net -18.58$
#     0.45-0.50 :  57 fenetres, 74% completees, net -11.60$   <- meilleur
#     0.50-0.55 :  55 fenetres, 69% completees, net -12.42$
#     0.55-0.60 :  19 fenetres, 63% completees, net  -5.43$
# Entrer sous 0.44 est doublement mauvais : moins de completions ET plus de
# pertes. Les deux tranches basses concentrent -47.87$ a elles seules, soit
# la moitie de la perte totale, pour un tiers des fenetres. On remonte donc
# le plancher de 0.38 a 0.44.
# PLANCHER ABAISSE A 0.40 (mesure du 06/08, arbs VRAIMENT decales seulement --
# les paires completees en moins de 10s sont des arbs simultanes et ont ete
# retirees, sinon elles gonflent artificiellement les tranches basses) :
#     entree sous 0.40   : 14 fenetres, 21% verrouillees
#     entree 0.40-0.43   : 21 fenetres, 52% verrouillees   <-- la MEILLEURE
#     entree 0.44-0.49   : 34 fenetres, 35% verrouillees
#     entree 0.50 et +   : 35 fenetres, 26% verrouillees
#
# Le plancher a 0.44 excluait donc la meilleure tranche. La bande 0.40-0.49
# prise en bloc verrouille a 42% contre 21% en dessous et 26% au-dessus : c'est
# cette comparaison-la qui est solide (55 fenetres contre 14 et 35). L'ecart
# entre 0.40-0.43 et 0.44-0.49 pris separement, lui, n'est PAS significatif --
# on garde donc toute la bande plutot que de sur-ajuster sur 21 echantillons.
STAGGER_ENTRY_MIN = 0.40
# PLAFOND SOUS 0.50 (Steven 06/08 : "il faut les acheter en dessous de 50c !!").
#
# J'avais monte ce plafond a 0.58 en optimisant la SURVIE DIRECTIONNELLE de la
# jambe -- les tranches hautes gagnent plus souvent, c'est vrai mais hors sujet.
# Ce qui decide du resultat ici, c'est le TAUX DE VERROUILLAGE. Mesure sur 67
# fenetres (26h) :
#     entree sous 0.50   : 48 fenetres, 58% verrouillees, 25% restent seules
#     entree a 0.50 pile :  7 fenetres, 29% verrouillees, 43% restent seules
#     entree au-dessus   : 12 fenetres, 33% verrouillees, 50% restent seules
#
# La raison est mecanique. Pour verrouiller il faut payer la 2e jambe sous
# 1 - p1, et comme les deux asks somment toujours a 1 + spread, la condition
# revient a : NOTRE jambe doit s'apprecier de plus que le spread. Une jambe
# prise a 0.46 est l'outsider : lui revenir a 0.50 est un mouvement de 4
# centimes qui arrive en permanence dans une fenetre de 5 minutes. Une jambe
# prise a 0.52 doit aller a 0.56+, c'est-a-dire PROLONGER un mouvement deja
# entame. A 0.50 pile il n'y a aucun coussin : pile ou face.
STAGGER_ENTRY_MAX = 0.49
STAGGER_MIN_SECS_LEFT = 180       # acheter TOT : il faut du temps pour que ca bouge
STAGGER_COMPLETE_MAX = 0.99       # verrou reel exige pour completer
STAGGER_GIVEUP_SECS = 50          # filet de fin de fenetre (garde en secours)
# SORTIE ANTICIPEE PAR LE TEMPS (Steven 06/08) -- l'amelioration la plus
# importante. Mesure sur 179 completions reussies : le delai entre la jambe 1
# et la completion est de 12s en MEDIANE, et 82% arrivent en moins de 40s
# (p90 = 79s). Passe ce delai, la completion n'arrive quasiment jamais : le
# marche a deja choisi son camp.
# Or on tenait la jambe 1 jusqu'a STAGGER_GIVEUP_SECS (50s de la fin), soit
# jusqu'a 250 SECONDES de risque porte pour rien. Et sur ces marches le prix
# s'effondre vite :
#     btc-1785984300 : 0.500 -> 0.330 en 27s
#     oge-1785991800 : 0.490 -> 0.210 en 74s
#     oge-1785988800 : 0.500 -> 0.170 en 30s
# Sortir tot coute 15-20% ; attendre coute 50-65%. Le stop-loss en pourcentage
# ne suffit pas sur un marche qui bouge par sauts : entre deux cycles il a
# deja traverse le seuil. Une limite de TEMPS, elle, ne peut pas etre sautee.
# DELAI MAX POUR COMPLETER, 45 -> 60s. Sur 109 paires completees, 92% arrivent
# dans les 60 secondes, et la tranche 30-60s est la MEILLEURE (84% de verrous).
# A 45s on coupait donc en plein dans le creneau le plus rentable. Au-dela de
# 60s en revanche, plus que 33% de verrous : ces completions tardives ne sont
# pas des arbs, c'est de la moyenne a la baisse sur une jambe perdante.
STAGGER_MAX_WAIT_S = 60           # sans verrou 60s apres la jambe 1 -> on solde
# FENETRE D'ENTREE (n'existait pas : on pouvait ouvrir une jambe 1 a n'importe
# quel moment des 5 minutes). Arbs vraiment decales, par moment d'entree :
#     0-15s apres l'ouverture : 34 fenetres, 44% verrouillees
#     apres 15s               : 65 fenetres, 26% verrouillees
# L'edge du decale vient de l'oscillation autour de 0.50 juste apres
# l'ouverture ; passe ce moment une tendance s'est formee et le cote oppose ne
# revient plus. Les 3 pertes ETH de ce matin sont entrees a +48s, +80s et
# +105s, aucune n'a jamais eu de 2e jambe.
#
# HONNETETE SUR CE CHIFFRE : 44% contre 26% n'est significatif qu'a la limite
# (z=1.8) et la degradation n'est pas monotone (la tranche 15-45s tombe a 14%,
# puis remonte a 30% ensuite) -- c'est le reglage le moins solide des trois.
# 20s plutot que 15 pour ne pas sur-ajuster sur une frontiere aussi incertaine.
STAGGER_MAX_LEAD_S = 20
STAGGER_STOP_LOSS = 0.30          # jambe 1 qui perd 30% -> on coupe sans attendre
STAGGER_BUDGET_FRAC = 0.10        # part du capital investissable
STAGGER_BUDGET_MIN = 2.0
STAGGER_BUDGET_MAX = 12.0

# EN PAUSE sur decision de Steven (06/08). Il avait d'abord choisi de le garder
# ("il fait gagner peu mais il fait gagner"), puis demande la pause pour ne
# laisser tourner que l'arb instantane et la pre-ouverture.
#
# Ce que dit la mesure, pour le jour ou on le rallumera : la regle NAIVE
# "acheter tout ce qui depasse 0.97 et porter a resolution" est perdante sur
# 694 fenetres -- 95.7% de reussite, mais a 0.98 paye le prix est deja juste,
# ROI -1.74%, et ca reste negatif a tous les ecarts de carnet testes. Nos
# propres near-certain mesuraient +1.6%, l'ecart venant probablement du fait
# que le bot CHOISIT ses moments et qu'une partie des remplissages etaient
# maker. C'est cette selection-la qu'il faudra isoler avant de rallumer.
NEARCERT_ENABLED = False
NEARCERT_MIN_PRICE = 0.95
NEARCERT_MAX_PRICE = 0.98
NEARCERT_MAX_SECS = 120   # tard dans la fenetre = issue deja largement jouee
NEARCERT_MIN_SECS = 8
# Mise relevee 2$ -> 4$ (Steven 05/08, "il faut mettre + que 2 euro pour en
# profiter un max"). L'edge reste MINCE (+1.6%) donc la mise reste bornee :
# toujours plafonnee par investable (cash - floor) et par
# MAX_MARKET_EXPOSURE_USD (8$/fenetre) plus bas dans _try_near_certain --
# doubler la mise ne double pas le risque de ruine, ca amplifie un edge deja
# mesure et positif, sur un cash actuel de ~8$.
NEARCERT_BUDGET_USD = 4.0

# TEMPORAIRE (Steven 02/09, "laisse uniquement oracle travailler, coupe le
# reste") : coupe l'OUVERTURE de toute nouvelle position hors TWAP-ORACLE
# (arb/bothside/fav/near-certain/copy-trade/pre-ouverture/maker-ouvert/
# renfort), pendant qu'il active tous les symboles. Ne touche PAS la gestion
# des positions DEJA ouvertes (SL/TP, orphans, resolution, stagger deja
# engage) -- rien n'est abandonne sans suivi. Repasser a False pour tout
# reactiver.
ORACLE_ONLY_MODE = True

FAV_ENABLED = True
FAV_MIN_PRICE = 0.60
FAV_MAX_PRICE = 0.99  # Steven 01/09 -- "on parie sur le gagnant", exemple donne = 90c
# BANDE DEDIEE A _try_favorite UNIQUEMENT (Steven 02/09, "achete parfois trop
# cher et reduit ses chances de tp, devrait rester proche de 50c") : achats a
# 0.95-0.98 confirmes en prod -- au-dela de ~0.90 le TP devient
# MATHEMATIQUEMENT impossible (palier 5% depuis 0.97 demanderait 1.02$, hors
# plage [0,1]). Constantes SEPAREES de FAV_MIN_PRICE/FAV_MAX_PRICE (qui
# restent le plancher UNIVERSEL utilise ailleurs -- _open_leg, COPY,
# BOTHSIDE-FAV, _open_hedge_pair_impl -- volontairement inchangees) pour ne
# pas rouvrir "jamais sous 0.50$" partout, seulement ici. 0.45 accepte
# explicitement par Steven pour CE mecanisme specifique seulement.
FAV_STRATEGY_MIN_PRICE = 0.45
FAV_STRATEGY_MAX_PRICE = 0.62
FAV_BINANCE_MARGIN = 0.0025   # 0.25% = signal "clair", pas juste "devant"
FAV_MIN_SECS = 10             # Steven 01/09 -- devenu le mecanisme PRINCIPAL, plus
# une strategie de repli rare -- doit pouvoir tirer sur presque toute la fenetre
FAV_MAX_SECS = 280
FAV_BUDGET_USD = 500.0         # Steven 19/08 -- borne de toute facon par investable

# ── TWAP-ORACLE (Steven 02/09) ────────────────────────────────────────────
# Deploye apres backtest 12h reelles (Binance 1s + prix Polymarket RECHERCHE
# reels) : 61/62 gagnants une fois les 2 faux-positifs corriges (fenetres a
# egalite exacte, cf. regle officielle Polymarket "TWAP >= prix de depart ->
# Up"), soit ~+129$/h en simulation avec mise 5$. Mecanisme INDEPENDANT de
# l'arb/favori Polymarket -- se fie exclusivement a la convergence TWAP
# Binance vers le strike (voir WSFeed.twap_oracle_signal). EXCEPTION
# EXPLICITE ET ISOLEE a la regle "jamais sous 0.50$" (decidee avec Steven,
# 02/09) : ce mecanisme ne se fie pas au prix marche donc n'est pas concerne
# par le risque (achat du cote que le marche croit perdant) qui a motive
# cette regle ailleurs -- c'est au contraire son cas le plus rentable en
# backtest (cotes 1-2c ou le marche n'a pas encore rattrape l'evidence).
# HOLD TO RESOLUTION STRICT : strat="twap_oracle" volontairement absent de
# la liste geree par _manage_pnl_tier_exits -> jamais de TP/SL dessus.
TWAP_ORACLE_ENABLED = False  # Steven 03/09 -- pause pour se concentrer sur Steven Engine, remettre a True pour reactiver
TWAP_ORACLE_BET_USD = 5.0      # Steven 02/09 -- plancher demande, "augmenter si ca tourne bien"
TWAP_ORACLE_MIN_SECS_LEFT = 5
TWAP_ORACLE_MAX_SECS_LEFT = 120  # Steven 02/09 -- releve 58->120, voir REGIME PROBABILISTE
# ci-dessous : au-dela de 60s (fenetre TWAP pas encore commencee, la formule
# "convergence" ne peut rien calculer), un second regime prend le relai.
# REGIME PROBABILISTE (Steven 02/09, "problème = solution", predire avant les
# 60 dernieres secondes) : mouvement brownien geometrique (probability_above_
# strike(), deja utilise ailleurs dans le bot pour le sizing Kelly, jamais
# branche sur l'oracle) -- P(le prix finit du bon cote) a partir du spot,
# du strike, du temps restant et de la volatilite 5min MESUREE. Backteste
# sur 12h reelles, 6 marches, sweep de seuil complet : a 120s restantes et
# seuil 95%, 96-100% d'accuracy selon le marche (100% sur BTC/ETH/XRP/DOGE),
# aucun desaccord avec le regime deterministe existant sur 125 fenetres
# co-classifiees (100% d'accord). Bascule automatique a 60s restantes vers
# la formule "convergence" existante (plus precise une fois la fenetre TWAP
# reellement commencee).
TWAP_ORACLE_PROB_THRESHOLD = 0.95
# ESSAI "PARI PRECOCE" (Steven 03/09, "essayons") : 1$ des le tout premier
# pred brut (meme non 'certain'), strat separee de l'oracle normal pour
# comparer les stats sans rien casser du mecanisme existant.
TWAP_ORACLE_EARLY_ENABLED = True
TWAP_ORACLE_EARLY_BET_USD = 5.0  # Steven 03/09 -- releve de 1$ a la demande, malgre l'echantillon encore petit (8 essais, pnl resolu legerement negatif a ce stade)
# Steven 03/09 ("n'ajoute SURTOUT PAS de barriere ici") -> puis REVERSE le
# meme jour ("faut empecher les early under 50c tout court") apres plusieurs
# pertes reelles concentrees sur les entrees tres bon marche (0.01-0.34$,
# le plus de variance = le plus de retournements brutaux avant qu'un TP
# puisse s'executer). Plancher applique desormais a l'ACHAT lui-meme, pas
# seulement au TP apres coup.
TWAP_ORACLE_EARLY_MIN_PRICE = 0.75  # Steven 03/09 -- releve de 0.50 a 0.75
# TP DEDIE au pari precoce (Steven 03/09, "cette ligne aurait declenche TP
# immediatement, laisser le reste courir") : ~81% de win rate seulement
# (backteste) contre 99%+ pour le pari certain -- des le premier gain reel,
# on securise l'essentiel des parts en une fois, valeurs de depart a affiner
# par backtest (voir session).
# DESACTIVE (Steven 03/09, "le tp du early qui vend 75% peut etre mis en
# pause aussi ... bah les deux ca servirait a rien sinon, go !") : le flip
# (<=42c) est mort depuis le plancher d'achat a 0.50 ; le TP general n'a pas
# cette limite de prix (s'applique encore aux entrees 50-99c actuelles) mais
# coupe quand meme sur demande explicite -- remettre a True pour reactiver.
TWAP_ORACLE_EARLY_TP_ENABLED = False
TWAP_ORACLE_EARLY_TP_ARM_PCT = 0.10
TWAP_ORACLE_EARLY_TP_SELL_FRACTION = 0.75
# Steven 03/09 ("regarde la pos early qui a perdu alors qu'on aurait pu TP !
# le reglage passe de under 5c a under 42c") : le flip immediat (vend TOUT
# des le premier tick de gain reel, pas d'attente de 10%) couvrait trop peu
# d'entrees -- releve pour couvrir la plupart des paris precoces, PAS
# TWAP_ORACLE_CHEAP_ENTRY_MAX (qui reste a 5c pour l'oracle 'certain', ne
# pas m'elange les deux mecanismes).
TWAP_ORACLE_EARLY_FLIP_MAX_ENTRY = 0.42

# STEVEN ENGINE (Steven 03/09, "un ami a un bot payant performant, on veut
# EN PLUS de l'oracle faire tourner un moteur base sur son comportement") :
# reproduit du mieux possible la logique "Constellation" vue sur son
# dashboard (config CONSTELLATION/AGGRESSIVE) -- Polymarket fait tourner 6
# marches Up/Down 5min correles (73-85%) sur BTC/ETH/SOL/XRP/DOGE/BNB. Si
# au moins STEVEN_MIN_ASSETS_AGREEING des 6 penchent nettement du meme cote,
# et qu'UN traine derriere (son prix sur ce cote est encore bas, pas encore
# "rattrape"), on parie qu'il rattrape le mouvement avant la cloture --
# AVEC filet de securite explicite (DCA + stop-loss), contrairement a
# l'oracle qui n'a ni l'un ni l'autre par construction (certitude
# statistique plutot que gestion de risque). Ceci est ma RECONSTRUCTION des
# reglages vus sur son ecran (pas son code source) -- backtest impossible
# sans historique multi-actifs aligne a la seconde, donc deploye avec
# logs detailles pour juger sur des resultats reels, comme demande.
STEVEN_ENGINE_ENABLED = True
STEVEN_SYMBOLS = ("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB")
STEVEN_MIN_ASSETS_AGREEING = 4     # "Min assets agreeing" (preset AGGRESSIVE)
STEVEN_LAGGARD_GAP = 0.08          # "Laggard gap"
STEVEN_INITIAL_BUY_USD = 10.0      # "Initial buy ($)"
STEVEN_MAX_CONCURRENT = 4          # "Max concurrent"
STEVEN_BUY_MIN_PRICE = 0.47        # "Buy price range" (traineur, cote laggard)
STEVEN_BUY_MAX_PRICE = 0.62
STEVEN_BANKROLL_USD = 20.0         # "Bankroll ($)" -- expo totale max de ce moteur
STEVEN_MAX_PER_TRADE_USD = 10.5    # "Max per trade ($)" -- plafond initial+DCA
STEVEN_DCA1_ADD_USD = 3.0          # "DCA #1 add ($)"
STEVEN_DCA2_ADD_USD = 2.5          # "DCA #2 add ($)"
STEVEN_DCA_TRIGGER_DROP = 0.09     # "DCA trigger drop ($)" -- par palier depuis l'entree
STEVEN_STOPLOSS_PRICE = 0.20       # "Stop-loss price ($)" -- prix ABSOLU, pas un %
STEVEN_ALLOW_UP = True             # "UP setups"
STEVEN_ALLOW_DOWN = True           # "DOWN setups"
# Steven 03/09 ("le signal c'est que les prix des cryptos bougent ensemble,
# pas le prix du contrat") : seuil minimal de mouvement de PRIX (fraction,
# pas %) pour qu'un actif compte comme "parti" dans un sens -- sous ce seuil,
# c'est juste du bruit de marche, ni Up ni Down.
STEVEN_MOVE_EPSILON = 0.0003  # Steven 03/09 -- backteste 24h : meilleur pnl total (+196.80$/137 signaux)
# VERROU DE PROFIT SUR LA VALEUR (Steven 02/09, "la pos valait 27$ ... il n'a
# pas TP et ca s'est resolu dans l'autre sens ... un TP qui traque la VALEUR
# de la pos, plus focus sur la valeur") : le hold-to-resolution strict expose
# un gain papier enorme (+450% observe) a un retournement total avant la
# cloture. Contrairement au TP normal (paliers fixes 5/15/20%, non applique
# aux positions oracle), celui-ci ne s'arme qu'une fois le gain VRAIMENT
# large (ARM_PCT) -- laisse les petits mouvements tranquilles, protege
# seulement contre l'aller-retour +450% -> perte totale. Vend TOUT (pas de
# palier) au retracement, capital recycle immediatement.
TWAP_ORACLE_TRAIL_ARM_PCT = 1.0     # s'arme a partir de +100% (position doublee)
TWAP_ORACLE_TRAIL_GIVEBACK_PCT = 0.30  # vend si le prix redonne 30% de son pic
# PALIERS POUR LES TICKETS "1c" (Steven 02/09, "les pos a 1c ne devraient
# jamais hold to resolution, souvent il y a quand meme de l'argent a recup --
# sinon quand il achete au-dessus (genre 80c) ca se passe bien") : un ticket
# a 1c est le pari a la variance la PLUS extreme (soit -100% soit +milliers%)
# -- le hold pur + le verrou unique ci-dessus (arme a +100% seulement une
# fois, vend TOUT) laisse tout le gain papier expose jusqu'a ce seuil. En
# dessous de TWAP_ORACLE_CHEAP_ENTRY_MAX, on prend la moitie des parts
# restantes a CHAQUE palier de gain -- au-dessus (achat deja convaincu comme
# 80c), rien ne change, le hold pur continue de bien fonctionner.
TWAP_ORACLE_CHEAP_ENTRY_MAX = 0.05
TWAP_ORACLE_CHEAP_TP_TARGETS = (1.0, 3.0, 8.0)  # +100% / +300% / +800%
# PALIER DE MISE DEGRESSIF PAR PRIX (Steven 02/09, "si ca passe on gagne quand
# meme bcp mais sinon ca perd que 1$") : une mise FIXE de 5$ traite pareil un
# pari a 90c (variance faible) et un pari a 1c (variance maximale : soit -100%
# soit ~+9900%). Le ratio de payout reste enorme meme avec une mise reduite
# sur les cotes extremes -- ca amortit juste le "ouch" d'un raté (confirme en
# prod le 02/09 : le seul raté du lot etait justement un pari a 1c). Cherche
# le premier palier dont le prix-plafond est >= l'ask, sinon TWAP_ORACLE_BET_USD.
TWAP_ORACLE_BET_TIERS = (
    (0.05, 1.0),   # ask <= 5c  -> mise 1$
    (0.10, 2.0),   # ask <= 10c -> mise 2$
    (0.30, 3.0),   # ask <= 30c -> mise 3$
)  # au-dela de 30c : TWAP_ORACLE_BET_USD (5$) plein


def _twap_oracle_bet_usd(ask):
    for max_px, bet in TWAP_ORACLE_BET_TIERS:
        if ask <= max_px:
            return bet
    return TWAP_ORACLE_BET_USD
# TP INSTANTANE UNIVERSEL (Steven 19/08, "meme prix 0.10, 0.35, 0.52, 0.65,
# toujours meme chose") : s'applique a TOUTE position geree par
# _manage_pnl_tier_exits (bothside/swing/fav/nearcert/copy), quel que soit le
# prix d'entree -- contourne les paliers 25/50/75 et sort TOUT des que ce
# seuil de PnL est atteint.
# TRAILING TP (Steven 01/09, "pas forcement que ça grimpe de 15, si ça monte
# 5.6.7.8 puis redescend à 7 on TP à 7") : plus un seuil fixe unique -- des
# qu'un vrai gain est vu (TP_TRAIL_ARM_PCT), on suit le pic et on vend des
# que le prix retombe de TP_TRAIL_GIVEBACK_PCT depuis ce pic, plutôt que
# d'attendre un objectif qui peut ne jamais arriver.
TP_TRAIL_ARM_PCT = 0.03       # arme le suivi des le premier vrai vert
TP_TRAIL_GIVEBACK_PCT = 0.15  # vend si le prix redonne 15% de son pic depuis l'entree
TP_INSTANT_PCT = 0.75  # plafond dur : vend TOUT de suite si atteint, meme
# sans jamais avoir retrace (evite d'attendre indefiniment sur un mouvement
# tres fort et lineaire). Backtest corrige (frais 4% sur GAINS
# uniquement, jamais sur les pertes -- correction du modele precedent qui
# appliquait les frais partout). Sweep 2D SL x TP sur 5656 series reelles :
# +2.85%/trade au reglage precedent (SL 0.5%/TP 2%) vs +6.21%/trade a
# SL=0.2%/TP=50% -- rendement decroissant au-dela de TP=50-75%. Laisser
# courir le gagnant plus loin capture les rares gros mouvements (jusqu'a
# 400% observe) qui dominent l'esperance malgre un win rate plus bas (12%
# contre 26% avant).
# SL REACTIVE (Steven 19/08, correction le meme soir) : la desactivation
# precedente reposait sur seulement 235 series (petit buffer memoire, pas
# representatif). Sur le dataset RECHERCHE complet (142 135 lignes, 5656
# series), le SL serre (voir PNL_SL_PCT) est nettement meilleur que pas de
# SL du tout. Voir le commentaire de PNL_SL_PCT pour le detail du sweep.
TP_INSTANT_SL_DISABLED = True  # Steven 02/09 -- coupe le SL (execution trop lente/erratique
# ce soir, cf. incidents -33%/-28.7%), on laisse vivre les positions. Le TP reste actif.
SPREAD_EXIT_DISABLED = True  # Steven 02/09 -- "SPREAD-EXIT aussi sur off" -- meme logique
# que TP_INSTANT_SL_DISABLED : coupait une jambe des qu'elle sous-performait l'autre de 10%,
# independamment du SL desactive ci-dessus. On laisse vivre les positions partout pareil.
# MARKET MAKING ASYMETRIQUE (Steven 19/08, "poser un ordre d'achat + un ordre
# de revente, encaisser le spread en boucle") : des qu'une position remplit,
# on pose IMMEDIATEMENT un ordre de vente GTC passif a entree+SPREAD, en plus
# du TP instantane au marche qui reste le filet de secours.
SPREAD_CAPTURE_PRICE = 0.01

# SNIPE OVER-REACTION (Steven 19/08, "le carnet se vide temporairement a
# cause de la panique") : si le prix Polymarket a chute bien plus que le
# mouvement reel Binance sur la meme fenetre, on achete le cote qui a
# sur-reagi en pariant sur le retour a la moyenne.
OVERREACT_ENABLED = False  # Steven 01/09 -- desactive, conflit direct avec
# "jamais d'achat sous 0.50$" : cette strategie achete PAR CONCEPTION le
# cote qui vient de s'effondrer (retour a la moyenne), donc toujours sous
# le plancher par construction.
OVERREACT_LOOKBACK_S = 120
OVERREACT_MULT = 3.0
OVERREACT_MIN_POLY_DROP = 0.05
OVERREACT_BUDGET_USD = 500.0  # borne par investable, comme FAV_BUDGET_USD
OVERREACT_MIN_SECS = 20
OVERREACT_MAX_SECS = 250

# CALCULATEUR TWAP (Steven 19/08, "l'impossibilite mathematique du marche").
# La resolution reelle des marches 5min utilise le TWAP Chainlink 30s (voir
# docs.polymarket.com/market-data/chainlink-twap) : moyenne du prix sur les
# 30 DERNIERES secondes de la fenetre, comparee au prix de depart. Dans cette
# fenetre de 30s, une fois qu'une bonne partie s'est ecoulee avec un ecart
# suffisant, le pire mouvement plausible restant ne peut plus faire basculer
# la moyenne -- c'est calculable, pas une intuition.
# HONNETETE : "mathematiquement impossible" suppose un pire mouvement borne
# (TWAP_MAX_MOVE_PCT_S). Le crypto peut gapper plus vite qu'un historique
# recent -- ce n'est donc pas une garantie absolue a 100%, d'ou une marge de
# securite (le pire cas des DEUX bornes doit s'accorder, pas juste la
# moyenne actuelle).
TWAP_LOCK_ENABLED = True
TWAP_WINDOW_S = 30            # duree reelle de la fenetre TWAP Chainlink (5min markets)
TWAP_MIN_ELAPSED_S = 15       # assez de la fenetre TWAP deja ecoulee pour juger
TWAP_MAX_MOVE_PCT_S = 0.0006  # 0.06%/s = mouvement extreme plausible (marge large)
TWAP_LOCK_BUDGET_FRAC = 0.95  # quasi tout le capital investissable
TWAP_LOCK_MAX_PRICE = 0.995   # jamais payer plus que ca (rejet Polymarket a 1.0 pile)

# MARKET MAKING PAR SPLIT (Steven 19/08, analyse wallet 0x6748...ee08).
SPLIT_MAKER_ENABLED = True
SPLIT_MAKER_MIN_SECS = 60      # assez de temps pour revendre les 2 jambes
SPLIT_MAKER_BUDGET_USD = 500.0  # borne par investable de toute facon
SPLIT_MAKER_MIN_BUDGET_USD = 1.0  # $1 = 1 part de chaque -> en dessous, poussiere

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
PNL_TP_TARGETS = (0.05, 0.15, 0.20)  # Steven 02/09 -- 3e palier resserre 35%->20%
PNL_TRAIL_ACTIVATION = 0.25  # trailing s'armee des TP1 (+25%)
PNL_TRAIL_GIVEBACK = 0.10  # 10% du pic depuis le palier atteint -> vente runner
SL_CONFIRM_S = 2.0  # Steven 02/09 -- la perte doit persister ce nb de secondes avant de vendre (filtre le bruit court)
PNL_SL_PCT = 0.001  # Steven 01/09 -- resserre 0.2%->0.1%, coherent avec le
# sweep qui s'ameliorait de facon monotone jusqu'a l'extreme testee. Steven 19/08 -- 2e correction le meme soir : le modele
# de frais initial (4% sur CHAQUE sortie) etait faux -- les frais ne
# s'appliquent que sur les GAINS, jamais sur les pertes (confirme). Refait
# le sweep 2D SL x TP sur les 5656 series reelles avec le bon modele :
# SL=0.2%/TP=50% (voir TP_INSTANT_PCT) donne +6.21%/trade contre
# +2.85%/trade au reglage precedent (SL 0.5%/TP 2%). Le SL plus serre
# coupe des pertes minuscules sans frais associes ; le TP plus large laisse
# le gagnant capturer les gros mouvements (jusqu'a 400% observes) qui
# dominent l'esperance malgre un WR plus bas (12%).
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

# BNB ajoute 06/08 (Steven, "ajoute les nouveaux marches"). Verifie avant
# branchement : marche 5m present sur les 3 prochaines fenetres, carnet a
# ~13000 parts de profondeur (comparable a SOL/XRP/DOGE), et surtout paire
# BNBUSDT cotee sur Binance -- sans flux de prix le bot serait aveugle.
# HYPE a ete ECARTE pour cette raison : le marche Polymarket existe mais il
# n'y a aucune paire HYPE sur Binance (HYPER* est un autre jeton).
SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB"]
# SOL/XRP/DOGE ajoutes 21/07 en PAPER uniquement : on collecte des donnees par
# marche (chacun a sa propre volatilite -> ses propres seuils, cf. ETH qui bouge
# trop peu pour les marges calibrees BTC) avant d'envisager le reel.
DEFAULT_MODES = {
    "BTC": "real",
    "ETH": "paper",
    "SOL": "paper",
    "XRP": "paper",
    "DOGE": "paper",
    # BNB en REEL des le depart (Steven 06/08, demande explicite d'augmenter le
    # volume) : le laisser en paper n'aurait produit aucun trade, donc aucun
    # volume supplementaire. C'est defendable ici parce que l'arb instantane est
    # purement arithmetique -- il compare les deux prix d'un meme marche et
    # n'exige rien du comportement du sous-jacent. Basculable depuis le
    # tableau de bord si tu veux l'observer en paper d'abord.
    "BNB": "real",
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
    "BNB": "hold",
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
MOMENTUM_FALLBACK_ENABLED = False  # Steven 02/09 -- desactive par coherence
# (achete PAR CONCEPTION sous 0.50$, meme logique qu'OVERREACT). Verifie :
# _try_momentum_fallback n'a aucun appelant dans le fichier -- code mort,
# aucun risque actif, mais le flag doit refleter l'intention.
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
        self._md_ts = {}    # throttle de la collecte de marche (cf. _collect_market_data)
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
        """Active le cooldown post-abort pour un slug.
        NB (Steven 05/08) : _in_cooldown() est desactive depuis le 30/07
        ("retire tous les cooldown existants") -> cet appel n'a plus d'effet
        bloquant. C'est MAX_MARKET_EXPOSURE_USD (ci-dessous) qui joue
        desormais le role de garde-fou anti-reentree, et lui ne depend
        d'aucun cooldown."""
        now = time.time()
        mk.setdefault("cooldowns", {})[slug] = now + SLUG_COOLDOWN_SECS

    def _hedge_would_break(self, mk, slug, side, own_price, entry_price):
        """Vendre CETTE jambe casserait-il une couverture encore en place ?

        REGRESSION CORRIGEE (Steven 05/08). En posant is_risk_free seulement
        sur les paires reellement verrouillees (_tag_pair_lock), j'ai rendu le
        SL et le SPREAD-EXIT actifs sur les paires DESEQUILIBREES -- qui ne
        sont pas verrouillees mais restent des couvertures. Resultat mesure en
        production, deux fenetres consecutives :
          ETH  +0s BUY Up 6.35@0.630 | +7s BUY Down 2.94@0.340 (comb 0.97)
              +18s SELL Down @0.160  -> Up reste a nu -> -4.71$
          SOL  +0s BUY Up 4.80@0.740 | +3s BUY Down 4.35@0.230 (comb 0.97)
              +10s SELL Down @0.190  -> Up reste a nu -> -3.89$
        Chiffrage sur ETH : paire gardee = +1.35$ si Up gagne / -2.06$ si Down
        gagne. Apres avoir coupe la couverture : +1.79$ / -4.56$. On a donc
        MULTIPLIE PAR DEUX le risque a la baisse pour encaisser 0.44$.

        Regle : tant que la jambe opposee est detenue, couper la jambe
        PERDANTE est destructeur -- c'est precisement elle qui borne la perte.

        EXEMPTION GAGNANTE SUPPRIMEE (Steven 11/08). La version precedente
        laissait passer la vente d'une jambe GAGNANTE ("c'est comme ca qu'on
        encaisse un TP"). Faux raisonnement : ce qui rend une jambe protectrice
        n'est pas son signe, c'est son EXISTENCE en face de l'autre. Mesure en
        production le 11/08 a 20:41 sur ETH :
          20:41:37 BUY Down 3.75@0.260   (couvre Up 6.67@0.600)
          20:41:43 SELL Down 3.75@0.270  -> +0.038$ encaisse, exemption
                                            "jambe gagnante" appliquee
          20:42:32 Up reste a nu, solde en catastrophe @0.450 -> -1.000$
        On a vendu la couverture pour 3.8 centimes et paye 1.00$. Desormais
        une jambe couverte est intouchable, gagnante ou non : le seul volume
        vendable est l'EXCEDENT non apparie, et il a son propre chemin
        (_solder_excedent), qui ne passe pas par ce garde-fou."""
        if own_price is None or entry_price is None:
            return False
        for k, other in mk.get("open", {}).items():
            if other is None or k == f"{slug}|{side}":
                continue
            if other.get("slug") != slug:
                continue
            if other.get("side") in (None, side):
                continue
            if (other.get("filled_shares") or 0) > 0.01 and not other.get("must_close"):
                return True
        return False

    # ── VERROU REEL D'UNE PAIRE (Steven 05/08) ──────────────────────────
    @staticmethod
    def _poly_fee(price, shares, sens="vente"):
        """Frais Polymarket : rate * min(p, 1-p) * parts (formule officielle,
        champ feeSchedule de l'API). Maximum au milieu du carnet, quasi nul
        aux extremes -- c'est pour ca que le near-certain a 0.96 paie tres peu
        alors qu'une paire a 0.50/0.50 paie plein pot.

        LES FRAIS SONT ASYMETRIQUES -- mesure le 13/08 sur l'historique
        on-chain reel du compte (Steven : "je suis bronze sur polymarket,
        les frais ne nous concernent pas tant que ca") :

            ACHATS : 317 trades, ecart median 0.00%, et 94% d'entre eux a
                     ecart RIGOUREUSEMENT nul -> aucun frais preleve
            VENTES :  70 trades, ecart median -4.27%, taux implicite median
                     0.0595 -> frais bien reels, et PLUS ELEVES que ce que
                     le bot supposait

        Le code appliquait 0.051 DANS LES DEUX SENS. Deux erreurs de signe
        oppose, qui se cumulaient dans la meme direction :
          - il facturait des frais fantomes sur la COMPLETION (un achat),
            donc sous-estimait son gain et en refusait de rentables ;
          - il sous-estimait les frais de VENTE (0.051 au lieu de ~0.06),
            donc surestimait le produit d'un TP ou d'un abandon.
        Resultat : vendre paraissait meilleur qu'en realite, acheter la
        seconde jambe paraissait pire. Exactement a l'envers.

        CAS CONCRET, fenetre XRP 10:40-10:45 ET : completion de 11 parts a
        0.62 sur une jambe a 0.35 (combine 0.97). Le bot calculait
        +0.117$ apres des frais qui n'existent pas ; le gain reel est
        +0.33$, ce qu'affichait l'interface Polymarket.

        `sens` vaut "achat" ou "vente". La valeur par defaut reste "vente",
        c'est-a-dire le comportement prudent : un appelant qui ne precise
        rien continue de payer le taux le plus cher.

        A RE-MESURER si le palier de frais du compte change (bronze ->
        superieur) : ces deux taux sont empiriques, pas contractuels.
        """
        try:
            p = float(price)
            n = float(shares)
        except (TypeError, ValueError):
            return 0.0
        if n <= 0 or not (0 < p < 1):
            return 0.0
        taux = POLY_FEE_RATE_ACHAT if sens == "achat" else POLY_FEE_RATE_VENTE
        return taux * min(p, 1 - p) * n

    def _pair_net_after_fees(self, px_a, sh_a, px_b, sh_b,
                             maker_a=False, maker_b=False):
        """Gain net GARANTI d'une paire, FRAIS INCLUS.

        C'est le seul critere qui compte : le payout du pire cas est
        min(parts_a, parts_b) et le cout inclut les frais des DEUX jambes.
        Sans ca, le bot loggait "gain garanti +0.09$" sur une paire qui
        perdait 0.07$ une fois les frais payes (constate on-chain le 06/08 :
        DOGE 4.65 parts a 0.430 + 4.65 a 0.550, annonce 4.56$ de cout, reel
        4.72$).

        maker_a / maker_b (Steven 11/08) : une jambe servie en APPORTEUR ne
        paie AUCUN frais. Les compter comme preneuses sous-estime la marge
        et fait declarer "PAS un arb" des paires reellement verrouillees.
        Constate en direct sur btc-updown-5m-1786479900 : 6.77 parts a
        0.35 + 0.63, les DEUX servies en apporteur -> brut +0.135$, donc un
        vrai verrou ; le bot a compte 0.249$ de frais jamais payes, conclu
        "manque 0.11$", laisse TP/SL actifs, et le TP a alors demonte la
        paire jambe par jambe jusqu'a 1.7 contre 5.1 parts -- fabriquant de
        toutes pieces le desequilibre qu'il fallait eviter."""
        try:
            payout = min(float(sh_a), float(sh_b))
        except (TypeError, ValueError):
            return -999.0
        # Les deux jambes d'une paire sont des ACHATS -> sens="achat".
        # Mesure du 13/08 : les achats ne paient rien, meme en preneur. Les
        # drapeaux maker_a/maker_b deviennent donc redondants ici, mais on
        # les garde : ils resteront justes si le palier de frais change.
        cout = (
            float(px_a) * float(sh_a)
            + (0.0 if maker_a else self._poly_fee(px_a, sh_a, "achat"))
            + float(px_b) * float(sh_b)
            + (0.0 if maker_b else self._poly_fee(px_b, sh_b, "achat"))
        )
        return round(payout - cout, 4)

    def _pair_is_locked(self, shares_a, cost_a, shares_b, cost_b):
        """Une paire n'est un arb GARANTI que si le payout du PIRE cas couvre
        le cout total. Sur un marche binaire le gagnant paie 1$ PAR PART, donc
        le pire cas vaut min(parts_a, parts_b) -- pas la moyenne, pas le total.

        C'est la seule definition qui tienne, et elle etait absente du code :
        `is_risk_free=True` etait pose des que les DEUX jambes etaient
        remplies, sans jamais verifier le verrou. Or ce tag EXEMPTE la
        position de toute gestion TP/SL (elle est censee rider jusqu'a une
        resolution garantie). Un faux arb tagge risk-free est donc le pire
        des deux mondes : expose comme un pari directionnel, mais protege de
        rien. Mesure on-chain sur 27.9h : 62 paires non verrouillees pour
        -41.79$, contre 16 vraies paires verrouillees a +11.23$.

        Retourne (locked: bool, marge_$: float)."""
        try:
            worst_payout = min(float(shares_a or 0), float(shares_b or 0))
            total_cost = float(cost_a or 0) + float(cost_b or 0)
        except (TypeError, ValueError):
            return False, 0.0
        return worst_payout > total_cost, round(worst_payout - total_cost, 3)

    def _tag_pair_lock(self, pos_a, pos_b, combined, tag=""):
        """Pose is_risk_free UNIQUEMENT si la paire est reellement verrouillee.
        Sinon la laisse geree normalement (TP/SL actifs) : mieux vaut une
        position surveillee qu'un faux arb abandonne a la resolution."""
        if not pos_a or not pos_b:
            return False
        # FRAIS INCLUS (Steven 06/08) : _pair_is_locked compare payout et cout
        # BRUTS. Or les frais Polymarket (rate * min(p,1-p) * parts) ne sont
        # pas dans "cost" -- d'ou un "gain garanti +0.09$" annonce sur une
        # paire qui perdait 0.07$ reellement. On recalcule le net frais inclus
        # a partir des prix d'entree, et on n'accorde is_risk_free que si ce
        # net est POSITIF : une paire qui perd apres frais doit rester geree
        # en TP/SL, pas etre tenue jusqu'a resolution comme un profit acquis.
        # DESEQUILIBRE DE PARTS = LA PREMIERE SOURCE DE PERTE (Steven 06/08).
        # Mesure sur nos 80 arbs instantanes reels (63 heures) :
        #     parts equilibrees <=1.05x : 28 arbs, ROI  +0.62%
        #     1.05 a 1.30x              : 15 arbs, ROI  -5.14%
        #     au-dela de 1.30x          : 37 arbs, ROI -14.03%
        # 46% de nos arbs sont severement desequilibres et portent 89% des
        # pertes totales. C'est mecanique : le gagnant paie 1$ par PART, donc
        # seul min(parts) est couvert -- l'excedent de la grosse jambe est un
        # pari directionnel nu, pas de l'arb.
        #
        # On MARQUE l'excedent ici, on ne le vend PAS : _tag_pair_lock est
        # appele depuis des chemins qui detiennent deja _order_lock, et
        # _sell_orphan prend ce meme verrou non reentrant -- vendre ici
        # bloquerait le bot. La vente est faite par la boucle de sortie, qui
        # appelle deja _sell_orphan hors verrou.
        _sa = pos_a.get("filled_shares") or 0
        _sb = pos_b.get("filled_shares") or 0
        if _sa > 0 and _sb > 0:
            _imb = max(_sa, _sb) / min(_sa, _sb)
            if _imb > PAIR_MAX_IMBALANCE:
                _gros = pos_a if _sa > _sb else pos_b
                _exc = round(abs(_sa - _sb), 2)
                if _exc >= MIN_SELL_SHARES:
                    _gros["excedent_a_solder"] = _exc
                    self._log(
                        f"⚖️ [DESEQUILIBRE]{tag} {_sa:.2f} contre {_sb:.2f} parts "
                        f"({_imb:.2f}x) -> {_exc:.2f} parts en trop sur "
                        f"{_gros.get('side')}, a solder : non couvertes par l'arb"
                    )
        # le verrou se juge sur les parts REELLEMENT appariees, donc apres
        # deduction de l'excedent qui va etre solde
        _eff_a = _sa - (pos_a.get("excedent_a_solder") or 0)
        _eff_b = _sb - (pos_b.get("excedent_a_solder") or 0)
        margin = self._pair_net_after_fees(
            pos_a.get("entry_price"), _eff_a,
            pos_b.get("entry_price"), _eff_b,
            maker_a=bool(pos_a.get("maker_fill")),
            maker_b=bool(pos_b.get("maker_fill")),
        )
        locked = margin > 0
        for _p in (pos_a, pos_b):
            _p["arb_combined"] = round(combined, 4) if combined else None
            _p["arb_edge"] = round(1 - combined, 4) if combined else None
            _p["arb_locked"] = locked
            _p["arb_lock_margin"] = margin
            _p["is_risk_free"] = locked
        if locked:
            self._log(
                f"🔒 [PAIRE-VERROUILLEE]{tag} pire cas {min(pos_a.get('filled_shares', 0), pos_b.get('filled_shares', 0))} parts "
                f"vs cout {round((pos_a.get('cost') or 0) + (pos_b.get('cost') or 0), 2)}$ "
                f"-> gain garanti {margin:+.2f}$ (tenue jusqu'a resolution)"
            )
        else:
            # ALARME PERTE GARANTIE (Steven 13/08) : 2 completions XRP sur 55
            # ont ete trouvees a combine 1.04-1.05 -- perte assuree DES
            # L'ACHAT, pas un simple manque a gagner. Le chemin MSF verifie
            # (_manage_maker_open, 2 controles MIN_GAIN distincts, relus
            # ligne par ligne) est correct : combine<=1.05 ET gain>0.02$ sont
            # bien exiges avant tout achat de 2e jambe. Les 2 cas trouves
            # n'ont donc PAS pu passer par ce chemin -- ils viennent d'ailleurs
            # (stagger/near_certain/favorite/preopen, chemins qui n'ont pas ce
            # meme garde-fou), sans certitude sur lequel exactement. Plutot que
            # de deviner et de risquer une regression sur un code que je ne
            # comprends pas encore assez, on rend le symptome IMPOSSIBLE A
            # MANQUER : `strat`/`maker_open` identifient la source, et le seuil
            # de -0.01$ (perte franche, pas juste "pas encore assez") isole le
            # signal du bruit des paires legitimement non verrouillees.
            if margin < -0.01:
                self._tlog(
                    f"pairelock_perte_garantie_{pos_a.get('slug','?')}",
                    f"🚨 [PAIRE-PERTE-GARANTIE]{tag} strat_a={pos_a.get('strat')} "
                    f"strat_b={pos_b.get('strat')} maker_open_a={pos_a.get('maker_open')} "
                    f"combine={round(combined,4) if combined else '?'} "
                    f"perte={margin:+.2f}$ -> ACHETEE A PERTE ASSUREE, "
                    f"identifier le chemin d'entree pour corriger a la source"
                )
            self._log(
                f"⚠️ [PAIRE-NON-VERROUILLEE]{tag} pire cas "
                f"{min(pos_a.get('filled_shares', 0), pos_b.get('filled_shares', 0))} parts "
                f"vs cout {round((pos_a.get('cost') or 0) + (pos_b.get('cost') or 0), 2)}$ "
                f"-> manque {abs(margin):.2f}$ : PAS un arb, TP/SL laisses ACTIFS"
            )
        return locked

    # ── EXPOSITION CUMULEE PAR MARCHE (Steven 05/08, spec ENGINEBTB3 s.10) ──
    def _slug_spent(self, mk, slug):
        """Total $ REELS deja engages a l'achat sur cette fenetre de 5 min."""
        return round(mk.setdefault("slug_spent", {}).get(slug, 0.0), 2)

    def _add_slug_spent(self, mk, slug, usd):
        """Comptabilise un achat reel dans l'exposition de la fenetre."""
        if not slug or usd is None or usd <= 0:
            return
        d = mk.setdefault("slug_spent", {})
        d[slug] = round(d.get(slug, 0.0) + usd, 2)

    def _investable(self):
        """Capital reellement engageable = cash - plancher. Base de tous les
        plafonds proportionnels (Steven 06/08)."""
        cash, _ = self._read_cash()
        if cash is None:
            return 0.0
        return max(0.0, cash - self.floor())

    def _partitioned_investable(self):
        """Partitionnement du capital entre marches actifs (Steven 19/08,
        "Liquidity Manager" -- eviter qu'un marche engage tout le cash et
        bloque les autres au meme instant, tous les marches 5min fermant en
        meme temps). Divise l'investissable par le nombre de symboles en
        mode='real' -- poche egale par marche, pas de sous-comptes figes en
        dur (s'adapte si un symbole est desactive)."""
        n_actifs = sum(1 for s in SYMBOLS if self.state["modes"].get(s) == "real")
        return round(self._investable() / max(1, n_actifs), 2)

    def _max_market_exposure(self):
        """Plafond d'exposition par marche, PROPORTIONNEL au capital.
        Grandit avec le compte au lieu de rester fige (cf. commentaire de
        MAX_MARKET_EXPOSURE_FRAC). Le plancher fixe reste actif pour ne pas
        bloquer les petits comptes, le plafond absolu pour ne pas concentrer
        tout le capital sur une seule fenetre de 5 minutes."""
        return round(
            min(
                MAX_MARKET_EXPOSURE_CEIL,
                max(MAX_MARKET_EXPOSURE_USD, self._investable() * MAX_MARKET_EXPOSURE_FRAC),
            ),
            2,
        )

    def _nearcert_budget(self):
        """Mise near-certain, PROPORTIONNELLE au capital mais volontairement
        prudente (~1/4 de Kelly) : l'edge mesure est mince (+1.6%) et le WR
        reel est incertain a +/-3.2 points sur 182 jambes -- a 92% de WR le
        Kelly devient negatif. On ne mise donc jamais gros dessus."""
        return round(
            min(
                NEARCERT_BUDGET_CEIL,
                max(NEARCERT_BUDGET_USD, self._investable() * NEARCERT_BUDGET_FRAC),
            ),
            2,
        )

    def _maker_open_expo_max(self):
        """Plafond d'exposition propre a MSF (voir MAKER_OPEN_EXPO_FRAC).

        Meme forme que _max_market_exposure -- plancher pour les petits
        comptes, garde-fou absolu en haut -- mais avec une fraction qui suit
        reellement le budget MSF, donc SANS le palier de 8$ ni le plafond
        definitif a 0.25 qui rendaient MAKER_OPEN_TOTAL_FRAC inoperant.
        """
        return round(
            min(
                MAX_MARKET_EXPOSURE_CEIL,
                max(MAKER_OPEN_EXPO_MIN, self._investable() * MAKER_OPEN_EXPO_FRAC),
            ),
            2,
        )

    def _exposure_ok(self, sym, mk, slug, add_usd, cap=None):
        """Refuse un achat qui ferait depasser le plafond d'exposition de ce
        marche. Filet STRUCTUREL contre les boucles de re-entree : peu importe
        quel bug ou quelle strategie relance l'achat, le cumul par fenetre est
        borne. Retourne (ok: bool, detail: str).

        `cap` permet a une strategie d'imposer SON plafond (MSF passe le sien,
        cf. _maker_open_expo_max). Absent -> plafond generique inchange."""
        spent = self._slug_spent(mk, slug)
        _cap = cap if cap is not None else self._max_market_exposure()
        if spent + (add_usd or 0.0) <= _cap:
            return True, ""
        return False, (
            f"expo={spent:.2f}$+{(add_usd or 0.0):.2f}$ > max={_cap:.2f}$"
        )

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

        mom = _momentum(sym) if sym in ("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB") else None
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

    # ── STEVEN ENGINE : config reglable depuis le dashboard (Steven 03/09,
    # "je veux aussi la meme page de parametres pour le Steven engine") --
    # meme pattern que arb_budget() : les STEVEN_* du haut de fichier ne
    # servent plus que de valeurs PAR DEFAUT, self.state["steven_engine"]
    # est la source de verite une fois modifiee depuis le dashboard.
    STEVEN_CONFIG_BOUNDS = {
        "min_assets_agreeing": (2, 6),
        "laggard_gap": (0.01, 0.30),
        "initial_buy_usd": (1.0, 50.0),
        "max_concurrent": (1, 6),
        "buy_min_price": (0.05, 0.90),
        "buy_max_price": (0.10, 0.95),
        "bankroll_usd": (5.0, 500.0),
        "max_per_trade_usd": (1.0, 100.0),
        "dca1_add_usd": (0.0, 50.0),
        "dca2_add_usd": (0.0, 50.0),
        "dca_trigger_drop": (0.01, 0.50),
        "stoploss_price": (0.0, 0.90),  # 0.0 = desactive (Steven 03/09, "faut couper le stop loss")
        "move_epsilon": (0.0001, 0.01),
        "avoid_min_price": (0.0, 1.0),
        "avoid_max_price": (0.0, 1.0),
        "streak_reversal_n": (2, 30),
        "confirmation_secs": (0, 120),
        "size_scale_max": (1.0, 5.0),
        "multi_laggard_max": (1, 6),
    }
    STEVEN_DCA_MODES = ("standard", "off", "capped", "on_confirm")
    STEVEN_PRESETS = ("selective", "balanced", "aggressive")

    def steven_config(self):
        defaults = {
            "enabled": STEVEN_ENGINE_ENABLED,
            "min_assets_agreeing": STEVEN_MIN_ASSETS_AGREEING,
            "laggard_gap": STEVEN_LAGGARD_GAP,
            "initial_buy_usd": STEVEN_INITIAL_BUY_USD,
            "max_concurrent": STEVEN_MAX_CONCURRENT,
            "buy_min_price": STEVEN_BUY_MIN_PRICE,
            "buy_max_price": STEVEN_BUY_MAX_PRICE,
            "bankroll_usd": STEVEN_BANKROLL_USD,
            "max_per_trade_usd": STEVEN_MAX_PER_TRADE_USD,
            "dca1_add_usd": STEVEN_DCA1_ADD_USD,
            "dca2_add_usd": STEVEN_DCA2_ADD_USD,
            "dca_trigger_drop": STEVEN_DCA_TRIGGER_DROP,
            "stoploss_price": STEVEN_STOPLOSS_PRICE,
            "allow_up": STEVEN_ALLOW_UP,
            "allow_down": STEVEN_ALLOW_DOWN,
            # AJOUTS (Steven 03/09, "je dois retrouver chaque parametre
            # present sur screen") : preset affiche cote UI seulement (les
            # champs numeriques ci-dessus restent la source de verite reelle
            # une fois modifies), mode DCA (comportement, pas juste des
            # montants), bande a eviter, heures coupees, inversion sur serie.
            "consensus_preset": "aggressive",
            "dca_mode": "standard",
            "avoid_min_price": 0.0,
            "avoid_max_price": 0.0,
            "skip_hours": [],
            "streak_reversal_enabled": False,
            "streak_reversal_n": 7,
            # Steven 03/09 : backteste sur 24h reelles, 0.03% donne le meilleur
            # pnl total (+196.80$/137 signaux) contre 0.05% (+183.54$/87) --
            # meilleur compromis volume/qualite que la valeur initiale.
            "move_epsilon": STEVEN_MOVE_EPSILON,
            # Steven 03/09 ("ca ne devrait pas arriver, enquete sur la
            # solution" -- retournement juste apres l'entree) : exige que la
            # DIRECTION tienne avant d'executer (voir plus bas : confirme
            # desormais sur le sens, pas le traineur exact, qui change trop
            # souvent). Backteste a 30s : win_rate 62.2%->67.3%,
            # pnl/trade +1.56$->+2.42$. Reduit a 20s (Steven 03/09, "je
            # voudrais qu'on le set a 20sec"). 0 = desactive (immediat).
            "confirmation_secs": 20,
            # Steven 03/09 ("un manque a gagner de 90e toute la soiree, on
            # teste un reverse engine, au pire on le coupera") : verifie sur
            # 10 vrais trades recents (verite Binance) -- pnl reel -14.40$
            # vs +16.95$ si inverse, meme mise/prix. ESSAI, togglable
            # instantanement depuis le dashboard.
            "reverse_mode": False,
            # Steven 03/09 ("comment optimiser d'avantage ?") : backteste par
            # symbole choisi comme traineur (reglages prod actuels, 24h reelles) :
            # DOGE -4.14$/trade (33% WR), BTC -0.30$/trade malgre n=42 (pas du
            # bruit) -- les deux moins bons. ETH +1.90$/trade et BNB +1.04$/trade
            # nettement meilleurs. Vide par defaut, wird via dashboard/API.
            "excluded_symbols": [],
            # Mise proportionnelle au gap_ratio (plus le traineur est en retard,
            # plus fort est historiquement le signal) : budget = initial_buy_usd
            # * min(size_scale_max, 1 + gap_ratio). False = mise fixe (avant).
            "size_scale_by_gap": True,
            "size_scale_max": 2.0,
            # Veto si l'oracle (convergence TWAP, meme actif) est en
            # desaccord avec le signal Steven Engine -- backteste : accord
            # +0.30$/trade (n=152) vs desaccord -1.31$/trade (n=8), veto net
            # +45.56$ contre +35.04$ sans filtre sur le meme echantillon.
            "oracle_veto_enabled": True,
            # MULTI-TRAINEURS (Steven 03/09, "qu'il ne vise pas qu'un seul
            # traineur, poste sur les 2-3-4 marches en meme temps selon
            # bankroll dispo") : au lieu de n'ouvrir QUE le meilleur gap, on
            # ouvre jusqu'a multi_laggard_max marches par cycle (bornes par
            # max_concurrent et la bankroll restante comme toujours). Si
            # aucun traineur ne franchit laggard_gap mais que le consensus
            # tient, multi_laggard_fallback parie quand meme sur les plus a
            # la traine plutot que de laisser passer le cycle.
            "multi_laggard_max": 3,
            "multi_laggard_fallback": True,
        }
        saved = self.state.get("steven_engine") or {}
        defaults.update({k: v for k, v in saved.items() if k in defaults})
        return defaults

    def set_steven_config(self, patch):
        if not isinstance(patch, dict):
            return {"ok": False, "message": "payload invalide"}
        cfg = self.steven_config()
        for k, v in patch.items():
            if k in ("enabled", "allow_up", "allow_down", "streak_reversal_enabled", "reverse_mode", "size_scale_by_gap", "oracle_veto_enabled", "multi_laggard_fallback"):
                cfg[k] = bool(v)
                continue
            if k == "excluded_symbols":
                if not isinstance(v, list):
                    return {"ok": False, "message": "excluded_symbols doit etre une liste de symboles"}
                syms = sorted({str(s).upper() for s in v if str(s).upper() in SYMBOLS})
                cfg[k] = syms
                continue
            if k == "dca_mode":
                if v not in self.STEVEN_DCA_MODES:
                    return {"ok": False, "message": f"dca_mode invalide (parmi {self.STEVEN_DCA_MODES})"}
                cfg[k] = v
                continue
            if k == "consensus_preset":
                if v not in self.STEVEN_PRESETS:
                    return {"ok": False, "message": f"consensus_preset invalide (parmi {self.STEVEN_PRESETS})"}
                cfg[k] = v
                continue
            if k == "skip_hours":
                if not isinstance(v, list):
                    return {"ok": False, "message": "skip_hours doit etre une liste d'heures 0-23"}
                try:
                    hours = sorted({int(h) for h in v if 0 <= int(h) <= 23})
                except Exception:
                    return {"ok": False, "message": "skip_hours : valeurs invalides"}
                cfg[k] = hours
                continue
            if k in self.STEVEN_CONFIG_BOUNDS:
                try:
                    fv = round(float(v), 4)
                except Exception:
                    return {"ok": False, "message": f"valeur invalide pour {k}"}
                lo, hi = self.STEVEN_CONFIG_BOUNDS[k]
                if not (lo <= fv <= hi):
                    return {"ok": False, "message": f"{k} hors bornes ({lo}-{hi})"}
                cfg[k] = fv
        if cfg["buy_min_price"] >= cfg["buy_max_price"]:
            return {"ok": False, "message": "buy_min_price doit etre < buy_max_price"}
        if cfg["avoid_max_price"] > 0 and cfg["avoid_min_price"] >= cfg["avoid_max_price"]:
            return {"ok": False, "message": "avoid_min_price doit etre < avoid_max_price (ou les deux a 0 pour desactiver)"}
        # GARDE ANTI-SPAM (Steven 03/09, "j'ai set un reglage et depuis ca
        # spam les logs") : si le dashboard (ou un navigateur qui rejoue une
        # soumission de formulaire) renvoie EXACTEMENT la meme config, on
        # n'ecrit rien et on ne logue rien -- silence au lieu de spammer le
        # journal avec des ecritures identiques toutes les 2-3s.
        if cfg == self.steven_config():
            return {"ok": True, "steven_engine": cfg, "unchanged": True}
        self.state["steven_engine"] = cfg
        self._save()
        self._log(f"⚙️ [STEVEN-ENGINE] config mise a jour : {cfg}")
        return {"ok": True, "steven_engine": cfg}

    def steven_stats(self):
        """Stats live du moteur, pour affichage dashboard -- separees des
        stats oracle/arb (filtre sur strat=='steven_engine')."""
        open_n, open_cost = 0, 0.0
        trades, wins, pnl_sum = 0, 0, 0.0
        for sym in STEVEN_SYMBOLS:
            mk = self.state["markets"].get(sym)
            if not mk:
                continue
            for pos in mk["open"].values():
                if pos.get("strat") == "steven_engine":
                    open_n += 1
                    open_cost += pos.get("cost", 0.0)
            for t in mk["trades"]:
                if t.get("strat") == "steven_engine":
                    trades += 1
                    pnl_sum += t.get("pnl", 0.0)
                    if t.get("win"):
                        wins += 1
        return {
            "open_positions": open_n,
            "open_cost": round(open_cost, 2),
            "trades": trades,
            "win_rate": round(100 * wins / trades, 1) if trades else None,
            "pnl": round(pnl_sum, 3),
        }

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
        # La collecte et le gatekeeper NE sont PAS demarres ici : ils tournent
        # au niveau du process (cf. demarrer_recherche), pour continuer meme
        # bot a l'arret.
        self.demarrer_recherche()
        # COPY-TRADING (Steven 05/08) : thread dedie, tourne meme si
        # COPY_TRADE_ENABLED est False -> le drapeau est verifie a chaque
        # iteration, pas au demarrage, pour pouvoir l'activer/desactiver
        # depuis le dashboard sans redemarrer le bot.
        self._copy_trade_thread = threading.Thread(target=self._copy_trade_loop, daemon=True)
        self._copy_trade_thread.start()
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
                # COLLECTE DE MARCHE INDEPENDANTE DU TRADING (Steven 10/08,
                # "meme quand bot inactif on doit recup data") : AVANT le
                # filtre de mode ci-dessous, donc tourne aussi quand tous les
                # symboles sont a 'off'. Corrige au passage un BIAIS DE
                # SELECTION majeur du dataset : jusqu'ici les carnets
                # n'etaient enregistres que depuis la boucle de suivi d'une
                # position, donc uniquement sur les fenetres ou l'on etait
                # DEJA entre. Un modele entraine la-dessus ne peut pas
                # repondre a "faut-il entrer sur cette fenetre ?", puisqu'il
                # n'a jamais vu une seule fenetre non prise.
                # Steven 03/09 ("mets en pause la collecte qui bug et prend
                # des ressources") : pilotable en live (etat, pas un
                # redeploiement) pour pouvoir couper/reactiver instantanement.
                if self.state.get("market_collect_enabled", True):
                    try:
                        self._collect_market_data()
                    except Exception as e:
                        self._tlog("collect_market_err", f"💥 [COLLECTE] erreur: {e}")
                # PASSE 1 -- SL/TP EN PRIORITE ABSOLUE, TOUS SYMBOLES D'ABORD
                # (Steven 02/09, enquete sur un SL declenche a -33% au lieu de
                # -0.1%). Trouve : cette boucle traitait chaque symbole en
                # SERIE, chacun enchainant preopen/maker/orphans/excedent PUIS
                # SEULEMENT ENSUITE le SL/TP -- une etape lente (souvent un
                # appel reseau qui retente sur 400, vu plusieurs fois ce soir)
                # sur UN symbole retardait d'autant la verification SL de
                # l'AUTRE. Preuve directe : prix deja a -16.4% a 15:51:00,
                # verification suivante seulement a 15:51:19 (deja -27.9%) --
                # 19s d'ecart pour un poll cense tourner toutes les 1.5s. Le
                # SL/TP passe desormais en 1ere chose faite chaque cycle, pour
                # TOUS les symboles, avant tout le reste (qui peut attendre
                # 1.5s de plus sans consequence sur le capital engage).
                # PARALLELE ENTRE SYMBOLES (Steven 02/09, "il fait n'importe
                # quoi avec son SL" -- confirme sur incident reel : meme
                # place en 1ere passe, un SL ETH s'est declenche a -28.7% au
                # lieu de -0.1%, parce que _sell_orphan attend jusqu'a 4s de
                # verification on-chain APRES chaque vente -- une execution
                # lente sur BTC retardait d'autant la relecture du prix ETH,
                # meme en 1ere position dans une boucle sequentielle. Chaque
                # symbole tourne desormais dans son propre thread (pool deja
                # utilise partout ailleurs dans ce fichier pour le meme
                # besoin) -- une verification lente sur l'un n'affecte plus
                # les autres.
                def _pass1_sl_tp(sym):
                    try:
                        self._log_position_prices(sym)
                    except Exception as e:
                        self._tlog(f"fastexit_price_err_{sym}", f"💥 [FAST-EXIT] {sym} prix erreur: {e}")
                    try:
                        self._manage_pnl_tier_exits(sym)
                    except Exception as e:
                        self._tlog(f"fastexit_pnl_err_{sym}", f"💥 [FAST-EXIT] {sym} pnl-exits erreur: {e}")
                    try:
                        self._manage_oracle_trailing(sym)
                    except Exception as e:
                        self._tlog(f"fastexit_oracletrail_err_{sym}", f"💥 [FAST-EXIT] {sym} oracle-trailing erreur: {e}")

                _pass1_syms = [
                    sym for sym in SYMBOLS
                    if self.state["modes"].get(sym) in ("real", "paper")
                    and self.state["markets"][sym]["open"]
                ]
                _pass1_futs = [self._pool.submit(_pass1_sl_tp, sym) for sym in _pass1_syms]
                for _f in _pass1_futs:
                    try:
                        _f.result(timeout=8.0)
                    except Exception as e:
                        self._tlog("fastexit_pass1_err", f"💥 [FAST-EXIT] passe 1 parallele erreur: {e}")

                # PASSE 2 -- tout le reste, moins sensible au delai
                for sym in SYMBOLS:
                    mode = self.state["modes"].get(sym)
                    # FIX (regression) : limiter au reel privait le PAPER de tout
                    # SL/TP (l'appel avait ete retire du scan lent pour les DEUX
                    # modes) -> positions paper jamais coupees, meme a -90%.
                    if mode not in ("real", "paper"):
                        continue
                    # PRE-OUVERTURE (Steven 06/08) : DOIT tourner AVANT le
                    # filtre ci-dessous -- il pose des ordres sur une fenetre
                    # pas encore ouverte, donc precisement quand mk["open"]
                    # est VIDE. Le placer apres revenait a ne jamais l'appeler.
                    if not ORACLE_ONLY_MODE:
                        try:
                            self._manage_preopen(sym)
                        except Exception as e:
                            self._tlog(f"fastexit_preopen_err_{sym}", f"💥 [FAST-EXIT] {sym} pre-ouverture erreur: {e}")
                        # MAKER EN FENETRE OUVERTE : meme raison que la
                        # pre-ouverture, il agit quand mk["open"] est vide.
                        try:
                            self._manage_maker_open(sym)
                        except Exception as e:
                            self._tlog(f"fastexit_makeropen_err_{sym}", f"💥 [FAST-EXIT] {sym} maker-ouvert erreur: {e}")
                    if not self.state["markets"][sym]["open"]:
                        continue
                    try:
                        self._manage_orphans(sym)
                    except Exception as e:
                        self._tlog(f"fastexit_orphan_err_{sym}", f"💥 [FAST-EXIT] {sym} orphans erreur: {e}")
                    # SOLDE DE L'EXCEDENT NON COUVERT (Steven 11/08). AVANT le
                    # TP par paliers : si une paire est desequilibree, la part
                    # en trop est un pari nu qu'il faut solder avant toute
                    # autre decision. _tag_pair_lock la MARQUE deja
                    # (excedent_a_solder) et _guard_both_side sait la vendre --
                    # mais cette fonction n'etait APPELEE NULLE PART, donc le
                    # bot ecrivait "1.00 parts en trop, a solder" et ne le
                    # faisait jamais. Verifie par recherche exhaustive : seule
                    # sa definition existait.
                    try:
                        self._solder_excedent(sym)
                    except Exception as e:
                        self._tlog(f"fastexit_exc_err_{sym}", f"💥 [FAST-EXIT] {sym} excedent erreur: {e}")
                    # RENFORT (Steven 05/08) : APRES les sorties, pour que le
                    # marqueur sl_fired du cycle courant soit deja pose et que
                    # le renfort travaille sur la taille reelle post-coupe.
                    if not ORACLE_ONLY_MODE:
                        try:
                            self._manage_reinforce(sym)
                        except Exception as e:
                            self._tlog(f"fastexit_reinf_err_{sym}", f"💥 [FAST-EXIT] {sym} renfort erreur: {e}")
                    # ARB DECALE : completion / abandon de la jambe 1
                    try:
                        self._manage_stagger(sym)
                    except Exception as e:
                        self._tlog(f"fastexit_stag_err_{sym}", f"💥 [FAST-EXIT] {sym} arb-decale erreur: {e}")
            except Exception as e:
                self._log(f"💥 [FAST-EXIT] erreur boucle: {e}")
            time.sleep(FAST_EXIT_POLL_S)

    # ── COPY-TRADING AUTOMATIQUE (Steven 05/08) ─────────────────────────
    # Cf. le commentaire des constantes COPY_TRADE_* pour le raisonnement
    # complet (pourquoi desactive par defaut, quels garde-fous).

    def _copy_trade_state(self):
        return self.state.setdefault(
            "copy_trade", {"enabled": False, "wallets": {}, "recent": [], "seen": []}
        )

    def get_copy_trade_status(self):
        ct = self._copy_trade_state()
        return {
            "ok": True,
            "enabled": bool(ct.get("enabled")),
            "autoselect_enabled": COPY_AUTOSELECT_ENABLED,
            "wallets": ct.get("wallets", {}),
            "recent": ct.get("recent", [])[-30:],
            "watchlist": ct.get("watchlist"),
            "budget_usd": COPY_TRADE_BUDGET_USD,
            "max_wallets": COPY_TRADE_MAX_WALLETS,
        }

    def set_copy_trade_enabled(self, enabled):
        ct = self._copy_trade_state()
        ct["enabled"] = bool(enabled)
        self._save()
        self._log(f"{'🟢' if enabled else '⭕'} [COPY] auto-copy {'ACTIVE' if enabled else 'desactive'}")
        return {"ok": True, "enabled": ct["enabled"]}

    def follow_copy_wallet(self, wallet, label=""):
        import re

        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet or ""):
            return {"ok": False, "message": "adresse wallet invalide"}
        ct = self._copy_trade_state()
        if wallet not in ct["wallets"] and len(ct["wallets"]) >= COPY_TRADE_MAX_WALLETS:
            return {"ok": False, "message": f"deja {COPY_TRADE_MAX_WALLETS} wallets suivis (max)"}
        ct["wallets"][wallet] = {"label": label or wallet[:10], "added_ts": time.time()}
        self._save()
        self._log(f"👥 [COPY] wallet suivi : {wallet} ({label or 'sans label'})")
        return {"ok": True, "wallets": ct["wallets"]}

    def unfollow_copy_wallet(self, wallet):
        ct = self._copy_trade_state()
        if ct["wallets"].pop(wallet, None) is not None:
            self._save()
            self._log(f"👥 [COPY] wallet retire : {wallet}")
        return {"ok": True, "wallets": ct["wallets"]}

    # ── SELECTION AUTOMATIQUE (Steven 05/08) ────────────────────────────
    @staticmethod
    def _ct_band_of(px):
        for lo, hi, name in (
            (0.0, 0.30, "0.00-0.30"), (0.30, 0.50, "0.30-0.50"),
            (0.50, 0.70, "0.50-0.70"), (0.70, 0.85, "0.70-0.85"),
            (0.85, 0.90, "0.85-0.90"), (0.90, 0.95, "0.90-0.95"),
            (0.95, 0.98, "0.95-0.98"), (0.98, 1.01, "0.98-1.00"),
        ):
            if lo <= px < hi:
                return name
        return None

    def _ct_fetch_wallet_events(self, wallet, requests_mod, max_pages=3):
        """Meme methode que _fetch_updown5m_events cote server.py (dupliquee
        volontairement -- trader.py et server.py sont deux modules distincts,
        et le thread d'auto-selection doit rester autonome, sans dependre du
        process Flask)."""
        headers = {"User-Agent": "Mozilla/5.0"}
        events = []
        seen = set()
        for off in range(0, max_pages * 500, 500):
            try:
                r = requests_mod.get(
                    "https://data-api.polymarket.com/activity",
                    params={"user": wallet, "limit": 500, "offset": off},
                    headers=headers, timeout=8,
                )
                batch = r.json()
            except Exception:
                break
            if not isinstance(batch, list) or not batch:
                break
            new = 0
            for a in batch:
                if "updown-5m" not in (a.get("slug") or ""):
                    continue
                k = (a.get("transactionHash"), a.get("slug"), a.get("outcome"), a.get("timestamp"), a.get("size"), a.get("type"))
                if k in seen:
                    continue
                seen.add(k)
                events.append(a)
                new += 1
            if new == 0 and len(batch) < 500:
                break
        return events

    def _ct_analyze(self, updown):
        """Version compacte de _analyze_updown5m (server.py) : memes bandes de
        prix, meme comptage sans biais de survie (tous les achats, pas
        seulement ceux qui ont un redeem), plus le detail par bande necessaire
        au score de concentration (_score_trader)."""
        legs = {}
        sides_by_slug = {}
        redeem_by_slug = {}
        for a in updown:
            slug = a.get("slug")
            if a.get("type") == "REDEEM":
                redeem_by_slug[slug] = redeem_by_slug.get(slug, 0.0) + (a.get("usdcSize") or 0.0)
                continue
            if a.get("type") != "TRADE":
                continue
            k = (slug, a.get("outcome"))
            e = legs.setdefault(k, {"buy_usd": 0.0, "buy_sh": 0.0, "sell_usd": 0.0})
            if a.get("side") == "BUY":
                e["buy_usd"] += a.get("usdcSize") or 0.0
                e["buy_sh"] += a.get("size") or 0.0
                sides_by_slug.setdefault(slug, set()).add(a.get("outcome"))
            else:
                e["sell_usd"] += a.get("usdcSize") or 0.0

        bands = {}
        n_paired = n_solo = 0
        total_cost = total_return = 0.0
        ts_all = [a["timestamp"] for a in updown if a.get("timestamp")]
        for (slug, outcome), e in legs.items():
            if e["buy_sh"] <= 0.05 or e["buy_usd"] <= 0:
                continue
            avg_px = e["buy_usd"] / e["buy_sh"]
            if not (0.01 < avg_px < 0.99):
                continue
            redeem = redeem_by_slug.get(slug, 0.0)
            won = redeem > 0 and abs(redeem - e["buy_sh"]) < max(0.6, 0.3 * e["buy_sh"])
            ret = e["sell_usd"] + (e["buy_sh"] if won else 0.0)
            total_cost += e["buy_usd"]
            total_return += ret
            if len(sides_by_slug.get(slug, set())) == 2:
                n_paired += 1
            else:
                n_solo += 1
            bname = self._ct_band_of(avg_px)
            if bname is None:
                continue
            b = bands.setdefault(bname, {"n": 0, "cost": 0.0, "ret": 0.0})
            b["n"] += 1
            b["cost"] += e["buy_usd"]
            b["ret"] += ret

        total_legs = n_paired + n_solo
        return {
            "days_active": round((max(ts_all) - min(ts_all)) / 86400, 2) if ts_all else 0,
            "total_cost_usd": round(total_cost, 2),
            "overall_roi_pct": round(100 * (total_return - total_cost) / total_cost, 1) if total_cost else None,
            "arb_usage_pct": round(100 * n_paired / total_legs, 1) if total_legs else None,
            "n_total": total_legs,
            "bands": bands,
        }

    def _score_trader(self, an):
        """Retourne (eligible: bool, score: float, reasons: list[str]).
        reasons contient TOUJOURS au moins une ligne -- soit ce qui disqualifie,
        soit ce qui justifie le score -- pour que le dashboard puisse montrer
        POURQUOI un wallet est suivi ou exclu (demande explicite de Steven :
        "faut aussi identifier ceux a ne pas suivre")."""
        reasons = []
        if an["total_cost_usd"] < COPY_AUTOSELECT_MIN_COST_USD:
            reasons.append(f"echantillon trop petit ({an['total_cost_usd']}$ < {COPY_AUTOSELECT_MIN_COST_USD}$)")
        if an["days_active"] < COPY_AUTOSELECT_MIN_DAYS:
            reasons.append(f"pas assez recent ({an['days_active']}j < {COPY_AUTOSELECT_MIN_DAYS}j d'activite)")
        if an["overall_roi_pct"] is None or an["overall_roi_pct"] <= 0:
            reasons.append(f"ROI global <= 0 ({an['overall_roi_pct']}%)")
        if an["total_cost_usd"] > 0:
            top_band = max(an["bands"].items(), key=lambda kv: kv[1]["cost"], default=(None, {"cost": 0, "n": 0}))
            if top_band[0] and top_band[1]["cost"] / an["total_cost_usd"] > COPY_AUTOSELECT_MAX_CONCENTRATION and top_band[1]["n"] < COPY_AUTOSELECT_MIN_BAND_N_FOR_CONCENTRATION:
                reasons.append(
                    f"edge concentre sur une seule bande ({top_band[0]}, "
                    f"{round(100 * top_band[1]['cost'] / an['total_cost_usd'])}% du capital, "
                    f"seulement {top_band[1]['n']} trades dedans -- pas repetable)"
                )
            lottery_cost = sum(b["cost"] for name, b in an["bands"].items() if name == "0.00-0.30")
            if lottery_cost / an["total_cost_usd"] > COPY_AUTOSELECT_MAX_LOTTERY_SHARE:
                reasons.append(
                    f"pattern billet de loterie ({round(100 * lottery_cost / an['total_cost_usd'])}% du capital "
                    f"sous 0.30 -- la bande la plus perdante sur notre propre historique, -28% de ROI)"
                )
        if reasons:
            return False, -999.0, reasons
        confidence = min(1.0, an["n_total"] / 60.0)
        arb_bonus = (an["arb_usage_pct"] or 0) / 100.0 * 5.0
        score = round(an["overall_roi_pct"] * confidence + arb_bonus, 2)
        reasons.append(
            f"ROI {an['overall_roi_pct']:+.1f}% (confiance {confidence:.0%} sur {an['n_total']} jambes), "
            f"arb {an['arb_usage_pct']}%, {an['days_active']}j actif -> score {score}"
        )
        return True, score, reasons

    def _ct_scan_active_wallets(self, requests_mod):
        """Scan LEGER (3 symboles x 2 fenetres, contre 5x3 cote
        /api/copy-discover manuel) : l'auto-selection tourne toutes les 30min
        en tache de fond, pas besoin du meme niveau de couverture qu'un scan
        a la demande."""
        headers = {"User-Agent": "Mozilla/5.0"}
        base = int(time.time() // 300) * 300
        freq = {}
        for sym in ("btc", "eth", "sol"):
            for off in (-300, -600):
                slug = f"{sym}-updown-5m-{base + off}"
                try:
                    m = requests_mod.get(
                        "https://gamma-api.polymarket.com/markets",
                        params={"slug": slug}, headers=headers, timeout=8,
                    ).json()
                except Exception:
                    continue
                mk = m[0] if isinstance(m, list) and m else None
                cid = mk.get("conditionId") if mk else None
                if not cid:
                    continue
                try:
                    trs = requests_mod.get(
                        "https://data-api.polymarket.com/trades",
                        params={"market": cid, "limit": 150}, headers=headers, timeout=8,
                    ).json()
                except Exception:
                    continue
                if not isinstance(trs, list):
                    continue
                for t in trs:
                    w = t.get("proxyWallet")
                    if w:
                        freq[w] = freq.get(w, 0) + 1
        return sorted(freq.items(), key=lambda kv: -kv[1])[:15]

    def _copy_autoselect(self):
        """Coeur de la selection automatique : scanne, analyse, score, puis
        suit/retire des wallets SANS intervention manuelle -- Steven controle
        toujours le toggle general (self.state["copy_trade"]["enabled"]),
        mais plus le choix de CHAQUE wallet individuel."""
        import requests

        # DIAGNOSTIC (Steven 05/08, mesure en test local) : jusqu'a 6 marches
        # + 15 wallets x plusieurs pages, chacun avec son propre timeout
        # sequentiel -> plusieurs minutes au total, meme decouple du thread de
        # sondage (80018cc). Log de debut/fin pour que ce ne soit pas une
        # boite noire silencieuse pendant tout ce temps.
        _t0 = time.time()
        self._log("🔎 [COPY-AUTO] scan de selection demarre")
        ct = self._copy_trade_state()
        candidates = self._ct_scan_active_wallets(requests)
        evaluated = []
        for wallet, freq in candidates:
            # 1 seule page (500 evenements) : suffisant pour juger un
            # echantillon >= COPY_AUTOSELECT_MIN_COST_USD, et ca divise par 2
            # le pire cas de latence par wallet compare aux 2 pages initiales.
            events = self._ct_fetch_wallet_events(wallet, requests, max_pages=1)
            if not events:
                continue
            an = self._ct_analyze(events)
            eligible, score, reasons = self._score_trader(an)
            evaluated.append({"wallet": wallet, "eligible": eligible, "score": score, "reasons": reasons, **an})

        eligible_sorted = sorted([e for e in evaluated if e["eligible"]], key=lambda e: -e["score"])
        excluded = [e for e in evaluated if not e["eligible"]]

        # RE-EVALUE d'abord les wallets DEJA suivis : un edge peut se degrader
        # (c'est tout le sens de "identifier ceux a ne pas suivre" applique en
        # continu, pas juste a l'ajout).
        # BUG CORRIGE (Steven 06/08, audit avant depot) : la version precedente
        # cherchait le wallet suivi dans `evaluated`, qui ne contient QUE les
        # ~15 wallets les plus actifs du scan courant. Un wallet suivi qui
        # n'apparaissait pas dans ce top-15 avait match=None -> jamais
        # reevalue, donc JAMAIS retire. Constate en prod : dernier scan =
        # 0 eligible / 15 exclus, et pourtant 5 wallets toujours suivis,
        # ajoutes par un scan anterieur et jamais reverifies depuis.
        # Desormais on va CHERCHER explicitement les donnees de chaque wallet
        # suivi, meme absent du scan -- c'est la seule facon que "identifier
        # ceux a ne pas suivre" fonctionne vraiment en continu.
        for wallet in list(ct["wallets"].keys()):
            match = next((e for e in evaluated if e["wallet"] == wallet), None)
            if match is None:
                _ev = self._ct_fetch_wallet_events(wallet, requests, max_pages=1)
                if not _ev:
                    self._log(
                        f"👥 [COPY-AUTO] retire {wallet[:10]} : plus aucune activite "
                        f"Up/Down 5min recuperable -> impossible de verifier son edge"
                    )
                    ct["wallets"].pop(wallet, None)
                    continue
                _an = self._ct_analyze(_ev)
                _ok, _sc, _rs = self._score_trader(_an)
                match = {"wallet": wallet, "eligible": _ok, "score": _sc, "reasons": _rs, **_an}
            if not match["eligible"]:
                self._log(f"👥 [COPY-AUTO] retire {wallet[:10]} : {'; '.join(match['reasons'])}")
                ct["wallets"].pop(wallet, None)

        # AJOUTE les meilleurs eligibles non deja suivis, jusqu'au plafond.
        for e in eligible_sorted:
            if len(ct["wallets"]) >= COPY_TRADE_MAX_WALLETS:
                break
            if e["wallet"] in ct["wallets"]:
                continue
            ct["wallets"][e["wallet"]] = {
                "label": f"auto (score {e['score']})", "added_ts": time.time(), "auto": True,
            }
            self._log(f"👥 [COPY-AUTO] suit {e['wallet'][:10]} : {'; '.join(e['reasons'])}")

        ct["watchlist"] = {
            "ts": time.time(),
            "eligible": [{"wallet": e["wallet"], "score": e["score"], "reasons": e["reasons"]} for e in eligible_sorted],
            "excluded": [{"wallet": e["wallet"], "reasons": e["reasons"]} for e in excluded],
        }
        self._save()
        self._log(
            f"🔎 [COPY-AUTO] scan termine en {time.time() - _t0:.0f}s : "
            f"{len(candidates)} wallets vus, {len(evaluated)} analyses, "
            f"{len(eligible_sorted)} eligibles, {len(excluded)} exclus, "
            f"{len(ct['wallets'])} suivis"
        )

    def _copy_trade_loop(self):
        """Sonde l'activite on-chain des wallets suivis toutes les
        COPY_TRADE_POLL_S secondes et repond aux NOUVEAUX achats sur les
        marches Up/Down 5min avec une petite mise fixe. Tourne toujours (le
        thread demarre avec le bot) mais ne fait rien tant que
        COPY_TRADE_ENABLED est False OU qu'aucun wallet n'est suivi -- verifie
        a CHAQUE iteration pour reagir immediatement a un changement depuis le
        dashboard, sans redemarrage."""
        import requests

        while self._running.is_set():
            try:
                if COPY_TRADE_ENABLED and not ORACLE_ONLY_MODE:
                    ct = self._copy_trade_state()
                    if ct.get("enabled"):
                        # SELECTION AUTOMATIQUE (Steven 05/08) : plus besoin de
                        # cliquer "Suivre" wallet par wallet. Tourne toutes les
                        # COPY_AUTOSELECT_INTERVAL_S, immediatement au premier
                        # cycle apres activation (last_autoselect_ts absent).
                        if COPY_AUTOSELECT_ENABLED:
                            last = ct.get("last_autoselect_ts", 0)
                            if time.time() - last >= COPY_AUTOSELECT_INTERVAL_S and not ct.get("_autoselect_running"):
                                # THREAD SEPARE (fix apres constat en test local) :
                                # le scan est reseau-bound et peut prendre
                                # plusieurs minutes (jusqu'a 6 marches + 15
                                # wallets x 2 pages, chacun avec son propre
                                # timeout). Le lancer ICI, dans la boucle
                                # principale, bloquerait le sondage des wallets
                                # DEJA suivis pendant tout ce temps -- exactement
                                # le moment ou la latence de copie compte le
                                # plus. Le scan tourne donc en tache de fond,
                                # independamment du sondage.
                                ct["last_autoselect_ts"] = time.time()
                                ct["_autoselect_running"] = True
                                self._save()

                                def _run_autoselect():
                                    try:
                                        self._copy_autoselect()
                                    except Exception as e:
                                        self._log(f"💥 [COPY-AUTO] erreur de selection: {e}")
                                    finally:
                                        self._copy_trade_state()["_autoselect_running"] = False
                                        self._save()

                                threading.Thread(target=_run_autoselect, daemon=True).start()
                        for wallet in list(ct.get("wallets", {}).keys()):
                            try:
                                self._copy_trade_poll_wallet(wallet, requests)
                            except Exception as e:
                                self._tlog(
                                    f"copytrade_err_{wallet}",
                                    f"💥 [COPY] {wallet[:10]} erreur de sondage: {e}",
                                )
            except Exception as e:
                self._log(f"💥 [COPY] erreur boucle: {e}")
            time.sleep(COPY_TRADE_POLL_S)

    def _copy_trade_poll_wallet(self, wallet, requests):
        ct = self._copy_trade_state()
        seen = set(ct.setdefault("seen", []))
        try:
            r = requests.get(
                "https://data-api.polymarket.com/activity",
                params={"user": wallet, "limit": 20},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=8,
            )
            events = r.json()
        except Exception:
            return
        if not isinstance(events, list):
            return
        now = time.time()
        for a in events:
            if a.get("type") != "TRADE" or a.get("side") != "BUY":
                continue
            slug = a.get("slug") or ""
            if "updown-5m" not in slug:
                continue
            key = f"{wallet}|{a.get('transactionHash')}|{slug}|{a.get('outcome')}"
            if key in seen:
                continue
            seen.add(key)
            # dedup une seule fois par (wallet, slug) : pas de sur-copie si le
            # trader source multiplie les achats sur la meme fenetre.
            slug_key = f"{wallet}|{slug}"
            if slug_key in seen:
                continue
            age = now - (a.get("timestamp") or 0)
            if age > COPY_TRADE_MAX_STALE_SECS:
                seen.add(slug_key)  # trop tard, mais on ne retentera pas non plus
                continue
            seen.add(slug_key)
            self._copy_trade_execute(wallet, slug, a.get("outcome"), a.get("price"), age)
        # purge simple, pas de fuite memoire sur une session longue
        if len(seen) > COPY_TRADE_SEEN_CAP:
            seen = set(list(seen)[-COPY_TRADE_SEEN_CAP // 2 :])
        ct["seen"] = list(seen)
        self._save()

    def _copy_trade_execute(self, wallet, slug, side, source_price, age_secs):
        from core.btc_updown import _fetch_one_market

        sym = slug.split("-")[0].upper()
        if sym not in SYMBOLS:
            return
        if self.state["modes"].get(sym) != "real":
            self._tlog(
                f"copyoff_{sym}", f"⏸️ [COPY] {sym} {slug} signal de {wallet[:10]} ignore (mode != real)"
            )
            return
        mk = self.state["markets"][sym]
        if f"{slug}|{side}" in mk["open"]:
            return  # deja une position dessus, rien a copier
        m = _fetch_one_market(slug)
        if not m:
            self._log(f"⚠️ [COPY] {sym} {slug} marche introuvable (peut-etre deja resolu)")
            return
        # end_ts se deduit du slug lui-meme (format sym-updown-5m-<start_ts>),
        # plus fiable que de reparser les dates du marche.
        try:
            start_ts = int(slug.rsplit("-", 1)[-1])
            end_ts = start_ts + 300
        except Exception:
            return
        secs_left = end_ts - time.time()
        if secs_left < COPY_TRADE_MIN_SECS_LEFT:
            self._tlog(f"copylate_{sym}", f"⏸️ [COPY] {sym} {slug} trop tard ({secs_left:.0f}s restantes)")
            return
        try:
            outcomes = json.loads(m.get("outcomes") or "[]")
            token_ids = json.loads(m.get("clobTokenIds") or "[]")
        except Exception:
            return
        if side not in outcomes or len(token_ids) != len(outcomes):
            return
        tid = token_ids[outcomes.index(side)]
        book = self._live.get_book_sync(tid)
        ask = book["asks"][0][0] if book and book.get("asks") else None
        if ask is None:
            return
        # PLANCHER 0.50$ (Steven 01/09, audit complet des 20 points d'achat
        # -- copy-trading est actuellement desactive mais n'avait aucun
        # plancher si reactive un jour, contrairement aux autres strategies).
        if ask < FAV_MIN_PRICE:
            self._tlog(
                f"copyfloor_{sym}",
                f"⛔ [COPY] {sym} {slug} {side} ask={ask:.3f} < {FAV_MIN_PRICE} "
                f"-> refuse, plancher universel",
            )
            return
        # DERIVE DE PRIX (coeur du garde-fou copy-trading) : entre l'achat
        # source et notre detection, le marche a pu bouger. On ne poursuit
        # jamais un prix qui s'est deja envole.
        if source_price and ask > source_price + COPY_TRADE_MAX_PRICE_DRIFT:
            self._tlog(
                f"copydrift_{sym}",
                f"⛔ [COPY] {sym} {slug} {side} ask={ask:.3f} > source {source_price:.3f}+"
                f"{COPY_TRADE_MAX_PRICE_DRIFT} -> prix perime, on n'y va pas",
            )
            return
        cash, _ = self._read_cash(max_age=0)
        if cash is None:
            return
        investable = max(0.0, cash - self.floor())
        budget = round(min(COPY_TRADE_BUDGET_USD, investable), 2)
        budget = max(budget, round(MIN_SELL_SHARES * ask, 2))
        if budget > investable or budget < MIN_BUDGET_USD:
            return
        ok_exp, why_exp = self._exposure_ok(sym, mk, slug, budget)
        if not ok_exp:
            self._tlog(f"copyexp_{sym}", f"⛔ [COPY] {sym} {slug} refuse : {why_exp}")
            return
        self._log(
            f"🎯 [COPY] {sym} {slug} {side} @ {ask:.3f} budget={budget:.2f}$ "
            f"-- signal de {wallet[:10]} (source @ {source_price}, detecte {age_secs:.1f}s apres)"
        )
        with self._order_lock:
            res = self._live.snipe_buy_market(tid, round(ask + 0.02, 2), budget)
        filled = res.get("filled_shares", 0.0)
        if filled <= 0:
            self._log(f"⚠️ [COPY] {sym} {slug} {side} non rempli ({res.get('error', '')})")
            return
        avg = res.get("avg_cost") or ask
        self._add_slug_spent(mk, slug, round(filled * avg, 2))
        mk["open"][f"{slug}|{side}"] = {
            "symbol": sym, "slug": slug, "side": side, "mode": "real",
            # strat "copy" -> jamais is_risk_free, TOUJOURS gere par le meme
            # TP/SL que le reste (ajoute au filtre de _manage_pnl_tier_exits).
            "strat": "copy", "token_id": tid, "entry_price": avg,
            "filled_shares": filled, "cost": round(filled * avg, 2),
            "start_ts": start_ts, "pair": None, "end_ts": end_ts,
            "opened_ts": time.time(), "buffer": 0.0,
            "copy_source_wallet": wallet, "copy_source_price": source_price,
        }
        ct = self._copy_trade_state()
        ct.setdefault("recent", []).append(
            {
                "ts": time.time(), "wallet": wallet, "symbol": sym, "slug": slug,
                "side": side, "price": avg, "budget": round(filled * avg, 2),
            }
        )
        ct["recent"] = ct["recent"][-50:]
        self._save()
        self._log(
            f"✅ [COPY] {sym} {slug} {side} {filled} parts @ {avg:.3f} "
            f"({round(filled * avg, 2)}$) -> ouverte, TP/SL actifs"
        )

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
        # PLANCHER PROPORTIONNEL AU CAPITAL (Steven 02/09, "il faut ajouter un
        # plancher proportionnel au capital ex mise mini = X% du solde") :
        # sur un favori >0.70 (edge en $ mecaniquement faible pres de 1.00),
        # le Kelly pur produisait des mises ridicules (4-5$ sur 53$ de solde)
        # meme avec le multiplicateur de palier. Le plancher releve la mise
        # sans jamais depasser le hard cap / MAX_FRACTION ci-dessous -- donc
        # sans risque d'engager plus que prevu, juste moins TIMIDE sur les
        # favoris confirmes.
        budget = max(budget, investable * MIN_STAKE_FRACTION)
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
                # STEVEN ENGINE (Steven 03/09, "il doit overide les reglages
                # generaux du bot") : a besoin des 6 marches pour evaluer le
                # consensus, INDEPENDAMMENT du mode on/off par symbole utilise
                # par les autres strategies -- sinon un symbole "off" n'est
                # meme pas recupere ici, avant meme d'atteindre la logique du
                # moteur. Ne force PAS le mode "real"/"paper" du symbole (ca
                # resterait "off" partout ailleurs), juste la RECUPERATION du
                # marche pour que le moteur puisse le lire.
                if self.steven_config().get("enabled"):
                    for _s in STEVEN_SYMBOLS:
                        if _s.lower() not in _active_tags:
                            _active_tags.append(_s.lower())
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

                # STEVEN ENGINE (Steven 03/09, "en plus de oracle faire tourner
                # un Steven engine base sur le comportement du bot d'un ami") :
                # cross-symbole, tourne UNE fois par cycle complet (pas par
                # symbole comme le reste) puisqu'il a besoin de voir les 6
                # marches ensemble pour detecter un "traineur".
                try:
                    self._manage_steven_engine(by_sym)
                except Exception as e:
                    self._log(f"💥 [STEVEN-ENGINE] erreur: {e}")

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
                    if self.state.get("mm", {}).get("enabled") and mm_markets and not ORACLE_ONLY_MODE:
                        m, p = mm_markets[0]
                        try:
                            self._mm_tick(sym, mode, m, p)
                        except Exception as e:
                            self._tlog(f"mm_err_{sym}", f"💥 [MM] {sym} erreur: {e}")
                    # DELTA-NEUTRE both-side au bid (Steven 23/07)
                    if self.state.get("dn_enabled") and mm_markets and not ORACLE_ONLY_MODE:
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
                    self._check_polymarket_status()
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
                # PLAFOND D'EXPOSITION PAR MARCHE (Steven 05/08) : identique au
                # chemin both-side, borne le cumul engage sur cette fenetre.
                _exp_ok, _exp_why = self._exposure_ok(sym, mk, slug, budget)
                if not _exp_ok:
                    self._log(
                        f"⛔ [EXPO-MAX] {sym} {slug} {sig['side']} achat refuse : {_exp_why}"
                    )
                    self._reject(sym, slug, "risk_limit", _exp_why)
                    return
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
                self._add_slug_spent(mk, slug, round(filled * avg, 2))
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
            # PLANCHER UNIVERSEL 0.50$ (Steven 01/09, "ou est mon filtre qui
            # aurait du forcer achat a 60 down ????" -- le filtre pose plus
            # haut dans le premier appelant a ete contourne par un chemin
            # different qui arrive ICI directement). _open_leg est le POINT
            # UNIQUE par lequel tout achat reel passe -- plancher applique
            # ici, aucun appelant ne peut plus le contourner. force=True
            # reste exempte (reserve aux completions d'urgence explicites,
            # aucune n'est active actuellement).
            if not force and ask < FAV_MIN_PRICE:
                self._tlog(
                    f"openleg_floor_{sym}",
                    f"⛔ [OPEN-LEG-FLOOR] {sym} {slug} {side} @ {ask:.3f} < "
                    f"{FAV_MIN_PRICE} -> refuse, plancher universel",
                )
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
                # GARDE-FOU MINIMUM VENDABLE (Steven 05/08, meme bug que dans le
                # chemin "opportunity" ligne ~2603, trouve ici via une jambe
                # FORCE-PAIR a 2.78 parts qui n'a jamais recu de TP/SL malgre un
                # peak PnL de +168% -- _manage_pnl_tier_exits skip toute position
                # sous MIN_ORDER_SIZE_SHARES). Sans ce plancher, _open_leg peut
                # ouvrir une jambe invendable qui devient un pari pile-ou-face
                # force et casse la symetrie de l'arb (voir aussi target_shares
                # ci-dessous, meme raisonnement en mode parts-egales).
                if target_shares is None:
                    _min_sellable_budget = round(MIN_ORDER_SIZE_SHARES * ask, 2)
                    if budget < _min_sellable_budget:
                        if investable >= _min_sellable_budget:
                            budget = _min_sellable_budget
                        else:
                            self._log(
                                f"⛔ [MIN-VENDABLE] {sym} {slug} {side} budget={budget:.2f} < "
                                f"min_vendable={_min_sellable_budget:.2f} (investable={investable:.2f}) "
                                f"-> jambe invendable evitee"
                            )
                            return False, 0.0
                elif target_shares < MIN_ORDER_SIZE_SHARES:
                    _min_sellable_budget = round(MIN_ORDER_SIZE_SHARES * ask, 2)
                    if investable >= _min_sellable_budget:
                        target_shares = MIN_ORDER_SIZE_SHARES
                    else:
                        self._log(
                            f"⛔ [MIN-VENDABLE] {sym} {slug} {side} target_shares={target_shares:.2f} < "
                            f"{MIN_ORDER_SIZE_SHARES} (investable={investable:.2f}) -> jambe invendable evitee"
                        )
                        return False, 0.0
                # PLAFOND D'EXPOSITION PAR MARCHE (Steven 05/08) : dernier
                # controle avant d'engager du vrai argent. Borne le CUMUL sur
                # la fenetre, pas juste cet ordre -> coupe toute boucle de
                # re-entree quelle qu'en soit la cause.
                _exp_cost = round((target_shares * ask) if target_shares is not None else budget, 2)
                _exp_ok, _exp_why = self._exposure_ok(sym, mk, slug, _exp_cost)
                if not _exp_ok:
                    self._log(
                        f"⛔ [EXPO-MAX] {sym} {slug} {side} achat refuse : {_exp_why} "
                        f"-> plafond d'exposition de la fenetre atteint"
                    )
                    self._reject(sym, slug, "risk_limit", _exp_why)
                    return False, 0.0
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
                # NB (Steven 05/08) : un TOP-UP etait tente ici pour amener un
                # fill partiel a 5 parts. Retire -- il reposait sur la meme
                # premisse fausse que celui de _sell_orphan (vente impossible
                # sous 5 parts). Les ventes sous 5 parts passent (verifie
                # on-chain, plus petite a 1.37 part) et la gestion TP/SL
                # accepte desormais toute taille > 0 : racheter pour "pouvoir
                # gerer" ne ferait qu'engager plus de capital sans raison.
                self._add_slug_spent(mk, slug, round(filled * avg, 2))
                if 0 < filled < MIN_ORDER_SIZE_SHARES:
                    self._log(
                        f"ℹ️ [FILL-PARTIEL] {sym} {slug} {side} {filled} parts remplies "
                        f"(< {MIN_ORDER_SIZE_SHARES}) -> gerees normalement en TP/SL"
                    )
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

    # LES 6 MARCHES (Steven 11/08, "6 marches a mettre en dataset") : la
    # collecte est independante du trading, donc elargir ne change RIEN aux
    # decisions -- ca ne fait qu'agrandir le jeu d'apprentissage, et permet
    # de comparer les marches entre eux pour savoir ou MSF travaille le mieux.
    MARKET_DATA_SYMBOLS = ("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB")
    MARKET_DATA_INTERVAL_S = 30
    # DUREES COLLECTEES (Steven 11/08, "s'interesser aux marches 15min/1h/1j").
    # Verifie sur l'API Gamma : les 6 cryptos ont bien des series 5m, 15m ET
    # 4h, toutes avec le MEME format de slug horodate (<sym>-updown-<duree>-<ts>),
    # donc adressables sans deviner. (nom_duree, secondes).
    #   5m  -> resolution Chainlink TWAP 30s
    #   15m -> resolution Chainlink TWAP 60s
    #   4h  -> resolution Chainlink TWAP 60s
    # Les series "hourly" et "daily" existent aussi mais utilisent un slug en
    # DATE LISIBLE (bitcoin-up-or-down-august-11-2026-11am-et) et resolvent
    # sur BINANCE, pas Chainlink : autre regime, non couvert ici.
    MARKET_DATA_DUREES = (("5m", 300), ("15m", 900), ("4h", 14400))

    def _collect_market_data(self):
        """Enregistre le carnet des fenetres EN COURS, que l'on trade ou non
        (Steven 10/08). Alimente le dataset du futur filtre Go/No-Go avec
        des fenetres NON prises -- indispensable : un modele qui n'a vu que
        les fenetres ou l'on est entre ne peut rien dire des autres.
        Throttle a MARKET_DATA_INTERVAL_S par symbole ; 2 lectures de carnet
        par symbole et par intervalle, rien d'autre. Aucune decision de
        trading ne depend de cette fonction."""
        if not self._live:
            return
        now = time.time()
        for sym in self.MARKET_DATA_SYMBOLS:
            mk = self.state["markets"].get(sym) or {}
            for duree, secondes in self.MARKET_DATA_DUREES:
                cle = f"{sym}:{duree}"
                # PAS D'ECHANTILLONNAGE PAR DUREE (Steven 11/08, apres mesure) :
                # a 30 s sur une fenetre de 5 min, 12% des cotes frolaient le
                # prix de pose sans qu'on les voie descendre -- l'etiquette
                # sous-comptait donc les remplissages, et une etiquette bruitee
                # empeche mecaniquement d'apprendre. On resserre a 10 s la ou
                # ca compte (5m), on garde large la ou ca ne sert a rien (4h,
                # ou une lecture toutes les 30 s n'empilerait que des lignes
                # quasi identiques).
                if secondes <= 300:
                    pas = 10
                elif secondes <= 900:
                    pas = 30
                else:
                    pas = 300
                if now - self._md_ts.get(cle, 0) < pas:
                    continue
                self._md_ts[cle] = now
                debut = int(now // secondes) * secondes
                slug = f"{sym.lower()}-updown-{duree}-{debut}"
                meta = self._market_meta(slug)
                if not meta:
                    continue
                outcomes, token_ids = meta
                for side, tid in zip(outcomes, token_ids):
                    try:
                        book = self._live.get_book_sync(tid)
                    except Exception:
                        continue
                    if not book:
                        continue
                    self._record_book_snapshot(
                        sym, slug, side, book,
                        entry_price=None, tp_seuil=None,
                        hold_s=now - debut, danger=mk.get("danger", 0),
                        triggered=False, source="veille", duree=duree,
                    )
        self._save()

    GATEKEEPER_INTERVAL_S = 3600      # une generation par heure
    GATEKEEPER_HIST_MAX = 500

    def demarrer_recherche(self):
        """Fils de RECHERCHE, independants du trading (Steven 11/08, "ca doit
        etre 100% autonome").

        Demarres au lancement du PROCESS, pas sur /api/start : la collecte de
        marche et l'apprentissage doivent tourner meme quand le bot est a
        l'arret ou tous les symboles a 'off'. Ni l'un ni l'autre ne passe
        d'ordre -- la collecte ne fait que LIRE des carnets, le gatekeeper ne
        fait qu'apprendre en mode ombre. Aucun risque de trading involontaire.
        Idempotent : appelable plusieurs fois sans creer de doublons."""
        if getattr(self, "_recherche_demarree", False):
            return
        self._recherche_demarree = True

        def _boucle_collecte():
            while True:
                try:
                    if self._live is None:
                        # provoque l'initialisation du client (lecture seule)
                        self._read_cash(max_age=300)
                    if self._live is not None and self.state.get("market_collect_enabled", True):
                        self._collect_market_data()
                except Exception as e:
                    self._tlog("collecte_rech_err", f"⚠️ [COLLECTE] {str(e)[:160]}")
                time.sleep(self.MARKET_DATA_INTERVAL_S)

        threading.Thread(target=_boucle_collecte, daemon=True, name="collecte-marche").start()
        threading.Thread(target=self._gatekeeper_loop, daemon=True, name="gatekeeper").start()
        threading.Thread(target=self._boucle_reconciliation, daemon=True, name="reconciliation").start()
        self._log(
            "🧠 [RECHERCHE] collecte de marche + gatekeeper + reconciliation "
            "demarres (independants du trading, mode ombre)"
        )

    RECONCILIATION_INTERVAL_S = 60

    def _boucle_reconciliation(self):
        """RECONCILIATION AUTONOME (Steven 02/09, "active reconciliation
        auto l'adresse est dans .env") : independante de _running, comme
        demarrer_recherche() -- tourne MEME quand le bot est arrete/stoppe.

        Trouve en audit : 3 positions resolues depuis 18-21h restaient
        "ouvertes" dans l'etat interne (gains eventuels non reclames)
        parce que redeem_resolved() n'etait appele QUE depuis la boucle
        principale de trading, elle-meme gatee par _running -- un arret
        manuel de plusieurs heures geleait donc aussi les redeem.

        Deux taches independantes de _running :
        1. redeem_resolved() -- reclame tout gain deja tranche par
           Polymarket, peu importe si le bot trade ou non.
        2. Comparaison positions reelles (data-api) vs mk['open'] trackees
           -- log tout ecart (position reelle non trackee ou inversement)
           au lieu de decouvrir le trou des heures plus tard en audit."""
        while True:
            try:
                if self._live is not None:
                    try:
                        n = self._live.redeem_resolved()
                        if n:
                            self._log(f"💰 [RECONCILIATION] {n} gain(s) reclame(s) (independant de _running)")
                    except Exception as e:
                        self._tlog("reconcil_redeem_err", f"⚠️ [RECONCILIATION] redeem echoue : {str(e)[:160]}")

                    funder = os.environ.get("POLY_FUNDER_ADDRESS", "")
                    if funder:
                        try:
                            import requests as _rq

                            r = _rq.get(
                                "https://data-api.polymarket.com/positions",
                                params={"user": funder}, timeout=10,
                                headers={"User-Agent": "Mozilla/5.0"},
                            )
                            real_positions = r.json() if r.status_code == 200 else []
                            if not isinstance(real_positions, list):
                                real_positions = []
                        except Exception:
                            real_positions = None

                        if real_positions is not None:
                            real_by_asset = {
                                str(p.get("asset")): p for p in real_positions
                                if (p.get("size") or 0) > 0.01
                            }
                            tracked_tids = set()
                            for sym in SYMBOLS:
                                for pos in self.state["markets"].get(sym, {}).get("open", {}).values():
                                    tid = pos.get("token_id")
                                    if tid:
                                        tracked_tids.add(str(tid))
                            # position reelle avec de la valeur, jamais trackee par le bot
                            for asset, p in real_by_asset.items():
                                if asset not in tracked_tids and (p.get("currentValue") or 0) > 0.05:
                                    self._tlog(
                                        f"reconcil_untracked_{asset[:8]}",
                                        f"🚨 [RECONCILIATION] position reelle NON TRACKEE : "
                                        f"{p.get('title', '?')} {p.get('outcome', '?')} "
                                        f"{p.get('size', 0):.2f} parts valeur={p.get('currentValue', 0):.2f}$ "
                                        f"-- verifier manuellement",
                                    )
                            # position trackee par le bot mais qui n'existe plus reellement
                            # (Steven 02/09, trouve en audit : 3 positions detectees "fantomes"
                            # a CHAQUE cycle pendant des heures sans jamais etre nettoyees --
                            # cette boucle ne faisait QUE logger, jamais retirer de mk["open"].
                            # Une position absente on-chain a un solde REEL de 0 (vendue ou
                            # perdue a la resolution) -- rien a reclamer, juste a nettoyer.
                            # Delai de grace de 45s pour ne pas effacer un ordre tout juste
                            # rempli avant que data-api.polymarket.com ne l'ait indexe.)
                            now_r = time.time()
                            for sym in SYMBOLS:
                                mk_r = self.state["markets"].get(sym, {})
                                open_r = mk_r.get("open", {})
                                for key in list(open_r.keys()):
                                    pos = open_r[key]
                                    tid = str(pos.get("token_id") or "")
                                    if not tid or tid in real_by_asset:
                                        continue
                                    if now_r - pos.get("opened_ts", now_r) < 45:
                                        continue
                                    self._tlog(
                                        f"reconcil_ghost_{tid[:8]}",
                                        f"👻 [RECONCILIATION] {sym} {key} position trackee mais absente "
                                        f"on-chain (token {tid[:12]}...) -- probablement deja resolue/vendue, "
                                        f"nettoyage de l'etat interne",
                                    )
                                    open_r.pop(key, None)
                            self._save()
            except Exception as e:
                self._tlog("reconcil_err", f"⚠️ [RECONCILIATION] cycle echoue : {str(e)[:200]}")
            time.sleep(self.RECONCILIATION_INTERVAL_S)

    def _gatekeeper_loop(self):
        """ENTRAINEMENT AUTONOME, MODE OMBRE (Steven 11/08, "il s'entraine
        seul dans son coin, ca doit pas etre en local").

        Tourne DANS le service Railway : le PC de Steven peut etre eteint,
        l'apprentissage continue. Une generation numerotee par heure. Ce fil
        ne prend AUCUNE decision de trading -- il lit le journal de marche,
        apprend, se note, ecrit son verdict. Le branchement eventuel sur les
        entrees est une decision humaine separee, pas un effet de bord.

        Isole de tout : une exception ici ne doit jamais toucher le trading.
        NE depend PAS de _running : l'apprentissage continue meme bot a
        l'arret (c'est tout l'interet -- les donnees s'accumulent quand
        meme)."""
        while True:
            try:
                time.sleep(self.GATEKEEPER_INTERVAL_S)
                self._gatekeeper_cycle()
            except Exception as e:
                self._tlog("gk_err", f"⚠️ [GATEKEEPER] cycle echoue : {str(e)[:200]}")

    def _gatekeeper_cycle(self, force=False):
        """Produit UNE generation et l'archive. Retourne le resume."""
        from real_web import gatekeeper as gk

        etat = self.state.setdefault("gatekeeper", {"generation": 0, "historique": []})
        gen = int(etat.get("generation", 0)) + 1
        rows = self.lire_market_data()
        res = gk.cycle(rows, generation=gen, force=force)

        etat["generation"] = gen
        etat["dernier"] = res
        resume = {k: res.get(k) for k in
                  ("generation", "ts", "statut", "n_fenetres", "n_croisantes",
                   "n_non_croisantes", "couverture_h", "auc_moyen", "gain_filtre",
                   "gain_alea", "gain_sans", "verdict", "par_symbole")}
        etat.setdefault("historique", []).append(resume)
        if len(etat["historique"]) > self.GATEKEEPER_HIST_MAX:
            del etat["historique"][: len(etat["historique"]) - self.GATEKEEPER_HIST_MAX]
        self._save()

        if res.get("statut") == "accumulation":
            m = res.get("manque", {})
            self._log(
                f"🧠 [GATEKEEPER] gen {gen} : accumulation ({res['n_fenetres']} fenetres, "
                f"{res['n_croisantes']} croisantes) -- manque {m.get('fenetres')} fenetres, "
                f"{m.get('croisantes')} croisantes, {m.get('non_croisantes')} non croisantes"
            )
        else:
            self._log(
                f"🧠 [GATEKEEPER] gen {gen} [{res.get('statut')}] {res['n_fenetres']} fenetres "
                f"({res['couverture_h']}h) | AUC {res.get('auc_moyen')} | "
                f"filtre {res.get('gain_filtre')}$/fen vs alea {res.get('gain_alea')} "
                f"vs tout {res.get('gain_sans')} -> {str(res.get('verdict')).upper()} "
                f"(mode ombre, aucune decision de trading)"
            )
        return resume

    def _cancel_verifie(self, order_id, tag="", essais=3):
        """Annule un ordre ET VERIFIE qu'il a bien disparu (Steven 10/08,
        "il n'annulait pas correctement l'ordre de l'autre cote qui etait
        parfois achete pile au moment du TP").

        PROBLEME CORRIGE : cancel_order() renvoie False sur TOUTE exception
        (reseau, rejet API, timeout) et ce retour etait IGNORE a chaque
        appel -- un simple `self._live.cancel_order(oid)`. Un echec
        silencieux laissait donc l'ordre VIVANT dans le carnet alors qu'on
        considerait la fenetre terminee (st.pop juste apres) : il pouvait se
        remplir plusieurs minutes plus tard, sans aucun suivi ni plan de
        sortie. Ici on reessaie et on relit la liste des ordres ouverts pour
        confirmer. Retourne True si l'ordre est reellement parti."""
        if not order_id:
            return True
        for i in range(essais):
            self._live.cancel_order(order_id)
            try:
                ouverts = self._live.get_open_orders_list() or []
                encore = any(
                    (o.get("id") or o.get("orderID") or o.get("order_id")) == order_id
                    for o in ouverts
                )
            except Exception:
                return True   # verification impossible : on ne boucle pas a l'aveugle
            if not encore:
                return True
            time.sleep(0.3)
        self._log(
            f"⚠️ [ANNULATION-RATEE]{tag} ordre {order_id} TOUJOURS VIVANT apres "
            f"{essais} tentatives -> s'il se remplit, la reconciliation globale "
            f"le rattrapera (position retrackee et geree en orpheline)"
        )
        return False

    BOOK_SNAPSHOT_HISTORY_SIZE = 8000

    def _record_book_snapshot(self, symbol, slug, side, book, entry_price, tp_seuil,
                              hold_s, danger, triggered, source="position", duree="5m"):
        """Historique brut de carnet pour un futur "gatekeeper" ML (Steven
        08/08) : QUE des features, aucune decision -- l'idee (proposee par
        Steven) est un modele leger (regression logistique/arbre) qui
        validerait a posteriori si les conditions actuelles ressemblent a
        celles ou le TP a bien tenu, une fois qu'on aura assez de semaines
        de donnees. Pas assez de volume aujourd'hui pour entrainer quoi que
        ce soit d'honnete -- ceci ne fait QUE commencer a accumuler."""
        try:
            import datetime
            _dt = datetime.datetime.fromtimestamp(time.time(), tz=datetime.timezone.utc)
            hour_utc, dow = _dt.hour, _dt.weekday()
        except Exception:
            hour_utc = dow = None
        bids = (book or {}).get("bids") or []
        asks = (book or {}).get("asks") or []
        bid_top = bids[0][0] if bids else None
        ask_top = asks[0][0] if asks else None
        bid_depth = round(sum(sz for _, sz in bids[:3]), 2) if bids else 0.0
        ask_depth = round(sum(sz for _, sz in asks[:3]), 2) if asks else 0.0
        _tot_depth = bid_depth + ask_depth
        # TWAP CHAINLINK OFFICIELLE (Steven 02/09, "recupere aussi ce flux dans
        # notre db comme le reste de recherche") : Polymarket resout sur CE
        # flux (voir description de marche -- "resolution source: Chainlink
        # BTC/USD TWAP-60s"), pas sur Binance. On l'accumule ici, cote a cote
        # avec le spot Binance deja capture, pour pouvoir un jour mesurer/
        # backtester l'ecart Binance-vs-Chainlink au lieu de le deviner.
        twap30 = twap60 = None
        try:
            if hasattr(self, "_ws"):
                twap30 = self._ws.twap(symbol, window_s=30)
                twap60 = self._ws.twap(symbol, window_s=60)
        except Exception:
            pass
        ligne = {
            "ts": round(time.time(), 1), "symbol": symbol, "slug": slug, "side": side,
            "hour_utc": hour_utc, "dow": dow, "danger": danger,
            # tp_seuil/entry_price sont None en mode veille (aucune position) --
            # round() sur None planterait, et le crash serait avale par le
            # wrapper FAST-EXIT comme celui de la semaine derniere.
            "entry_price": entry_price,
            "tp_seuil": round(tp_seuil, 4) if tp_seuil is not None else None,
            "hold_s": round(hold_s, 1),
            "source": source,
            # duree de la fenetre : permet de separer les jeux 5m / 15m / 4h
            # a l'analyse (les economies de MSF sont calibrees sur 5m).
            "duree": duree,
            "bid_top": bid_top, "ask_top": ask_top,
            "spread": round(ask_top - bid_top, 4) if (bid_top is not None and ask_top is not None) else None,
            "bid_depth_top3": bid_depth, "ask_depth_top3": ask_depth,
            "imbalance_bid_pct": round(100 * bid_depth / _tot_depth, 1) if _tot_depth > 0 else None,
            "triggered": triggered,
            "twap30_chainlink": twap30, "twap60_chainlink": twap60,
        }
        # AJOUT SEUL dans un fichier dedie (cf. MARKET_DATA_FILE) : ne passe
        # plus par self.state, donc _save() reste leger meme avec des jours
        # de donnees accumulees.
        try:
            MARKET_DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(MARKET_DATA_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(ligne, ensure_ascii=False) + "\n")
            self._md_ecrits = getattr(self, "_md_ecrits", 0) + 1
            # rotation rare : on ne verifie la taille que tous les 5000 ajouts
            if self._md_ecrits % 5000 == 0:
                self._rotate_market_data()
        except Exception as e:
            self._tlog("md_write_err", f"⚠️ [COLLECTE] ecriture impossible : {e}")

    def _rotate_market_data(self):
        """Garde la MOITIE la plus recente quand le fichier depasse la taille
        maximale -- on prefere perdre le passe lointain que saturer le disque
        du service de trading."""
        try:
            if not MARKET_DATA_FILE.exists():
                return
            if MARKET_DATA_FILE.stat().st_size < MARKET_DATA_MAX_MB * 1024 * 1024:
                return
            lignes = MARKET_DATA_FILE.read_text(encoding="utf-8").splitlines()
            garde = lignes[len(lignes) // 2:]
            MARKET_DATA_FILE.write_text("\n".join(garde) + "\n", encoding="utf-8")
            self._log(f"♻️ [COLLECTE] rotation : {len(lignes)} -> {len(garde)} lignes")
        except Exception as e:
            self._tlog("md_rot_err", f"⚠️ [COLLECTE] rotation impossible : {e}")

    @staticmethod
    def lire_market_data(max_lignes=200000):
        """Relit le journal de marche (les plus recentes en dernier)."""
        if not MARKET_DATA_FILE.exists():
            return []
        out = []
        try:
            with open(MARKET_DATA_FILE, encoding="utf-8") as f:
                for l in f:
                    l = l.strip()
                    if not l:
                        continue
                    try:
                        out.append(json.loads(l))
                    except json.JSONDecodeError:
                        continue
        except Exception:
            return out
        return out[-max_lignes:]

    SLIPPAGE_HISTORY_SIZE = 2000

    def _record_slippage(self, symbol, bid, requested_qty, filled_qty):
        """Enregistre une sortie agressive (TP/cutoff/unwind) pour la base de
        slippage multi-dimensionnelle (Steven 08/08). Une entree par vente,
        avec assez de dimensions pour trancher plus tard "a quelle heure/quel
        niveau de volatilite le coussin de sortie coute le plus cher" -- ce
        que le filtre horaire backteste plus tot manquait cruellement de
        donnees pour faire honnetement (5 jours seulement)."""
        if not symbol:
            return
        try:
            import datetime
            _dt = datetime.datetime.fromtimestamp(time.time(), tz=datetime.timezone.utc)
            hour_utc = _dt.hour
            dow = _dt.weekday()
        except Exception:
            hour_utc = dow = None
        mk = self.state.get("markets", {}).get(symbol) or {}
        danger = mk.get("danger", 0)
        posted_price = round(max(0.01, bid - 0.02), 2)  # formule exacte de sell_position(aggressive=True)
        fill_ratio = round(min(1.0, filled_qty / requested_qty), 3) if requested_qty > 0 else None
        hist = self.state.setdefault("slippage_history", [])
        hist.append({
            "ts": round(time.time(), 1),
            "symbol": symbol,
            "hour_utc": hour_utc,
            "dow": dow,
            "danger": danger,
            "bid": round(bid, 4),
            "posted_price": posted_price,
            "buffer": round(bid - posted_price, 4),
            "requested_qty": round(requested_qty, 2),
            "filled_qty": round(filled_qty, 2),
            "fill_ratio": fill_ratio,
        })
        if len(hist) > self.SLIPPAGE_HISTORY_SIZE:
            del hist[: len(hist) - self.SLIPPAGE_HISTORY_SIZE]

    def _annuler_ordres_slug(self, sym, slug):
        """Annule TOUT ordre encore ouvert rattache a ce slug (Steven 11/08).

        Motif : "ca sert a rien de se laisser acheter une leg a -1min de la
        resolution, on n'a meme plus le temps de faire une completion". Tant
        qu'on n'arrive pas a solder une jambe nue, les achats passifs encore
        en carnet restent servables ; etre rempli si tard n'ouvre plus aucune
        issue (ni verrou, ni completion, ni revente) et ne fait qu'empiler une
        seconde perte sur celle qu'on essaie de fuir.

        Les ordres sont suivis dans des sous-etats distincts selon la strategie
        (MSF, pre-ouverture, bothside, TP passif...). Plutot que de plomber
        chaque chemin, on balaie recursivement l'etat du symbole et on annule
        tout identifiant d'ordre rencontre sous une branche qui porte ce slug.
        Annuler un ordre deja mort est sans effet -> aucun risque a ratisser
        large, et rien d'un AUTRE slug n'est touche."""
        mk = self.state["markets"].get(sym)
        if not mk:
            return 0
        vus, oids = set(), set()

        def _scan(noeud, dedans):
            if id(noeud) in vus:
                return
            vus.add(id(noeud))
            if isinstance(noeud, dict):
                ici = dedans or noeud.get("slug") == slug
                for cle, val in noeud.items():
                    if isinstance(cle, str) and slug in cle:
                        _scan(val, True)
                        continue
                    if ici and isinstance(cle, str) and (
                        cle == "oid" or cle == "order_id" or cle.endswith("_order_id")
                    ):
                        if isinstance(val, str) and val:
                            oids.add(val)
                        continue
                    _scan(val, ici)
            elif isinstance(noeud, (list, tuple)):
                for val in noeud:
                    _scan(val, dedans)

        _scan(mk, False)
        n = 0
        for oid in oids:
            try:
                self._live.cancel_order(oid)
                n += 1
            except Exception:
                pass
        return n

    @staticmethod
    def _prix_vente_absorbant(book, shares, urgence=0):
        """Prix de FAK qui absorbe REELLEMENT `shares`, au lieu de bid-0.02.

        Diagnostic Steven 11/08 sur ETH (4 ventes a 0 part remplie d'affilee,
        puis solde force a 0.450 pour -1.00$) : le prix de sortie etait fige a
        "meilleur bid - 2 centimes". Deux mesures expliquent l'echec :
          - profondeur mediane top-3 : BTC 960 parts, ETH 245 -- ETH est 4x
            plus fin, mais 6.67 parts y restent absorbables 91.8% du temps
            (BTC 93.0%). La finesse SEULE n'explique donc pas 4 echecs de
            suite : c'etait bien le prix, pas la taille.
          - le prix tombait de 0.760 a 0.450 en 45s (~0.7 centime/seconde).
            Avec la latence de lecture+envoi, un ordre poste 2 centimes sous
            un bid deja perime n'a plus rien a croiser -> FAK tue net.
        On descend donc le carnet jusqu'au niveau qui couvre notre taille, et
        l'agressivite s'escalade a chaque echec (urgence) au lieu de rester
        constante. Sur un carnet epais (BTC) le niveau trouve est le sommet :
        comportement inchange, aucune concession de prix -- c'est uniquement
        quand le carnet est fin ou en fuite que l'on va plus loin.
        Retourne (prix, profondeur_cumulee) ou (None, 0.0)."""
        bids = (book or {}).get("bids") or []
        if not bids:
            return None, 0.0
        try:
            top = float(bids[0][0])
        except (TypeError, ValueError, IndexError):
            return None, 0.0
        cum = 0.0
        niveau = top
        for lvl in bids:
            try:
                px, sz = float(lvl[0]), float(lvl[1])
            except (TypeError, ValueError, IndexError):
                continue
            cum += sz
            niveau = px
            if cum >= shares:
                break
        u = max(0, int(urgence))
        marge = 0.02 + 0.02 * u        # tampon anti-derive du carnet
        plafond = 0.05 + 0.05 * u      # concession max sous le meilleur bid
        prix = min(niveau, top) - marge
        prix = max(prix, top - plafond)   # au 1er essai on ne brade pas
        return round(max(0.01, prix), 2), round(cum, 2)

    def _sell_orphan(self, token_id, shares, tag="", entry_price=None, symbol=None, slug=None,
                     side=None, urgence=0, loss_tag=None, entry_ts=None):
        """Revend `shares` parts au meilleur bid et VERIFIE ON-CHAIN que la vente
        est reellement passee (Steven 22/07 : plus jamais de vente supposee).
        Retourne le nombre de parts effectivement vendues. Log explicite.
        NB (corrige Steven 05/08) : contrairement a ce qui etait suppose ici,
        le CLOB ACCEPTE les ventes sous 5 parts -- verifie on-chain, 8 ventes
        sous le plancher passees dont une de 1.37 part. Le minimum de 5 parts
        ne s'applique qu'a l'ACHAT. Aucun appelant n'a donc a "gerer" ce cas.
        `entry_price`/`symbol`/`slug`/`side` (Steven 30/07, "solde a 13.04 mais
        pnl dit +4.85 ?!") : quand fournis, ENREGISTRE le trade (mode=real,
        pnl reel achat->vente) dans mk['trades'] -> sans ca, decouvert que
        les cycles achat-puis-revente-immediate (unwind d'orphelin) depensaient
        et recuperaient du vrai argent SANS JAMAIS que le delta (souvent une
        petite perte de spread) soit compte nulle part -> pnl_total_real
        mentait par omission, ecart de plusieurs dollars invisible.
        `loss_tag`/`entry_ts` (Steven 15/08, backtest "10 idees") : le champ
        `loss_tag` existait deja dans le schema mais n'etait JAMAIS rempli --
        chaque appelant taguait un texte de LOG different (ABANDON, TP-PASSIF-
        REPLI, CALM-SL, MMBOT-SELL...) mais le trade PERSISTE ne gardait que
        resolved_by="unwind", identique pour tous -- impossible ensuite de
        savoir quel declencheur a cause quelle perte sans fouiller les logs
        texte. `entry_ts`, quand fourni par l'appelant (ex: leg_seule['fill_ts']),
        remplace le timestamp de VENTE par le vrai timestamp d'ENTREE pour
        `opened_ts`/`start_ts` -- avant ce fix les deux etaient identiques
        (les deux tamponnes a l'instant de la vente), rendant toute mesure de
        duree de detention impossible a reconstruire apres coup."""
        if shares < 0.01:
            return 0.0
        # TOP-UP DESACTIVE (Steven 05/08) : ce bloc partait du principe que le
        # CLOB refuse les ventes < 5 parts, et ACHETAIT donc le complement
        # d'une position DEJA perdante juste pour pouvoir la solder. C'est
        # faux : verifie sur data-api.polymarket.com/activity, 8 ventes sous
        # 5 parts sont passees sur ce wallet, la plus petite a 1.37 part
        # (sol-updown-5m-1785819600). Le plancher de 5 parts est une regle
        # d'ACHAT, pas de vente. Le top-up ne servait donc a rien et coutait
        # de l'argent : observe on-chain sur btc-updown-5m-1785900300, achat
        # de 8.30 parts a 0.20 (1.75$) immediatement revendues a 0.13 (1.01$)
        # = -0.74$ jetes pour "pouvoir vendre" ce qui etait deja vendable.
        # FIX (Steven 02/09, "les 2 mecanismes se battent pour le meme
        # solde") : un ordre PASSIF encore ouvert (SPREAD-CAPTURE, TP-PASSIF)
        # sur ce meme slug peut se faire servir PENDANT qu'on tente une vente
        # AGRESSIVE ici -- les deux consomment le meme solde on-chain en
        # parallele. Confirme en prod : "not enough balance ... sum of
        # matched orders: 5090000" (un ordre concurrent avait deja matche
        # une partie du solde), et une verification avant/apres qui a
        # attribue TOUT le mouvement (5.09 parts, la position entiere) a un
        # appel qui n'en demandait que la moitie (2.54). On annule tout
        # ordre resident du slug AVANT de tenter la vente agressive -> une
        # seule main sur le solde a la fois.
        if symbol and slug:
            self._annuler_ordres_slug(symbol, slug)
        book = self._live.get_book_sync(token_id)
        bid = book["bids"][0][0] if book and book.get("bids") else None
        if bid is None:
            self._log(
                f"🚫 [VENTE]{tag} pas de bid (carnet vide) -> vente impossible pour l'instant"
            )
            return 0.0
        before = self._live.position_size(token_id)
        before = before if before >= 0 else shares
        # FIX (Steven 02/09, vraie cause trouvee via l'erreur enfin lisible :
        # "not enough balance / allowance: balance: 1767542, order amount:
        # 1770000") : round(shares, 2) arrondissait parfois VERS LE HAUT
        # au-dela du solde on-chain reel (1.767542 arrondi a 1.77, qui
        # n'existe pas) -> l'API rejette, la vente echoue a 400, retente,
        # rejete pareil, jusqu'a resolution. Plafonne desormais sur le solde
        # REEL deja lu ci-dessus (`before`) et arrondit vers le BAS (jamais
        # au-dela de ce qu'on possede vraiment).
        if before > 0:
            shares = min(shares, before)
        shares = math.floor(shares * 100) / 100
        if shares < 0.01:
            return 0.0
        with self._order_lock:
            # AGRESSIF (Steven 30/07, "orphelin evitable ?") : GTC pile au bid
            # etait un ordre MAKER, aucune garantie de croiser -> observe
            # plusieurs fois a 0/N vendues apres le delai de verif complet.
            # _sell_orphan sert TOUJOURS a sortir vite (unwind, stop-loss,
            # fin de fenetre) -> la vitesse prime sur le prix ici.
            _chrono_sell_t0 = time.time()
            _px_vente, _prof = self._prix_vente_absorbant(book, shares, urgence)
            if _px_vente is None:
                _px_vente = round(max(0.01, bid - 0.02), 2)
            if _prof < shares or urgence > 0:
                self._log(
                    f"📉 [CARNET]{tag} bid={round(bid,2)} profondeur={_prof} parts pour "
                    f"{round(shares,2)} demandees -> FAK @{_px_vente} (urgence {urgence})"
                )
            # marge=0 : le prix est deja le niveau absorbant calcule ci-dessus.
            _sell_resp = self._live.sell_position(token_id, _px_vente, round(shares, 2),
                                                  aggressive=True, marge=0.0)
            _chrono_sell_ms = round((time.time() - _chrono_sell_t0) * 1000)
            # LOG DE LA VRAIE ERREUR API (Steven 01/09, "pourquoi le tp
            # n'agit pas" -- vente a 0/N repetee malgre une profondeur de
            # carnet enorme). Avant : la reponse de sell_position n'etait
            # JAMAIS inspectee ici, seule la verification on-chain APRES
            # coup determinait "0 vendues", sans jamais dire pourquoi
            # l'ordre lui-meme avait echoue (rejet API, erreur de signature,
            # etc). Rend enfin la vraie cause visible dans les logs.
            if _sell_resp is not None and not _sell_resp.get("success", True):
                self._log(
                    f"❗ [VENTE-ERREUR]{tag} px={_px_vente} shares={round(shares,2)} "
                    f"sell_position a echoue : "
                    f"{str(_sell_resp.get('error', ''))[:1200]}"
                )
            # CHRONO SORTIE (Steven 08/08) : c'est le chemin TP/cutoff/unwind
            # MSF -- jusqu'ici zero mesure, contrairement a l'entree bothside.
            _tim_sell = (_sell_resp or {}).get("timing") or {}
            self._log(
                f"⏱️ [CHRONO-MSF-SORTIE]{tag} total={_chrono_sell_ms}ms "
                f"sig={_tim_sell.get('signature_ms','?')}ms "
                f"rust={_tim_sell.get('rust_resign_ms','?')}ms"
                f"[{'RUST' if _tim_sell.get('rust_used') else 'py'}] "
                f"post={_tim_sell.get('post_orders_ms','?')}ms"
            )
            self.state.setdefault("latency_history", []).append({
                "ts": _chrono_sell_t0, "symbol": symbol, "strategy": "sell_orphan",
                "signature_ms": _tim_sell.get("signature_ms"),
                "rust_resign_ms": _tim_sell.get("rust_resign_ms"),
                "rust_used": _tim_sell.get("rust_used", False),
                "post_orders_ms": _tim_sell.get("post_orders_ms"),
                "total_ms": _tim_sell.get("total_ms"),
            })
            if len(self.state["latency_history"]) > 1000:
                del self.state["latency_history"][: len(self.state["latency_history"]) - 1000]
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
        # BASE DE SLIPPAGE MULTI-DIMENSIONNELLE (Steven 08/08) : sur une sortie
        # AGRESSIVE (FAK sous le bid, cf. sell_position), le "cout" reel est le
        # coussin qu'on a du concéder (bid observe - prix vraiment poste) pour
        # garantir l'execution, plus le taux de remplissage effectif -- pas un
        # vrai prix moyen d'execution (l'API ne le renvoie pas ici), mais une
        # mesure honnete et reproductible du prix qu'on a du payer pour la
        # vitesse. Segmentee par heure UTC / jour de semaine / volatilite
        # (danger_score deja calcule ailleurs, reutilise tel quel).
        self._record_slippage(symbol, bid, shares, sold)
        if entry_price is not None and symbol is not None and sold > 0:
            _pnl = round(sold * (bid - entry_price), 3)
            mk_rec = self.state["markets"].get(symbol)
            if mk_rec is not None:
                _now_ts = time.time()
                # opened_ts/start_ts restent _now_ts par defaut (FIX Steven
                # 30/07 ci-dessous, ne jamais les laisser vides) -- seulement
                # remplaces par le vrai entry_ts quand l'appelant le fournit,
                # pour ne rien casser sur les chemins qui ne le passent pas
                # encore.
                _open_ts = entry_ts if entry_ts else _now_ts
                mk_rec["trades"].append({
                    "symbol": symbol, "slug": slug, "side": side, "mode": "real",
                    "strat": "orphan", "entry_price": entry_price, "exit_price": bid,
                    "filled_shares": sold, "cost": round(sold * entry_price, 2),
                    "pnl": _pnl, "win": _pnl > 0, "resolved_by": "unwind",
                    "loss_tag": loss_tag,
                    "hold_s": round(_now_ts - entry_ts, 1) if entry_ts else None,
                    # FIX (Steven 30/07, "je vois toujours pas nos dernieres
                    # trades") : le dashboard trie/filtre sur "opened_ts", pas
                    # "ts" -> mes trades UNWIND n'avaient pas ce champ, donc
                    # (champ manquant) triaient comme "aucune date" et
                    # remontaient EN PREMIER en tri desc (devant les vrais
                    # trades recents qui, eux, ont un opened_ts). Les 2 champs
                    # sont maintenant remplis, coherent avec le reste du code.
                    "opened_ts": _open_ts, "start_ts": _open_ts, "ts": _now_ts,
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
        # BNB : volatilite mesuree 1.34x celle de BTC sur 60 bougies 1min,
        # entre ETH (1.55) et BTC -> stop legerement elargi.
        "DOGE": 1.2,
        "BNB": 1.1,
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

    def _stagger_budget(self):
        """Mise de la jambe 1 d'un arb decale, proportionnelle au capital."""
        return round(
            min(STAGGER_BUDGET_MAX, max(STAGGER_BUDGET_MIN, self._investable() * STAGGER_BUDGET_FRAC)),
            2,
        )

    def _try_stagger_entry(self, sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
        """ARB DECALE, jambe 1 (Steven 06/08). Achete le cote BAS tot dans la
        fenetre, en pariant que le cote oppose descendra assez pour verrouiller
        avant la resolution -- "on fabrique notre propre combined".

        Voir les constantes STAGGER_* pour le raisonnement complet et les
        mesures. Ne s'active que si aucun arb simultane n'etait possible."""
        if self._preopen_only(sym):
            return False   # symbole reserve a la pre-ouverture

        if not STAGGER_ENABLED or mode != "real":
            return False
        now = synced_now()
        secs_left = p.get("end_ts", now) - now
        if secs_left < STAGGER_MIN_SECS_LEFT:
            return False                      # trop tard pour esperer un mouvement
        # ENTREE SEULEMENT EN DEBUT DE FENETRE (cf. STAGGER_MAX_LEAD_S). Le
        # start_ts est l'ouverture du marche ; on refuse d'ouvrir une jambe 1
        # une fois la tendance formee.
        _open_ts = p.get("start_ts") or (p.get("end_ts", now) - 300)
        _lead = now - _open_ts
        if _lead > STAGGER_MAX_LEAD_S:
            self._tlog(
                f"stagger_tard_{sym}",
                f"⏱️ [ARB-DECALE] {sym} {slug} ouverture il y a {_lead:.0f}s > "
                f"{STAGGER_MAX_LEAD_S}s -> trop tard, la tendance est formee "
                f"(44% de verrous avant 15s contre 26% apres)",
            )
            return False
        if mk.setdefault("stagger_tried", {}).get(slug):
            return False                      # une seule tentative par fenetre
        if any(k.startswith(f"{slug}|") for k in mk["open"]):
            return False                      # deja une position sur ce marche

        # on prend le cote le MOINS cher, et seulement dans la bande utile
        cands = []
        for side in outcomes:
            q = quotes.get(side)
            ask = q[1] if q else None
            if ask is not None:
                cands.append((ask, side))
        if len(cands) != 2:
            return False
        cands.sort()
        ask, side = cands[0]
        other_ask = cands[1][0]
        if not (STAGGER_ENTRY_MIN <= ask <= STAGGER_ENTRY_MAX):
            return False
        # marge a parcourir : de combien l'autre cote doit-il baisser ?
        besoin = round(STAGGER_COMPLETE_MAX - ask, 3)
        if other_ask <= besoin:
            return False   # deja verrouillable -> c'est un arb simultane, pas notre role

        cash, _ = self._read_cash(max_age=0)
        if cash is None:
            return False
        investable = max(0.0, cash - self.floor())
        budget = round(min(self._stagger_budget(), investable), 2)
        budget = max(budget, round(MIN_SELL_SHARES * ask, 2))
        if budget > investable or budget < MIN_BUDGET_USD:
            return False

        # ── RESERVATION DU CAPITAL DE LA JAMBE 2 (Steven 06/08) ──────────
        # CAUSE REELLE des non-completions, diagnostiquee par Steven : "on
        # n'avait pas assez de fonds pour ouvrir toutes les legs dont on avait
        # besoin, sinon a chaque ouverture de marche on a systematiquement
        # reussi a creer l'arb".
        # Verifie sur les 3 echecs de la premiere heure : SOL ouverte avec
        # 20.25$ en caisse (completion possible), mais DOGE et XRP ouvertes
        # SIMULTANEMENT avec 5.47$ -> les deux jambes 1 ont consomme 4.16$, il
        # restait 1.31$ alors qu'il fallait ~2.10$ pour completer UNE seule
        # d'entre elles. Le bot se mettait lui-meme dans l'impossibilite de
        # finir : ce n'etait pas la strategie, c'etait l'absence de reserve.
        # Desormais on n'engage une jambe 1 QUE si le capital de la jambe 2
        # est deja disponible, RESERVES COMPRIS pour les staggers deja en
        # attente sur d'autres marches.
        shares_prev = round(budget / ask, 2)
        besoin_leg2 = round(shares_prev * besoin, 2)   # pire cas de completion
        deja_reserve = 0.0
        for _k, _p in mk["open"].items():
            if _p.get("strat") == "stagger" and _p.get("mode") == "real":
                deja_reserve += (_p.get("filled_shares") or 0) * (
                    _p.get("stagger_need_below") or 0
                )
        besoin_total = round(budget + besoin_leg2 + deja_reserve, 2)
        if besoin_total > investable:
            self._tlog(
                f"stagfunds_{sym}",
                f"⛔ [ARB-DECALE] {sym} {slug} pas de reserve pour la jambe 2 : "
                f"jambe1 {budget:.2f}$ + jambe2 ~{besoin_leg2:.2f}$ "
                f"+ {deja_reserve:.2f}$ deja reserves = {besoin_total:.2f}$ "
                f"> {investable:.2f}$ dispo -> on n'ouvre pas ce qu'on ne pourra pas finir",
            )
            return False

        ok_exp, why_exp = self._exposure_ok(sym, mk, slug, budget)
        if not ok_exp:
            return False

        mk["stagger_tried"][slug] = time.time()
        tid = token_ids[outcomes.index(side)]
        self._log(
            f"🎲 [ARB-DECALE] {sym} {slug} jambe1 {side} @ {ask:.3f} budget={budget:.2f}$ "
            f"-- l'autre cote est a {other_ask:.3f}, il doit descendre sous {besoin:.3f} "
            f"pour verrouiller ({secs_left:.0f}s devant)"
        )
        with self._order_lock:
            res = self._live.snipe_buy_market(tid, round(ask + 0.02, 2), budget)
        filled = res.get("filled_shares", 0.0)
        if filled <= 0:
            self._log(f"⚠️ [ARB-DECALE] {sym} {slug} jambe1 non remplie ({res.get('error', '')})")
            return False
        avg = res.get("avg_cost") or ask
        self._add_slug_spent(mk, slug, round(filled * avg, 2))
        mk["open"][f"{slug}|{side}"] = {
            "symbol": sym, "slug": slug, "side": side, "mode": "real",
            # strat "stagger" : gere EXCLUSIVEMENT par _manage_stagger tant que
            # la paire n'est pas completee (ni bothside ni pnl_tier_exits n'y
            # touchent -- ils filtrent sur leur propre strat).
            "strat": "stagger", "token_id": tid, "entry_price": avg,
            "filled_shares": filled, "cost": round(filled * avg, 2),
            "start_ts": p["start_ts"], "pair": p.get("pair"), "end_ts": p["end_ts"],
            "opened_ts": time.time(), "buffer": 0.0,
            "stagger_need_below": besoin,
        }
        self._log(
            f"✅ [ARB-DECALE] {sym} {slug} jambe1 {side} {filled} parts @ {avg:.3f} "
            f"-> en attente du verrou (cible : autre cote < {besoin:.3f})"
        )
        return True

    def _preopen_only(self, sym):
        """True si ce symbole est reserve a la pre-ouverture (Steven 06/08).
        Empeche toute AUTRE strategie d'y ouvrir une position -- les deux
        mecanismes se marchaient dessus (cf. PREOPEN_EXCLUSIVE)."""
        return PREOPEN_ENABLED and PREOPEN_EXCLUSIVE and sym in PREOPEN_SYMBOLS

    def _preopen_budget(self):
        """TOTAL a engager sur la fenetre (les DEUX jambes ensemble).
        Cf. PREOPEN_TOTAL_FRAC : cette valeur designe bien le total, pas
        le montant par jambe comme dans la version precedente."""
        return round(
            min(PREOPEN_BUDGET_MAX, max(PREOPEN_BUDGET_MIN, self._investable() * PREOPEN_TOTAL_FRAC)),
            2,
        )

    def _preopen_state(self, mk):
        return mk.setdefault("preopen", {})

    def _preopen_record(self, sym, slug, issue, **kw):
        """JOURNAL DES TENTATIVES PRE-OUVERTURE (Steven 06/08).

        Steven : "en pre ouverture je ne vois aucune perte j'ai meme
        l'impression de n'avoir aucun frais quand ca ne passe pas mon solde
        ne bouge pas". C'est exact et c'est structurel : Polymarket ne regle
        QUE les trades executes -- verifie sur l'historique on-chain, qui ne
        contient que des types TRADE et REDEEM, aucun ORDER/CANCEL. Poser
        puis annuler ne produit aucun evenement, donc aucun mouvement de
        solde. Le cout d'une tentative ratee est exactement zero.

        Consequence directe : le taux de reussite ne se lit PAS dans le PnL
        (les echecs y sont invisibles), il faut le journaliser ici. Sans ca
        on ne peut pas savoir si poser plus agressivement ameliore ou degrade
        quoi que ce soit.

        issue : 'both' (verrouille), 'solo' (une jambe, soldee), 'none'
        (rien rempli, gratuit), 'rejected' (ordre refuse, gratuit)."""
        h = self.state.setdefault("preopen_hist", [])
        h.append({
            "ts": time.time(), "symbol": sym, "slug": slug, "issue": issue, **kw,
        })
        # borne memoire : ~2 fenetres/10min -> 400 couvre plusieurs jours
        if len(h) > 400:
            del h[: len(h) - 400]

    def _manage_preopen(self, sym):
        """ARB PRE-OUVERTURE EN MAKER (Steven 06/08).

        Deux phases, gerees a chaque cycle :
          A. POSE  -- sur une fenetre pas encore ouverte, si le combine des
             deux meilleurs bids verrouille, on POSE un achat GTC passif de
             chaque cote (maker -> zero frais).
          B. SUIVI -- on regarde ce qui s'est rempli. Les deux : arb
             verrouille sans frais. Une seule : on annule l'autre et on solde
             la jambe avant l'ouverture. Aucune : on annule tout a T-30s.

        Voir les constantes PREOPEN_* pour les mesures qui justifient tout ca."""
        if not PREOPEN_ENABLED or sym not in PREOPEN_SYMBOLS:
            return
        if self.state["modes"].get(sym) != "real":
            return
        mk = self.state["markets"][sym]
        st = self._preopen_state(mk)
        now = time.time()

        # ── B. SUIVI DES ORDRES DEJA POSES ──────────────────────────────
        for slug in list(st.keys()):
            e = st[slug]
            lead = e.get("open_ts", 0) - now
            fills = {}
            for side in ("a", "b"):
                leg = e.get(side) or {}
                tid = leg.get("token_id")
                if not tid:
                    continue
                held = self._live.position_size(tid)
                fills[side] = max(0.0, held if held and held > 0 else 0.0)
            fa, fb = fills.get("a", 0.0), fills.get("b", 0.0)
            got_a, got_b = fa > 0.01, fb > 0.01

            if got_a and got_b:
                # DESEQUILIBRE = LE SEUL DEFAUT D'EXECUTION QUI COUTE
                # (Steven 06/08). Mesure sur les 5 fenetres pre-ouverture :
                # les 4 a parts equilibrees (1.00x) sont 4 gagnantes sur 4,
                # +0.90$ pour 19.14$ engages ; la SEULE perdante (-0.71$) est
                # la seule desequilibree (2.03x). Le gagnant paie 1$ par PART,
                # donc seul min(parts) est couvert -- l'excedent de la grosse
                # jambe est un pari directionnel nu, pas de l'arb.
                #
                # Avant, ce bloc verrouillait des le MOINDRE remplissage des
                # deux cotes, y compris 2.51 contre 5.09 parts, et laissait en
                # plus les ordres vivants apres avoir enregistre les tailles.
                # Desormais : tant qu'il reste du temps, on laisse la petite
                # jambe se remplir ; a T-30s on fige et on solde l'excedent.
                imb = max(fa, fb) / max(0.01, min(fa, fb))
                if imb > PREOPEN_MAX_IMBALANCE and lead > PREOPEN_CANCEL_BEFORE_S:
                    self._tlog(
                        f"preopen_imb_{sym}",
                        f"⏳ [PRE-OUVERTURE] {sym} {slug} remplissage inegal "
                        f"({fa:.2f}/{fb:.2f} parts, {imb:.2f}x) -> on laisse la petite "
                        f"jambe se remplir, encore {lead:.0f}s",
                    )
                    continue

                # on FIGE avant d'enregistrer : sans ca les ordres restaient
                # vivants et continuaient a remplir apres coup, en dehors de
                # toute comptabilite.
                for side in ("a", "b"):
                    oid = (e.get(side) or {}).get("order_id")
                    if oid:
                        self._live.cancel_order(oid)
                for side in ("a", "b"):
                    leg = e.get(side) or {}
                    tid = leg.get("token_id")
                    if tid:
                        held = self._live.position_size(tid)
                        fills[side] = max(0.0, held if held and held > 0 else 0.0)
                fa, fb = fills.get("a", 0.0), fills.get("b", 0.0)
                if not (fa > 0.01 and fb > 0.01):
                    continue        # relu apres annulation : on repasse par le cas normal

                # EXCEDENT : on ramene les deux jambes a la meme taille. Ce qui
                # depasse min(parts) n'est couvert par rien, on le solde.
                gros = "a" if fa > fb else "b"
                excedent = round(abs(fa - fb), 2)
                if excedent >= MIN_SELL_SHARES:
                    legx = e[gros]
                    vendu = self._sell_orphan(
                        legx["token_id"], excedent,
                        f" {sym} {slug} {legx['side']} PRE-OUVERTURE-EXCEDENT",
                        entry_price=legx["price"], symbol=sym, slug=slug, side=legx["side"],
                    )
                    self._log(
                        f"⚖️ [PRE-OUVERTURE] {sym} {slug} excedent {excedent:.2f} parts sur "
                        f"{legx['side']} -> {vendu:.2f} soldees, les 2 jambes reviennent a "
                        f"{min(fa, fb):.2f} parts (l'excedent n'etait couvert par rien)"
                    )
                    fills[gros] = round(fills[gros] - vendu, 2)
                    fa, fb = fills.get("a", 0.0), fills.get("b", 0.0)
                elif excedent > 0.01:
                    # sous le plancher de vente : on garde, le risque porte sur
                    # moins d'une part, ca ne vaut pas un aller-retour de spread.
                    self._tlog(
                        f"preopen_dust_{sym}",
                        f"[PRE-OUVERTURE] {sym} {slug} excedent {excedent:.2f} parts sous le "
                        f"plancher de vente -> conserve, risque negligeable",
                    )

                # arb verrouille, on enregistre et on sort
                for side in ("a", "b"):
                    leg = e[side]
                    n = fills[side]
                    mk["open"][f"{slug}|{leg['side']}"] = {
                        "symbol": sym, "slug": slug, "side": leg["side"], "mode": "real",
                        # strat reste 'bothside' A DESSEIN : chaque nouveau
                        # strat doit etre ajoute a la main dans les filtres de
                        # _manage_pnl_tier_exits, sinon la position se retrouve
                        # sans aucune gestion TP/SL (piege deja rencontre avec
                        # fav/nearcert/copy). On marque l'origine a cote.
                        "strat": "bothside", "preopen": True,
                        "token_id": leg["token_id"],
                        "entry_price": leg["price"], "filled_shares": round(n, 2),
                        "cost": round(n * leg["price"], 2),
                        "start_ts": e.get("open_ts"), "pair": None,
                        "end_ts": e.get("open_ts", now) + 300,
                        "opened_ts": now, "buffer": 0.0,
                    }
                    self._add_slug_spent(mk, slug, round(n * leg["price"], 2))
                pa, pb = e["a"]["price"], e["b"]["price"]
                self._log(
                    f"🎉 [PRE-OUVERTURE] {sym} {slug} LES DEUX JAMBES REMPLIES "
                    f"{pa:.3f}+{pb:.3f}={pa + pb:.3f} ({fa:.2f}/{fb:.2f} parts) "
                    f"-- maker, ZERO frais"
                )
                self._tag_pair_lock(
                    mk["open"].get(f"{slug}|{e['a']['side']}"),
                    mk["open"].get(f"{slug}|{e['b']['side']}"),
                    pa + pb, tag=f" {sym} {slug} PRE-OUVERTURE",
                )
                self._preopen_record(
                    sym, slug, "both", combined=round(pa + pb, 4),
                    shares=round(min(fa, fb), 2), cost=round(fa * pa + fb * pb, 2),
                    imbalance=round(max(fa, fb) / max(0.01, min(fa, fb)), 3),
                    posted_ts=e.get("posted_ts"), tick=e.get("tick"),
                )
                st.pop(slug, None)
                self._save()
                continue

            # pas encore les deux : on laisse courir tant qu'on a le temps
            if lead > PREOPEN_CANCEL_BEFORE_S:
                continue

            # ── T-30s : on ne peut plus attendre ──
            for side in ("a", "b"):
                oid = (e.get(side) or {}).get("order_id")
                if oid and not fills.get(side, 0) > 0.01:
                    self._live.cancel_order(oid)
            if not got_a and not got_b:
                self._tlog(
                    f"preopen_none_{sym}",
                    f"⭕ [PRE-OUVERTURE] {sym} {slug} aucun remplissage -> ordres annules, "
                    f"rien engage",
                )
                self._preopen_record(
                    sym, slug, "none",
                    combined=round((e["a"]["price"] + e["b"]["price"]), 4),
                    cost=0.0, posted_ts=e.get("posted_ts"), tick=e.get("tick"),
                )
                st.pop(slug, None)
                self._save()
                continue

            # UNE SEULE remplie : on la solde AVANT l'ouverture (pas de pari
            # directionnel subi -- c'est toute la difference avec l'arb decale).
            side = "a" if got_a else "b"
            leg = e[side]
            n = fills[side]
            self._log(
                f"⚠️ [PRE-OUVERTURE] {sym} {slug} une seule jambe remplie "
                f"({leg['side']} {n:.2f} parts @ {leg['price']:.3f}) -> on solde avant l'ouverture"
            )
            sold = self._sell_orphan(
                leg["token_id"], round(n, 2),
                f" {sym} {slug} {leg['side']} PRE-OUVERTURE-SOLO",
                entry_price=leg["price"], symbol=sym, slug=slug, side=leg["side"],
                loss_tag="pre_ouverture_solo",
            )
            if sold < n - 0.01:
                # invendable pour l'instant -> on la track pour gestion normale
                mk["open"][f"{slug}|{leg['side']}"] = {
                    "symbol": sym, "slug": slug, "side": leg["side"], "mode": "real",
                    "strat": "orphan", "token_id": leg["token_id"],
                    "entry_price": leg["price"], "filled_shares": round(n - sold, 2),
                    "cost": round((n - sold) * leg["price"], 2),
                    "start_ts": e.get("open_ts"), "pair": None,
                    "end_ts": e.get("open_ts", now) + 300,
                    "opened_ts": now, "buffer": 0.0, "must_close": True,
                }
            self._preopen_record(
                sym, slug, "solo", combined=round(e["a"]["price"] + e["b"]["price"], 4),
                shares=round(n, 2), cost=round(n * leg["price"], 2),
                sold=round(sold, 2), leg=leg["side"], posted_ts=e.get("posted_ts"), tick=e.get("tick"),
            )
            st.pop(slug, None)
            self._save()

        # ── A. POSE SUR UNE NOUVELLE FENETRE ────────────────────────────
        if len(st) >= 1:
            return                      # une seule fenetre en cours a la fois
        base = int(now // 300) * 300
        for off in (300, 600):
            open_ts = base + off
            lead = open_ts - now
            if not (PREOPEN_MIN_LEAD_S <= lead <= PREOPEN_MAX_LEAD_S):
                continue
            slug = f"{sym.lower()}-updown-5m-{open_ts}"
            if slug in st or any(k.startswith(f"{slug}|") for k in mk["open"]):
                continue
            if mk.setdefault("preopen_cooldown", {}).get(slug, 0) > now:
                continue
            meta = self._market_meta(slug)
            if not meta:
                continue
            outcomes, token_ids = meta
            bids = []
            for tid in token_ids:
                b = self._live.get_book_sync(tid)
                bb = b["bids"][0][0] if b and b.get("bids") else None
                aa = b["asks"][0][0] if b and b.get("asks") else None
                if bb is None:
                    bids = []
                    break
                # TETE DE FILE (Steven 06/08) : on ameliore le meilleur bid
                # d'un tick pour etre servi en PREMIER. Garde-fou essentiel :
                # ne JAMAIS atteindre l'ask, sinon l'ordre traverse le carnet
                # et on redevient TAKER -- ce qui ferait perdre les 4.2% de
                # frais economises, toute la raison d'etre du mecanisme.
                # tick REGLABLE A CHAUD (Steven 06/08 : "on a fait le bid+1
                # mais on pourrait ptt meme +2 si ca permet de vendre mieux").
                # Reglable sans redeploiement pour pouvoir comparer les deux
                # sur des tentatives reelles -- une tentative ratee coutant
                # zero, ce test ne peut rien couter d'autre que du temps.
                _tick = self.state.get("preopen_tick")
                if _tick is None:
                    _tick = PREOPEN_IMPROVE_TICK
                px = round(bb + float(_tick), 2)
                if aa is not None and px >= aa:
                    px = bb          # pas la place d'ameliorer, on reste au bid
                bids.append(px)
            if len(bids) != 2:
                continue
            comb = round(sum(bids), 4)
            if comb > PREOPEN_MAX_COMBINED:
                self._tlog(
                    f"preopen_wide_{sym}",
                    f"⏸️ [PRE-OUVERTURE] {sym} {slug} combine bid {comb:.3f} > "
                    f"{PREOPEN_MAX_COMBINED} -> pas assez de marge, on ne pose pas",
                )
                continue
            # TAILLE MINIMUM (Steven 06/08, ordres refuses en prod) : j'avais
            # utilise MIN_SELL_SHARES (1.0), qui est le plancher de VENTE.
            # Polymarket impose orderMinSize = 5 parts sur les ordres LIMITE
            # -> les poses a 4.26 parts etaient rejetees en 400. On part donc
            # de MIN_ORDER_SIZE_SHARES et on arrondit AU-DESSUS.
            budget = self._preopen_budget()
            # budget = TOTAL des 2 jambes, et le cout total vaut
            # shares * comb -> shares = budget / comb (avant : budget etait
            # pris par jambe, ce qui engageait le double du montant annonce).
            shares = round(max(MIN_ORDER_SIZE_SHARES, budget / max(0.01, comb)) + 0.01, 2)
            besoin = round(shares * comb, 2)
            if besoin > self._investable():
                self._tlog(
                    f"preopen_funds_{sym}",
                    f"⛔ [PRE-OUVERTURE] {sym} {slug} il faut {besoin:.2f}$ pour les 2 jambes, "
                    f"{self._investable():.2f}$ dispo -> on ne pose pas ce qu'on ne peut pas honorer",
                )
                continue
            # MEME PLAFOND QUE MSF : la pre-ouverture vise elle aussi
            # PREOPEN_TOTAL_FRAC = 0.35, elle se heurtait donc au meme plafond
            # generique a 0.25 -- et ici le refus etait carrement MUET (un
            # `continue` sans le moindre log), donc invisible dans le journal.
            ok_exp, why = self._exposure_ok(sym, mk, slug, besoin,
                                            cap=self._maker_open_expo_max())
            if not ok_exp:
                self._tlog(
                    f"preopen_expo_{sym}",
                    f"⛔ [PRE-OUVERTURE] {sym} {slug} plafond d'exposition atteint "
                    f"({why}) -> on ne pose pas",
                )
                continue

            self._log(
                f"📮 [PRE-OUVERTURE] {sym} {slug} ouvre dans {lead:.0f}s | "
                f"pose 2 ordres MAKER {outcomes[0]}@{bids[0]:.3f} + {outcomes[1]}@{bids[1]:.3f} "
                f"= {comb:.3f} ({shares} parts, {besoin:.2f}$) -> +{(1 - comb) * 100:.1f}% si les 2 passent"
            )
            posted = {}
            for i, (side, tid) in enumerate(zip(outcomes, token_ids)):
                r = self._live.post_limit_buy(tid, bids[i], shares)
                posted["a" if i == 0 else "b"] = {
                    "side": side, "token_id": tid, "price": bids[i],
                    "order_id": r.get("order_id"), "ok": bool(r.get("success")),
                }
                if not r.get("success"):
                    # message COMPLET : les 120 premiers caracteres ne
                    # contenaient que le hash de l'ordre, pas la cause.
                    _err = str(r.get("error") or "")
                    _cause = _err.split("error_message=")[-1] if "error_message=" in _err else _err
                    self._log(
                        f"⚠️ [PRE-OUVERTURE] {sym} {slug} {side} @ {bids[i]:.3f} "
                        f"x{shares} parts ordre refuse : {_cause[:300]}"
                    )
            # si un des deux n'a pas ete accepte, on annule l'autre tout de suite
            if not all(v.get("ok") for v in posted.values()):
                for v in posted.values():
                    if v.get("order_id"):
                        self._live.cancel_order(v["order_id"])
                # COOLDOWN (Steven 06/08) : sans lui le bot reposait toutes les
                # 2s et spammait l'API de commandes rejetees. Une pose qui
                # echoue echoue en general pour une raison persistante
                # (taille, solde, marche pas pret) -> on laisse passer du temps.
                mk.setdefault("preopen_cooldown", {})[slug] = now + 60
                self._log(f"⭕ [PRE-OUVERTURE] {sym} {slug} pose incomplete -> tout annule")
                self._preopen_record(sym, slug, "rejected", combined=comb, cost=0.0)
                continue
            posted["open_ts"] = open_ts
            posted["posted_ts"] = now
            posted["tick"] = float(self.state.get("preopen_tick") or PREOPEN_IMPROVE_TICK)
            st[slug] = posted
            self._save()
            return

    def _maker_open_state(self, mk):
        return mk.setdefault("maker_open", {})

    def _manage_maker_open(self, sym):
        """ARB MAKER EN FENETRE OUVERTE (Steven 06/08).

        C'est la pre-ouverture de Steven, appliquee la ou se trouve reellement
        la liquidite. Le raisonnement vient du backtest sur 694 fenetres :

          - en PRENEUR, l'arb est structurellement perdant : les frais des deux
            jambes (0.0455 x min(p,1-p) chacune, soit ~4.4% de la mise) mangent
            la marge d'arb (4%). Point mort a 0.956, et seules 2.7% des
            fenetres offrent mieux -> 1.6 arb/h, corrobore par nos 1.3/h reels.
          - en APPORTEUR les frais sont NULS (verifie on-chain : 9% de nos
            transactions ont un ecart prix x parts exactement nul, ce sont les
            remplissages maker), et une pose non servie ne coute rien du tout
            (l'historique on-chain ne contient que des TRADE et des REDEEM,
            jamais d'ORDER ni de CANCEL).

        Le flux vendeur avant ouverture est trop maigre pour ca : sur BTC, des
        ventes des DEUX cotes n'existent que dans 7% des fenetres. Une fois la
        fenetre OUVERTE, en revanche, en posant a 0.46 des deux cotes les deux
        jambes sont servies dans 58.5% des fenetres, pour 8% de marge brute.

        Le verrou n'exige PAS la simultaneite : acheter Up a 0.46 a la 10e
        seconde et Down a 0.46 a la 200e donne quand meme un combine de 0.92.

        L'INCONNUE, que seul le reel peut trancher : notre place dans la file.
        Une vente a 0.46 prouve que des ordres a 0.46 ont ete servis, pas que
        ce serait le notre. D'ou le journal des tentatives, qui mesurera le
        taux de remplissage effectif. Le risque est borne : ce qui n'est pas
        servi ne coute rien, et une jambe seule est soldee avant la fin."""
        if sym not in MAKER_OPEN_SYMBOLS:
            return
        if self.state["modes"].get(sym) != "real":
            return
        mk = self.state["markets"][sym]
        st = self._maker_open_state(mk)
        now = time.time()

        # ── SUIVI DES ORDRES POSES ──────────────────────────────────────
        for slug in list(st.keys()):
            e = st[slug]
            reste = e.get("fin_ts", 0) - now
            fills = {}
            for cote in ("a", "b"):
                leg = e.get(cote) or {}
                tid = leg.get("token_id")
                if not tid:
                    continue
                held = self._live.position_size(tid)
                fills[cote] = max(0.0, held if held and held > 0 else 0.0)
            fa, fb = fills.get("a", 0.0), fills.get("b", 0.0)

            # ── RETRAIT D'UN COTE QUE LE SPOT A DEJA CONDAMNE ────────────
            # Voir MAKER_OPEN_SPOT_GUARD_BP. On ne retire QUE des ordres non
            # servis : un cote deja rempli n'est jamais touche ici, et retirer
            # un ordre qui n'a pas ete servi coute exactement zero.
            if (MAKER_OPEN_SPOT_GUARD_ENABLED
                    and sym in MAKER_OPEN_SPOT_GUARD_SYMBOLES
                    and reste > _cancel_before_s(sym)):
                for _cote in ("a", "b"):
                    _leg_g = e.get(_cote) or {}
                    if fills.get(_cote, 0.0) > 0.01:
                        continue          # deja servi : on ne touche pas
                    if not _leg_g.get("order_id"):
                        continue          # deja retire
                    _bp = _mouvement_spot_bp(sym, slug, e.get("debut_ts"),
                                             _leg_g.get("side"))
                    if _bp is None or _bp >= MAKER_OPEN_SPOT_GUARD_BP:
                        continue          # fail-open, ou spot encore tolerable
                    if self._cancel_verifie(
                            _leg_g["order_id"],
                            f" {sym} {slug} {_leg_g.get('side','?')} SPOT-GUARD"):
                        _leg_g["order_id"] = None
                        _leg_g["retire_spot"] = True
                        self._log(
                            f"🧭 [MSF-SPOT-GUARD] {sym} {slug} {_leg_g.get('side')} "
                            f"le spot a bouge {_bp:+.1f}bp CONTRE ce cote "
                            f"(seuil {MAKER_OPEN_SPOT_GUARD_BP:+.1f}) -> ordre retire "
                            f"avant d'etre servi. Se faire remplir ici, c'est "
                            f"acheter le perdant et rester en jambe seule "
                            f"(-1.06$/fenetre mesure sur 30 cas reels)"
                        )
                        self._save()
                # si les DEUX ordres ont saute et que rien n'est servi, la
                # fenetre n'a plus d'objet : on la ferme proprement.
                if (fills.get("a", 0.0) <= 0.01 and fills.get("b", 0.0) <= 0.01
                        and not (e.get("a") or {}).get("order_id")
                        and not (e.get("b") or {}).get("order_id")
                        and ((e.get("a") or {}).get("retire_spot")
                             or (e.get("b") or {}).get("retire_spot"))):
                    self._maker_open_record(
                        sym, slug, "retrait_spot", combine=None, parts=0.0,
                        prix=(e.get("a") or {}).get("price"), vendu=0.0,
                        sortie=None, exec_mode="passif", calm=e.get("calm", False),
                    )
                    mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                    st.pop(slug, None)
                    self._save()
                    continue

            # ── RETRAIT SI TOUJOURS RIEN SERVI (Steven 11/08) ────────────
            # Ne coute aucun verrou deja acquis : on ne retire que des ordres
            # dont AUCUN cote n'a ete touche. Cf. MAKER_OPEN_NOFILL_CANCEL_S.
            _ecoule = now - (e.get("debut_ts") or now)
            if (fa <= 0.01 and fb <= 0.01
                    and _ecoule >= MAKER_OPEN_NOFILL_CANCEL_S
                    and reste > _cancel_before_s(sym)):
                # ANNULATION VERIFIEE, PUIS RELECTURE DES POSITIONS
                # (Steven 13/08, trouve sur la fenetre XRP 00:25-00:30 ET).
                #
                # LE BUG : cette annulation etait tiree "en aveugle" avec
                # cancel_order(), sans verifier qu'elle avait abouti, et le
                # slug etait retire de l'etat MSF juste apres. Si un ordre
                # etait deja en train d'etre servi -- ou si l'annulation
                # echouait -- le fill arrivait dans une fenetre que MSF avait
                # DEJA OUBLIEE. La position devenait alors un simple "orphan"
                # sans le tag maker_open, donc geree par _manage_pnl_tier_exits
                # et PLUS DU TOUT par _manage_maker_open : plus de TP a x1.5,
                # plus de completion, plus d'abandon raisonne, plus de
                # MSF-TPNOW, plus de garde-fou spot. Tous les reglages de la
                # nuit sautaient d'un coup pour ces positions.
                #
                # CAS REEL MESURE, fenetre XRP 00:25-00:30 ET : ordre servi a
                # 111s (donc APRES les 90s de retrait), 11.14 parts a 0.35.
                # Le bid est ensuite reste AU-DESSUS du seuil de TP (0.525)
                # pendant 70 secondes d'affilee, jusqu'a 0.67 -- sans que rien
                # ne se declenche, puisque MSF ne savait plus que la position
                # existait. Elle a fini a +7.13$ par pur hasard de resolution,
                # apres etre repassee a 0.27 juste avant le cutoff.
                #
                # AMPLEUR : 15 des 108 fills reels a 0.35 (14%) arrivent apres
                # 90s. C'est donc une position sur sept qui echappait a toute
                # la gestion MSF.
                #
                # LE CORRECTIF, en deux temps : (1) on exige une annulation
                # VERIFIEE, comme partout ailleurs dans ce fichier ; (2) meme
                # si elle reussit, on RELIT les positions avant d'oublier la
                # fenetre, parce qu'un fill peut etre parti entre la lecture du
                # debut de boucle et maintenant. Au moindre doute on GARDE le
                # slug sous gestion MSF : le pire cas est de gerer une fenetre
                # vide, ce qui ne coute rien, alors que l'oublier a tort coute
                # une position entiere livree a elle-meme.
                _annul_ok = True
                for cote in ("a", "b"):
                    oid = (e.get(cote) or {}).get("order_id")
                    if oid:
                        if self._cancel_verifie(
                                oid,
                                f" {sym} {slug} {(e.get(cote) or {}).get('side','?')} "
                                f"MSF-RETRAIT"):
                            e[cote]["order_id"] = None
                        else:
                            _annul_ok = False
                # relecture : un fill a-t-il eu lieu entre-temps ?
                _apres = {}
                for cote in ("a", "b"):
                    _tid_a = (e.get(cote) or {}).get("token_id")
                    if not _tid_a:
                        continue
                    _h = self._live.position_size(_tid_a)
                    _apres[cote] = max(0.0, _h if _h and _h > 0 else 0.0)
                if not _annul_ok or any(v > 0.01 for v in _apres.values()):
                    self._tlog(
                        f"makeropen_retrait_course_{sym}",
                        f"🛟 [MSF-RETRAIT] {sym} {slug} annulation incomplete ou "
                        f"fill arrive pendant le retrait "
                        f"(a={_apres.get('a', 0):.2f} b={_apres.get('b', 0):.2f}) "
                        f"-> la fenetre RESTE sous gestion MSF. Sans ca la position "
                        f"devenait orphan : ni TP, ni completion, ni abandon.",
                    )
                    self._save()
                    continue
                self._log(
                    f"🚪 [MSF-RETRAIT] {sym} {slug} aucune jambe servie apres "
                    f"{int(_ecoule)}s -> les 2 ordres retires. Un remplissage a "
                    f"ce stade n'achete plus que le perdant (croisement 25-33%, "
                    f"seuil 32%)."
                )
                self._maker_open_record(
                    sym, slug, "retrait_sans_fill", combine=None, parts=0.0,
                    prix=(e.get("a") or {}).get("price"), vendu=0.0, sortie=None,
                    exec_mode="passif", calm=e.get("calm", False),
                )
                mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                st.pop(slug, None)
                self._save()
                continue

            if fa > 0.01 and fb > 0.01:
                # LES DEUX SERVIS -> arb verrouille, sans le moindre frais.
                for cote in ("a", "b"):
                    oid = (e.get(cote) or {}).get("order_id")
                    if oid:
                        self._live.cancel_order(oid)
                for cote in ("a", "b"):
                    leg = e[cote]
                    tid = leg.get("token_id")
                    held = self._live.position_size(tid)
                    fills[cote] = max(0.0, held if held and held > 0 else 0.0)
                fa, fb = fills.get("a", 0.0), fills.get("b", 0.0)
                if not (fa > 0.01 and fb > 0.01):
                    continue
                for cote in ("a", "b"):
                    leg = e[cote]
                    n = fills[cote]
                    mk["open"][f"{slug}|{leg['side']}"] = {
                        "symbol": sym, "slug": slug, "side": leg["side"], "mode": "real",
                        # strat 'bothside' A DESSEIN : tout nouveau strat doit
                        # etre ajoute a la main dans les filtres de
                        # _manage_pnl_tier_exits sous peine de perdre toute
                        # gestion TP/SL. On marque l'origine a cote.
                        "strat": "bothside", "maker_open": True,
                        # les 2 jambes viennent d'ordres PASSIFS -> apporteur,
                        # zero frais (cf. _pair_net_after_fees)
                        "maker_fill": True,
                        "token_id": leg["token_id"], "entry_price": leg["price"],
                        "filled_shares": round(n, 2),
                        "cost": round(n * leg["price"], 2),
                        "start_ts": e.get("debut_ts"), "pair": None,
                        "end_ts": e.get("fin_ts"), "opened_ts": now, "buffer": 0.0,
                    }
                    self._add_slug_spent(mk, slug, round(n * leg["price"], 2))
                pa, pb = e["a"]["price"], e["b"]["price"]
                self._log(
                    f"🎉 [MAKER-OUVERT] {sym} {slug} LES DEUX JAMBES SERVIES "
                    f"{pa:.3f}+{pb:.3f}={pa + pb:.3f} ({fa:.2f}/{fb:.2f} parts) "
                    f"-- apporteur, ZERO frais"
                    + (" (mode CALME : couverture, pas un arb)" if e.get("calm") else "")
                )
                # _tag_pair_lock solde tout excedent de parts (cf. la-bas)
                self._tag_pair_lock(
                    mk["open"].get(f"{slug}|{e['a']['side']}"),
                    mk["open"].get(f"{slug}|{e['b']['side']}"),
                    pa + pb, tag=f" {sym} {slug} MAKER-OUVERT",
                )
                self._maker_open_record(sym, slug, "les_deux", combine=round(pa + pb, 4),
                                        parts=round(min(fa, fb), 2), prix=pa,
                                        calm=e.get("calm", False))
                st.pop(slug, None)
                self._save()
                continue

            # TP SUR JAMBE SEULE (voir MAKER_OPEN_TP_MULT). Exactement une
            # jambe est remplie a ce stade (le cas "les deux" est deja sorti
            # plus haut). On horodate le PREMIER instant ou on la voit remplie
            # (une seule fois -- ne pas ecraser a chaque cycle), et si elle a
            # suffisamment monte apres un delai minimal anti-bruit, on prend
            # le profit et on ABANDONNE la tentative sur l'autre cote, comme
            # demande : plus la peine d'esperer completer la paire.
            # MODE CALME (Steven 09/08) : une jambe peut avoir ete SAUTEE
            # (perdant bradé, price=None, pas d'ordre pose). On choisit donc
            # toujours une jambe POSTEE (avec un prix reel) -- sinon
            # leg_seule["price"] serait None et le x1.8 du bloc ci-dessous
            # crasherait.
            if fa > 0.01:
                cote_seule = "a"
            elif fb > 0.01:
                cote_seule = "b"
            elif (e.get("a") or {}).get("price") is not None:
                cote_seule = "a"
            else:
                cote_seule = "b"
            leg_seule = e[cote_seule]
            if "fill_ts" not in leg_seule:
                leg_seule["fill_ts"] = now
            _hold_s = now - leg_seule["fill_ts"]

            # ── RL-TRADE (Steven 14/08, "le RL doit vraiment trader lui meme") ──
            # L'agent DQN entraine cette nuit PREND LA MAIN sur la gestion de
            # la jambe seule : COMPLETE et SELL_MARKET executent un vrai
            # ordre reel, HOLD_TO_RESOLUTION et NOOP sautent ce cycle SANS
            # rien faire d'autre (pas d'annulation, pas de vente) -- toute la
            # heuristique qui suit (completion auto, TPNOW, abandon, TP,
            # cutoff) est BYPASSEE pour ce slug tant que l'agent decide.
            # Throttle 10s/slug (Steven 14/08) : calee sur la cadence REELLE
            # de collecte des donnees d'entrainement (~10s, irreguliere,
            # cf. rl/dataset.py). Plus lent = trop passif face au marche ;
            # plus rapide = des etats quasi identiques d'un appel a l'autre
            # (le prix n'a souvent pas bouge en 1-2s), donc plus de lectures
            # de carnet sans info nouvelle, et un rythme que l'agent n'a
            # jamais vu pendant l'entrainement.
            # BUG CRITIQUE CORRIGE (Steven 14/08, "GROS BUG" #2, trouve en
            # rejouant la session reelle) : le `continue` qui neutralise
            # l'ancienne heuristique etait A L'INTERIEUR du `if throttle`.
            # Entre deux decisions du RL (10s), la boucle principale
            # repasse ici toutes les ~2s -- et comme le throttle n'etait
            # PAS ecoule, RIEN ne bypassait l'ancienne heuristique : elle
            # tournait librement en parallele du RL, sans jamais consulter
            # ses decisions. Observe en direct sur BTC 20:15-20:17 : le RL
            # decidait NOOP (je tiens) a repetition, et PENDANT CE TEMPS
            # l'ancien MAKER-OUVERT-COMPLETION postait sa propre completion,
            # puis MAKER-OUVERT-ABANDON a vendu 7.55 parts a 0.06 (entree
            # 0.35, perte -2.19$) -- sans que le RL n'ait jamais ete
            # consulte sur cette sortie. Le RL n'avait donc PAS le controle
            # exclusif qu'il etait cense avoir depuis fc4801f.
            #
            # CORRECTIF : le bypass (`continue`) doit etre INCONDITIONNEL
            # des qu'on gere une jambe seule -- seul le CALCUL de la
            # decision (lecture du carnet + forward pass + execution d'un
            # ordre) reste limite a une fois toutes les 10s. Sur les cycles
            # entre deux decisions, on ne fait rien de NOUVEAU, mais on
            # bypasse quand meme -- exactement comme prevu depuis le debut,
            # sauf que maintenant c'est reellement applique a chaque cycle.
            try:
                if now - (leg_seule.get("_rl_ts") or 0) >= 10:
                    leg_seule["_rl_ts"] = now
                    _autre_leg_rl = e.get("b" if cote_seule == "a" else "a") or {}
                    _tid_notre_rl = leg_seule.get("token_id")
                    _tid_autre_rl = _autre_leg_rl.get("token_id")
                    _bk_notre_rl = self._live.get_book_sync(_tid_notre_rl) if _tid_notre_rl else None
                    _bk_autre_rl = self._live.get_book_sync(_tid_autre_rl) if _tid_autre_rl else None

                    def _bb(bk):
                        return bk["bids"][0][0] if bk and bk.get("bids") else None

                    def _ba(bk):
                        return bk["asks"][0][0] if bk and bk.get("asks") else None

                    def _bd(bk):
                        return sum(q for _, q in (bk or {}).get("bids", [])[:3]) or None

                    def _ad(bk):
                        return sum(q for _, q in (bk or {}).get("asks", [])[:3]) or None

                    if leg_seule.get("side") == "Up":
                        _ub_rl, _ua_rl = _bb(_bk_notre_rl), _ba(_bk_notre_rl)
                        _db_rl, _da_rl = _bb(_bk_autre_rl), _ba(_bk_autre_rl)
                        _ubd_rl, _uad_rl = _bd(_bk_notre_rl), _ad(_bk_notre_rl)
                        _dbd_rl, _dad_rl = _bd(_bk_autre_rl), _ad(_bk_autre_rl)
                    else:
                        _db_rl, _da_rl = _bb(_bk_notre_rl), _ba(_bk_notre_rl)
                        _ub_rl, _ua_rl = _bb(_bk_autre_rl), _ba(_bk_autre_rl)
                        _dbd_rl, _dad_rl = _bd(_bk_notre_rl), _ad(_bk_notre_rl)
                        _ubd_rl, _uad_rl = _bd(_bk_autre_rl), _ad(_bk_autre_rl)

                    # NETTOYAGE DES SLUGS FANTOMES (Steven 14/08, "sa fait
                    # deux cycles sans aucune position vraiment ouverte").
                    # HOLD_TO_RESOLUTION ne fait deliberement RIEN -- c'est
                    # voulu, on laisse la position aller a resolution. Mais
                    # PERSONNE ne revenait ensuite nettoyer `st[slug]` une
                    # fois la resolution reellement survenue (redemption
                    # automatique, taille -> 0) : le slug restait gere pour
                    # toujours, generant des decisions MMBOT sur un carnet
                    # vieux de plusieurs dizaines de minutes. Observe en
                    # direct : hold=1347s (22 min) sur une fenetre de 300s.
                    # On lit donc la taille reelle EN PREMIER : si elle est
                    # nulle, la position n'existe plus (deja resolue/
                    # redimee ailleurs) -> on nettoie et on ne consulte
                    # meme pas l'agent sur du vide.
                    _n_rl = self._live.position_size(_tid_notre_rl) if _tid_notre_rl else 0.0
                    _n_rl = round(_n_rl, 2) if _n_rl and _n_rl > 0.01 else 0.0
                    if _n_rl < 0.01:
                        self._tlog(
                            f"rltrade_fantome_{sym}",
                            f"🧹 [MMBOT] {sym} {slug} {leg_seule.get('side')} "
                            f"position vide (deja resolue) apres {int(_hold_s)}s "
                            f"-> nettoyage du slug fantome",
                        )
                        mk.setdefault("makeropen_cooldown", {})[slug] = now + 5
                        st.pop(slug, None)
                        self._save()
                        continue

                    _dt_rl = now - e.get("debut_ts", now)
                    _obs_rl = rl_shadow.observation(
                        _ub_rl, _ua_rl, _db_rl, _da_rl,
                        _ubd_rl, _uad_rl, _dbd_rl, _dad_rl,
                        _dt_rl, _cancel_before_s(sym),
                        leg_seule.get("side"), leg_seule.get("price"), False,
                    )

                    # gain garanti SI l'agent choisit COMPLETE a cet instant
                    # -- calcule AVANT decide() pour que le garde-fou
                    # MIN_GAIN_COMPLETION (rl_shadow.py) puisse retirer
                    # cette option de la course si elle est trop marginale.
                    _gain_complete_rl = None
                    _ask_pot_rl = _da_rl if leg_seule.get("side") == "Up" else _ua_rl
                    if _ask_pot_rl is not None and leg_seule.get("price") is not None and _n_rl >= 0.01:
                        _gain_complete_rl = (1.0 - leg_seule["price"] - _ask_pot_rl) * _n_rl

                    _action_rl, _q_rl = rl_shadow.decide(_obs_rl, gain_complete=_gain_complete_rl)

                    # GARDE-FOU PRIX ABSOLU (Steven 15/08, backteste cette nuit sur
                    # 2 periodes independantes -- ANCIEN 12-14/08 et FRAIS 16-18/08,
                    # 6 jours calendaires, JAMAIS d'inversion de signe par symbole
                    # ou par jour) : une fois que NOTRE bid atteint 0.95, VENDRE
                    # bat TENIR jusqu'a resolution. Consequence directe du biais
                    # "favori cher surpaye" deja confirme cette nuit (bootstrap
                    # cluster, survit a verification adversariale) : le vrai taux
                    # de victoire a ce niveau de prix est plus bas que ce que le
                    # marche affiche, donc vendre au prix inflate (frais minimes a
                    # ce niveau) bat rester expose au vrai taux, plus bas.
                    # Remplace la decision de l'agent plutot que de laisser un 2e
                    # systeme tourner en parallele -- cf le bug critique du 14/08
                    # ou l'ancienne heuristique agissait sans jamais consulter le
                    # RL (perte reelle -2.19$ sans consultation). Un seul point de
                    # decision par cycle ; ce garde-fou est prioritaire sur l'agent.
                    SEUIL_TP_ABSOLU_SURPAYE = 0.95
                    _notre_bid_rl = _ub_rl if leg_seule.get("side") == "Up" else _db_rl
                    if _notre_bid_rl is not None and _notre_bid_rl >= SEUIL_TP_ABSOLU_SURPAYE:
                        if _action_rl != "SELL_MARKET":
                            self._tlog(
                                f"rltrade_tp_absolu_{sym}",
                                f"💰 [MMBOT] {sym} {slug} {leg_seule.get('side')} "
                                f"bid={_notre_bid_rl:.3f} >= {SEUIL_TP_ABSOLU_SURPAYE} "
                                f"-> override SELL_MARKET (agent avait decide {_action_rl})",
                            )
                        _action_rl = "SELL_MARKET"

                    # COMPLETION A PERTE BORNEE -- RETIRE (Steven 15/08). Code le
                    # 15/08 sur la base d'un backtest utilisant le MEILLEUR combine
                    # jamais atteint sur TOUTE la fenetre -- une information qu'on
                    # n'a pas en temps reel (biais retrospectif/look-ahead). Rejoue
                    # en causal strict (declenchement au 1er instant reel ou la
                    # condition est remplie, pas au meilleur moment retrospectif) :
                    # le resultat s'INVERSE. Completer a ce moment perd contre TENIR
                    # jusqu'a resolution (-0.02 a -0.10$/part selon le jeu, IC95
                    # cluster exclut 0 sur 5/6 tests), meme s'il bat encore vendre
                    # la jambe au bid courant (+0.012 a +0.017$/part). Comme ce
                    # garde-fou ne remplacait QUE les decisions HOLD_TO_RESOLUTION/
                    # NOOP -- precisement le cas ou tenir est superieur -- il aurait
                    # degrade la performance reelle. Retire avant tout impact sur
                    # de l'argent reel.

                    if _action_rl is not None:
                        self._tlog(
                            f"rltrade_{sym}",
                            f"🤖 [MMBOT] {sym} {slug} {leg_seule.get('side')} "
                            f"entree={leg_seule.get('price')} hold={int(_hold_s)}s -> "
                            f"agent decide {_action_rl} "
                            f"(Q={[round(x,3) for x in _q_rl]}"
                            + (f", gain_completion={_gain_complete_rl:.3f}$"
                               if _gain_complete_rl is not None else "")
                            + ")",
                        )

                    if _action_rl == "COMPLETE" and _n_rl >= 0.01 and _tid_autre_rl:
                        _ask_rl = _da_rl if leg_seule.get("side") == "Up" else _ua_rl
                        if _ask_rl is not None:
                            if _autre_leg_rl.get("order_id"):
                                if not self._cancel_verifie(
                                        _autre_leg_rl["order_id"],
                                        f" {sym} {slug} {_autre_leg_rl.get('side','?')} MMBOT"):
                                    self._tlog(f"rltrade_annul_{sym}",
                                               f"⚠️ [MMBOT] {sym} {slug} ordre d'origine "
                                               f"non annulable -> on renonce a completer")
                                    continue
                                _autre_leg_rl["order_id"] = None
                            _budget_rl = round(_n_rl * _ask_rl, 2)
                            with self._order_lock:
                                _res_rl = self._live.snipe_buy_market(
                                    _tid_autre_rl, round(min(0.99, _ask_rl + 0.01), 2), _budget_rl)
                            _fill_rl = _res_rl.get("filled_shares", 0.0)
                            if _fill_rl > 0.01:
                                _avg_rl = _res_rl.get("avg_cost") or _ask_rl
                                for _cote_rl, _n2_rl, _px2_rl in (
                                        (cote_seule, _n_rl, leg_seule["price"]),
                                        ("b" if cote_seule == "a" else "a", round(_fill_rl, 2), _avg_rl)):
                                    _lg_rl = e[_cote_rl]
                                    mk["open"][f"{slug}|{_lg_rl['side']}"] = {
                                        "symbol": sym, "slug": slug, "side": _lg_rl["side"],
                                        "mode": "real", "strat": "bothside", "maker_open": True,
                                        "maker_fill": (_cote_rl == cote_seule),
                                        "token_id": _lg_rl["token_id"], "entry_price": _px2_rl,
                                        "filled_shares": round(_n2_rl, 2),
                                        "cost": round(_n2_rl * _px2_rl, 2),
                                        "start_ts": e.get("debut_ts"), "pair": None,
                                        "end_ts": e.get("fin_ts"), "opened_ts": now,
                                        "buffer": 0.0,
                                    }
                                    self._add_slug_spent(mk, slug, round(_n2_rl * _px2_rl, 2))
                                self._tag_pair_lock(
                                    mk["open"].get(f"{slug}|{leg_seule['side']}"),
                                    mk["open"].get(f"{slug}|{_autre_leg_rl['side']}"),
                                    leg_seule["price"] + _avg_rl,
                                    tag=f" {sym} {slug} MMBOT-COMPLETE")
                                self._maker_open_record(
                                    sym, slug, "les_deux",
                                    combine=round(leg_seule["price"] + _avg_rl, 4),
                                    parts=round(min(_n_rl, _fill_rl), 2),
                                    prix=leg_seule["price"], exec_mode="rl_complete")
                                mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                                st.pop(slug, None)
                                self._save()
                        continue

                    if _action_rl == "SELL_MARKET" and _n_rl >= 0.01:
                        _vendu_rl = self._sell_orphan(
                            _tid_notre_rl, _n_rl,
                            f" {sym} {slug} {leg_seule['side']} MMBOT-SELL",
                            entry_price=leg_seule["price"], symbol=sym,
                            slug=slug, side=leg_seule["side"],
                            loss_tag="rl_sell_market",
                            entry_ts=leg_seule.get("fill_ts"),
                        )
                        if _vendu_rl >= _n_rl - 0.01:
                            self._maker_open_record(
                                sym, slug, "abandon", combine=None,
                                parts=round(_n_rl, 2), prix=leg_seule["price"],
                                vendu=round(_vendu_rl, 2), exec_mode="rl_sell",
                                calm=e.get("calm", False),
                            )
                            mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                            st.pop(slug, None)
                            self._save()
                        continue

                    # HOLD_TO_RESOLUTION ou NOOP : rien de NOUVEAU ce cycle
                    # (ni annulation, ni vente, ni completion) -- le
                    # `continue` inconditionnel juste apres ce bloc `try`
                    # fait deja le bypass, sur CE cycle comme sur tous les
                    # suivants avant la prochaine decision.
                _rl_bypass_ok = True
            except Exception as _exc_rl:
                _rl_bypass_ok = False
                # fail-open : une erreur dans le pilotage RL ne doit jamais
                # planter le cycle -- mais ici, contrairement au shadow, on
                # NE BYPASSE PAS : on laisse la heuristique existante gerer
                # ce cycle comme filet de securite.
                self._tlog(f"rltrade_err_{sym}",
                           f"⚠️ [MMBOT] {sym} erreur pilotage RL (repli sur "
                           f"l'heuristique ce cycle) : {_exc_rl}")

            # LE VRAI CORRECTIF (voir commentaire plus haut, "BUG CRITIQUE
            # CORRIGE #2") : ce `continue` est INCONDITIONNEL des que le
            # pilotage RL n'a pas leve d'exception -- qu'une decision ait
            # ete prise ce cycle (throttle ecoule) ou non (on attend
            # toujours la prochaine fenetre de 10s). Avant ce correctif,
            # seuls les cycles ou le throttle etait ecoule bypassaient
            # l'ancienne heuristique ; sur tous les autres (la grande
            # majorite, la boucle tournant toutes les ~2s), rien
            # n'empechait TP/completion/abandon de s'executer en parallele
            # du RL sans jamais le consulter.
            if _rl_bypass_ok:
                continue

            # ── COMPLETION ACTIVE DE LA JAMBE SEULE (Steven 11/08) ──────────
            # La jambe seule est LE poste de perte de MSF : -0.249 $/part en
            # esperance (mesure sur 586 fenetres). Plutot que d'attendre le TP
            # ou de subir le cutoff, on ACHETE l'autre cote au marche des que
            # son prix rend le verrou encore rentable -- ce qui transforme une
            # esperance negative en gain GARANTI, petit mais certain.
            #
            # Backteste avec les VRAIS prix imprimes de l'autre cote (une 1re
            # version supposait prix_autre = 1 - prix_notre : faux, les deux
            # cotes somment a 1.01 en mediane, ce qui offrait gratuitement un
            # centime par part et inventait des completions inexistantes) :
            #   MSF actuel                 TRAIN +1.59 $/j | TEST -1.67 $/j
            #   + completion <= 0.97       TRAIN +6.79 $/j | TEST +24.97 $/j
            # 185 completions, ZERO perdante, mediane +0.027 $/part, les 5
            # meilleures ne pesant que 11% du total (reparti, pas concentre).
            #
            # Le mecanisme existe deja ailleurs dans le bot
            # (PAIR_COMPLETION_MAX_COMBINED) mais n'avait jamais ete branche
            # sur MSF -- verifie.
            _autre_c = "b" if cote_seule == "a" else "a"
            _leg_autre_c = e.get(_autre_c) or {}

            # ── SUIVI D'UNE COMPLETION DEJA POSTEE EN MAKER (Steven 11/08) ──
            # Backteste isolement contre 4 autres idees : c'est la SEULE qui
            # ameliore (TEST sell 8.91% -> 9.79%, 24.97 -> 27.45 $/j), et elle
            # ameliore dans les 2 modes de remplissage ET en train comme en
            # test. Mecanisme evident : la 2e jambe achetee en apporteur ne
            # paie AUCUN frais, la meme achetee au marche paie ~5% de
            # min(p,1-p). Sur 63 completions, 59 finissent servies en maker.
            if leg_seule.get("comp_maker_oid"):
                _n_autre_m = self._live.position_size(_leg_autre_c.get("token_id"))
                if _n_autre_m and _n_autre_m > 0.01:
                    # servie en APPORTEUR -> zero frais, c'est le cas ideal
                    _n_notre_m = self._live.position_size(leg_seule["token_id"])
                    _n_notre_m = round(_n_notre_m, 2) if _n_notre_m and _n_notre_m > 0.01 else 0.0
                    _px_m = leg_seule.get("comp_maker_px", 0.0)
                    self._log(
                        f"💚 [MAKER-OUVERT-COMPLETION-MAKER] {sym} {slug} 2e jambe "
                        f"{_leg_autre_c['side']} servie a {_px_m:.3f} EN APPORTEUR "
                        f"(zero frais) -> verrou {leg_seule['price'] + _px_m:.3f}"
                    )
                    for _cote, _n, _px in ((cote_seule, _n_notre_m, leg_seule["price"]),
                                           (_autre_c, round(_n_autre_m, 2), _px_m)):
                        _lg = e[_cote]
                        mk["open"][f"{slug}|{_lg['side']}"] = {
                            "symbol": sym, "slug": slug, "side": _lg["side"],
                            "mode": "real", "strat": "bothside", "maker_open": True,
                            # pose initiale ET completion servies en apporteur
                            "maker_fill": True,
                            "token_id": _lg["token_id"], "entry_price": _px,
                            "filled_shares": round(_n, 2), "cost": round(_n * _px, 2),
                            "start_ts": e.get("debut_ts"), "pair": None,
                            "end_ts": e.get("fin_ts"), "opened_ts": now, "buffer": 0.0,
                        }
                        self._add_slug_spent(mk, slug, round(_n * _px, 2))
                    self._tag_pair_lock(
                        mk["open"].get(f"{slug}|{leg_seule['side']}"),
                        mk["open"].get(f"{slug}|{_leg_autre_c['side']}"),
                        leg_seule["price"] + _px_m,
                        tag=f" {sym} {slug} MAKER-OUVERT-COMPLETION-MAKER")
                    self._maker_open_record(
                        sym, slug, "les_deux",
                        combine=round(leg_seule["price"] + _px_m, 4),
                        parts=round(min(_n_notre_m, _n_autre_m), 2),
                        prix=leg_seule["price"], exec_mode="completion_maker")
                    mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                    st.pop(slug, None)
                    self._save()
                    continue
                _att = now - leg_seule.get("comp_maker_ts", now)
                if _att < MAKER_OPEN_COMPLETION_MAKER_S and reste > _cancel_before_s(sym):
                    continue          # on laisse encore sa chance a l'apporteur
                # delai epuise -> on annule et on repasse en agressif plus bas
                self._cancel_verifie(leg_seule["comp_maker_oid"],
                                     f" {sym} {slug} completion-maker")
                self._tlog(
                    f"makeropen_comp_repli_{sym}",
                    f"⏱️ [MAKER-OUVERT-COMPLETION] {sym} {slug} non servie en apporteur "
                    f"apres {_att:.0f}s -> repli sur l'achat au marche",
                )
                for _k in ("comp_maker_oid", "comp_maker_px", "comp_maker_ts"):
                    leg_seule.pop(_k, None)

            # INTERRUPTEUR DE COMPLETION (Steven 13/08, reglable a chaud
            # depuis le dashboard -- voir /api/msf/completion-toggle).
            #
            # POURQUOI IL EXISTE. Steven demandait depuis des heures a voir
            # des TP se declencher. Ils ne le peuvent pas : les deux cotes
            # sommant a ~1.01, la completion part des que l'autre cote passe
            # sous 0.70 (combine 1.05), soit quand NOTRE jambe vaut ~0.31 --
            # tres loin des 0.525 qu'exige le TP. Notre jambe qui monte de X
            # EST l'autre cote qui baisse de X : c'est le meme evenement vu
            # des deux cotes, et la completion franchit son seuil en premier,
            # toujours. Mesure : sur la fenetre XRP 10:40-10:45 ET, completion
            # 12 SECONDES apres le fill. Le TP n'a jamais la main.
            #
            # Eteindre la completion est donc le SEUL moyen de tester ce que
            # vaut vraiment le TP. C'est une experience, pas un reglage : la
            # completion reste la branche mesuree comme rentable (52 fenetres
            # reelles, 83% gagnantes, +0.251$/fenetre, contre 13% et
            # -1.064$ pour les jambes seules). L'eteindre expose donc
            # volontairement le compte a la branche perdante -- a ne laisser
            # OFF que le temps de mesurer, et a rallumer si le TP ne tient
            # pas sa promesse.
            _comp_on = bool(self.state.get("msf_completion_enabled", True))
            if (MAKER_OPEN_COMPLETION_ENABLED and _comp_on
                    and leg_seule.get("price") is not None
                    and _leg_autre_c.get("token_id")
                    and not leg_seule.get("tp_passif_order_id")
                    and _hold_s >= MAKER_OPEN_COMPLETION_MIN_HOLD_S
                    and reste > _cancel_before_s(sym)):
                _n_comp = self._live.position_size(leg_seule["token_id"])
                _n_comp = round(_n_comp, 2) if _n_comp and _n_comp > 0.01 else 0.0
                _bk_c = self._live.get_book_sync(_leg_autre_c["token_id"])
                _ask_c = _bk_c["asks"][0][0] if _bk_c and _bk_c.get("asks") else None
                _bid_c = _bk_c["bids"][0][0] if _bk_c and _bk_c.get("bids") else None

                # TENTATIVE APPORTEUR D'ABORD : on poste au meilleur bid (donc
                # sans franchir le carnet, donc maker garanti) et on laisse
                # MAKER_OPEN_COMPLETION_MAKER_S a un vendeur pour nous croiser.
                # Le combine y est MEILLEUR qu'au marche (bid < ask) en plus
                # d'etre sans frais.
                if (MAKER_OPEN_COMPLETION_MAKER_S and _n_comp >= 0.01
                        and _bid_c is not None
                        and not leg_seule.get("comp_maker_tente")):
                    _comb_m = leg_seule["price"] + _bid_c
                    # meme controle de gain que le chemin au marche, par
                    # symetrie : l'apporteur ne paie pas de frais, mais un
                    # combine tres proche de 1.00 sur une petite position ne
                    # vaut pas la peine d'immobiliser du capital.
                    _gain_m = _n_comp * (1 - _comb_m)
                    if (_comb_m <= MAKER_OPEN_COMPLETION_MAX
                            and _gain_m > MAKER_OPEN_COMPLETION_MIN_GAIN):
                        # ANNULER D'ABORD L'ORDRE D'ORIGINE DE CE COTE
                        # (Steven 11/08, trouve sur la 1re completion reelle).
                        # Sans ca DEUX achats restent vivants sur la meme
                        # jambe : celui de la pose initiale (0.35) et celui de
                        # la completion (0.62). Deux consequences, une
                        # comptable et une financiere :
                        #   - on ne sait plus lequel a ete servi, et le bloc
                        #     "les deux jambes servies" attribue alors le prix
                        #     de la pose initiale -> verrou annonce a 0.70
                        #     alors qu'il a coute 0.97 (gain affiche +2.98$
                        #     contre +0.34$ reels, observe en direct) ;
                        #   - si le prix retombe sous 0.35, les DEUX se
                        #     remplissent -> position doublee d'un seul cote,
                        #     donc plus verrouillee du tout.
                        if _leg_autre_c.get("order_id"):
                            if not self._cancel_verifie(
                                    _leg_autre_c["order_id"],
                                    f" {sym} {slug} {_leg_autre_c.get('side','?')} avant-completion"):
                                self._tlog(
                                    f"makeropen_comp_annul_{sym}",
                                    f"⚠️ [MAKER-OUVERT-COMPLETION] {sym} {slug} ordre d'origine "
                                    f"non annulable -> on renonce a completer (risque de double "
                                    f"remplissage sur la meme jambe)",
                                )
                                continue
                            _leg_autre_c["order_id"] = None
                        _r_m = self._live.post_limit_buy(
                            _leg_autre_c["token_id"], round(_bid_c, 2), round(_n_comp, 2))
                        if _r_m.get("success") and _r_m.get("order_id"):
                            leg_seule["comp_maker_oid"] = _r_m["order_id"]
                            leg_seule["comp_maker_px"] = round(_bid_c, 2)
                            leg_seule["comp_maker_ts"] = now
                            leg_seule["comp_maker_tente"] = True
                            # le prix de CE cote est desormais celui de la
                            # completion : toute branche qui lira e[cote]["price"]
                            # (dont "les deux jambes servies") rapportera juste.
                            _leg_autre_c["price"] = round(_bid_c, 2)
                            self._log(
                                f"📮 [MAKER-OUVERT-COMPLETION] {sym} {slug} 2e jambe "
                                f"{_leg_autre_c['side']} postee EN APPORTEUR a {_bid_c:.3f} "
                                f"(combine {_comb_m:.3f}, zero frais si servie) -- "
                                f"repli au marche dans {MAKER_OPEN_COMPLETION_MAKER_S}s"
                            )
                            self._save()
                            continue

                if _n_comp >= 0.01 and _ask_c is not None:
                    _comb_c = leg_seule["price"] + _ask_c
                    if _comb_c <= MAKER_OPEN_COMPLETION_MAX:
                        # cout reel = ask + frais taker sur CETTE jambe seulement
                        # ACHAT de la 2e jambe -> aucun frais (mesure 13/08).
                        # Avant, on retranchait ici des frais fantomes : sur
                        # la fenetre XRP 10:40-10:45 ET le gain reel etait
                        # +0.33$ et le bot calculait +0.117$.
                        _gain_c = _n_comp * (1 - _comb_c) - self._poly_fee(
                            _ask_c, _n_comp, "achat")
                        if _gain_c > MAKER_OPEN_COMPLETION_MIN_GAIN:
                            # MSF-TPNOW : la completion est-elle marginale ET
                            # notre jambe montre-t-elle DEJA un mouvement
                            # franc vers le TP ? Si oui, on renonce a acheter
                            # la 2e jambe ce cycle -- le flux plus bas (TP)
                            # aura sa chance. Aucun cash depense ici, aucun
                            # ordre annule : juste "pas cette fois".
                            _tpnow_agi = False
                            if sym in MAKER_OPEN_TPNOW_SYMBOLES:
                                try:
                                    _bk_notre = self._live.get_book_sync(leg_seule["token_id"])
                                    _bid_notre = (_bk_notre["bids"][0][0]
                                                 if _bk_notre and _bk_notre.get("bids") else None)
                                except Exception:
                                    _bid_notre = None
                                if _bid_notre is not None:
                                    # LE MEILLEUR DES DEUX, calcule sur le meme
                                    # carnet au meme instant. Aucun seuil de prix :
                                    # on compare deux gains en dollars.
                                    _gain_tp = ((_bid_notre - leg_seule["price"]) * _n_comp
                                                - self._poly_fee(_bid_notre, _n_comp))
                                    if _gain_tp > _gain_c + MAKER_OPEN_TPNOW_MARGE_MIN:
                                        _tpnow_agi = True
                                        self._log(
                                            f"💸 [MSF-TPNOW] {sym} {slug} {leg_seule['side']} "
                                            f"vendre notre jambe a {_bid_notre:.3f} rapporte "
                                            f"{_gain_tp:+.3f}$ contre {_gain_c:+.3f}$ pour la "
                                            f"completion a {_comb_c:.3f} (+{_gain_tp - _gain_c:.3f}$) "
                                            f"-> on ENCAISSE au lieu de DEPENSER, cash immediat "
                                            f"plutot qu'un gain bloque jusqu'a la resolution 🚀💵"
                                        )
                            if _tpnow_agi:
                                # ON PREND LE TP TOUT DE SUITE, A LA PLACE DE LA
                                # COMPLETION (Steven 13/08 : "tout ce que je
                                # demande depuis le depart c'est que MSF-TPNOW
                                # prenne le TP a sa place").
                                #
                                # La version precedente se contentait de SAUTER
                                # la completion en esperant que le flux de TP
                                # plus bas s'arme au cycle suivant. Il ne s'armait
                                # quasiment jamais : le TP passif exige que le
                                # prix atteigne le seuil PLEIN (x1.8, ou x1.5),
                                # alors que TPNOW se declenche a 70% du chemin.
                                # On renoncait donc a la completion sans rien
                                # encaisser -- le pire des deux mondes.
                                #
                                # L'ARITHMETIQUE EST SANS APPEL a ce point precis :
                                # la porte d'entree exige deja que la completion
                                # rapporte MOINS de 0.06$ ET que notre jambe ait
                                # parcouru 70% du chemin vers le seuil. Sur XRP
                                # (entree 0.35, seuil 0.525) ces 70% valent
                                # +0.1225/part, soit ~+0.66$ net sur 6.72 parts,
                                # contre <0.06$ pour la completion. On ne sacrifie
                                # pas un gros gain garanti : on en prend un ~10x
                                # plus gros, et en cash immediat.
                                #
                                # Mesure sur 36 h de carnet XRP (197 declenchements
                                # reels) : a l'instant de la completion, vendre
                                # notre jambe rapporte 1.576$ contre 1.601$ pour
                                # la completion -- 2.5 centimes d'ecart, 1.5%.
                                # Mais la completion DEPENSE ~3.29$ et bloque le
                                # capital 164s (median) plus le delai de
                                # redemption (2.8min median, 22min au p90), la
                                # ou le TP fait RENTRER ~4.70$ immediatement.
                                #
                                # On annule d'abord TOUT ordre encore vivant sur
                                # ce slug -- surtout le bid de l'autre cote : se
                                # faire servir apres avoir vendu notre jambe
                                # ouvrirait une position nue.
                                for _oid_tn in (leg_seule.get("tp_passif_order_id"),
                                                (_leg_autre_c or {}).get("order_id")):
                                    if _oid_tn:
                                        self._live.cancel_order(_oid_tn)
                                for _k in ("tp_passif_order_id", "tp_passif_price",
                                           "tp_passif_qty", "tp_passif_posted_ts"):
                                    leg_seule.pop(_k, None)
                                _vendu_tn = self._sell_orphan(
                                    leg_seule["token_id"], _n_comp,
                                    f" {sym} {slug} {leg_seule['side']} MSF-TPNOW",
                                    entry_price=leg_seule["price"], symbol=sym,
                                    slug=slug, side=leg_seule["side"],
                                    loss_tag="tpnow_prefere_a_completion",
                                    entry_ts=leg_seule.get("fill_ts"),
                                )
                                if _vendu_tn >= _n_comp - 0.01:
                                    self._log(
                                        f"💸 [MSF-TPNOW] {sym} {slug} {leg_seule['side']} "
                                        f"ENCAISSE {_vendu_tn:.2f} parts au lieu de completer "
                                        f"-> cash immediat, zero capital immobilise 🚀💵"
                                    )
                                    self._maker_open_record(
                                        sym, slug, "tp", combine=round(_comb_c, 4),
                                        parts=round(_n_comp, 2), prix=leg_seule["price"],
                                        vendu=round(_vendu_tn, 2), exec_mode="tpnow",
                                        calm=e.get("calm", False),
                                    )
                                    mk.setdefault("makeropen_cooldown", {})[slug] = \
                                        e.get("fin_ts", now) + 5
                                    st.pop(slug, None)
                                    self._save()
                                    continue
                                # vente ratee ou partielle -> on ne complete PAS
                                # ce cycle (on vient d'annuler l'ordre de l'autre
                                # cote) et on repassera au prochain poll.
                                self._log(
                                    f"⚠️ [MSF-TPNOW] {sym} {slug} vente incomplete "
                                    f"({_vendu_tn:.2f}/{_n_comp:.2f} parts) -> on retente "
                                    f"au prochain cycle"
                                )
                                self._save()
                                continue
                            self._log(
                                f"🧩 [MAKER-OUVERT-COMPLETION] {sym} {slug} jambe seule "
                                f"{leg_seule['side']}@{leg_seule['price']:.3f} + achat "
                                f"{_leg_autre_c['side']}@{_ask_c:.3f} = {_comb_c:.3f} "
                                f"-> verrou garanti +{_gain_c:.3f}$ au lieu de subir la jambe seule"
                            )
                            # on annule d'abord notre ordre passif de ce cote,
                            # sinon on pourrait etre servi DEUX fois. On
                            # RENONCE si l'annulation echoue : mieux vaut
                            # garder une jambe seule que doubler une jambe et
                            # perdre le verrou (cf. commentaire du chemin
                            # apporteur).
                            if _leg_autre_c.get("order_id"):
                                if not self._cancel_verifie(
                                        _leg_autre_c["order_id"],
                                        f" {sym} {slug} {_leg_autre_c.get('side','?')}"):
                                    self._tlog(
                                        f"makeropen_comp_annul2_{sym}",
                                        f"⚠️ [MAKER-OUVERT-COMPLETION] {sym} {slug} ordre "
                                        f"d'origine non annulable -> on renonce a completer",
                                    )
                                    continue
                                _leg_autre_c["order_id"] = None
                            _budget_c = round(_n_comp * _ask_c, 2)
                            with self._order_lock:
                                _res_c = self._live.snipe_buy_market(
                                    _leg_autre_c["token_id"],
                                    round(min(0.99, _ask_c + 0.01), 2), _budget_c)
                            _fill_c = _res_c.get("filled_shares", 0.0)
                            if _fill_c > 0.01:
                                _avg_c = _res_c.get("avg_cost") or _ask_c
                                for _cote, _n, _px in ((cote_seule, _n_comp, leg_seule["price"]),
                                                       (_autre_c, round(_fill_c, 2), _avg_c)):
                                    _lg = e[_cote]
                                    mk["open"][f"{slug}|{_lg['side']}"] = {
                                        "symbol": sym, "slug": slug, "side": _lg["side"],
                                        "mode": "real", "strat": "bothside", "maker_open": True,
                                        # notre pose initiale est apporteur ; la
                                        # completion achetee au marche est preneuse
                                        "maker_fill": (_cote == cote_seule),
                                        "token_id": _lg["token_id"], "entry_price": _px,
                                        "filled_shares": round(_n, 2),
                                        "cost": round(_n * _px, 2),
                                        "start_ts": e.get("debut_ts"), "pair": None,
                                        "end_ts": e.get("fin_ts"), "opened_ts": now,
                                        "buffer": 0.0,
                                    }
                                    self._add_slug_spent(mk, slug, round(_n * _px, 2))
                                self._tag_pair_lock(
                                    mk["open"].get(f"{slug}|{leg_seule['side']}"),
                                    mk["open"].get(f"{slug}|{_leg_autre_c['side']}"),
                                    leg_seule["price"] + _avg_c,
                                    tag=f" {sym} {slug} MAKER-OUVERT-COMPLETION")
                                self._maker_open_record(
                                    sym, slug, "les_deux",
                                    combine=round(leg_seule["price"] + _avg_c, 4),
                                    parts=round(min(_n_comp, _fill_c), 2),
                                    prix=leg_seule["price"], exec_mode="completion")
                                mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                                st.pop(slug, None)
                                self._save()
                                continue
                            self._log(
                                f"⚠️ [MAKER-OUVERT-COMPLETION] {sym} {slug} achat non rempli "
                                f"({_res_c.get('error', '?')}) -> on garde la gestion habituelle"
                            )

                # ── ABANDON : la paire est hors d'atteinte ────────────────
                # Voir MAKER_OPEN_ABANDON_MAX. On ne laisse plus la jambe
                # deriver jusqu'au solde force de T-45s : a ce stade elle ne
                # vaut deja plus rien et le carnet s'est vide.
                _combine_actuel = (leg_seule["price"] + _ask_c) if _ask_c is not None else None
                if _combine_actuel is not None and _combine_actuel > _abandon_max(sym):
                    if leg_seule.get("abandon_depuis") is None:
                        leg_seule["abandon_depuis"] = now
                    _depuis = now - leg_seule["abandon_depuis"]
                else:
                    # le combine est redescendu sous le seuil : le pic s'est
                    # resorbe tout seul, on efface le chrono -- une FUTURE
                    # incursion au-dessus du seuil repartira de zero.
                    leg_seule.pop("abandon_depuis", None)
                    _depuis = None

                # SCORE DE RISQUE DE RETOURNEMENT (danger pres du strike x
                # temps restant) : voir _score_risque_retournement. None ->
                # fail-open, ne bloque rien. Une valeur haute retient
                # l'abandon meme si persistance ET min_hold sont satisfaits --
                # le marche est en train de faire du yoyo pres de sa frontiere
                # de decision, laisser vivre encore un peu.
                _risque_ab = _score_risque_retournement(sym, slug, reste)
                _risque_ok = _risque_ab is None or _risque_ab < MAKER_OPEN_ABANDON_RISK_MAX

                if (_n_comp >= 0.01 and _combine_actuel is not None
                        and _combine_actuel > _abandon_max(sym)
                        and _depuis is not None and _depuis >= _abandon_persist_s(sym)
                        and _hold_s >= _abandon_min_hold_s(sym)
                        and _risque_ok
                        and reste > _cancel_before_s(sym)):
                    _comb_ab = round(_combine_actuel, 3)
                    # on retire d'abord TOUT ordre encore vivant sur ce slug :
                    # l'ask de scalp (qui ne sera jamais servi a ce stade) et
                    # le bid de l'autre cote (se faire remplir maintenant
                    # n'ajouterait qu'une seconde perte).
                    for _oid_ab in (leg_seule.get("tp_passif_order_id"),
                                    (_leg_autre_c or {}).get("order_id")):
                        if _oid_ab:
                            self._live.cancel_order(_oid_ab)
                    for _k in ("tp_passif_order_id", "tp_passif_price",
                               "tp_passif_qty", "tp_passif_posted_ts"):
                        leg_seule.pop(_k, None)
                    self._log(
                        f"🏳️ [MAKER-OUVERT-ABANDON] {sym} {slug} {leg_seule['side']} "
                        f"combine {_comb_ab} > {_abandon_max(sym)} (persiste {round(_depuis)}s, "
                        f"hold {round(_hold_s)}s, risque={_risque_ab}) -> paire hors "
                        f"d'atteinte, on solde maintenant ({int(300 - reste)}s de fenetre) "
                        f"plutot que d'attendre T-{MAKER_OPEN_CANCEL_BEFORE_S}s dans un "
                        f"carnet qui se vide"
                    )
                    _vendu_ab = self._sell_orphan(
                        leg_seule["token_id"], _n_comp,
                        f" {sym} {slug} {leg_seule['side']} MAKER-OUVERT-ABANDON",
                        entry_price=leg_seule["price"], symbol=sym,
                        slug=slug, side=leg_seule["side"],
                        loss_tag="abandon_combine_hors_atteinte",
                        entry_ts=leg_seule.get("fill_ts"),
                    )
                    if _vendu_ab >= _n_comp - 0.01:
                        self._maker_open_record(
                            sym, slug, "abandon", combine=_comb_ab,
                            parts=round(_n_comp, 2), prix=leg_seule["price"],
                            vendu=round(_vendu_ab, 2), exec_mode="agressif",
                            calm=e.get("calm", False),
                        )
                        mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                        st.pop(slug, None)
                        self._save()
                        continue
                    # vente partielle ou ratee -> on repasse par la gestion
                    # habituelle et on retentera au prochain cycle.

            # MODE CALME (Steven 09/08) : TP a prix ABSOLU (0.85). En calme on
            # est entre a ~0.65, et le x1.8 historique (0.65*1.8=1.17>1.0) ne
            # se declencherait jamais. En croisement (entree ~0.35) le x1.8
            # reste le bon outil.
            if e.get("calm"):
                _tp_seuil = CALM_MSF_TP_PRICE
            else:
                # RETOUR AU MULTIPLICATIF (cf. bloc SCALP DESACTIVE ci-dessus) :
                # l'offset +0.02 vendait la jambe avant que le verrou puisse
                # se former.
                _tp_seuil = _tp_seuil_prix(sym, leg_seule["price"])

            # ── SUIVI D'UN TP PASSIF DEJA POSTE (Steven 08/08, "sortie MAKER
            # au lieu d'agressive") : backteste sur 586 fenetres, frais de
            # sortie inclus -- poster l'ask au lieu de vendre au marche fait
            # passer le ROI realiste de +0.36% a +1.43% (BTC seul : +0.96% a
            # +2.15%), parce qu'un fill MAKER ne paie AUCUN frais (~5% economises
            # a chaque fois qu'un vrai acheteur croise notre ask, contre une
            # sortie agressive qui paie systematiquement). Un cycle anterieur a
            # deja poste cet ask -> on verifie juste s'il a ete rempli avant de
            # re-evaluer quoi que ce soit d'autre.
            if leg_seule.get("tp_passif_order_id"):
                _qty_avant = leg_seule.get("tp_passif_qty", 0.0)
                _n_reste = self._live.position_size(leg_seule["token_id"])
                _n_reste = _n_reste if _n_reste is not None and _n_reste >= 0 else _qty_avant
                if _n_reste <= 0.01 or _n_reste <= _qty_avant * 0.03:
                    # REMPLI EN MAKER -> zero frais, exactement ce que la piste visait.
                    _vendu_passif = round(max(0.0, _qty_avant - _n_reste), 2)
                    _px_passif = leg_seule["tp_passif_price"]
                    self._log(
                        f"💚 [MAKER-OUVERT-TP-PASSIF] {sym} {slug} {leg_seule['side']} "
                        f"rempli EN MAKER a {_px_passif:.3f} (zero frais) apres "
                        f"{now - leg_seule.get('tp_passif_posted_ts', now):.0f}s d'attente passive"
                    )
                    self._maker_open_record(
                        sym, slug, "tp", combine=None, parts=round(_qty_avant, 2),
                        prix=leg_seule["price"], vendu=_vendu_passif, sortie=_px_passif,
                        exec_mode="passif", calm=e.get("calm", False),
                    )
                    mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                    st.pop(slug, None)
                    self._save()
                    continue
                if reste > _cancel_before_s(sym):
                    # pas encore rempli, mais il reste du temps -> on laisse
                    # l'ordre passif ouvert, aucune raison de forcer une sortie
                    # payante tant que la fenetre n'est pas sur le point de finir.
                    continue
                # LA FENETRE SE TERMINE ET LE PASSIF N'A JAMAIS ETE REMPLI ->
                # repli agressif (paie le frais, comme avant cette piste), pour
                # ne jamais rester bloque avec une position non geree a la
                # resolution. Correspond exactement au "cutoff_apres_tp_rate"
                # du backtest.
                self._live.cancel_order(leg_seule["tp_passif_order_id"])
                _n_repli = self._live.position_size(leg_seule["token_id"])
                _n_repli = round(_n_repli, 2) if _n_repli and _n_repli > 0.01 else 0.0
                if _n_repli < 0.01:
                    for _k in ("tp_passif_order_id", "tp_passif_price", "tp_passif_qty", "tp_passif_posted_ts"):
                        leg_seule.pop(_k, None)
                    continue
                self._log(
                    f"⏰ [MAKER-OUVERT-TP-PASSIF-REPLI] {sym} {slug} {leg_seule['side']} "
                    f"jamais rempli en maker, fin de fenetre proche -> sortie agressive forcee"
                )
                _vendu_repli = self._sell_orphan(
                    leg_seule["token_id"], _n_repli,
                    f" {sym} {slug} {leg_seule['side']} MAKER-OUVERT-TP-PASSIF-REPLI",
                    entry_price=leg_seule["price"], symbol=sym, slug=slug, side=leg_seule["side"],
                    loss_tag="tp_passif_jamais_rempli_repli_cutoff",
                    entry_ts=leg_seule.get("fill_ts"),
                )
                self._maker_open_record(
                    sym, slug, "tp", combine=None, parts=_n_repli, prix=leg_seule["price"],
                    vendu=round(_vendu_repli, 2), sortie=leg_seule.get("tp_passif_price"),
                    exec_mode="agressif_repli", calm=e.get("calm", False),
                )
                mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                st.pop(slug, None)
                self._save()
                continue

            # INTERRUPTEUR TP (Steven 07/08, MSF) : reglable a chaud depuis le
            # dashboard, sans redeploiement. OFF -> saute directement au
            # comportement d'avant le TP (attente jusqu'au cutoff habituel,
            # plus bas dans cette meme fonction) -- ni lecture de carnet, ni
            # decision de vente ici.
            # EXCEPTION MODE CALME (Steven 09/08, "je n'ai vu aucun tp sl") :
            # ce switch a ete concu pour le TP du mode CROISEMENT (jambe seule
            # a 0.35 qu'on peut laisser courir jusqu'au cutoff sans drame). En
            # mode CALME l'entree est directionnelle a 0.65 : sans TP ni SL la
            # position n'a AUCUNE gestion et va jusqu'a la resolution -- c'est
            # exactement le "ca fait fondre le compte". Le TP calme ignore donc
            # le switch ; le SL calme, lui, n'a jamais ete gate par ce switch.
            _msf_tp_on = bool(self.state.get("msf_tp_enabled", True)) or bool(e.get("calm"))
            _cur = None
            if _msf_tp_on:
                # PRIX DE DECLENCHEMENT = VRAI BID, PAS _live_price (Steven 07/08).
                # Incident reel : _live_price se rabat sur le prix de l'ASK SEUL
                # des que le carnet BID est vide -- un prix auquel personne ne veut
                # nous acheter. Le TP se declenchait alors sur un prix fantome,
                # annulait l'autre jambe pour rien, puis echouait a vendre
                # ("pas de bid, carnet vide", ~27% des tentatives de vente vues en
                # reel). Backteste sur 519+65 fenetres : ce bug fait chuter le gain
                # du TP de 78% en scenario conservateur (12.04$/j -> 2.68$/j) ; se
                # limiter au vrai bid recupere l'essentiel (6.81$/j).
                # On lit donc le carnet DIRECTEMENT et on n'accepte que le
                # meilleur BID reel -- si le carnet bid est vide, _cur est None et
                # le TP ne se declenche simplement pas ce cycle (retente au
                # prochain, ou finit par le chemin cutoff habituel).
                _book_tp = self._live.get_book_sync(leg_seule["token_id"])
                _cur = _book_tp["bids"][0][0] if _book_tp and _book_tp.get("bids") else None
                # LOGGING CARNET POUR FUTUR GATEKEEPER ML (Steven 08/08) : on a
                # deja le carnet en main pour la decision TP, zero appel reseau
                # supplementaire. Throttle a 1 snapshot/5s par jambe pour ne pas
                # explaser l'historique -- l'objectif est d'accumuler assez de
                # semaines de contexte reel (profondeur, imbalance, spread) pour
                # entrainer plus tard un filtre Go/No-Go, PAS de decider quoi
                # que ce soit aujourd'hui. Ne touche a aucune decision de trading.
                if now - leg_seule.get("_snap_ts", 0) >= 5:
                    leg_seule["_snap_ts"] = now
                    self._record_book_snapshot(
                        sym, slug, leg_seule["side"], _book_tp, leg_seule["price"],
                        _tp_seuil, _hold_s, mk.get("danger", 0),
                        triggered=bool(_cur is not None and _hold_s >= _tp_min_hold_s(sym) and _cur >= _tp_seuil),
                    )
            # POSE IMMEDIATE ANNULEE (Steven 12/08, solde 20$ -> 3$) : poster
            # l'ask des le remplissage vendait la jambe avant que l'autre cote
            # ait eu le temps de toucher 0.35 -> plus aucun verrou. On exige a
            # nouveau que le prix ATTEIGNE le seuil avant de poster.
            if _cur is not None and _hold_s >= _tp_min_hold_s(sym) and _cur >= _tp_seuil:
                # RELECTURE FRAICHE DE LA QUANTITE (Steven 07/08, "24 TP dont
                # 11 rates a parts=0.0"). `fills[cote_seule]` vient d'une
                # lecture faite PLUS TOT dans ce meme cycle -- position_size()
                # renvoie parfois 0 de facon transitoire (retard cote
                # Polymarket), et le code faisait confiance a ce zero perime :
                # _sell_orphan(token, 0, ...) retourne 0 SANS RIEN LOGUER (son
                # garde `if shares < 0.01: return 0.0` est muet), donc le TP
                # "se declenchait" au bon prix (sortie correcte dans le
                # journal) mais ne vendait jamais rien, sans la moindre trace.
                # On relit ici, juste avant d'agir, et si c'est ENCORE 0 on
                # abandonne ce cycle plutot que d'enregistrer un TP fantome --
                # la position reste suivie, on retente au prochain cycle.
                n_tp = self._live.position_size(leg_seule["token_id"])
                n_tp = round(n_tp, 2) if n_tp and n_tp > 0.01 else 0.0
                if n_tp < 0.01:
                    self._tlog(
                        f"makeropen_tp_zero_{sym}",
                        f"⚠️ [MAKER-OUVERT-TP] {sym} {slug} prix TP atteint mais "
                        f"position_size() renvoie 0 la -> on retente au prochain cycle",
                    )
                    continue
                autre_seule = "b" if cote_seule == "a" else "a"
                leg_autre = e[autre_seule]
                oid_autre = (leg_autre or {}).get("order_id")
                if oid_autre:
                    # ANNULATION VERIFIEE (Steven 10/08) : etait un
                    # fire-and-forget dont l'echec passait inapercu -- voir
                    # _cancel_verifie. C'est LE chemin critique : on s'apprete
                    # a vendre notre jambe et a oublier la fenetre (st.pop),
                    # donc un ordre survivant ici n'aurait plus jamais de
                    # gardien.
                    self._cancel_verifie(
                        oid_autre, f" {sym} {slug} {(leg_autre or {}).get('side', '?')}"
                    )
                # RE-VERIFICATION (Steven 07/08, "si on a les 2 leg pas de tp") :
                # entre la decision de prendre le TP et l'annulation qui arrive
                # REELLEMENT a l'echange, l'autre ordre peut se faire remplir
                # par le marche -- on tiendrait alors un vrai verrou sans le
                # savoir, et le vendre serait PIRE que de le garder. Meme
                # pattern deja utilise juste au-dessus pour la branche "les
                # deux" : on ne fait jamais confiance a un etat lu AVANT
                # l'annulation, on relit la verite juste avant d'agir.
                _held_autre = (
                    self._live.position_size(leg_autre.get("token_id"))
                    if leg_autre.get("token_id") else 0.0
                )
                if _held_autre and _held_autre > 0.01:
                    self._log(
                        f"🔒 [MAKER-OUVERT-TP-RATTRAPE] {sym} {slug} l'autre cote "
                        f"({leg_autre['side']}) s'est rempli pendant l'annulation -> "
                        f"on garde les 2 jambes, VERROU au lieu du TP"
                    )
                    n_autre = round(_held_autre, 2)
                    for cote, n in ((cote_seule, n_tp), (autre_seule, n_autre)):
                        leg = e[cote]
                        mk["open"][f"{slug}|{leg['side']}"] = {
                            "symbol": sym, "slug": slug, "side": leg["side"], "mode": "real",
                            "strat": "bothside", "maker_open": True,
                            "token_id": leg["token_id"], "entry_price": leg["price"],
                            "filled_shares": round(n, 2), "cost": round(n * leg["price"], 2),
                            "start_ts": e.get("debut_ts"), "pair": None,
                            "end_ts": e.get("fin_ts"), "opened_ts": now, "buffer": 0.0,
                        }
                        self._add_slug_spent(mk, slug, round(n * leg["price"], 2))
                    _pa_r, _pb_r = leg_seule["price"], leg_autre["price"]
                    self._tag_pair_lock(
                        mk["open"].get(f"{slug}|{leg_seule['side']}"),
                        mk["open"].get(f"{slug}|{leg_autre['side']}"),
                        _pa_r + _pb_r, tag=f" {sym} {slug} MAKER-OUVERT-TP-RATTRAPE",
                    )
                    self._maker_open_record(
                        sym, slug, "les_deux", combine=round(_pa_r + _pb_r, 4),
                        parts=round(min(n_tp, n_autre), 2), prix=_pa_r,
                    )
                    mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                    st.pop(slug, None)
                    self._save()
                    continue
                # SORTIE PASSIVE D'ABORD (Steven 08/08) : au lieu de vendre au
                # marche (agressif, ~5% de frais taker), on tente de poster un
                # ASK juste au-dessus du bid observe (1 tick, pour ne jamais
                # traverser le carnet et redevenir preneur par accident). Si un
                # vrai acheteur croise cet ask, la vente est MAKER -> zero frais.
                # Backteste sur 586 fenetres (offset 1 tick) : bat la sortie
                # agressive dans les 2 modes de remplissage. Repli agressif
                # immediat si le post lui-meme echoue (jamais de position
                # laissee sans plan de sortie).
                _ask_passif = round(_cur + 0.01, 2)
                _post_passif = self._live.post_limit_sell(leg_seule["token_id"], _ask_passif, round(n_tp, 2))
                if _post_passif.get("success") and _post_passif.get("order_id"):
                    leg_seule["tp_passif_order_id"] = _post_passif["order_id"]
                    leg_seule["tp_passif_price"] = _ask_passif
                    leg_seule["tp_passif_qty"] = round(n_tp, 2)
                    leg_seule["tp_passif_posted_ts"] = now
                    self._log(
                        f"📮 [MAKER-OUVERT-TP-PASSIF] {sym} {slug} {leg_seule['side']} "
                        f"{leg_seule['price']:.3f}->{_cur:.3f} (+{100*(_cur/leg_seule['price']-1):.0f}%) "
                        f"apres {_hold_s:.0f}s -> ask MAKER poste a {_ask_passif:.3f} "
                        f"(zero frais si rempli), repli agressif si non rempli avant le cutoff"
                    )
                    self._save()
                    continue
                self._log(
                    f"⚠️ [MAKER-OUVERT-TP-PASSIF] {sym} {slug} echec du post ask "
                    f"({_post_passif.get('error', '?')}) -> repli immediat sur la vente agressive"
                )
                vendu_tp = self._sell_orphan(
                    leg_seule["token_id"], round(n_tp, 2),
                    f" {sym} {slug} {leg_seule['side']} MAKER-OUVERT-TP",
                    entry_price=leg_seule["price"], symbol=sym, slug=slug,
                    side=leg_seule["side"],
                    loss_tag="tp_seuil_atteint_post_passif_echec",
                    entry_ts=leg_seule.get("fill_ts"),
                )
                if vendu_tp < n_tp - 0.01:
                    mk["open"][f"{slug}|{leg_seule['side']}"] = {
                        "symbol": sym, "slug": slug, "side": leg_seule["side"], "mode": "real",
                        "strat": "orphan", "token_id": leg_seule["token_id"],
                        "entry_price": leg_seule["price"], "filled_shares": round(n_tp - vendu_tp, 2),
                        "cost": round((n_tp - vendu_tp) * leg_seule["price"], 2),
                        "start_ts": e.get("debut_ts"), "pair": None,
                        "end_ts": e.get("fin_ts"), "opened_ts": now,
                        "buffer": 0.0, "must_close": True,
                    }
                # RESIDU DE COURSE (Steven 07/08, "8.97 parts Down oubliees,
                # perte de 3.14$ invisible"). Meme apres la re-verification
                # d'avant-vente, l'autre ordre peut encore se remplir dans le
                # court instant ou _sell_orphan poste et confirme LA NOTRE --
                # course impossible a fermer a 100% sans garantie atomique de
                # l'echange. Une DERNIERE lecture ici ne l'empeche pas, mais
                # evite qu'elle reste invisible : si l'autre cote montre un
                # remplissage, on la trackee comme orpheline (geree par les
                # mecanismes de sortie existants) au lieu de l'abandonner.
                _held_autre_post = (
                    self._live.position_size(leg_autre.get("token_id"))
                    if leg_autre.get("token_id") else 0.0
                )
                if _held_autre_post and _held_autre_post > 0.01:
                    self._log(
                        f"🦺 [MAKER-OUVERT-TP-RESIDU] {sym} {slug} {leg_autre['side']} "
                        f"{_held_autre_post:.2f} parts remplies pendant la vente TP -> "
                        f"trackee comme orpheline (n'etait suivie nulle part avant ce fix)"
                    )
                    mk["open"][f"{slug}|{leg_autre['side']}"] = {
                        "symbol": sym, "slug": slug, "side": leg_autre["side"], "mode": "real",
                        "strat": "orphan", "token_id": leg_autre["token_id"],
                        "entry_price": leg_autre["price"], "filled_shares": round(_held_autre_post, 2),
                        "cost": round(_held_autre_post * leg_autre["price"], 2),
                        "start_ts": e.get("debut_ts"), "pair": None,
                        "end_ts": e.get("fin_ts"), "opened_ts": now,
                        "buffer": 0.0, "must_close": True,
                    }
                    self._add_slug_spent(mk, slug, round(_held_autre_post * leg_autre["price"], 2))
                self._maker_open_record(sym, slug, "tp", combine=None,
                                        parts=round(n_tp, 2), prix=leg_seule["price"],
                                        vendu=round(vendu_tp, 2), sortie=round(_cur, 4),
                                        exec_mode="agressif_echec_post",
                                        calm=e.get("calm", False))
                mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                st.pop(slug, None)
                self._save()
                continue

            # ── SL MODE CALME (Steven 09/08, "gestion sl/tp" + "l'inverse") ──
            # En mode CROISEMENT on garde la jambe seule jusqu'au cutoff (elle
            # peut retomber sur le TP x1.8). En mode CALME on a pose a
            # CALM_MSF_PRICE pour attraper le GAGNANT : si la jambe tenue PERD
            # au lieu de gagner, on coupe des que le bid passe sous le seuil,
            # au lieu d'attendre passivement T-45s (c'est LE fix du "on finit
            # avec la seule jambe perdante"). L'autre ordre est annule : on ne
            # laisse jamais un ordre non-suivi en vie apres avoir encaisse la
            # perte.
            if e.get("calm"):
                _book_sl = self._live.get_book_sync(leg_seule["token_id"])
                _bid_sl = _book_sl["bids"][0][0] if _book_sl and _book_sl.get("bids") else None
                # SUIVI VISIBLE (Steven 09/08, "je ne voyais pas non plus de
                # SL") : sans cette ligne, une position calme sous surveillance
                # est totalement muette dans le journal tant qu'aucun seuil
                # n'est franchi -- impossible de distinguer "ca surveille" de
                # "ca ne tourne pas". Throttle 15s, aucune lecture reseau en
                # plus (le carnet vient d'etre lu juste au-dessus).
                if _bid_sl is not None:
                    _px_in = leg_seule.get("price")
                    self._tlog(
                        f"makeropen_calm_suivi_{sym}",
                        f"👁️ [MAKER-OUVERT-CALME] {sym} {slug} {leg_seule['side']} "
                        f"entree {_px_in if _px_in is None else f'{_px_in:.3f}'} | "
                        f"bid {_bid_sl:.3f} | TP {CALM_MSF_TP_PRICE:.2f} | "
                        f"SL {CALM_MSF_SL_PRICE:.2f} -> sous surveillance",
                    )
                # MEME PLANCHER DE DETENTION que l'abandon (Steven 13/08) :
                # ce chemin-ci n'avait AUCUN min_hold, il pouvait donc couper
                # dans la seconde suivant le fill. C'est exactement le
                # comportement que la consigne interdit.
                if (_bid_sl is not None and _bid_sl <= CALM_MSF_SL_PRICE
                        and _hold_s < MSF_SL_MIN_HOLD_S):
                    self._tlog(
                        f"makeropen_calm_sl_jeune_{sym}",
                        f"⏳ [MAKER-OUVERT-CALM-SL] {sym} {slug} bid {_bid_sl:.3f} "
                        f"sous le seuil MAIS position agee de {_hold_s:.0f}s "
                        f"seulement (< {MSF_SL_MIN_HOLD_S:.0f}s) -> on NE COUPE PAS, "
                        f"on laisse le prix respirer",
                    )
                elif _bid_sl is not None and _bid_sl <= CALM_MSF_SL_PRICE:
                    _n_sl = self._live.position_size(leg_seule["token_id"])
                    _n_sl = round(_n_sl, 2) if _n_sl and _n_sl > 0.01 else 0.0
                    if _n_sl >= 0.01:
                        self._log(
                            f"🛑 [MAKER-OUVERT-CALM-SL] {sym} {slug} {leg_seule['side']} "
                            f"entree {leg_seule['price']:.3f} -> bid {_bid_sl:.3f} "
                            f"(seuil {CALM_MSF_SL_PRICE:.2f}, mode CALME) -> on coupe, "
                            f"plus d'attente jusqu'au cutoff"
                        )
                        _vendu_sl = self._sell_orphan(
                            leg_seule["token_id"], round(_n_sl, 2),
                            f" {sym} {slug} {leg_seule['side']} MAKER-OUVERT-CALM-SL",
                            entry_price=leg_seule["price"], symbol=sym, slug=slug,
                            side=leg_seule["side"],
                            loss_tag="calm_sl_bid_sous_seuil",
                            entry_ts=leg_seule.get("fill_ts"),
                        )
                        if _vendu_sl < _n_sl - 0.01:
                            mk["open"][f"{slug}|{leg_seule['side']}"] = {
                                "symbol": sym, "slug": slug, "side": leg_seule["side"],
                                "mode": "real", "strat": "orphan",
                                "token_id": leg_seule["token_id"],
                                "entry_price": leg_seule["price"],
                                "filled_shares": round(_n_sl - _vendu_sl, 2),
                                "cost": round((_n_sl - _vendu_sl) * leg_seule["price"], 2),
                                "start_ts": e.get("debut_ts"), "pair": None,
                                "end_ts": e.get("fin_ts"), "opened_ts": now,
                                "buffer": 0.0, "must_close": True,
                            }
                        _leg_autre_sl = e["b" if cote_seule == "a" else "a"]
                        _oid_autre_sl = (_leg_autre_sl or {}).get("order_id")
                        if _oid_autre_sl:
                            self._live.cancel_order(_oid_autre_sl)
                        self._maker_open_record(
                            sym, slug, "calm_sl", combine=None,
                            parts=round(_n_sl, 2), prix=leg_seule["price"],
                            vendu=round(_vendu_sl, 2), sortie=round(_bid_sl, 4),
                            exec_mode="calme", calm=True,
                        )
                        mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                        st.pop(slug, None)
                        self._save()
                        continue

            # tant qu'il reste du temps, on laisse courir : la 2e jambe peut
            # arriver bien plus tard dans la fenetre, le verrou n'exige pas la
            # simultaneite.
            if reste > _cancel_before_s(sym):
                continue

            # ── FIN DE FENETRE : on annule ce qui dort ──
            for cote in ("a", "b"):
                oid = (e.get(cote) or {}).get("order_id")
                if oid and not fills.get(cote, 0) > 0.01:
                    self._live.cancel_order(oid)
            if fa <= 0.01 and fb <= 0.01:
                self._tlog(
                    f"makeropen_rien_{sym}",
                    f"⭕ [MAKER-OUVERT] {sym} {slug} aucun remplissage -> annule, "
                    f"cout ZERO",
                )
                self._maker_open_record(sym, slug, "aucun", combine=None, parts=0, prix=None)
                # une fenetre traitee ne doit jamais etre reposee : le garde
                # MAKER_OPEN_MIN_REMAIN_S y suffit aujourd'hui, ce verrou
                # explicite evite qu'un futur changement de constante ne
                # rouvre la porte a une reprise en boucle sur la meme fenetre.
                mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
                st.pop(slug, None)
                self._save()
                continue

            # UNE SEULE servie : on la solde avant la resolution. C'est la
            # difference decisive avec l'arb decale, qui la gardait : une jambe
            # nue portee a resolution vaut EXACTEMENT -100% quand elle perd,
            # mesure sur 51 cas sur 51.
            cote = "a" if fa > 0.01 else "b"
            leg = e[cote]
            # RELECTURE FRAICHE (meme raison que la branche TP juste au-dessus) :
            # fills[cote] vient d'une lecture plus tot dans ce cycle, et
            # position_size() peut renvoyer 0 de facon transitoire. Ici les
            # 2 ordres viennent d'etre annules (bloc juste au-dessus) : si la
            # relecture est 0, la position est probablement bien la, on
            # retente au prochain cycle plutot que d'enregistrer une vente
            # fantome.
            n = self._live.position_size(leg["token_id"])
            n = round(n, 2) if n and n > 0.01 else 0.0
            if n < 0.01:
                self._tlog(
                    f"makeropen_solo_zero_{sym}",
                    f"⚠️ [MAKER-OUVERT] {sym} {slug} position_size() renvoie 0 "
                    f"la -> on retente au prochain cycle",
                )
                continue
            leg_autre_cutoff = e["b" if cote == "a" else "a"]
            self._log(
                f"⚠️ [MAKER-OUVERT] {sym} {slug} une seule jambe servie "
                f"({leg['side']} {n:.2f} parts @ {leg['price']:.3f}) -> on solde avant la fin"
            )
            # PRIX DE SORTIE CAPTURE POUR LE JOURNAL SEULEMENT (Steven 07/08,
            # notification sonore gain/perte) : simple LECTURE du meilleur bid,
            # aucun effet sur la decision -- _sell_orphan relit le sien de son
            # cote pour l'ordre reel. Sans ca "une_seule" n'avait pas de prix
            # de sortie enregistre, impossible de calculer un gain/perte a
            # afficher (contrairement a "tp" qui l'a deja via "sortie").
            _book_exit = self._live.get_book_sync(leg["token_id"])
            _px_sortie = _book_exit["bids"][0][0] if _book_exit and _book_exit.get("bids") else None
            vendu = self._sell_orphan(
                leg["token_id"], round(n, 2),
                f" {sym} {slug} {leg['side']} MAKER-OUVERT-SOLO",
                entry_price=leg["price"], symbol=sym, slug=slug, side=leg["side"],
                loss_tag="cutoff_final_t45s",
                entry_ts=leg.get("fill_ts"),
            )
            if vendu < n - 0.01:
                mk["open"][f"{slug}|{leg['side']}"] = {
                    "symbol": sym, "slug": slug, "side": leg["side"], "mode": "real",
                    "strat": "orphan", "token_id": leg["token_id"],
                    "entry_price": leg["price"], "filled_shares": round(n - vendu, 2),
                    "cost": round((n - vendu) * leg["price"], 2),
                    "start_ts": e.get("debut_ts"), "pair": None,
                    "end_ts": e.get("fin_ts"), "opened_ts": now,
                    "buffer": 0.0, "must_close": True,
                }
            # RESIDU DE COURSE, meme raison que la branche TP : l'ordre qu'on
            # vient d'annuler peut s'etre rempli entre l'annulation et la
            # confirmation de notre propre vente. Sans ce filet, cette jambe
            # serait invisible -- exactement ce qui est arrive sur
            # btc-updown-5m-1786133100 (8.97 parts Down, 3.14$ perdus sans
            # trace).
            _held_autre_cutoff = (
                self._live.position_size(leg_autre_cutoff.get("token_id"))
                if leg_autre_cutoff.get("token_id") else 0.0
            )
            if _held_autre_cutoff and _held_autre_cutoff > 0.01:
                self._log(
                    f"🦺 [MAKER-OUVERT-RESIDU] {sym} {slug} {leg_autre_cutoff['side']} "
                    f"{_held_autre_cutoff:.2f} parts remplies pendant la vente -> "
                    f"trackee comme orpheline"
                )
                mk["open"][f"{slug}|{leg_autre_cutoff['side']}"] = {
                    "symbol": sym, "slug": slug, "side": leg_autre_cutoff["side"], "mode": "real",
                    "strat": "orphan", "token_id": leg_autre_cutoff["token_id"],
                    "entry_price": leg_autre_cutoff["price"], "filled_shares": round(_held_autre_cutoff, 2),
                    "cost": round(_held_autre_cutoff * leg_autre_cutoff["price"], 2),
                    "start_ts": e.get("debut_ts"), "pair": None,
                    "end_ts": e.get("fin_ts"), "opened_ts": now,
                    "buffer": 0.0, "must_close": True,
                }
                self._add_slug_spent(mk, slug, round(_held_autre_cutoff * leg_autre_cutoff["price"], 2))
            self._maker_open_record(sym, slug, "une_seule", combine=None,
                                    parts=round(n, 2), prix=leg["price"], vendu=round(vendu, 2),
                                    sortie=(round(_px_sortie, 4) if _px_sortie is not None else None),
                                    calm=e.get("calm", False))
            mk.setdefault("makeropen_cooldown", {})[slug] = e.get("fin_ts", now) + 5
            st.pop(slug, None)
            self._save()

        # ── POSE SUR LA FENETRE EN COURS ────────────────────────────────
        # MAKER_OPEN_ENABLED verifie ICI seulement (Steven 01/09, "on doit
        # suivre toute les pos !") -- desactiver ne doit JAMAIS couper le
        # suivi (SUIVI DES ORDRES POSES ci-dessus) des ordres deja en carnet
        # ou positions deja remplies. Avant ce correctif le meme flag
        # coupait aussi le suivi, ce qui abandonnait toute position en
        # attente de fill sans TP/SL des que le flag passait a False.
        if not MAKER_OPEN_ENABLED:
            return
        if st:
            return                      # une seule fenetre a la fois
        debut = int(now // 300) * 300
        fin = debut + 300
        reste = fin - now
        if reste < MAKER_OPEN_MIN_REMAIN_S:
            return                      # trop tard pour esperer etre servi
        slug = f"{sym.lower()}-updown-5m-{debut}"
        if any(k.startswith(f"{slug}|") for k in mk["open"]):
            return                      # deja une position sur cette fenetre
        if mk.setdefault("makeropen_cooldown", {}).get(slug, 0) > now:
            return
        meta = self._market_meta(slug)
        if not meta:
            return
        outcomes, token_ids = meta
        # PLUS DE PAIRE (Steven 19/08, meme decision que BOTHSIDE-SEQ) : une
        # seule jambe posee ici aussi -- divise par ~2 le besoin en cash
        # (5 parts x 1 jambe au lieu de 2) et le TP instantane rend la
        # 2e jambe inutile de toute facon.
        outcomes, token_ids = outcomes[:1], token_ids[:1]

        # ── MODE CALME vs CROISEMENT (Steven 09/08) ──
        # danger_score bas (< CALM_MSF_DANGER_MAX) = marche calme qui prend une
        # direction sans croiser -> on fait l'INVERSE de d'habitude : on pose
        # a CALM_MSF_PRICE (0.65, le prix du gagnant) au lieu de 0.35 (le prix
        # du perdant), combined autorise jusqu'a 1.05 (couverture directionnelle,
        # pas un arb), et la gestion coupe la jambe qui decroche. Mode
        # CROISEMENT : comportement historique exactement inchange.
        from core.btc_updown import danger_score as _danger_score_mo
        from core.btc_updown import _strike_at as _strike_at_mo

        _pair_mo = f"{sym.upper()}USDT"
        _strike_mo = _strike_at_mo(_pair_mo, debut, slug=slug)
        _d_mo = _danger_score_mo(_pair_mo, _strike_mo) if _strike_mo is not None else 0
        _calm_mo = bool(
            CALM_MSF_ENABLED and _strike_mo is not None and _d_mo < CALM_MSF_DANGER_MAX
        )
        if _calm_mo:
            self._tlog(
                f"makeropen_calm_{sym}",
                f"🌊 [MAKER-OUVERT-CALME] {sym} {slug} danger={_d_mo} < "
                f"{CALM_MSF_DANGER_MAX} -> mode CALME, pose a {CALM_MSF_PRICE:.2f} "
                f"(l'inverse du croisement : on vise le gagnant)",
            )
        if self.state.get("calm_msf_stats", {}).get("disabled"):
            _calm_mo = False
            self._tlog(
                f"makeropen_calm_disabled_{sym}",
                f"⛔ [MAKER-OUVERT-CALME] {sym} {slug} auto-stop actif (ROI<0 apres "
                f"{CALM_MSF_AUTOSTOP_N} trades) -> on reste en mode croisement",
            )
        _px_cap = CALM_MSF_PRICE if _calm_mo else MAKER_OPEN_PRICE
        if _calm_mo:
            # PLUS D'ADAPTATION EN CALME (Steven 09/08, "on ne pose QUE a
            # 0.65 des 2 cotes") : plancher = plafond -> des qu'un cote est
            # deja sous 0.65 (donc PAS le favori -- c'est justement le cote
            # qu'on ne veut jamais acheter en mode calme), on saute cette
            # jambe au lieu de l'acheter a prix adapte. Vu en reel : une
            # jambe remplie a un prix adapte plus bas (0.35, cote perdant)
            # au lieu du favori vise -- exactement l'inverse de l'intention
            # du mode calme.
            _px_floor = CALM_MSF_PRICE
        elif MAKER_OPEN_ADAPT_ENABLED:
            _px_floor = MAKER_OPEN_ADAPT_FLOOR
        else:
            # PAUSE (Steven 09/08) : plancher = plafond -> le garde-fou
            # existant ("prix_adapte >= aa -> abandon") se declenche alors
            # SYSTEMATIQUEMENT des que l'ask est deja sous MAKER_OPEN_PRICE,
            # reproduisant exactement le comportement d'avant l'adaptatif.
            _px_floor = MAKER_OPEN_PRICE
        _comb_max = CALM_MSF_MAX_COMBINED if _calm_mo else MAKER_OPEN_MAX_COMBINED

        prix = []
        _skip = []
        for i, tid in enumerate(token_ids):
            b = self._live.get_book_sync(tid)
            aa = b["asks"][0][0] if b and b.get("asks") else None
            if aa is None:
                return
            # JAMAIS AU-DESSUS DE L'ASK : un achat limite qui atteint l'ask
            # traverse le carnet et nous rend PRENEUR -- ce qui reperdrait
            # exactement les frais que ce mecanisme existe pour eviter.
            if _px_cap < aa:
                prix.append(_px_cap)
                _skip.append(False)
                continue
            # ADAPTATIF : l'ask est deja sous notre prix de pose (marche deja
            # parti d'un cote) -- au lieu d'abandonner toute la fenetre, on se
            # cale sous l'ask actuel, JAMAIS au-dessus du plafond du mode.
            prix_adapte = round(max(_px_floor, aa - MAKER_OPEN_ADAPT_DISCOUNT), 2)
            if prix_adapte >= aa:
                if _calm_mo:
                    # MODE CALME (Steven 09/08, "rien n'est reellement pose") :
                    # ce cote est deja tranche/bradé (ask < plancher 0.35). Le
                    # poser a CALM_MSF_PRICE nous rendrait PRENEUR sur le
                    # PERDANT -- exactement ce qu'on ne veut PLUS en mode calme.
                    # On SAUTE cette jambe au lieu d'abandonner toute la fenetre
                    # : l'autre cote (le gagnant) est encore posee et geree par
                    # TP absolu / SL.
                    self._tlog(
                        f"makeropen_calm_skip_{sym}",
                        f"↪️ [MAKER-OUVERT-CALME] {sym} {slug} {outcomes[i]} ask "
                        f"{aa:.3f} < plancher {_px_floor:.2f} (perdant bradé) -> "
                        f"on saute cette jambe, le gagnant est quand meme pose",
                    )
                    prix.append(None)
                    _skip.append(True)
                    continue
                self._tlog(
                    f"makeropen_cher_{sym}",
                    f"⏸️ [MAKER-OUVERT] {sym} {slug} ask a {aa:.3f} trop bas meme "
                    f"pour le prix adaptatif -> on ne pose pas",
                )
                return
            self._tlog(
                f"makeropen_adapte_{sym}",
                f"🔧 [MAKER-OUVERT-ADAPTE] {sym} {slug} ask deja a {aa:.3f} "
                f"(poser a {_px_cap:.2f} nous rendrait PRENEUR) -> "
                f"pose adaptee a {prix_adapte:.2f} a la place",
            )
            prix.append(prix_adapte)
            _skip.append(False)
        _prix_post = [p for p in prix if p is not None]
        if not _prix_post:
            self._tlog(
                f"makeropen_rien_{sym}",
                f"⭕ [MAKER-OUVERT] {sym} {slug} aucune jambe posable (marche "
                f"tranche des 2 cotes) -> on ne pose pas",
            )
            return
        comb = round(sum(_prix_post), 4)
        if comb > _comb_max:
            return

        budget = self._maker_open_budget(sym)
        parts = round(max(MIN_ORDER_SIZE_SHARES, budget / max(0.01, comb)) + 0.01, 2)
        besoin = round(parts * comb, 2)
        if besoin > self._investable():
            self._tlog(
                f"makeropen_fonds_{sym}",
                f"⛔ [MAKER-OUVERT] {sym} {slug} il faut {besoin:.2f}$ pour les 2 jambes, "
                f"{self._investable():.2f}$ dispo -> on ne pose pas ce qu'on ne peut pas honorer",
            )
            return
        ok_exp, why_exp = self._exposure_ok(sym, mk, slug, besoin,
                                            cap=self._maker_open_expo_max())
        if not ok_exp:
            # ETAIT SILENCIEUX (Steven 09/08) : ce garde-fou pouvait bloquer
            # 100% des tentatives sans laisser AUCUNE trace dans le journal --
            # exactement ce qui rendait le bug budget/expo ci-dessus invisible.
            self._tlog(
                f"makeropen_expo_{sym}",
                f"⛔ [MAKER-OUVERT] {sym} {slug} plafond d'exposition atteint "
                f"({why_exp}) -> on ne pose pas",
            )
            return

        _mode_lbl = "CALME" if _calm_mo else "CROISEMENT"
        _pose_desc = " + ".join(
            f"{outcomes[i]}@{px:.3f}" for i, px in enumerate(prix) if px is not None
        )
        if comb < 1.0:
            _gain_lbl = f"-> +{(1 - comb) * 100:.1f}% si tous les ordres sont servis"
        else:
            _gain_lbl = f"-> couverture {comb:.3f} (TP/SL gere, pas un arb)"
        self._log(
            f"📮 [MAKER-OUVERT-{_mode_lbl}] {sym} {slug} {reste:.0f}s restantes | "
            f"danger={_d_mo} | pose {len(_prix_post)} ordre(s) PASSIF(S) {_pose_desc} "
            f"= {comb:.3f} ({parts} parts, {besoin:.2f}$) {_gain_lbl}"
        )
        pose = {}
        pose["calm"] = _calm_mo
        # POSE EN PARALLELE (Steven 08/08, "HTTP/2 = multiplexing, plusieurs
        # requetes sur la meme connexion sans attendre la reponse de la
        # precedente") : les 2 jambes etaient postees en SERIE ici (contrairement
        # au chemin bothside qui les parallelise deja depuis le 04/08) -- rien
        # n'empechait la jambe B d'attendre bêtement la reponse HTTP de la
        # jambe A avant meme de commencer a se signer. Meme technique
        # (ThreadPoolExecutor) que post_limit_pair_no_slippage. En mode calme,
        # une jambe sautee (perdant bradé) n'est PAS soumise : on la marque
        # dans le suivi comme "skipped" (ok=True, pas d'ordre) pour ne pas
        # fausser le "pose incomplete -> tout annule".
        _post = [
            (i, side, tid, prix[i])
            for i, (side, tid) in enumerate(zip(outcomes, token_ids))
            if prix[i] is not None
        ]
        for i, (side, tid) in enumerate(zip(outcomes, token_ids)):
            if prix[i] is None:
                pose["a" if i == 0 else "b"] = {
                    "side": side, "token_id": tid, "price": None,
                    "order_id": None, "ok": True, "skipped": True,
                }
        from concurrent.futures import ThreadPoolExecutor

        _chrono_t0 = time.time()
        with ThreadPoolExecutor(max_workers=2) as _ex_msf:
            _futs_msf = {
                i: _ex_msf.submit(self._live.post_limit_buy, tid, px, parts)
                for i, side, tid, px in _post
            }
            _results_msf = {i: f.result() for i, f in _futs_msf.items()}

        # ── RETRY DES REFUS TRANSITOIRES (Steven 13/08) ──────────────────
        # Observe en direct sur XRP 1786596600 : les DEUX jambes refusees
        # avec {'error': 'order manager not ready, please retry'}, donc
        # "pose incomplete -> tout annule" + 60s de cooldown. La fenetre
        # entiere est perdue alors que l'API demande explicitement de
        # RESSAYER : ce n'est pas un refus, c'est un "pas encore".
        # Le chemin agressif avait deja son RETRY-425 depuis le 24/07 ;
        # le chemin MSF, lui, n'en avait aucun.
        # On ne retente QUE sur les messages transitoires connus, une seule
        # fois, et seulement s'il reste assez de fenetre pour que la pose
        # ait un sens. Un refus de fond (taille, solde, marche ferme) tombe
        # exactement comme avant.
        # LISTE ELARGIE (Steven 13/08, 2e observation) : la 1re version ne
        # couvrait que "order manager not ready". Vu en direct sur
        # xrp-updown-5m-1786598700, les deux jambes sont tombees sur des
        # motifs DIFFERENTS et non couverts -- "Request exception!" d'un cote,
        # "order timed out" de l'autre -- avec pose_parallele=11423ms. Onze
        # secondes pour poster : ce n'est pas un refus metier, c'est le
        # reseau ou le CLOB qui rame, exactement le cas ou il faut ressayer.
        #
        # On ne liste QUE des defaillances de transport ou de disponibilite.
        # Un refus de fond -- solde insuffisant, taille invalide, marche
        # ferme, prix hors bornes -- ne contient aucun de ces motifs et
        # continue de tomber immediatement, sans retry.
        _MSG_TRANSITOIRES = (
            "not ready", "please retry", "try again", "425",
            "timed out", "timeout", "request exception", "connection",
            "temporarily", "unavailable", "502", "503", "504", "429",
        )
        _a_refaire = [
            (i, side, tid, px) for i, side, tid, px in _post
            if not _results_msf[i].get("success")
            and any(_m in str(_results_msf[i].get("error") or "").lower()
                    for _m in _MSG_TRANSITOIRES)
        ]
        if _a_refaire and (fin - time.time()) > _cancel_before_s(sym) + MSF_RETRY_POST_S:
            self._log(
                f"🔁 [MSF-RETRY] {sym} {slug} {len(_a_refaire)} jambe(s) refusee(s) "
                f"pour cause transitoire ('order manager not ready') -> nouvelle "
                f"tentative dans {MSF_RETRY_POST_S}s au lieu de perdre la fenetre"
            )
            time.sleep(MSF_RETRY_POST_S)
            with ThreadPoolExecutor(max_workers=2) as _ex_r:
                _futs_r = {
                    i: _ex_r.submit(self._live.post_limit_buy, tid, px, parts)
                    for i, side, tid, px in _a_refaire
                }
                for i, f in _futs_r.items():
                    _r2 = f.result()
                    if _r2.get("success"):
                        _results_msf[i] = _r2
                    else:
                        # on garde le refus d'origine pour le journal, mais on
                        # trace la 2e cause si elle differe
                        _c2 = str(_r2.get("error") or "")[:150]
                        self._tlog(
                            f"msf_retry_ko_{sym}",
                            f"⚠️ [MSF-RETRY] {sym} {slug} 2e tentative refusee "
                            f"aussi : {_c2}",
                        )
            _ok2 = sum(1 for i, *_ in _a_refaire if _results_msf[i].get("success"))
            if _ok2:
                self._log(
                    f"✅ [MSF-RETRY] {sym} {slug} {_ok2}/{len(_a_refaire)} jambe(s) "
                    f"acceptee(s) au 2e essai -- fenetre sauvee"
                )
        _chrono_t1 = time.time()
        _chrono_total_ms = round((_chrono_t1 - _chrono_t0) * 1000)
        _tim_parts = []
        for i, side, tid, px in _post:
            r = _results_msf[i]
            pose["a" if i == 0 else "b"] = {
                "side": side, "token_id": tid, "price": px,
                "order_id": r.get("order_id"), "ok": bool(r.get("success")),
            }
            _tim = r.get("timing") or {}
            _tim_parts.append(
                f"{side}:sig={_tim.get('signature_ms','?')}ms "
                f"rust={_tim.get('rust_resign_ms','?')}ms[{'RUST' if _tim.get('rust_used') else 'py'}] "
                f"post={_tim.get('post_orders_ms','?')}ms"
            )
            # HISTORIQUE STRUCTURE (meme table que /api/latency, une entree par
            # jambe pour rester dans le schema plat deja lu par les percentiles
            # p50/p95/p99 existants -- MSF vient enfin alimenter ces stats aussi).
            self.state.setdefault("latency_history", []).append({
                "ts": _chrono_t0, "symbol": sym, "strategy": "msf_entry",
                "signature_ms": _tim.get("signature_ms"),
                "rust_resign_ms": _tim.get("rust_resign_ms"),
                "rust_used": _tim.get("rust_used", False),
                "post_orders_ms": _tim.get("post_orders_ms"),
                "total_ms": _tim.get("total_ms"),
            })
            if not r.get("success"):
                _err = str(r.get("error") or "")
                _cause = _err.split("error_message=")[-1] if "error_message=" in _err else _err
                self._log(
                    f"⚠️ [MAKER-OUVERT] {sym} {slug} {side} @ {px:.3f} x{parts} "
                    f"refuse : {_cause[:300]}"
                )
        if len(self.state["latency_history"]) > 1000:
            del self.state["latency_history"][: len(self.state["latency_history"]) - 1000]
        self._log(
            f"⏱️ [CHRONO-MSF] {sym} {slug} pose_parallele={_chrono_total_ms}ms "
            f"({' | '.join(_tim_parts)})"
        )
        # BUG CRITIQUE CORRIGE (Steven 09/08, "je n'ai vu aucun tp sl") :
        # pose contient AUSSI pose["calm"] = bool -> iterer sur pose.values()
        # tombait dessus et faisait .get("ok") sur un booleen ->
        # "'bool' object has no attribute 'get'". Le crash arrivait APRES
        # l'envoi reel des ordres mais AVANT `st[slug] = pose` : les ordres
        # partaient chez Polymarket et le bot les OUBLIAIT instantanement.
        # Consequences observees en reel : aucune position suivie (donc ni TP
        # ni SL ne pouvaient tourner), et repose de nouveaux ordres toutes les
        # ~2s sur la meme fenetre (d'ou les doublons et les prix incoherents
        # type "0.35 face a 0.65", chaque cycle recalculant son prix).
        # On n'itere donc plus QUE sur les jambes reelles ("a"/"b"), jamais
        # sur les cles de metadonnees.
        _legs_pose = [pose[c] for c in ("a", "b") if isinstance(pose.get(c), dict)]
        if not all(v.get("ok") for v in _legs_pose):
            for v in _legs_pose:
                if v.get("order_id"):
                    self._live.cancel_order(v["order_id"])
            mk.setdefault("makeropen_cooldown", {})[slug] = now + 60
            self._log(f"⭕ [MAKER-OUVERT] {sym} {slug} pose incomplete -> tout annule")
            self._maker_open_record(sym, slug, "refuse", combine=comb, parts=0, prix=None)
            return
        pose["debut_ts"] = debut
        pose["fin_ts"] = fin
        st[slug] = pose
        self._save()

    # CORRELATION CROSS-SYMBOLE (Steven 19/08) : mesure cette nuit sur 5804
    # fenetres (2 periodes disjointes, 6 jours calendaires) -- quand plusieurs
    # symboles ouvrent une fenetre 5m au meme instant, leurs issues Up/Down
    # sont d'accord ~77% du temps (contre 50% si independantes), survit a
    # verification adversariale severe (bootstrap cluster, test de
    # permutation, split par periode, split par taille de groupe). 6 symboles
    # ouverts en meme temps ne sont donc PAS 6 paris independants -- plutot
    # ~1.6-1.7 "paris effectifs" (N_eff = 6/(1+5*rho), rho=2*taux-1=0.54).
    # CHIFFRE RETENU ICI = 2.0, PAS le 1.6-1.7 mesure : la mesure structurelle
    # est dominee a 85% par des groupes de 5-6 symboles co-ouverts, alors que
    # le regime qui compte en pratique (2-3 symboles ouverts en meme temps,
    # le seul observe jusqu'ici sur les vrais trades) n'a jamais ete mesure
    # directement -- verifie cette nuit lors d'une tentative de chiffrer un
    # plafond precis (27%), qui n'a PAS survecu a la verification pour cette
    # raison. 2.0 est le choix conservateur qui ne penalise PAS le cas k<=2
    # (le plus frequent en reel) tout en reduisant l'exposition simultanee
    # quand 3+ symboles s'alignent -- a affiner plus tard avec des donnees
    # reelles sur k>=3, pas a prendre comme un chiffre definitif.
    CORRELATION_N_EFF = 2.0

    def _maker_open_facteur_correlation(self, sym):
        """Reduit le budget d'une nouvelle position MSF quand d'autres
        symboles ont deja une tentative MSF active en meme temps -- le risque
        reel n'est pas divise par le nombre de symboles ouverts (ils bougent
        ensemble ~77% du temps), donc empiler des positions sur 4-6 symboles
        au meme instant n'est pas la diversification qu'elle parait etre."""
        try:
            autres_actifs = sum(
                1 for s in MAKER_OPEN_SYMBOLS
                if s != sym and self.state["markets"].get(s, {}).get("maker_open")
            )
        except Exception:
            return 1.0  # fail-open : jamais bloquant si l'etat est inattendu
        k = autres_actifs + 1  # +1 pour la tentative qu'on est en train de dimensionner
        return min(1.0, self.CORRELATION_N_EFF / k)

    def _maker_open_budget(self, sym=None):
        # BUG TROUVE EN LIVE (Steven 09/08, "je ne vois toujours aucun
        # mouvement") : MAKER_OPEN_TOTAL_FRAC=0.35 > MAX_MARKET_EXPOSURE_FRAC
        # =0.25 -> des que investable() depasse ~25$, ce budget demande
        # STRUCTURELLEMENT plus que ce que _exposure_ok() autorise, et
        # _manage_maker_open() se fait rejeter EN SILENCE (`if not ok_exp:
        # return`, aucun log) a CHAQUE cycle, peu importe le marche. Confirme
        # en live : a 35$ investable, budget=12.25$ > plafond expo=8.75$ ->
        # 0 pose possible depuis que le compte a depasse ~25$. Plafonner ici
        # au meme garde-fou qu'on devra de toute facon respecter plus bas.
        # Depuis le 13/08 on borne par le plafond PROPRE A MSF
        # (_maker_open_expo_max) et non plus par le plafond generique : ce
        # dernier valait max(8$, inv x 0.25), donc inferieur au budget vise
        # (inv x 0.35) des 23$ et POUR TOUJOURS au-dela de 32$. La ligne
        # ci-dessous reste indispensable -- c'est elle qui garantit qu'on ne
        # demande jamais plus que ce que _exposure_ok acceptera juste apres --
        # mais elle ne doit borner qu'au vrai plafond, pas a un plafond
        # etranger plus bas.
        _facteur_corr = self._maker_open_facteur_correlation(sym) if sym else 1.0
        return round(
            min(
                MAKER_OPEN_BUDGET_MAX,
                self._maker_open_expo_max(),
                max(MAKER_OPEN_BUDGET_MIN, self._investable() * MAKER_OPEN_TOTAL_FRAC),
            ) * _facteur_corr,
            2,
        )

    def _maker_open_record(self, sym, slug, issue, **kw):
        """Journal des tentatives, meme raison qu'en pre-ouverture : les echecs
        ne coutent rien, donc ils sont INVISIBLES dans le PnL. Le taux de
        remplissage -- la seule vraie inconnue de ce mecanisme -- ne peut se
        lire que dans ce journal."""
        h = self.state.setdefault("makeropen_hist", [])
        h.append({"ts": time.time(), "symbol": sym, "slug": slug, "issue": issue, **kw})
        if len(h) > 400:
            del h[: len(h) - 400]
        # AUTO-STOP MODE CALME (Steven 09/08) : le mode calme est une hypothese
        # a mesurer (l'historique interne a montre que "poursuivre le gagnant"
        # au-dessus de 0.35 perdait SANS le filtre danger). On cumule PnL + nb
        # de trades calmes reels, et on coupe le mode des que 20 trades ont
        # rendu un ROI negatif -- le bot retombe en croisement, le toggle
        # CALM_MSF_ENABLED restant lui-meme inchange.
        if kw.get("calm"):
            _net = None
            _comb = kw.get("combine")
            _prix = kw.get("prix")
            _vendu = kw.get("vendu")
            _sortie = kw.get("sortie")
            if issue == "les_deux" and _comb and kw.get("parts"):
                _net = kw["parts"] * (1 - _comb)
            elif _prix and _vendu and _sortie:
                _net = _vendu * (_sortie - _prix)
            if _net is not None:
                s = self.state.setdefault("calm_msf_stats", {"n": 0, "pnl": 0.0, "disabled": False})
                s["n"] += 1
                s["pnl"] = round(s["pnl"] + _net, 4)
                self._tlog(
                    f"makeropen_calm_stat",
                    f"📈 [MAKER-OUVERT-CALME] stat {s['n']}/{CALM_MSF_AUTOSTOP_N} "
                    f"trades -> pnl cumule {s['pnl']:+.2f}$",
                )
                if s["n"] >= CALM_MSF_AUTOSTOP_N and s["pnl"] < 0:
                    s["disabled"] = True
                    self._log(
                        f"⛔ [MAKER-OUVERT-CALME] auto-stop : {s['n']} trades, "
                        f"ROI < 0 (pnl {s['pnl']:+.2f}$) -> mode calme coupe, "
                        f"retour au mode croisement seul"
                    )

    def _manage_stagger(self, sym):
        """ARB DECALE, suite : complete la paire des que le verrou est
        atteignable, coupe si la jambe 1 decroche, et SOLDE si la fenetre se
        termine sans verrou -- jamais de jambe nue tenue jusqu'a zero (c'est
        ce qui a coute -81% de ROI sur les jambes nues gardees)."""
        mk = self.state["markets"][sym]
        if self.state["modes"].get(sym) != "real":
            return
        now = synced_now()
        for key, pos in list(mk["open"].items()):
            if pos.get("strat") != "stagger" or pos.get("mode") != "real":
                continue
            slug = pos.get("slug")
            secs_left = pos.get("end_ts", now) - now
            shares = pos.get("filled_shares", 0)
            if shares <= 0:
                del mk["open"][key]
                continue
            entry = pos.get("entry_price") or 0
            # ── 1) STOP-LOSS : la jambe 1 decroche franchement ──
            cur = self._live_price(pos.get("token_id"), None, pos.get("side"))
            if cur is not None and entry > 0 and (entry - cur) / entry >= STAGGER_STOP_LOSS:
                pos["strat"] = "orphan"
                pos["must_close"] = True
                self._log(
                    f"🛑 [ARB-DECALE] {sym} {slug} jambe1 {pos['side']} {entry:.3f}->{cur:.3f} "
                    f"(-{100 * (entry - cur) / entry:.0f}%) -> on coupe, pas d'attente"
                )
                continue
            # ── 2) LIMITE DE TEMPS ──
            # Mesure a jour (06/08, 109 paires completees) : mediane 9s, 92%
            # des completions arrivent dans les 60s. Passe ce delai le marche a
            # choisi son camp : il reste 33% de verrous seulement, autrement dit
            # les completions tardives ne sont plus des arbs mais de la moyenne
            # a la baisse sur une jambe perdante. Voir STAGGER_MAX_WAIT_S.
            _age = time.time() - (pos.get("opened_ts") or 0)
            if _age >= STAGGER_MAX_WAIT_S:
                pos["strat"] = "orphan"
                pos["must_close"] = True
                self._log(
                    f"⏱️ [ARB-DECALE] {sym} {slug} {pos['side']} pas de verrou apres "
                    f"{_age:.0f}s (92% des completions arrivent en <60s) "
                    f"-> on solde maintenant, tant que le prix tient"
                )
                continue
            # ── 2bis) FILET DE FIN DE FENETRE (secours) ──
            if secs_left <= STAGGER_GIVEUP_SECS:
                pos["strat"] = "orphan"
                pos["must_close"] = True
                self._log(
                    f"⌛ [ARB-DECALE] {sym} {slug} {pos['side']} verrou jamais atteint "
                    f"({secs_left:.0f}s restantes) -> on solde la jambe 1"
                )
                continue
            # ── 3) VERROU ATTEIGNABLE ? -> on complete en parts EGALES ──
            m = self._market_meta(slug)
            if not m:
                continue
            outcomes, token_ids = m
            other = [o for o in outcomes if o != pos.get("side")]
            if not other:
                continue
            other = other[0]
            otid = token_ids[outcomes.index(other)]
            book = self._live.get_book_sync(otid)
            oask = book["asks"][0][0] if book and book.get("asks") else None
            if oask is None:
                continue
            comb = entry + oask
            # VERROU CALCULE FRAIS INCLUS (Steven 06/08) : le combine seul ne
            # suffit pas -- a 0.98 la paire est PERDANTE une fois les frais
            # payes (mesure : cout reel = combine * 1.048 sur ce compte). On
            # exige donc un gain net positif, calcule jambe par jambe.
            need = round(shares, 2)
            net = self._pair_net_after_fees(entry, shares, oask, need)
            if net <= 0:
                self._tlog(
                    f"stagwait_{key}",
                    f"⏳ [ARB-DECALE] {sym} {slug} attente : {entry:.3f}+{oask:.3f}={comb:.3f} "
                    f"-> net APRES FRAIS {net:+.3f}$ (il faut > 0), {secs_left:.0f}s restantes",
                )
                continue
            cost = round(need * oask, 2)
            cash, _ = self._read_cash(max_age=0)
            if cash is None:
                continue
            if cost > max(0.0, cash - self.floor()):
                self._tlog(
                    f"stagcash_{key}",
                    f"⛔ [ARB-DECALE] {sym} {slug} verrou possible a {comb:.3f} mais "
                    f"capital insuffisant ({cost:.2f}$ requis)",
                )
                continue
            ok_exp, why_exp = self._exposure_ok(sym, mk, slug, cost)
            if not ok_exp:
                continue
            self._log(
                f"🔓 [ARB-DECALE] {sym} {slug} VERROU {entry:.3f}+{oask:.3f}={comb:.3f} "
                f"-> gain net APRES FRAIS {net:+.3f}$ | achat jambe2 {other} "
                f"{need} parts ({cost:.2f}$)"
            )
            with self._order_lock:
                res = self._live.snipe_buy_limit_exact(otid, oask, need)
            filled = res.get("filled_shares", 0.0)
            if filled <= 0:
                self._tlog(
                    f"stagfail_{key}",
                    f"⚠️ [ARB-DECALE] {sym} {slug} jambe2 non remplie ({res.get('error', '')}) "
                    f"-> on retentera au prochain cycle",
                )
                continue
            avg = res.get("avg_cost") or oask
            self._add_slug_spent(mk, slug, round(filled * avg, 2))
            mk["open"][f"{slug}|{other}"] = {
                "symbol": sym, "slug": slug, "side": other, "mode": "real",
                "strat": "bothside", "token_id": otid, "entry_price": avg,
                "filled_shares": filled, "cost": round(filled * avg, 2),
                "start_ts": pos.get("start_ts"), "pair": pos.get("pair"),
                "end_ts": pos.get("end_ts"), "opened_ts": time.time(), "buffer": 0.0,
            }
            pos["strat"] = "bothside"   # la paire existe : gestion normale reprend
            # verrou verifie sur les fills REELS, comme partout ailleurs
            self._tag_pair_lock(pos, mk["open"][f"{slug}|{other}"], comb,
                                tag=f" {sym} {slug} ARB-DECALE")

    def _market_meta(self, slug):
        """(outcomes, token_ids) d'un slug, via le cache marche partage."""
        from core.btc_updown import _fetch_one_market

        m = _fetch_one_market(slug)
        if not m:
            return None
        try:
            outcomes = json.loads(m.get("outcomes") or "[]")
            token_ids = json.loads(m.get("clobTokenIds") or "[]")
        except Exception:
            return None
        if len(outcomes) != 2 or len(token_ids) != 2:
            return None
        return outcomes, token_ids

    def _try_near_certain(self, sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
        """NEAR-CERTAIN : achat d'un cote quasi-acquis, tard dans la fenetre.

        Seule strategie directionnelle que l'historique valide (cf. les
        constantes NEARCERT_*) : 182 jambes a 0.95-0.98, 95% de reussite,
        +1.6% de ROI -- mesure SANS biais de survie. Les bandes voisines
        perdent (-16.6% en 0.90-0.95, -4.7% au-dessus de 0.98), la fenetre de
        prix est donc etroite par necessite, pas par prudence.

        L'edge est MINCE (+1.6%). Il ne survit que si on ne paie pas plus cher
        que la bande et qu'on ne le dilue pas : mise fixe, une seule tentative
        par fenetre, et confirmation Binance exigee en plus du prix."""
        if self._preopen_only(sym):
            return False   # symbole reserve a la pre-ouverture

        from core.btc_updown import _binance_price, _strike_at

        if not NEARCERT_ENABLED or mode != "real":
            return False
        now = synced_now()
        secs_left = p.get("end_ts", now) - now
        if not (NEARCERT_MIN_SECS <= secs_left <= NEARCERT_MAX_SECS):
            return False
        if mk.setdefault("nc_tried", {}).get(slug):
            return False
        if any(k.startswith(f"{slug}|") for k in mk["open"]):
            return False
        # le cote quasi-acquis = celui dont l'ask est dans la bande
        cand = None
        for side in outcomes:
            _, ask, _ = quotes.get(side, (None, None, None))
            if ask is not None and NEARCERT_MIN_PRICE <= ask <= NEARCERT_MAX_PRICE:
                cand = (side, ask)
                break
        if not cand:
            return False
        side, ask = cand
        # Binance doit CONFIRMER : le prix seul ne suffit pas, c'est ce qui
        # separe cette bande (95% de reussite) de la bande d'en dessous (72%).
        pair = p.get("pair")
        if pair:
            spot = _binance_price(pair)
            strike = _strike_at(pair, p.get("start_ts"), slug=slug)
            if spot is None or strike is None:
                return False
            gap = abs(spot - strike)
            if gap < spot * BINANCE_CONFIRM_MARGIN:
                return False
            if ("Up" if spot > strike else "Down") != side:
                self._tlog(
                    f"ncconflict_{sym}",
                    f"🌫️ [NEAR-CERTAIN] {sym} {slug} marche donne {side} @ {ask:.3f} "
                    f"mais Binance dit l'inverse -> on s'abstient",
                )
                return False
        cash, _ = self._read_cash(max_age=0)
        if cash is None:
            return False
        investable = max(0.0, cash - self.floor())
        budget = round(min(self._nearcert_budget(), investable), 2)
        budget = max(budget, round(MIN_SELL_SHARES * ask, 2))
        if budget > investable or budget < MIN_BUDGET_USD:
            return False
        ok_exp, why_exp = self._exposure_ok(sym, mk, slug, budget)
        if not ok_exp:
            return False
        mk["nc_tried"][slug] = time.time()
        tid = token_ids[outcomes.index(side)]
        self._log(
            f"🎯 [NEAR-CERTAIN] {sym} {slug} {side} @ {ask:.3f} budget={budget:.2f}$ "
            f"({secs_left:.0f}s restantes, Binance confirme) -> bande 0.95-0.98 "
            f"(95% de reussite mesuree sur 182 jambes)"
        )
        with self._order_lock:
            res = self._live.snipe_buy_market(tid, round(min(0.99, ask + 0.01), 2), budget)
        filled = res.get("filled_shares", 0.0)
        if filled <= 0:
            self._log(f"⚠️ [NEAR-CERTAIN] {sym} {slug} {side} non rempli ({res.get('error', '')})")
            return False
        avg = res.get("avg_cost") or ask
        self._add_slug_spent(mk, slug, round(filled * avg, 2))
        mk["open"][f"{slug}|{side}"] = {
            "symbol": sym, "slug": slug, "side": side, "mode": "real",
            "strat": "nearcert", "token_id": tid, "entry_price": avg,
            "filled_shares": filled, "cost": round(filled * avg, 2),
            "start_ts": p["start_ts"], "pair": pair, "end_ts": p["end_ts"],
            "opened_ts": time.time(), "buffer": 0.0,
        }
        self._log(
            f"✅ [NEAR-CERTAIN] {sym} {slug} {side} {filled} parts @ {avg:.3f} "
            f"({round(filled * avg, 2)}$) -> ouverte, TP/SL actifs"
        )
        return True

    def _try_twap_oracle(self, sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
        """TWAP-ORACLE (Steven 02/09) : voir le bloc de constantes TWAP_ORACLE_*
        pour la justification complete (backtest 12h reelles, 61/62). Un seul
        pari par fenetre, HOLD TO RESOLUTION -- strat="twap_oracle" n'est PAS
        dans la liste geree par _manage_pnl_tier_exits, donc jamais de TP/SL
        applique dessus une fois ouvert (voulu). PEUT acheter sous 0.50$,
        exception assumee a la regle generale (decidee avec Steven)."""
        if not TWAP_ORACLE_ENABLED or mode != "real":
            return False
        now = synced_now()
        secs_left = p.get("end_ts", now) - now
        # SUIVI DE L'ORDRE PASSIF (Steven 02/09) : DOIT passer AVANT le gate
        # "tried" ci-dessous -- sinon un ordre passif pose puis marque
        # "tried" n'est plus jamais reverifie/finalise les cycles suivants
        # (bug trouve en ecrivant ce fix : le gate aurait sorti la fonction
        # avant meme d'atteindre ce bloc).
        _pending = mk.setdefault("twap_oracle_pending", {})
        _pend = _pending.get(slug)
        if _pend:
            _held = self._live.position_size(_pend["token_id"])
            if _held and _held > 0.01:
                avg = _pend.get("price", 0.98)
                filled = round(_held, 2)
                self._add_slug_spent(mk, slug, round(filled * avg, 2))
                mk["open"][f"{slug}|{_pend['side']}"] = {
                    "symbol": sym, "slug": slug, "side": _pend["side"], "mode": "real",
                    "strat": "twap_oracle", "token_id": _pend["token_id"], "entry_price": avg,
                    "filled_shares": filled, "cost": round(filled * avg, 2),
                    "start_ts": p["start_ts"], "pair": p.get("pair"), "end_ts": p["end_ts"],
                    "opened_ts": time.time(), "buffer": 0.0, "hold_to_resolution": True,
                }
                del _pending[slug]
                self._log(
                    f"✅ [TWAP-ORACLE] {sym} {slug} {_pend['side']} {filled} parts @ {avg:.3f} "
                    f"(ordre passif rempli) -> ouverte, HOLD TO RESOLUTION (pas de TP/SL)"
                )
                return True
            if secs_left <= 3:
                try:
                    self._live.cancel_order(_pend["order_id"])
                except Exception:
                    pass
                del _pending[slug]
                self._log(
                    f"🔮 [TWAP-ORACLE] {sym} {slug} {_pend['side']} ordre passif jamais rempli "
                    f"({secs_left:.0f}s restantes) -> annule"
                )
            return False  # ordre deja en attente sur ce slug, rien d'autre a faire ce cycle
        if mk.setdefault("twap_oracle_tried", {}).get(slug):
            return False
        if not (TWAP_ORACLE_MIN_SECS_LEFT <= secs_left <= TWAP_ORACLE_MAX_SECS_LEFT):
            return False
        pair = p.get("pair")
        if not pair:
            return False
        from core.btc_updown import _strike_at
        strike = _strike_at(pair, p["start_ts"], slug=slug)
        if not strike:
            return False
        if not hasattr(self, "_ws"):
            return False
        if secs_left > 60:
            # REGIME PROBABILISTE (voir TWAP_ORACLE_PROB_THRESHOLD) : la
            # fenetre TWAP 60s n'a pas encore commence, la formule
            # "convergence" ne peut rien calculer -- mouvement brownien a
            # la place, deja valide par backtest (voir commentaire de la
            # constante).
            from core.btc_updown import probability_above_strike
            spot = self._ws.spot_price(pair)
            p_up = probability_above_strike(pair, spot, strike, secs_left) if spot else None
            if p_up is None:
                sig = None
            else:
                if p_up >= TWAP_ORACLE_PROB_THRESHOLD:
                    pred, certain = "Up", True
                elif p_up <= 1 - TWAP_ORACLE_PROB_THRESHOLD:
                    pred, certain = "Down", True
                else:
                    pred, certain = ("Up" if p_up >= 0.5 else "Down"), False
                sig = {"pred": pred, "certain": certain, "spot": spot, "x_req": strike,
                       "band": p_up, "sigma1": 0.0, "mode": "proba"}
        else:
            sig = self._ws.twap_oracle_signal(sym, strike, now, secs_left)
        # DIAGNOSTIC CONTINU (Steven 02/09, "plus de ligne de log a propos
        # de oracle") : jusqu'ici l'oracle ne loggait QUE quand il tirait --
        # aucune trace de son raisonnement les 95% du temps ou il observe
        # sans agir. Throttled a 1 ligne/2s par marche (comme FAV-DIAG) pour
        # ne pas noyer le journal. band=P(Up) en regime probabiliste (pas
        # un ecart de prix), x_req=strike dans ce regime (pas un prix requis).
        if sig:
            self._tlog(
                f"twapdiag_{sym}",
                f"🔮 [TWAP-ORACLE-DIAG] {sym} {slug} pred={sig['pred']} certain={sig['certain']} "
                f"x_req={sig['x_req']:.4f} spot={sig['spot']:.4f} strike={strike:.4f} "
                f"band={sig['band']:.4f} sigma1s={sig['sigma1']:.5f} {secs_left:.0f}s restantes",
                every=2.0,
            )
        else:
            self._tlog(
                f"twapdiag_{sym}",
                f"🔮 [TWAP-ORACLE-DIAG] {sym} {slug} pas assez de donnees (historique WS "
                f"insuffisant ou hors fenetre {secs_left:.0f}s restantes)",
                every=5.0,
            )
        # ESSAI "PARI PRECOCE" (Steven 03/09, "au moment ou il attend si
        # certain=true il peut deja placer 1$ ... essayons") : plutot que
        # d'attendre la confirmation statistique (certain=True), place un
        # tout petit 1$ des le tout PREMIER pred brut de la fenetre, meme
        # non confirme. strat DEDIEE ("twap_oracle_early") pour comparer ses
        # stats separement du pari 'certain' normal ci-dessous, qui continue
        # de tourner INCHANGE (les 2 peuvent coexister sur la meme fenetre).
        # Backtest 24h/6 marches AVANT deploiement : win rate du tout premier
        # pred brut vs celui du pred certain -- voir log de session pour le
        # resultat chiffre.
        if TWAP_ORACLE_EARLY_ENABLED and sig and mode == "real":
            _early_tried = mk.setdefault("twap_oracle_early_tried", {})
            _early_key = f"{slug}|{sig['pred']}"
            if _early_key not in _early_tried and sig["pred"] in outcomes:
                _early_side = sig["pred"]
                _early_open_key = f"{slug}|{_early_side}"
                # PAS DE PARI OPPOSE (Steven 03/09, "early pose 2 fois dans
                # les deux sens sur ETH") : si le pred flip en cours de
                # fenetre (~28% des cas, mesure) et qu'un early est DEJA
                # ouvert sur l'AUTRE cote de ce meme marche, ne pas en
                # ouvrir un 2e a contre-sens -- on tiendrait mecaniquement
                # une jambe perdante garantie (marche binaire, un seul cote
                # peut gagner).
                _opp_side = "Down" if _early_side == "Up" else "Up"
                _opp_open_key = f"{slug}|{_opp_side}"
                _opp_pos = mk["open"].get(_opp_open_key)
                if _opp_pos and _opp_pos.get("strat") == "twap_oracle_early":
                    self._tlog(
                        f"twap_early_opposite_{sym}",
                        f"🧪 [TWAP-ORACLE-EARLY-DIAG] {sym} {slug} {_early_side} pred flip mais un "
                        f"early {_opp_side} est deja ouvert sur ce marche -> pas de 2e pari a contre-sens",
                        every=10.0,
                    )
                elif _early_open_key not in mk["open"]:
                    _etid = token_ids[outcomes.index(_early_side)]
                    _e_ask = quotes.get(_early_side, (None, None, None))[1]
                    if _e_ask is not None and 0 < _e_ask < TWAP_ORACLE_EARLY_MIN_PRICE:
                        self._tlog(
                            f"twap_early_toolow_{sym}",
                            f"🧪 [TWAP-ORACLE-EARLY-DIAG] {sym} {slug} {_early_side} prix {_e_ask:.3f} "
                            f"< plancher {TWAP_ORACLE_EARLY_MIN_PRICE} -> trop de variance, skip "
                            f"(reessaie si le prix remonte)",
                            every=5.0,
                        )
                    elif _e_ask is not None and _e_ask < 1:
                        _e_budget = round(min(TWAP_ORACLE_EARLY_BET_USD, self._investable()), 2)
                        if _e_budget < MIN_BUDGET_USD:
                            self._tlog(
                                f"twap_early_budget_{sym}",
                                f"🧪 [TWAP-ORACLE-EARLY-DIAG] {sym} {slug} {_early_side} budget "
                                f"insuffisant ({_e_budget:.2f}$ < {MIN_BUDGET_USD}$) -> pas encore essaye",
                                every=5.0,
                            )
                        if _e_budget >= MIN_BUDGET_USD:
                            # marque "essaye" seulement ICI (Steven 03/09, bug trouve :
                            # marquer avant de savoir si l'ask/budget est valide
                            # desactivait l'essai pour le reste de la fenetre si le
                            # tout premier passage tombait sur un carnet pas encore
                            # pret -- observe en prod, une fenetre entiere sans
                            # aucun essai malgre un pred stable 30s+).
                            _early_tried[_early_key] = time.time()
                            with self._order_lock:
                                _e_res = self._live.snipe_buy_market(
                                    _etid, round(min(_e_ask + 0.05, 0.99), 2), _e_budget,
                                )
                            _e_filled = _e_res.get("filled_shares", 0.0)
                            if _e_filled <= 0:
                                # Steven 03/09, "j'ai pas vu ligne essai ni le
                                # trade associe" : avant, un fill rate ici ne
                                # laissait AUCUNE trace (log absent) et ne
                                # reessayait jamais (deja marque "essaye")
                                # -- silence total, impossible a diagnostiquer.
                                self._log(
                                    f"⚠️ [TWAP-ORACLE-EARLY] {sym} {slug} {_early_side} "
                                    f"essai non rempli @ {_e_ask:.3f} (err={_e_res.get('error', '')})"
                                )
                            if _e_filled > 0:
                                _e_avg = _e_res.get("avg_cost") or _e_ask
                                self._add_slug_spent(mk, slug, round(_e_filled * _e_avg, 2))
                                mk["open"][_early_open_key] = {
                                    "symbol": sym, "slug": slug, "side": _early_side, "mode": "real",
                                    "strat": "twap_oracle_early", "token_id": _etid, "entry_price": _e_avg,
                                    "filled_shares": _e_filled, "cost": round(_e_filled * _e_avg, 2),
                                    "start_ts": p["start_ts"], "pair": pair, "end_ts": p["end_ts"],
                                    "opened_ts": time.time(), "buffer": 0.0, "hold_to_resolution": True,
                                }
                                self._log(
                                    f"🧪 [TWAP-ORACLE-EARLY] {sym} {slug} {_early_side} "
                                    f"{_e_filled} parts @ {_e_avg:.3f} ({round(_e_filled * _e_avg, 2)}$) "
                                    f"certain={sig['certain']} {secs_left:.0f}s restantes -> ESSAI, hold to resolution"
                                )
                    else:
                        self._tlog(
                            f"twap_early_noask_{sym}",
                            f"🧪 [TWAP-ORACLE-EARLY-DIAG] {sym} {slug} {_early_side} pas d'ask "
                            f"disponible ce cycle (carnet={_e_ask}) -> reessaie au prochain cycle",
                            every=5.0,
                        )
                else:
                    self._tlog(
                        f"twap_early_open_{sym}",
                        f"🧪 [TWAP-ORACLE-EARLY-DIAG] {sym} {slug} {_early_side} position essai "
                        f"deja ouverte sur ce cote",
                        every=15.0,
                    )

        if not sig or not sig.get("certain"):
            return False
        side = sig["pred"]
        # VETO CHAINLINK (Steven 02/09, "en reel on constate quasiment QUE
        # des pertes") : trouve sur un vrai cas -- notre calcul (Binance)
        # disait Down avec 82% du band de confiance, mais l'ecart REEL final
        # (strike vs moyenne) n'etait que de 6$ sur 77460$ (0.008%), et
        # Polymarket resout sur le TWAP OFFICIEL Chainlink (voir description
        # de marche : "resolution source... Chainlink BTC/USD TWAP-60s"), pas
        # sur Binance -- un ecart aussi mince peut basculer selon la source.
        # self._ws.twap() lit ce MEME flux Chainlink officiel (RTDS, deja
        # cable ailleurs dans ce fichier) : si dispo et frais, on verifie que
        # la VRAIE source de resolution est d'accord avec nous avant de
        # parier, plutot que de ne se fier qu'a notre proxy Binance.
        _cl_twap60 = self._ws.twap(pair, window_s=60)
        if _cl_twap60 is not None:
            _cl_side = "Up" if _cl_twap60 >= strike else "Down"
            if _cl_side != side:
                self._tlog(
                    f"twaporacle_cl_veto_{sym}",
                    f"🚫 [TWAP-ORACLE] {sym} {slug} {side} rejete -> Chainlink TWAP60 "
                    f"({_cl_twap60:.4f} vs strike {strike:.4f}) donne {_cl_side}, en desaccord "
                    f"avec la source reelle de resolution",
                    every=5.0,
                )
                return False
        if side not in outcomes:
            return False
        key = f"{slug}|{side}"
        if key in mk["open"]:
            self._tlog(
                f"twaporacle_skip_pos_{sym}",
                f"🔮 [TWAP-ORACLE] {sym} {slug} {side} certain mais position deja ouverte sur ce cote -> skip",
                every=10.0,
            )
            return False
        tid = token_ids[outcomes.index(side)]

        # ORDRE PASSIF EN SECOURS (Steven 02/09, "l'oracle n'arrive plus a
        # acheter") : confirme sur des dizaines de cas reels -- l'oracle ne
        # tire que dans les toutes dernieres secondes, precisement quand un
        # contrat quasi-certain (0.01/0.99) n'a plus AUCUN vendeur au marche
        # (les acheteurs ont deja nettoye le carnet, personne ne cede un
        # quasi-gagnant). Le marche market echouait alors 100% du temps. Si
        # aucun ask n'est disponible, poste desormais un ordre LIMITE passif
        # (GTC) au lieu d'attendre un match instantane, et le laisse vivre
        # le temps restant de la fenetre (suivi/finalise en haut de
        # fonction, voir _pending).
        _, ask, _ = quotes.get(side, (None, None, None))
        if ask is None or ask <= 0 or ask >= 1:
            mk["twap_oracle_tried"][slug] = time.time()
            _passive_budget = round(min(_twap_oracle_bet_usd(0.98), self._investable()), 2)
            if _passive_budget < MIN_BUDGET_USD:
                self._tlog(
                    f"twaporacle_skip_ask_{sym}",
                    f"🔮 [TWAP-ORACLE] {sym} {slug} {side} certain, aucun ask, budget "
                    f"insuffisant pour un ordre passif -> skip",
                    every=5.0,
                )
                return False
            _passive_price = 0.98
            _passive_shares = round(_passive_budget / _passive_price, 2)
            res_p = self._live.post_limit_buy(tid, _passive_price, _passive_shares)
            if res_p.get("success") and res_p.get("order_id"):
                _pending[slug] = {
                    "side": side, "token_id": tid, "order_id": res_p["order_id"],
                    "price": _passive_price, "posted_ts": time.time(),
                }
                self._log(
                    f"🔮 [TWAP-ORACLE] {sym} {slug} {side} certain, aucun ask -> ordre PASSIF "
                    f"pose @ {_passive_price:.2f} ({_passive_shares} parts, {_passive_budget:.2f}$), "
                    f"{secs_left:.0f}s pour se remplir"
                )
            else:
                self._tlog(
                    f"twaporacle_skip_ask_{sym}",
                    f"🔮 [TWAP-ORACLE] {sym} {slug} {side} certain, aucun ask, ordre passif "
                    f"a echoue aussi (err={res_p.get('error', '')}) -> skip",
                    every=5.0,
                )
            return False
        mk["twap_oracle_tried"][slug] = time.time()
        investable = self._investable()
        budget = round(min(_twap_oracle_bet_usd(ask), investable), 2)
        if budget < MIN_BUDGET_USD:
            self._tlog(
                f"twaporacle_skip_budget_{sym}",
                f"🔮 [TWAP-ORACLE] {sym} {slug} {side} certain mais budget insuffisant "
                f"({budget:.2f}$ < {MIN_BUDGET_USD}$) -> skip",
                every=10.0,
            )
            return False
        self._log(
            f"🔮 [TWAP-ORACLE] {sym} {slug} {side} @ {ask:.3f} budget={budget:.2f}$ "
            f"x_req={sig['x_req']:.4f} spot={sig['spot']:.4f} strike={strike:.4f} "
            f"band={sig['band']:.4f} {secs_left:.0f}s restantes -> pari CERTAIN, hold to resolution"
        )
        with self._order_lock:
            res = self._live.snipe_buy_market(tid, round(min(ask + 0.05, 0.99), 2), budget)
        filled = res.get("filled_shares", 0.0)
        if filled <= 0:
            self._log(f"⚠️ [TWAP-ORACLE] {sym} {slug} {side} non rempli (err={res.get('error', '')})")
            return False
        avg = res.get("avg_cost") or ask
        # CORRECTION POST-REMPLISSAGE (Steven 02/09, "le sizing de l'oracle
        # part en steak") : confirme sur incident reel -- decision prise a
        # ask=0.84 (palier plein 5$, coherent, le contrat n'etait PAS
        # extreme a cet instant), mais le marche s'est effondre PENDANT
        # l'execution de l'ordre -> rempli a 0.05 (100 parts, 5$), un niveau
        # de prix qui aurait du declencher le palier 1$, pas 5$. Le palier
        # etait calcule sur le prix AVANT l'ordre, jamais revu sur le prix
        # REELLEMENT paye. Si le prix moyen reel tombe dans un palier
        # nettement plus bas, revend l'excedent tout de suite pour ramener
        # l'exposition a ce que ce palier autorise vraiment.
        _correct_budget = _twap_oracle_bet_usd(avg)
        _real_cost = round(filled * avg, 2)
        if _real_cost > _correct_budget * 1.15:
            _target_shares = round(_correct_budget / avg, 2) if avg > 0 else 0.0
            _excess = round(filled - _target_shares, 2)
            if _excess >= MIN_SELL_SHARES:
                _sold_ex = self._sell_orphan(
                    tid, _excess, f" {sym} {slug} {side} TWAP-ORACLE-RESIZE",
                    entry_price=avg, symbol=sym, slug=slug, side=side,
                )
                if _sold_ex > 0:
                    self._log(
                        f"🔮 [TWAP-ORACLE] {sym} {slug} {side} rempli a {avg:.3f} "
                        f"(palier reel {_correct_budget:.2f}$ != decide {budget:.2f}$) "
                        f"-> excedent {_sold_ex} parts revendu pour recaler l'exposition"
                    )
                    filled = round(filled - _sold_ex, 2)
        self._add_slug_spent(mk, slug, round(filled * avg, 2))
        mk["open"][key] = {
            "symbol": sym, "slug": slug, "side": side, "mode": "real",
            "strat": "twap_oracle", "token_id": tid, "entry_price": avg,
            "filled_shares": filled, "cost": round(filled * avg, 2),
            "start_ts": p["start_ts"], "pair": pair, "end_ts": p["end_ts"],
            "opened_ts": time.time(), "buffer": 0.0, "hold_to_resolution": True,
        }
        self._log(
            f"✅ [TWAP-ORACLE] {sym} {slug} {side} {filled} parts @ {avg:.3f} "
            f"({round(filled * avg, 2)}$) -> ouverte, HOLD TO RESOLUTION (pas de TP/SL)"
        )
        return True

    def _manage_oracle_trailing(self, sym):
        """VERROU DE PROFIT sur la VALEUR pour les positions TWAP-ORACLE
        (Steven 02/09) : voir TWAP_ORACLE_TRAIL_* pour la justification.
        Ne touche QUE strat="twap_oracle" -- les autres restent gerees par
        _manage_pnl_tier_exits, aucun chevauchement possible (cette derniere
        ignore deja explicitement "twap_oracle" dans sa liste de strats)."""
        mk = self.state["markets"][sym]
        for key, pos in list(mk["open"].items()):
            if pos.get("strat") not in ("twap_oracle", "twap_oracle_early") or pos.get("mode") != "real":
                continue
            entry = pos.get("entry_price", 0)
            shares = pos.get("filled_shares", 0)
            if entry <= 0 or shares <= 0:
                continue
            # Steven 02/09 ("elle aurait pu capturer du benefice mais il ne
            # s'est rien passe") : sur ces contrats a 1c, le bid disparait
            # souvent quelques secondes (carnet fin) MEME quand le prix a
            # reellement pique -- l'ancien code utilisait _get_bid() (bid
            # strict) pour TOUT, et un simple `continue` sur bid absent
            # geleait le pic memorise, ratant des pics reels (confirme : un
            # bid a 0.29 vu sur l'appli Polymarket n'a jamais ete lu ici,
            # price_log reste bloque a 0.010 tout du long). On separe
            # desormais : le PIC est mesure au mieux dispo (mid/ask via
            # _live_price, jamais bloquant), la VENTE reste conditionnee a
            # un vrai bid executable (_get_bid).
            live_px = self._live_price(pos.get("token_id"), None, pos.get("side"))
            if live_px is not None:
                live_pct = (live_px - entry) / entry
                peak = pos.get("_oracle_trail_peak", 0.0)
                if live_pct > peak:
                    pos["_oracle_trail_peak"] = peak = live_pct
            peak = pos.get("_oracle_trail_peak", 0.0)

            # FLIP IMMEDIAT sur pari precoce <=42c (Steven 03/09, "si under
            # 5c [releve a 42c] on achete et on vend immediatement tout de
            # suite apres") : plus agressif que le TP precoce general -- des
            # le premier tick de gain reel, vend TOUT (pas 75%), pas
            # d'attente de 10% (qui ratait des positions parties trop vite,
            # cf entree 0.230 pic 0.255 jamais vendu, retombee a 0.010).
            if (TWAP_ORACLE_EARLY_TP_ENABLED
                    and pos.get("strat") == "twap_oracle_early" and not pos.get("_early_tp_done")
                    and entry <= TWAP_ORACLE_EARLY_FLIP_MAX_ENTRY):
                _cb = self._get_bid(pos)
                if _cb is not None and _cb > entry:
                    _sold = self._sell_orphan(
                        pos["token_id"], shares,
                        f" {sym} {pos['slug']} {pos['side']} TWAP-ORACLE-EARLY-FLIP",
                        entry_price=entry, symbol=sym, slug=pos.get("slug"), side=pos.get("side"),
                    )
                    if _sold > 0:
                        _realized = round(_sold * (_cb - entry), 3)
                        pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + _realized, 3)
                        pos["filled_shares"] = shares = round(shares - _sold, 2)
                        pos["_early_tp_done"] = True
                        self._log(
                            f"⚡ [TWAP-ORACLE-EARLY-FLIP] {sym} {pos['slug']} {pos['side']} "
                            f"vend {_sold} @ {_cb:.3f} (entree {entry:.3f}) "
                            f"realise={_realized:+.3f}$ reste={shares}"
                        )
                        if shares < MIN_SELL_SHARES:
                            pnl = pos["realized_pnl"]
                            pos.update(win=pnl > 0, pnl=pnl, resolved_by="oracle_early_flip", exit_price=round(_cb, 3))
                            mk["trades"].append(pos)
                            del mk["open"][key]
                            self._record_trade_pnl(sym, pnl)
                        continue

            # PALIERS "TICKET 1c" (voir TWAP_ORACLE_CHEAP_* ci-dessus) : prend
            # la moitie des parts RESTANTES a chaque palier de gain franchi,
            # tant que l'entree est un ticket bon marche. Verifie sur le VRAI
            # bid (pas le pic estime) -- on ne vend jamais dans une cotation
            # fantome.
            # TP DEDIE AUX PARIS "PRECOCES" (Steven 03/09, "surtout les early
            # est un TP ... cette ligne aurait declenche TP immediatement,
            # laisser le reste courir") : contrairement au pari 'certain'
            # (99%+ win rate mesure, vraiment hold-to-resolution), le pari
            # precoce n'a que ~81% de win rate (backteste) -- moins de raison
            # d'accepter le risque plein d'un retournement total. Des le
            # premier gain reel constate, on securise une grosse partie des
            # parts en UNE fois, le reliquat continue jusqu'a resolution
            # (ou le verrou/paliers ci-dessous s'il grimpe encore).
            if TWAP_ORACLE_EARLY_TP_ENABLED and pos.get("strat") == "twap_oracle_early" and not pos.get("_early_tp_done"):
                if peak >= TWAP_ORACLE_EARLY_TP_ARM_PCT:
                    _cb = self._get_bid(pos)
                    if _cb is not None and (_cb - entry) / entry >= TWAP_ORACLE_EARLY_TP_ARM_PCT:
                        _sell_n = round(shares * TWAP_ORACLE_EARLY_TP_SELL_FRACTION, 2)
                        if _sell_n >= MIN_SELL_SHARES:
                            _sold = self._sell_orphan(
                                pos["token_id"], _sell_n,
                                f" {sym} {pos['slug']} {pos['side']} TWAP-ORACLE-EARLY-TP",
                                entry_price=entry, symbol=sym, slug=pos.get("slug"), side=pos.get("side"),
                            )
                            if _sold > 0:
                                _realized = round(_sold * (_cb - entry), 3)
                                pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + _realized, 3)
                                pos["filled_shares"] = shares = round(shares - _sold, 2)
                                pos["_early_tp_done"] = True
                                self._log(
                                    f"💰 [TWAP-ORACLE-EARLY-TP] {sym} {pos['slug']} {pos['side']} "
                                    f"+{(_cb - entry) / entry:.0%} vend {_sold} @ {_cb:.3f} "
                                    f"(entree {entry:.3f}) realise={_realized:+.3f}$ reste={shares} "
                                    f"(continue jusqu'a resolution)"
                                )
                                if shares < MIN_SELL_SHARES:
                                    pnl = pos["realized_pnl"]
                                    pos.update(win=pnl > 0, pnl=pnl, resolved_by="oracle_early_tp", exit_price=round(_cb, 3))
                                    mk["trades"].append(pos)
                                    del mk["open"][key]
                                    self._record_trade_pnl(sym, pnl)
                                continue

            if entry <= TWAP_ORACLE_CHEAP_ENTRY_MAX:
                stage = pos.get("_oracle_cheap_tp_stage", 0)
                if stage < len(TWAP_ORACLE_CHEAP_TP_TARGETS) and peak >= TWAP_ORACLE_CHEAP_TP_TARGETS[stage]:
                    _cb = self._get_bid(pos)
                    if _cb is not None and (_cb - entry) / entry >= TWAP_ORACLE_CHEAP_TP_TARGETS[stage]:
                        _sell_n = round(shares / 2, 2)
                        if _sell_n >= MIN_SELL_SHARES:
                            _sold = self._sell_orphan(
                                pos["token_id"], _sell_n,
                                f" {sym} {pos['slug']} {pos['side']} TWAP-ORACLE-CHEAP-TP{stage + 1}",
                                entry_price=entry, symbol=sym, slug=pos.get("slug"), side=pos.get("side"),
                            )
                            if _sold > 0:
                                _realized = round(_sold * (_cb - entry), 3)
                                pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + _realized, 3)
                                pos["filled_shares"] = shares = round(shares - _sold, 2)
                                pos["_oracle_cheap_tp_stage"] = stage + 1
                                self._log(
                                    f"💰 [TWAP-ORACLE-CHEAP-TP{stage + 1}] {sym} {pos['slug']} {pos['side']} "
                                    f"+{TWAP_ORACLE_CHEAP_TP_TARGETS[stage] * 100:.0f}% vend {_sold} @ {_cb:.3f} "
                                    f"(entree {entry:.3f}) realise={_realized:+.3f}$ reste={shares}"
                                )
                                if shares < MIN_SELL_SHARES:
                                    pnl = pos["realized_pnl"]
                                    pos.update(win=pnl > 0, pnl=pnl, resolved_by="oracle_cheap_tp", exit_price=round(_cb, 3))
                                    mk["trades"].append(pos)
                                    del mk["open"][key]
                                    self._record_trade_pnl(sym, pnl)
                                continue  # reevalue le verrou de trail au prochain cycle

            if peak < TWAP_ORACLE_TRAIL_ARM_PCT:
                continue  # pas encore assez de gain pour armer le verrou
            cur_bid = self._get_bid(pos)
            if cur_bid is None:
                continue  # arme mais pas de bid executable ce cycle -> reessaie au prochain
            pct = (cur_bid - entry) / entry
            if pct > peak * (1 - TWAP_ORACLE_TRAIL_GIVEBACK_PCT):
                continue  # pas encore assez retrace depuis le pic
            sold = self._sell_orphan(
                pos["token_id"], shares,
                f" {sym} {pos['slug']} {pos['side']} TWAP-ORACLE-TRAIL",
                entry_price=entry, symbol=sym, slug=pos.get("slug"), side=pos.get("side"),
            )
            if sold <= 0:
                continue
            realized = round(sold * (cur_bid - entry), 3)
            pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
            pos["filled_shares"] = round(shares - sold, 2)
            if pos["filled_shares"] < MIN_SELL_SHARES:
                pnl = pos["realized_pnl"]
                pos.update(win=pnl > 0, pnl=pnl, resolved_by="oracle_trail", exit_price=round(cur_bid, 3))
                mk["trades"].append(pos)
                del mk["open"][key]
                self._record_trade_pnl(sym, pnl)
                self._log(
                    f"🔒 [TWAP-ORACLE-TRAIL] {sym} {pos['slug']} {pos['side']} pic={peak:+.0%} "
                    f"-> verrouille @ {cur_bid:.3f} (entree {entry:.3f}) pnl={pnl:+.3f}$"
                )

    def _oracle_agrees(self, sym, p, side):
        """Avis de l'oracle (regime dual, MEME logique que _try_twap_oracle)
        sur `sym` a l'instant present, pour verifier l'accord avec le signal
        Steven Engine -- lecture seule, ne pose AUCUN ordre, n'a pas besoin
        que TWAP_ORACLE_ENABLED soit actif. Retourne True/False (avis donne,
        d'accord ou pas) ou None (pas assez de donnees -> ne pas veto)."""
        try:
            now = synced_now()
            secs_left = p.get("end_ts", now) - now
            pair = p.get("pair")
            if not pair or not hasattr(self, "_ws"):
                return None
            from core.btc_updown import _strike_at
            strike = _strike_at(pair, p["start_ts"], slug=p.get("slug"))
            if not strike:
                return None
            if secs_left > 60:
                from core.btc_updown import probability_above_strike
                spot = self._ws.spot_price(pair)
                if not spot:
                    return None
                p_up = probability_above_strike(pair, spot, strike, secs_left)
                if p_up is None:
                    return None
                pred = "Up" if p_up >= 0.5 else "Down"
            else:
                sig = self._ws.twap_oracle_signal(sym, strike, now, secs_left)
                if not sig:
                    return None
                pred = sig.get("pred")
                if not pred:
                    return None
            return pred == side
        except Exception:
            return None

    def _manage_steven_engine(self, by_sym):
        """Detection + gestion du 'Steven Engine' (voir STEVEN_* ci-dessus).
        Cross-symbole par construction : lit les 6 marches ensemble a CHAQUE
        cycle plutot que d'etre appelee par symbole comme le reste du bot.
        Reglages LUS DEPUIS LE DASHBOARD (steven_config()), les constantes
        STEVEN_* du haut de fichier ne servent que de valeurs par defaut."""
        cfg = self.steven_config()
        if not cfg["enabled"]:
            return
        now = synced_now()
        from core.btc_updown import _binance_price, _strike_at

        # ── 1) lit le MOUVEMENT DE PRIX REEL (Binance, spot vs strike du
        # debut de fenetre) des 6 marches -- Steven 03/09, "tu as mal compris,
        # le signal c'est que les prix des cryptos bougent ensemble (correles
        # 73-85%), pas le prix du contrat de prediction". Le prix du contrat
        # (bande 0.47-0.62) n'intervient qu'a l'EXECUTION plus bas, jamais
        # dans la detection du traineur.
        # Steven 03/09 ("il doit overide les reglages generaux du bot") : PAS
        # de filtre sur self.state["modes"] ici -- ce moteur lit et trade les
        # 6 marches independamment du on/off par symbole des autres
        # strategies (le fetch en amont a deja ete etendu pour les 6, voir
        # _loop()). Seul DISABLED_SYMBOLS (desactivation mesuree, pas un
        # simple on/off manuel) reste respecte.
        moves = {}   # sym -> {"m","p","outcomes","token_ids","slug","movement"}
        for sym in STEVEN_SYMBOLS:
            if sym in DISABLED_SYMBOLS:
                continue
            cands = [mp for mp in by_sym.get(sym, []) if mp[1]["end_ts"] > now]
            if not cands:
                continue
            m, p = cands[0]
            try:
                outcomes = json.loads(m.get("outcomes") or "[]")
                token_ids = json.loads(m.get("clobTokenIds") or "[]")
            except Exception:
                continue
            if len(outcomes) != 2 or len(token_ids) != 2 or "Up" not in outcomes:
                continue
            pair = p.get("pair")
            if not pair:
                continue
            strike = _strike_at(pair, p["start_ts"], slug=m.get("slug"))
            spot = _binance_price(pair)
            if not strike or not spot:
                continue
            moves[sym] = {
                "m": m, "p": p, "outcomes": outcomes, "token_ids": token_ids,
                "slug": m.get("slug"), "movement": (spot - strike) / strike,
            }

        # ── 2) gere les positions DEJA ouvertes (DCA + stop-loss) AVANT toute
        # nouvelle entree -- le risque sur le capital deja engage prime. ──
        n_open = 0
        total_cost = 0.0
        for sym in STEVEN_SYMBOLS:
            mk = self.state["markets"].get(sym)
            if not mk:
                continue
            for key, pos in list(mk["open"].items()):
                if pos.get("strat") != "steven_engine" or pos.get("mode") != "real":
                    continue
                n_open += 1
                total_cost += pos.get("cost", 0.0)
                tid = pos["token_id"]
                bid, ask, mid = self._book_quote(tid)
                cur = bid if bid is not None else mid
                if cur is None:
                    continue
                entry = pos.get("entry_price", 0)
                shares = pos.get("filled_shares", 0)
                if entry <= 0 or shares <= 0:
                    continue
                # STOP-LOSS ABSOLU (Steven engine a un vrai SL, contrairement a
                # l'oracle). Desactivable (Steven 03/09, "faut couper le stop
                # loss") : 0.0 = jamais de coupe, hold to resolution comme
                # l'oracle sur cette position -- risque de -100% assume.
                if cfg["stoploss_price"] > 0 and cur <= cfg["stoploss_price"]:
                    sold = self._sell_orphan(
                        tid, shares, f" {sym} {pos['slug']} {pos['side']} STEVEN-SL",
                        entry_price=entry, symbol=sym, slug=pos.get("slug"), side=pos.get("side"),
                    )
                    if sold > 0:
                        realized = round(sold * (cur - entry), 3)
                        pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
                        pos["filled_shares"] = round(shares - sold, 2)
                        if pos["filled_shares"] < MIN_SELL_SHARES:
                            pnl = pos["realized_pnl"]
                            pos.update(win=pnl > 0, pnl=pnl, resolved_by="steven_sl", exit_price=round(cur, 3))
                            mk["trades"].append(pos)
                            del mk["open"][key]
                            self._record_trade_pnl(sym, pnl)
                            n_open -= 1
                            total_cost -= pos.get("cost", 0.0)
                            self._log(
                                f"🛑 [STEVEN-SL] {sym} {pos['slug']} {pos['side']} "
                                f"@ {cur:.3f} (entree {entry:.3f}) pnl={pnl:+.3f}$"
                            )
                    continue
                # DCA (moyenne a la baisse) : comportement pilote par
                # dca_mode -- "off" = jamais, "capped" = 1 seul palier,
                # "on_confirm" = ne renforce que si le consensus tient
                # TOUJOURS ce cycle-ci, "standard" = 2 paliers inconditionnels.
                stage = pos.get("dca_stage", 0)
                drop = entry - cur
                dca_mode = cfg.get("dca_mode", "standard")
                if dca_mode == "off":
                    add_usd = 0.0
                elif dca_mode == "capped":
                    add_usd = cfg["dca1_add_usd"] if (stage == 0 and drop >= cfg["dca_trigger_drop"]) else 0.0
                else:
                    if stage == 0 and drop >= cfg["dca_trigger_drop"]:
                        add_usd = cfg["dca1_add_usd"]
                    elif stage == 1 and drop >= cfg["dca_trigger_drop"] * 2:
                        add_usd = cfg["dca2_add_usd"]
                    else:
                        add_usd = 0.0
                    if add_usd > 0 and dca_mode == "on_confirm":
                        _mv = moves.get(sym)
                        _still_agrees = _mv is not None and (
                            (pos["side"] == "Up" and _mv["movement"] >= cfg["move_epsilon"])
                            or (pos["side"] == "Down" and _mv["movement"] <= -cfg["move_epsilon"])
                        )
                        if not _still_agrees:
                            self._tlog(
                                f"steven_dca_skip_{sym}",
                                f"🌟 [STEVEN-ENGINE] {sym} {pos['slug']} {pos['side']} DCA saute "
                                f"(on_confirm : le consensus ne tient plus sur cet actif)",
                                every=30.0,
                            )
                            add_usd = 0.0
                if add_usd > 0 and pos.get("cost", 0.0) + add_usd <= cfg["max_per_trade_usd"]:
                    _ask_now = ask if ask is not None else cur
                    if _ask_now and 0 < _ask_now < 1 and self._investable() >= add_usd:
                        with self._order_lock:
                            _dca_res = self._live.snipe_buy_market(
                                tid, round(min(_ask_now + 0.05, 0.99), 2), add_usd,
                            )
                        _dca_filled = _dca_res.get("filled_shares", 0.0)
                        if _dca_filled > 0:
                            _dca_avg = _dca_res.get("avg_cost") or _ask_now
                            new_cost = round(pos["cost"] + _dca_filled * _dca_avg, 2)
                            new_shares = round(shares + _dca_filled, 2)
                            pos["entry_price"] = round(new_cost / new_shares, 4) if new_shares > 0 else entry
                            pos["filled_shares"] = new_shares
                            pos["cost"] = new_cost
                            pos["dca_stage"] = stage + 1
                            self._add_slug_spent(mk, pos["slug"], round(_dca_filled * _dca_avg, 2))
                            self._log(
                                f"➕ [STEVEN-DCA{stage + 1}] {sym} {pos['slug']} {pos['side']} "
                                f"+{_dca_filled} parts @ {_dca_avg:.3f} (+{add_usd}$) "
                                f"-> nouvelle entree moy. {pos['entry_price']:.3f}"
                            )

        # ── 3) cherche une NOUVELLE entree (consensus de MOUVEMENT + traineur) ──
        # DIAGNOSTIC CONTINU (Steven 03/09, "j'aimerais bien avoir des logs
        # meme quand il trade pas, pour comprendre pourquoi") : meme principe
        # que TWAP-ORACLE-DIAG -- une ligne throttled a CHAQUE raison de ne
        # pas trader, pas seulement quand un trade part.
        def _diag(msg):
            self._tlog(
                "steven_diag",
                f"🌟 [STEVEN-ENGINE-DIAG] [{n_open}/{cfg['max_concurrent']} pos, "
                f"{total_cost:.2f}$/{cfg['bankroll_usd']}$] {msg}",
                every=5.0,
            )

        # HEURES COUPEES (UTC) : ne bloque QUE les nouvelles entrees, jamais
        # la gestion (DCA/SL) des positions deja ouvertes ci-dessus.
        import datetime as _dt
        _cur_hour_utc = _dt.datetime.fromtimestamp(now, tz=_dt.timezone.utc).hour
        if _cur_hour_utc in (cfg.get("skip_hours") or []):
            _diag(f"heure {_cur_hour_utc}h UTC coupee dans les reglages -> pas de nouvelle entree")
            self.state.pop("steven_pending_signal", None)
            return
        if n_open >= cfg["max_concurrent"] or total_cost >= cfg["bankroll_usd"]:
            _diag("limite de positions/bankroll atteinte -> pas de nouvelle entree")
            self.state.pop("steven_pending_signal", None)
            return
        if len(moves) < cfg["min_assets_agreeing"]:
            _diag(f"seulement {len(moves)}/6 marches lisibles ce cycle "
                  f"(min requis {cfg['min_assets_agreeing']}) -- symboles vus: {sorted(moves.keys())}")
            self.state.pop("steven_pending_signal", None)
            return

        up_count = sum(1 for v in moves.values() if v["movement"] >= cfg["move_epsilon"])
        down_count = sum(1 for v in moves.values() if v["movement"] <= -cfg["move_epsilon"])
        if up_count >= cfg["min_assets_agreeing"] and cfg["allow_up"]:
            majority_side = "Up"
        elif down_count >= cfg["min_assets_agreeing"] and cfg["allow_down"]:
            majority_side = "Down"
        else:
            _mv_str = " ".join(f"{s}={v['movement']:+.3%}" for s, v in sorted(moves.items()))
            _diag(f"pas de consensus (besoin {cfg['min_assets_agreeing']}/6) : "
                  f"up={up_count} down={down_count} | {_mv_str}")
            self.state.pop("steven_pending_signal", None)
            return

        # INVERSION SUR SERIE (optionnel) : si les N derniers paris places par
        # CE moteur ont tous ete du meme cote, on prend le cote OPPOSE pour
        # celui-ci -- parie sur un retour a l'equilibre plutot que de suivre
        # une serie deja longue.
        if cfg.get("streak_reversal_enabled"):
            _hist = self.state.setdefault("steven_side_history", [])
            _n = int(cfg.get("streak_reversal_n", 7))
            if len(_hist) >= _n and all(h == majority_side for h in _hist[-_n:]):
                _flipped = "Down" if majority_side == "Up" else "Up"
                if (_flipped == "Up" and cfg["allow_up"]) or (_flipped == "Down" and cfg["allow_down"]):
                    self._log(
                        f"🔁 [STEVEN-ENGINE] serie de {_n} paris {majority_side} -> inversion, "
                        f"prochain pari en {_flipped}"
                    )
                    majority_side = _flipped
                else:
                    _diag(f"inversion sur serie voudrait parier {_flipped} mais ce sens est desactive -> skip")
                    return

        # mouvement de CET actif dans le sens majoritaire (positif = confirme
        # le mouvement, negatif = est parti a contre-sens)
        def signed_move(v):
            return v["movement"] if majority_side == "Up" else -v["movement"]

        agreeing = {s: v for s, v in moves.items() if signed_move(v) >= cfg["move_epsilon"]}
        if len(agreeing) < cfg["min_assets_agreeing"]:
            _diag(f"consensus {majority_side} initial mais retombe a {len(agreeing)} "
                  f"actifs en le recalculant -> skip")
            return
        avg_move = sum(signed_move(v) for v in agreeing.values()) / len(agreeing)
        if avg_move <= 0:
            _diag(f"consensus {majority_side} mais mouvement moyen non positif ({avg_move:+.3%}) -> skip")
            return

        # le traineur : parmi TOUS les actifs (y compris ceux pas encore
        # "agreeing"), celui qui a le plus de retard EN PROPORTION du
        # mouvement moyen du groupe (gap_ratio=1 -> n'a pas bouge du tout,
        # gap_ratio=0 -> a deja suivi autant que la moyenne).
        # SELECTION MULTI-TRAINEURS (Steven 03/09, "qu'il ne vise pas qu'un
        # seul traineur, poster sur les 2-3-4 marches en meme temps selon
        # bankroll dispo") : au lieu de n'executer QUE sur le meilleur gap,
        # on prend jusqu'a multi_laggard_max traineurs (les plus en retard
        # d'abord). Si AUCUN ne franchit le seuil laggard_gap mais que le
        # consensus tient quand meme (avg_move>0 deja verifie ci-dessus), on
        # parie en fallback sur les plus a la traine (gap>0, meme sous le
        # seuil) plutot que de laisser passer le cycle sans rien faire.
        _excluded = set(cfg.get("excluded_symbols") or [])
        _check_side = majority_side
        if cfg.get("reverse_mode"):
            _check_side = "Down" if majority_side == "Up" else "Up"
        _all_gaps = {}
        for s, v in moves.items():
            if s in _excluded:
                continue
            # verifie le cote REELLEMENT parie (apres reverse eventuel),
            # sinon le dedoublonnage checke le mauvais cote en mode reverse.
            key_check = f"{v['slug']}|{_check_side}"
            mk_s = self.state["markets"].get(s)
            if mk_s and key_check in mk_s["open"]:
                continue
            _all_gaps[s] = (avg_move - signed_move(v)) / avg_move
        _sorted_gaps = sorted(_all_gaps.items(), key=lambda kv: -kv[1])
        _max_n = max(1, int(cfg.get("multi_laggard_max", 3)))
        selected = [s for s, g in _sorted_gaps if g >= cfg["laggard_gap"]][:_max_n]
        if not selected and cfg.get("multi_laggard_fallback", True):
            selected = [s for s, g in _sorted_gaps if g > 0][:_max_n]
        if not selected:
            _gap_str = " ".join(f"{s}={g:.2f}" for s, g in _sorted_gaps)
            _diag(f"consensus {majority_side} ({len(agreeing)} actifs, avg_move={avg_move:+.3%}) "
                  f"mais aucun traineur exploitable : {_gap_str}")
            self.state.pop("steven_pending_signal", None)
            return
        best_sym, best_gap = selected[0], _all_gaps[selected[0]]

        # CONFIRMATION (Steven 03/09, "ca ne devrait pas arriver, enquete
        # sur la solution" -- un retournement juste apres l'entree) :
        # backteste sur 24h reelles -- exiger que le MEME signal (cote +
        # traineur) tienne pendant confirmation_secs avant d'executer
        # ameliore le win rate (62%->67%) et le pnl/trade (+1.56$->+2.42$),
        # au prix d'un peu moins de volume. Un signal qui change de cote ou
        # de traineur entre-temps repart a zero (pas de confirmation
        # "partielle").
        _confirm_s = cfg.get("confirmation_secs", 0)
        if _confirm_s > 0:
            # Steven 03/09 ("le compte se remet a 0 en plein milieu alors
            # que ca va dans la meme direction") : confirme sur le TERRAIN,
            # pas sur le traineur exact -- le traineur precis change souvent
            # de symbole (SOL->BTC->BNB) meme quand le consensus Up/Down
            # reste stable, ce qui reinitialisait le compteur a tort. On ne
            # confirme desormais que la DIRECTION (majority_side) ; le
            # traineur execute est celui detecte au moment ou la
            # confirmation aboutit (le plus a jour), pas celui du premier
            # instant.
            _pending = self.state.get("steven_pending_signal")
            if not _pending or _pending.get("side") != majority_side:
                self.state["steven_pending_signal"] = {"side": majority_side, "first_seen": now}
                _diag(f"traineur {best_sym} {majority_side} (gap={best_gap:.2f}) detecte, "
                      f"en attente de confirmation ({_confirm_s:.0f}s)")
                return
            elapsed = now - _pending["first_seen"]
            if elapsed < _confirm_s:
                _diag(f"traineur {best_sym} {majority_side} (gap={best_gap:.2f}) toujours en attente "
                      f"de confirmation ({elapsed:.0f}/{_confirm_s:.0f}s)")
                return
            # confirme -- efface l'etat en attente, on execute ci-dessous
            self.state.pop("steven_pending_signal", None)

        # EXECUTION SUR CHAQUE TRAINEUR SELECTIONNE (jusqu'a multi_laggard_max
        # marches simultanes, 1 seul appel _live.snipe_buy_market par
        # traineur -- chacun a son propre budget/bande/veto, et total_cost/
        # n_open sont mis a jour a CHAQUE achat pour ne jamais depasser la
        # bankroll ni max_concurrent en cours de boucle).
        for best_sym in selected:
            if n_open >= cfg["max_concurrent"]:
                _diag(f"multi-traineur : max_concurrent ({cfg['max_concurrent']:.0f}) atteint, "
                      f"{len(selected) - selected.index(best_sym)} traineur(s) restants ignores ce cycle")
                break
            best_gap = _all_gaps[best_sym]
            v = moves[best_sym]
            mk = self.state["markets"][best_sym]
            # VETO ORACLE<->STEVEN ENGINE (Steven 03/09, "ca serait interessant
            # qu'elles se coordonnent" + "continue, que manque-t-il pour devenir
            # SOTA") : backteste sur 24h reelles (287 fenetres) -- quand l'oracle
            # (convergence TWAP sur ce meme actif) est en DESACCORD avec le
            # signal Steven Engine, ces trades sont NETS NEGATIFS (-1.31$/trade,
            # n=8) alors que l'accord est positif (+0.30$/trade, n=152). Veto
            # simple bat le "1/2 mise en desaccord" sur le meme echantillon
            # (+45.56$ vs +40.30$ vs +35.04$ actuel). Ne se declenche QUE si
            # l'oracle a un avis (avis absent = pas de veto, on garde le trade).
            if cfg.get("oracle_veto_enabled", True):
                _ov = self._oracle_agrees(best_sym, v["p"], majority_side)
                if _ov is False:
                    _diag(f"traineur {best_sym} {majority_side} (gap={best_gap:.2f}) VETO : "
                          f"oracle en desaccord sur ce meme actif -> skip")
                    continue
            # REVERSE ENGINE (Steven 03/09, "ca s'est passe toute la soiree, un
            # manque a gagner de 90e... un filtre post achat qui pose Down au
            # lieu de Up et inversement") : verifie sur 10 vrais trades recents
            # (verite Binance independante) -- pnl reel -14.40$ vs +16.95$ si
            # inverse, meme mise, meme prix. Toute la logique de DETECTION
            # (consensus, traineur, gap, confirmation) reste INCHANGEE ; seul le
            # cote EXECUTE a l'achat est flip. Togglable (reverse_mode=False
            # coupe instantanement, revient au comportement normal).
            _exec_side = majority_side
            if cfg.get("reverse_mode"):
                _exec_side = "Down" if majority_side == "Up" else "Up"
            tid = v["token_ids"][v["outcomes"].index(_exec_side)]
            _, ask, _ = self._book_quote(tid)
            if ask is None or ask <= 0 or ask >= 1:
                _diag(f"traineur {best_sym} {majority_side}{'->reverse '+_exec_side if cfg.get('reverse_mode') else ''} "
                      f"(gap={best_gap:.2f}) trouve mais ask indisponible -> skip")
                continue
            # BANDE D'ACHAT (filtre d'EXECUTION du traineur DEJA identifie par le
            # mouvement de prix ci-dessus, PAS le signal lui-meme) : n'achete que
            # si son propre marche de prediction n'est pas deja extreme -- payer
            # 90c laisse trop peu de marge de gain, un prix a 5c signifie que le
            # marche lui-meme ne croit pas du tout au rattrapage.
            if not (cfg["buy_min_price"] <= ask <= cfg["buy_max_price"]):
                _diag(f"traineur {best_sym} {majority_side}{'->reverse '+_exec_side if cfg.get('reverse_mode') else ''} "
                      f"(gap={best_gap:.2f}) trouve mais prix {ask:.3f} "
                      f"hors bande [{cfg['buy_min_price']:.2f}, {cfg['buy_max_price']:.2f}] -> skip")
                continue
            # BANDE A EVITER (optionnelle) : meme si dans la bande d'achat
            # normale, un sous-intervalle peut etre exclu explicitement.
            _avoid_max = cfg.get("avoid_max_price", 0.0)
            if _avoid_max > 0 and cfg.get("avoid_min_price", 0.0) <= ask <= _avoid_max:
                _diag(f"traineur {best_sym} {majority_side} @ {ask:.3f} tombe dans la bande a eviter "
                      f"[{cfg.get('avoid_min_price', 0.0):.2f}, {_avoid_max:.2f}] -> skip")
                continue
            # MISE PROPORTIONNELLE AU GAP (Steven 03/09, "optimiser d'avantage")
            # : plus le traineur a de retard sur la moyenne du groupe, plus le
            # signal historique est fort -- on scale la mise de base en
            # consequence (borne a size_scale_max pour limiter le risque).
            _stake_usd = cfg["initial_buy_usd"]
            if cfg.get("size_scale_by_gap", True):
                _scale = min(cfg.get("size_scale_max", 2.0), 1 + best_gap)
                _stake_usd = round(cfg["initial_buy_usd"] * _scale, 2)
            budget = round(min(_stake_usd, self._investable(), cfg["bankroll_usd"] - total_cost), 2)
            if budget < MIN_BUDGET_USD:
                _diag(f"traineur {best_sym} {majority_side} @ {ask:.3f} valide mais budget insuffisant "
                      f"({budget:.2f}$ < {MIN_BUDGET_USD}$)")
                continue
            with self._order_lock:
                res = self._live.snipe_buy_market(tid, round(min(ask + 0.05, 0.99), 2), budget)
            filled = res.get("filled_shares", 0.0)
            if filled <= 0:
                self._log(f"⚠️ [STEVEN-ENGINE] {best_sym} {v['slug']} {_exec_side} non rempli (err={res.get('error', '')})")
                continue
            avg = res.get("avg_cost") or ask
            self._add_slug_spent(mk, v["slug"], round(filled * avg, 2))
            key = f"{v['slug']}|{_exec_side}"
            mk["open"][key] = {
                "symbol": best_sym, "slug": v["slug"], "side": _exec_side, "mode": "real",
                "strat": "steven_engine", "token_id": tid, "entry_price": avg,
                "filled_shares": filled, "cost": round(filled * avg, 2),
                "start_ts": v["p"]["start_ts"], "pair": v["p"].get("pair"), "end_ts": v["p"]["end_ts"],
                "opened_ts": time.time(), "buffer": 0.0, "dca_stage": 0,
            }
            n_open += 1
            total_cost += round(filled * avg, 2)
            _hist = self.state.setdefault("steven_side_history", [])
            _hist.append(_exec_side)  # traque le cote REELLEMENT parie (pour l'inversion sur serie)
            del _hist[:-50]  # ne garde que les 50 derniers, largement assez pour streak_reversal_n<=30
            # DETAIL DU CONSENSUS (Steven 03/09, "on voit pas de ligne qui dit
            # clairement qu'il y a eu un consensus") : meme richesse que la
            # ligne "pas de consensus" (mouvement de CHAQUE actif), pas juste le
            # compte agrege -- pour voir d'un coup d'oeil qui a vote quoi.
            _mv_detail = " ".join(f"{s}={v2['movement']:+.3%}" for s, v2 in sorted(moves.items()))
            _rev_note = f" [REVERSE : signal={majority_side} -> parie {_exec_side}]" if cfg.get("reverse_mode") else ""
            _multi_note = f" [multi {selected.index(best_sym)+1}/{len(selected)}]" if len(selected) > 1 else ""
            self._log(
                f"🌟 [STEVEN-ENGINE] {best_sym} {v['slug']} {_exec_side} "
                f"{filled} parts @ {avg:.3f} ({round(filled * avg, 2)}$) "
                f"gap_ratio={best_gap:.2f} consensus={len(agreeing)}/{len(moves)} avg_move={avg_move:+.4%} "
                f"| {_mv_detail}{_rev_note}{_multi_note} -> ouverte, hold to resolution"
            )

    def _try_favorite(self, sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
        """PARI DIRECTIONNEL SUR LE FAVORI (Steven 05/08, demande explicite).

        Appele UNIQUEMENT quand l'arb a ete declare impossible sur cette
        fenetre (FIRST-LEG-BLOCKED). Mise petite, assumee directionnelle,
        strat="fav" -> jamais is_risk_free, donc toujours geree en TP/SL.

        Honnetete sur l'esperance : l'historique corrige donne -2% de ROI sur
        la tranche 0.70-0.80 et -20% sur 0.80-0.90. Ce qui est NOUVEAU ici et
        n'a jamais ete teste, c'est le filtre Binance strict a l'entree
        (0.25% d'ecart au strike, pas juste "devant"). C'est une hypothese a
        mesurer, pas un edge etabli -- d'ou la taille volontairement petite."""
        if self._preopen_only(sym):
            return False   # symbole reserve a la pre-ouverture

        from core.btc_updown import _binance_price, _strike_at

        if not FAV_ENABLED:
            return False
        if mode != "real":
            return False
        now = synced_now()
        secs_left = p.get("end_ts", now) - now
        if not (FAV_MIN_SECS <= secs_left <= FAV_MAX_SECS):
            return False
        # une seule tentative FAV par fenetre, et jamais si on tient deja qqch
        if mk.setdefault("fav_tried", {}).get(slug):
            return False
        if any(k.startswith(f"{slug}|") for k in mk["open"]):
            return False
        # LE MARCHE SEUL DECIDE (Steven 01/09, "il voit down favoris mais
        # l'achete pas !!!!!! c'est inacceptable" -- Binance exigeait un
        # ecart de 0.25% au strike meme quand le marche montrait Down a
        # 84-87%, sans ambiguite aucune). Meme regle que partout ailleurs
        # ce soir : le cote au-dessus de FAV_MIN_PRICE est le favori, plus
        # aucun role pour Binance dans cette decision.
        pair = p.get("pair")
        _fav_prices2 = [(s, a) for s, (_, a, _) in zip(outcomes, [quotes.get(s, (None, None, None)) for s in outcomes]) if a is not None]
        fav_side = None
        if len(_fav_prices2) == 2:
            fav_side = max(_fav_prices2, key=lambda x: x[1])[0]
        if fav_side is None or fav_side not in outcomes:
            return False
        tid = token_ids[outcomes.index(fav_side)]
        _, ask, _ = quotes.get(fav_side, (None, None, None))
        if ask is None:
            return False
        if not (FAV_STRATEGY_MIN_PRICE <= ask <= FAV_STRATEGY_MAX_PRICE):
            self._tlog(
                f"favpx_{sym}",
                f"🌫️ [FAV] {sym} {slug} {fav_side} @ {ask:.3f} hors bande "
                f"[{FAV_STRATEGY_MIN_PRICE}, {FAV_STRATEGY_MAX_PRICE}] -> pas de pari directionnel "
                f"(reste proche de 50c pour garder de la marge de TP)",
            )
            return False
        cash, _ = self._read_cash(max_age=0)
        if cash is None:
            return False
        investable = self._partitioned_investable()
        budget = round(min(FAV_BUDGET_USD, investable), 2)
        # plancher vendable : jamais une position qu'on ne pourra pas sortir
        budget = max(budget, round(MIN_SELL_SHARES * ask, 2))
        if budget > investable or budget < MIN_BUDGET_USD:
            return False
        ok_exp, why_exp = self._exposure_ok(sym, mk, slug, budget)
        if not ok_exp:
            self._tlog(f"favexp_{sym}", f"⛔ [FAV] {sym} {slug} refuse : {why_exp}")
            return False
        mk["fav_tried"][slug] = time.time()
        self._log(
            f"🎯 [FAV] {sym} {slug} {fav_side} @ {ask:.3f} budget={budget:.2f}$ "
            f"-- arb impossible, marche tranche NET, "
            f"{secs_left:.0f}s restantes -> pari directionnel assume, gere en TP/SL"
        )
        with self._order_lock:
            res = self._live.snipe_buy_market(tid, round(ask + 0.02, 2), budget)
        filled = res.get("filled_shares", 0.0)
        if filled <= 0:
            self._log(f"⚠️ [FAV] {sym} {slug} {fav_side} non rempli (err={res.get('error', '')})")
            return False
        avg = res.get("avg_cost") or ask
        self._add_slug_spent(mk, slug, round(filled * avg, 2))
        mk["open"][f"{slug}|{fav_side}"] = {
            "symbol": sym, "slug": slug, "side": fav_side, "mode": "real",
            # strat "fav" : suivi par _manage_pnl_tier_exits comme une position
            # normale (TP par paliers + SL + trailing). JAMAIS is_risk_free.
            "strat": "fav", "token_id": tid, "entry_price": avg,
            "filled_shares": filled, "cost": round(filled * avg, 2),
            "start_ts": p["start_ts"], "pair": pair, "end_ts": p["end_ts"],
            "opened_ts": time.time(), "buffer": 0.0,
        }
        self._log(
            f"✅ [FAV] {sym} {slug} {fav_side} {filled} parts @ {avg:.3f} "
            f"({round(filled * avg, 2)}$) -> ouverte, TP/SL actifs"
        )
        return True

    def _try_overreaction(self, sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
        """SNIPE OVER-REACTION (Steven 19/08, "le carnet se vide temporairement
        a cause de la panique, achete au fond du trou"). Compare la chute du
        prix Polymarket sur OVERREACT_LOOKBACK_S a la chute REELLE Binance sur
        la meme fenetre -- si Polymarket a chute >= OVERREACT_MULT fois plus
        que la realite, on achete le cote qui a sur-reagi en pariant sur le
        retour a la moyenne. Non backteste avec des donnees propres -- petite
        mise, geree en TP/SL normal comme fav/nearcert."""
        if not OVERREACT_ENABLED or mode != "real":
            return False
        if self._preopen_only(sym):
            return False
        from core.btc_updown import momentum as _momentum

        now = synced_now()
        secs_left = p.get("end_ts", now) - now
        if not (OVERREACT_MIN_SECS <= secs_left <= OVERREACT_MAX_SECS):
            return False
        if mk.setdefault("overreact_tried", {}).get(slug):
            return False
        if any(k.startswith(f"{slug}|") for k in mk["open"]):
            return False
        ticks = mk.get("price_log", {}).get(slug, [])
        if len(ticks) < 2:
            return False
        t_now = ticks[-1]
        t_ref = None
        for t in reversed(ticks):
            if t_now["ts"] - t["ts"] >= OVERREACT_LOOKBACK_S:
                t_ref = t
                break
        if t_ref is None:
            return False
        best_side, best_drop = None, 0.0
        for side in outcomes:
            a_ref, a_now = t_ref.get(f"{side}_ask"), t_now.get(f"{side}_ask")
            if a_ref is None or a_now is None or a_ref <= 0:
                continue
            drop = (a_ref - a_now) / a_ref
            if drop > best_drop:
                best_drop, best_side = drop, side
        if best_side is None or best_drop < OVERREACT_MIN_POLY_DROP:
            return False
        pair = p.get("pair")
        mom = _momentum(pair) if pair else None
        binance_move_pct = abs(mom["slow_pct_s"]) * OVERREACT_LOOKBACK_S / 100 if mom else 0.0
        if binance_move_pct <= 0 or best_drop < OVERREACT_MULT * binance_move_pct:
            self._tlog(
                f"overreact_weak_{sym}",
                f"🌫️ [OVERREACT] {sym} {slug} {best_side} chute {100*best_drop:.1f}% "
                f"vs Binance {100*binance_move_pct:.2f}% -> pas assez disproportionne",
            )
            return False
        tid = token_ids[outcomes.index(best_side)]
        _, ask, _ = quotes.get(best_side, (None, None, None))
        if ask is None:
            return False
        cash, _ = self._read_cash(max_age=0)
        if cash is None:
            return False
        investable = self._partitioned_investable()
        budget = round(min(OVERREACT_BUDGET_USD, investable), 2)
        budget = max(budget, round(MIN_SELL_SHARES * ask, 2))
        if budget > investable or budget < MIN_BUDGET_USD:
            return False
        ok_exp, why_exp = self._exposure_ok(sym, mk, slug, budget)
        if not ok_exp:
            self._tlog(f"overreactexp_{sym}", f"⛔ [OVERREACT] {sym} {slug} refuse : {why_exp}")
            return False
        mk["overreact_tried"][slug] = time.time()
        self._log(
            f"🕳️ [OVERREACT] {sym} {slug} {best_side} @ {ask:.3f} budget={budget:.2f}$ "
            f"-- chute Polymarket {100*best_drop:.1f}% vs Binance {100*binance_move_pct:.2f}% "
            f"-> snipe du retour a la moyenne"
        )
        with self._order_lock:
            res = self._live.snipe_buy_market(tid, round(ask + 0.02, 2), budget)
        filled = res.get("filled_shares", 0.0)
        if filled <= 0:
            self._log(f"⚠️ [OVERREACT] {sym} {slug} {best_side} non rempli (err={res.get('error', '')})")
            return False
        avg = res.get("avg_cost") or ask
        self._add_slug_spent(mk, slug, round(filled * avg, 2))
        mk["open"][f"{slug}|{best_side}"] = {
            "symbol": sym, "slug": slug, "side": best_side, "mode": "real",
            "strat": "overreact", "token_id": tid, "entry_price": avg,
            "filled_shares": filled, "cost": round(filled * avg, 2),
            "start_ts": p["start_ts"], "pair": pair, "end_ts": p["end_ts"],
            "opened_ts": time.time(), "buffer": 0.0,
        }
        self._log(
            f"✅ [OVERREACT] {sym} {slug} {best_side} {filled} parts @ {avg:.3f} "
            f"-> ouverte, TP/SL actifs"
        )
        return True

    def _try_twap_lock(self, sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
        """CALCULATEUR TWAP (Steven 19/08). La resolution reelle utilise le
        TWAP Chainlink sur les 30 DERNIERES secondes de la fenetre vs le prix
        de depart. On accumule les echantillons Binance de cette fenetre de
        30s ; une fois assez de temps ecoule, on calcule les DEUX bornes
        (pire mouvement bas et pire mouvement haut plausibles sur le temps
        restant) -- si les deux bornes tombent du meme cote du prix de
        depart, c'est mathematiquement tranche : on charge le cote gagnant
        avec la quasi-totalite du capital.
        Approximation Binance spot du TWAP Chainlink -- pas la donnee exacte
        de resolution, d'ou la marge de securite TWAP_MAX_MOVE_PCT_S."""
        if not TWAP_LOCK_ENABLED or mode != "real":
            return False
        from core.btc_updown import _binance_price, _strike_at

        now = synced_now()
        secs_left = p.get("end_ts", now) - now
        if not (0 < secs_left <= TWAP_WINDOW_S):
            return False
        pair = p.get("pair")
        if not pair:
            return False
        spot = _binance_price(pair)
        strike = _strike_at(pair, p.get("start_ts"), slug=slug)
        if spot is None or strike is None:
            return False

        buf = mk.setdefault("twap_buf", {}).setdefault(slug, [])
        buf.append((now, spot))
        # ne garder que les echantillons DANS la fenetre TWAP de 30s
        cutoff = p["end_ts"] - TWAP_WINDOW_S
        while buf and buf[0][0] < cutoff:
            buf.pop(0)

        elapsed = TWAP_WINDOW_S - secs_left
        if elapsed < TWAP_MIN_ELAPSED_S or len(buf) < 3:
            return False
        if mk.setdefault("twap_lock_tried", {}).get(slug):
            return False
        if any(k.startswith(f"{slug}|") for k in mk["open"]):
            return False

        avg_so_far = sum(px for _, px in buf) / len(buf)
        # projection : le reste de la fenetre au pire cas (hausse extreme ou
        # baisse extreme plausible), pondere par le temps restant / 30s.
        move = spot * TWAP_MAX_MOVE_PCT_S * secs_left
        proj_haut = (avg_so_far * elapsed + (spot + move) * secs_left) / TWAP_WINDOW_S
        proj_bas = (avg_so_far * elapsed + (spot - move) * secs_left) / TWAP_WINDOW_S

        cote_haut = proj_haut >= strike
        cote_bas = proj_bas >= strike
        if cote_haut != cote_bas:
            # les deux bornes ne s'accordent pas -> pas encore tranche
            return False
        winning_side = "Up" if cote_haut else "Down"
        if winning_side not in outcomes:
            return False
        tid = token_ids[outcomes.index(winning_side)]
        _, ask, _ = quotes.get(winning_side, (None, None, None))
        if ask is None or ask > TWAP_LOCK_MAX_PRICE:
            return False

        cash, _ = self._read_cash(max_age=0)
        if cash is None:
            return False
        investable = self._partitioned_investable()
        budget = round(min(investable * TWAP_LOCK_BUDGET_FRAC, investable), 2)
        budget = max(budget, round(MIN_SELL_SHARES * ask, 2))
        if budget > investable or budget < MIN_BUDGET_USD:
            return False
        ok_exp, why_exp = self._exposure_ok(sym, mk, slug, budget)
        if not ok_exp:
            self._tlog(f"twaplockexp_{sym}", f"⛔ [TWAP-LOCK] {sym} {slug} refuse : {why_exp}")
            return False

        mk["twap_lock_tried"][slug] = time.time()
        self._log(
            f"🔒 [TWAP-LOCK] {sym} {slug} {winning_side} @ {ask:.3f} budget={budget:.2f}$ "
            f"-- TWAP {elapsed:.0f}/{TWAP_WINDOW_S}s ecoules, moy={avg_so_far:.2f} "
            f"vs strike={strike:.2f}, projections [{proj_bas:.2f}, {proj_haut:.2f}] "
            f"toutes deux {'>=' if cote_haut else '<'} strike -> issue mathematiquement tranchee"
        )
        with self._order_lock:
            res = self._live.snipe_buy_market(tid, round(ask + 0.02, 2), budget)
        filled = res.get("filled_shares", 0.0)
        if filled <= 0:
            self._log(f"⚠️ [TWAP-LOCK] {sym} {slug} {winning_side} non rempli (err={res.get('error', '')})")
            return False
        avg = res.get("avg_cost") or ask
        self._add_slug_spent(mk, slug, round(filled * avg, 2))
        mk["open"][f"{slug}|{winning_side}"] = {
            "symbol": sym, "slug": slug, "side": winning_side, "mode": "real",
            "strat": "twaplock", "token_id": tid, "entry_price": avg,
            "filled_shares": filled, "cost": round(filled * avg, 2),
            "start_ts": p["start_ts"], "pair": pair, "end_ts": p["end_ts"],
            "opened_ts": time.time(), "buffer": 0.0,
        }
        self._log(
            f"✅ [TWAP-LOCK] {sym} {slug} {winning_side} {filled} parts @ {avg:.3f} "
            f"-> ouverte, issue quasi-certaine"
        )
        return True

    def _try_split_maker(self, sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
        """MARKET MAKING PAR SPLIT (Steven 19/08, analyse du wallet
        0x6748...ee08, $35.6k splits/2.5h, quasi break-even -> mecanisme
        viable, pas magique). Au lieu d'acheter chaque jambe separement au
        carnet (risque de jambe manquante, slippage), on convertit
        ATOMIQUEMENT du USDC en 1 part de CHAQUE issue via CTF.splitPosition
        -- cout exact, jamais de jambe orpheline. On pose ensuite un ordre
        de vente passif sur les DEUX cotes (capture du spread, meme logique
        que SPREAD_CAPTURE_PRICE), et le TP instantane universel reste le
        filet de secours si le prix ne coopere pas."""
        if not SPLIT_MAKER_ENABLED or mode != "real":
            return False
        if self._preopen_only(sym):
            return False
        now = synced_now()
        secs_left = p.get("end_ts", now) - now
        if secs_left < SPLIT_MAKER_MIN_SECS:
            return False
        if mk.setdefault("split_tried", {}).get(slug):
            return False
        if any(k.startswith(f"{slug}|") for k in mk["open"]):
            return False
        cond = m.get("conditionId")
        if not cond:
            return False
        investable = self._partitioned_investable()
        budget = round(min(SPLIT_MAKER_BUDGET_USD, investable), 2)
        if budget < SPLIT_MAKER_MIN_BUDGET_USD or budget > investable:
            return False
        ok_exp, why_exp = self._exposure_ok(sym, mk, slug, budget)
        if not ok_exp:
            self._tlog(f"splitexp_{sym}", f"⛔ [SPLIT-MAKER] {sym} {slug} refuse : {why_exp}")
            return False

        mk["split_tried"][slug] = time.time()
        self._log(f"🔀 [SPLIT-MAKER] {sym} {slug} split de {budget:.2f}$ -> 1 part de chaque issue en cours...")
        res = self._live.split_position(cond, budget)
        if not res.get("success"):
            self._log(f"⚠️ [SPLIT-MAKER] {sym} {slug} split echoue : {res.get('error', '')}")
            return False

        shares = budget  # 1 part de chaque issue par $ de collateral
        for side, tid in zip(outcomes, token_ids):
            key = f"{slug}|{side}"
            self._add_slug_spent(mk, slug, round(budget / 2, 2))
            mk["open"][key] = {
                "symbol": sym, "slug": slug, "side": side, "mode": "real",
                "strat": "splitpair", "token_id": tid, "entry_price": 0.50,
                "filled_shares": shares, "cost": round(budget / 2, 2),
                "start_ts": p["start_ts"], "pair": p.get("pair"), "end_ts": p["end_ts"],
                "opened_ts": time.time(), "buffer": 0.0, "is_risk_free": False,
                "_spread_sell_posted": False,
            }
        self._log(
            f"✅ [SPLIT-MAKER] {sym} {slug} split reussi (tx={res.get('tx_hash', '')[:12]}...) "
            f"-> {shares} parts Up + {shares} parts Down, TP/spread-capture actifs"
        )
        return True

    def _manage_reinforce(self, sym):
        """RENFORT DE LA JAMBE GAGNANTE (Steven 05/08).

        Declenche UNIQUEMENT apres qu'un stop-loss ait deja coupe sur cette
        fenetre : a ce moment la position n'est plus une paire couverte mais
        un pari directionnel assume (toute coupe reduit min(parts), donc le
        payout du pire cas -- le verrou est deja entame). La decision devient
        alors une pure question d'esperance, et la mesure sur l'historique
        montre que la bande 0.50-0.80 est nettement gagnante (cf. les
        constantes REINFORCE_*). Au-dela de 0.80 l'esperance devient negative,
        d'ou le plafond dur.

        Toutes les conditions doivent tenir : SL deja declenche sur la
        fenetre, jambe NON verrouillee, Binance qui confirme le sens au-dela
        du bruit, prix dans la bande +EV, fenetre temporelle, et les plafonds
        habituels (exposition par marche, plancher de cash)."""
        from core.btc_updown import _binance_price, _strike_at

        if not REINFORCE_ENABLED:
            return
        mk = self.state["markets"][sym]
        if self.state["modes"].get(sym) != "real":
            return
        now = synced_now()
        for key, pos in list(mk["open"].items()):
            slug = pos.get("slug")
            # 1) un SL doit avoir deja coupe sur CETTE fenetre
            if not mk.get("sl_fired", {}).get(slug):
                continue
            # 2) jamais toucher a une paire reellement verrouillee
            if pos.get("is_risk_free") or pos.get("must_close"):
                continue
            if pos.get("mode") != "real" or pos.get("filled_shares", 0) <= 0:
                continue
            # un seul renfort par jambe
            if pos.get("reinforced"):
                continue
            # 3) fenetre temporelle : assez de temps pour ressortir, pas trop
            secs_left = pos.get("end_ts", now) - now
            if not (REINFORCE_MIN_SECS <= secs_left <= REINFORCE_MAX_SECS):
                continue
            # 4) prix dans la bande d'esperance positive
            book = self._live.get_book_sync(pos["token_id"])
            ask = book["asks"][0][0] if book and book.get("asks") else None
            if ask is None or not (REINFORCE_MIN_PRICE <= ask <= REINFORCE_MAX_PRICE):
                continue
            # 5) Binance doit CONFIRMER le sens, au-dela du bruit
            if not pos.get("pair"):
                continue
            spot = _binance_price(pos["pair"])
            strike = _strike_at(pos["pair"], pos.get("start_ts"), slug=slug)
            if spot is None or strike is None:
                continue
            gap = abs(spot - strike)
            if gap < spot * REINFORCE_BINANCE_MARGIN:
                continue  # trop proche du strike : aucune conviction
            binance_side = "Up" if spot > strike else "Down"
            if binance_side != pos.get("side"):
                continue  # Binance dit l'inverse -> surtout pas renforcer
            # 6) taille : plafonnee aux parts deja detenues, puis aux plafonds
            held = pos["filled_shares"]
            add_shares = round(held * REINFORCE_MAX_MULT, 2)
            cost = round(add_shares * ask, 2)
            cash, _ = self._read_cash(max_age=0)
            if cash is None:
                continue
            investable = max(0.0, cash - self.floor())
            if cost > investable:
                add_shares = round(investable / ask, 2)
                cost = round(add_shares * ask, 2)
            if add_shares < MIN_SELL_SHARES or cost < MIN_BUDGET_USD:
                continue
            ok_exp, why_exp = self._exposure_ok(sym, mk, slug, cost)
            if not ok_exp:
                self._tlog(
                    f"reinf_exp_{sym}",
                    f"⛔ [RENFORT] {sym} {slug} {pos['side']} refuse : {why_exp}",
                )
                continue
            self._log(
                f"📈 [RENFORT] {sym} {slug} {pos['side']} @ {ask:.3f} "
                f"(+{add_shares} parts / {cost:.2f}$) -- SL deja declenche donc "
                f"directionnel assume, Binance confirme (ecart {gap:.2f}), "
                f"{secs_left:.0f}s restantes"
            )
            with self._order_lock:
                res = self._live.snipe_buy_market(pos["token_id"], round(ask + 0.02, 2), cost)
            filled = res.get("filled_shares", 0.0)
            if filled <= 0:
                self._log(
                    f"⚠️ [RENFORT] {sym} {slug} {pos['side']} non rempli "
                    f"(err={res.get('error', '')})"
                )
                pos["reinforced"] = True  # pas de re-tir en boucle
                continue
            avg = res.get("avg_cost") or ask
            self._add_slug_spent(mk, slug, round(filled * avg, 2))
            _old_sh, _old_cost = held, pos.get("cost", 0.0)
            pos["filled_shares"] = round(_old_sh + filled, 2)
            pos["cost"] = round(_old_cost + filled * avg, 2)
            pos["entry_price"] = round(pos["cost"] / pos["filled_shares"], 4)
            pos["reinforced"] = True
            # le palier de TP se recalibre sur la NOUVELLE taille
            pos["init_shares"] = pos["filled_shares"]
            self._log(
                f"✅ [RENFORT] {sym} {slug} {pos['side']} +{filled} parts @ {avg:.3f} "
                f"-> {pos['filled_shares']} parts, prix moyen {pos['entry_price']:.3f}"
            )

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

            # ── MARKET MAKING ASYMETRIQUE, meme sur les orphelines (Steven 19/08) ──
            _oe = pos.get("entry_price", 0)
            if not pos.get("_spread_sell_posted") and _oe > 0 and pos.get("filled_shares", 0) > 0:
                _sc_price = min(0.99, round(_oe + SPREAD_CAPTURE_PRICE, 2))
                try:
                    _sc_res = self._live.post_limit_sell(pos["token_id"], _sc_price, pos["filled_shares"])
                    pos["_spread_sell_posted"] = True
                    if _sc_res.get("success"):
                        self._log(
                            f"📌 [SPREAD-CAPTURE] {sym} {pos['slug']} {pos['side']} ordre vente "
                            f"pose @ {_sc_price:.3f} (entree {_oe:.3f}) {pos['filled_shares']} parts"
                        )
                except Exception as e:
                    self._tlog(f"spreadcapture_orphan_err_{sym}", f"💥 [SPREAD-CAPTURE] {sym} erreur: {e}")

            # ── TP INSTANTANE UNIVERSEL, meme sur les orphelines (Steven 19/08) ──
            # Prend le pas sur tout le reste (y compris must_close) : des que
            # le prix depasse l'entree de TP_INSTANT_PCT, on vend TOUT.
            _tp_entry = pos.get("entry_price", 0)
            if _tp_entry > 0:
                _tp_ask = self._live_ask(pos.get("token_id"))
                _tp_px = _tp_ask if _tp_ask is not None else self._live_price(pos.get("token_id"), None, pos.get("side"))
                _tp_pct_o = ((_tp_ask - _tp_entry) / _tp_entry) if _tp_ask is not None else None
                _tp_peak_o = pos.get("_tp_peak_pct", 0.0)
                if _tp_pct_o is not None and _tp_pct_o > _tp_peak_o:
                    pos["_tp_peak_pct"] = _tp_peak_o = _tp_pct_o
                # PALIER FIXE EN PLUS DU TRAILING (Steven 01/09, meme fix que
                # _manage_pnl_tier_exits) : sans ca, une hausse continue sans
                # jamais redescendre ne declenchait RIEN -- vu en reel, +52%
                # jamais vendu, puis retombe a -88%.
                _tp_trigger_o = _tp_pct_o is not None and (
                    _tp_pct_o >= TP_INSTANT_PCT
                    or _tp_pct_o >= PNL_TP_TARGETS[0]
                    or (_tp_peak_o >= TP_TRAIL_ARM_PCT
                        and _tp_pct_o <= _tp_peak_o * (1 - TP_TRAIL_GIVEBACK_PCT)
                        and _tp_pct_o > 0)   # meme fix que _manage_pnl_tier_exits (8b0389f) :
                                              # le trailing ne doit jamais vendre sous l'entree
                )
                if _tp_trigger_o:
                    _tp_shares = pos.get("filled_shares", 0)
                    if _tp_shares > 0:
                        _tp_sold = self._sell_orphan(
                            pos["token_id"], _tp_shares,
                            f" {sym} {pos['slug']} {pos['side']} TP-INSTANT-ORPHAN",
                            entry_price=_tp_entry, symbol=sym,
                            slug=pos.get("slug"), side=pos.get("side"),
                        )
                        if _tp_sold > 0:
                            realized = round(_tp_sold * (_tp_px - _tp_entry), 3)
                            pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
                            pos["filled_shares"] = round(_tp_shares - _tp_sold, 2)
                            self._log(
                                f"⚡ [TP-INSTANT-ORPHAN] {sym} {pos['slug']} {pos['side']} "
                                f"@ entree {_tp_entry:.3f} ask={_tp_ask:.3f} "
                                f"-> vendu {_tp_sold} parts @ {_tp_px:.3f}"
                            )
                            if pos["filled_shares"] <= 0.01:
                                pnl = pos["realized_pnl"]
                                pos.update(win=pnl > 0, pnl=pnl, resolved_by="tp_instant_orphan", exit_price=round(_tp_px, 3))
                                mk["trades"].append(pos)
                                del mk["open"][key]
                                self._record_trade_pnl(sym, pnl)
                            continue

            # ── ZERO JAMBE NUE (Steven 05/08, "pas de demi-mesure") ──
            # Une jambe issue d'une paire qui n'a pas pu se completer est un
            # pari directionnel non voulu. Mesure on-chain sur 27.9h : les 64
            # marches ou un SEUL cote a ete achete pesent -32.98$ pour un win
            # rate de ~33% -- structurellement perdants, alors qu'un arb
            # complet est gagnant par construction. On ne "gere" donc pas ces
            # jambes au momentum : on les FERME, et on continue d'essayer
            # tant qu'elles ne sont pas soldees (avant, une vente ratee
            # laissait la position courir jusqu'a resolution).
            if pos.get("must_close"):
                _mc_shares = pos.get("filled_shares", 0)
                if _mc_shares > 0:
                    _tries = pos.get("close_attempts", 0)
                    # ── PLUS D'ORDRE OUVERT PENDANT QU'ON N'ARRIVE PAS A SORTIR ──
                    # (Steven 11/08) : tant que la vente echoue, les achats
                    # passifs encore en carnet peuvent etre servis. Se faire
                    # remplir une jambe a ~1 minute de la resolution est le
                    # pire des cas : il ne reste meme plus le temps d'une
                    # completion, et on n'achete alors qu'une perte de plus
                    # par-dessus une position qu'on cherche justement a fuir.
                    _t0_close = pos.get("close_since")
                    if _t0_close is None:
                        pos["close_since"] = _t0_close = now
                    if (now - _t0_close) >= 60 and not pos.get("close_cancelled"):
                        _annules = self._annuler_ordres_slug(sym, pos["slug"])
                        pos["close_cancelled"] = True
                        self._log(
                            f"🛑 [ZERO-JAMBE-NUE] {sym} {pos['slug']} vente bloquee depuis "
                            f"{int(now - _t0_close)}s -> {_annules} ordre(s) annule(s), "
                            f"plus aucune jambe ne peut etre achetee ici"
                        )
                    _mc_sold = self._sell_orphan(
                        pos["token_id"], _mc_shares,
                        f" {sym} {pos['slug']} {pos['side']} ZERO-JAMBE-NUE(essai {_tries + 1})",
                        entry_price=pos["entry_price"], symbol=sym,
                        slug=pos.get("slug"), side=pos.get("side"),
                        urgence=_tries,
                    )
                    if _mc_sold >= _mc_shares - 0.01:
                        del mk["open"][key]
                        self._log(
                            f"✅ [ZERO-JAMBE-NUE] {sym} {pos['slug']} {pos['side']} "
                            f"jambe non couverte soldee ({_mc_sold} parts)"
                        )
                    else:
                        pos["filled_shares"] = round(_mc_shares - _mc_sold, 2)
                        pos["close_attempts"] = _tries + 1
                        self._tlog(
                            f"mustclose_{key}",
                            f"🔁 [ZERO-JAMBE-NUE] {sym} {pos['slug']} {pos['side']} "
                            f"{_mc_sold}/{_mc_shares} vendues, reste "
                            f"{pos['filled_shares']} -> nouvelle tentative au prochain cycle",
                        )
                else:
                    del mk["open"][key]
                continue

            # ── ORPHELINE BON MARCHE = BILLET DE LOTERIE (Steven 05/08) ──
            # Place AVANT toute logique "Binance dit que ca monte, on tient" :
            # une jambe nue sous ORPHAN_KEEP_MIN_PRICE est structurellement
            # perdante (cf. commentaire de la constante -- -17% a -28% de ROI
            # historique sous 0.50, et deux pertes a -100% le jour meme sur
            # ETH @ 0.138 et DOGE @ 0.242, toutes deux tenues parce que
            # Binance les donnait gagnantes). Le signal ne rachete pas un
            # mauvais prix : il faudrait un retournement complet, que le
            # marche price deja correctement. On ferme.
            _op_px = self._live_price(pos.get("token_id"), None, pos.get("side"))
            if _op_px is not None and _op_px < ORPHAN_KEEP_MIN_PRICE:
                pos["must_close"] = True
                self._tlog(
                    f"orphcheap_{key}",
                    f"⛔ [ORPHELINE-BON-MARCHE] {sym} {pos['slug']} {pos['side']} "
                    f"@ {_op_px:.3f} < {ORPHAN_KEEP_MIN_PRICE} -> billet de loterie, "
                    f"marquee A FERMER (plus de 'on tient' sur signal Binance)",
                )
                continue

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
            if _shares > 0:
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
                    # MARGE DE CONFIRMATION (Steven 05/08) : notre strike n'est
                    # PAS le strike officiel Polymarket (il n'est pas publie --
                    # cf. BINANCE_CONFIRM_MARGIN), c'est l'ouverture Binance 1m,
                    # fiable a 93.2% mais fausse dans TOUTES les fenetres qui
                    # bougent de moins de 0.07%. Sans marge, un seul tick
                    # au-dessus du strike suffisait a declarer "gagnant" et a
                    # TENIR la jambe nue jusqu'a la resolution -- pile dans la
                    # zone ou le proxy est un tirage au sort. On exige
                    # desormais un ecart net ; en dessous, winning reste None
                    # (indetermine) et la position part en gestion/vente au
                    # lieu d'etre tenue sur une conviction qu'on n'a pas.
                    _gap_w = abs(price - strike)
                    if _gap_w >= price * BINANCE_CONFIRM_MARGIN:
                        winning = (pos["side"] == "Up") == (price > strike)
                    else:
                        self._tlog(
                            f"orphnoconf_{key}",
                            f"🌫️ [ORPHAN] {sym} {pos['slug']} {pos['side']} ecart au strike "
                            f"{_gap_w:.4f} < {price * BINANCE_CONFIRM_MARGIN:.4f} "
                            f"(0.1%) -> signal Binance NON confirmatoire, on ne tient pas",
                        )
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
                    # Palier respecte (Steven 05/08) : on ne bascule sur "vend
                    # tout" que si le palier lui-meme ou le reliquat tombe sous
                    # le plancher anti-poussiere reel (1 part), pas sous 5.
                    if sell_n < MIN_SELL_SHARES or shares - sell_n < MIN_SELL_SHARES:
                        sell_n = shares
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
            if shares <= 0:
                continue
            # RETRAIT DU SKIP "< min CLOB" (Steven 05/08) : le plancher
            # MIN_ORDER_SIZE_SHARES est une regle d'ACHAT, pas de vente --
            # vendre TOUT ce qu'on detient (meme sous 5 parts) fonctionne
            # cote Polymarket (confirme par l'UI + un ancien comportement
            # du bot sur des trades a 1$). En dessous du plancher, la
            # logique plus bas force deja sell_n=shares (fin_fenetre).
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
            if shares <= 0:
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
                else:
                    # LOG D'ECHEC EXPLICITE (Steven 05/08, "on sait pas si down
                    # laissait encore place a arb") : avant, un echec de la
                    # reevaluation ne laissait AUCUNE trace -- impossible de
                    # savoir apres coup si le combine frais avait ete verifie
                    # et rejete, ou jamais teste du tout. Affiche desormais le
                    # combine frais reel calcule et la raison precise du rejet.
                    _reason_ko = (
                        f"combine {_comb_frais:.3f} > {REAL_MAX_COMBINED} (seuil reel)"
                        if _comb_frais > REAL_MAX_COMBINED
                        else f"edge {_edge_frais*100:.1f}% < {EDGE_REDUCE_THRESHOLD*100:.0f}% requis"
                    )
                    self._log(
                        f"♻️❌ [PREFLIGHT-REEVAL] {sym} {slug} combine FRAIS "
                        f"{side1}@{_a1:.3f}+{side2}@{_a2:.3f}={_comb_frais:.3f} verifie "
                        f"MAIS toujours invalide : {_reason_ko} -> abandon confirme"
                    )
            elif _depth_ko:
                self._log(
                    f"♻️❌ [PREFLIGHT-REEVAL] {sym} {slug} profondeur insuffisante "
                    f"sur au moins une jambe -> reevaluation impossible, abandon"
                )
            else:
                self._log(
                    f"♻️❌ [PREFLIGHT-REEVAL] {sym} {slug} prix frais indisponible "
                    f"({side1}={_a1}, {side2}={_a2}) -> reevaluation impossible, abandon"
                )
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
                        # (Steven 05/08) Ce residu vient d'un unwind qui n'a pas
                        # confirme : l'intention etait de SORTIR. Il naissait
                        # sans must_close, donc _manage_orphans pouvait decider
                        # de le "tenir" sur signal Binance -- exactement ce qui
                        # a tue ETH @ 0.138 et DOGE @ 0.242. Sous
                        # ORPHAN_KEEP_MIN_PRICE il reste marque a fermer ;
                        # au-dessus, il redevient gerable en TP/SL.
                        "must_close": px < ORPHAN_KEEP_MIN_PRICE,
                    }
                    self._log(
                        f"🦺 [ORPHAN] {sym} {slug} {side} {_leftover} parts residuelles "
                        f"@ {px:.3f} (vente non confirmee) -> "
                        f"{'A FERMER (sous ' + str(ORPHAN_KEEP_MIN_PRICE) + ')' if px < ORPHAN_KEEP_MIN_PRICE else 'gerees en TP/SL'}"
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
                # Valeur PROVISOIRE (Steven 05/08) : recalculee juste apres la
                # boucle par _tag_pair_lock, qui verifie le verrou reel au lieu
                # de le supposer.
                "is_risk_free": True,
                "arb_combined": round(combined, 4),
                "arb_edge": round(1 - combined, 4),
            }
            # Comptabilise l'exposition de la fenetre (Steven 05/08).
            self._add_slug_spent(mk, slug, round(M * px, 2))
            if side in _residual_excess:
                mk["open"][f"{slug}|{side}|excess"] = {
                    "symbol": sym, "slug": slug, "side": side, "mode": "real",
                    "strat": "orphan", "token_id": tid, "entry_price": px,
                    "filled_shares": _residual_excess[side],
                    "cost": round(_residual_excess[side] * px, 2),
                    "start_ts": p["start_ts"], "pair": p["pair"],
                    "end_ts": p["end_ts"], "opened_ts": time.time(), "buffer": 0.0,
                }
        # VERROU VERIFIE (Steven 05/08) : les 2 jambes viennent d'etre ecrites
        # avec is_risk_free=True par defaut. On recalcule ici sur les fills
        # REELS -- si le pire cas ne couvre pas le cout, ce n'est pas un arb et
        # la paire doit rester sous surveillance TP/SL au lieu d'etre laissee
        # rider jusqu'a resolution.
        self._tag_pair_lock(
            mk["open"].get(f"{slug}|{side1}"),
            mk["open"].get(f"{slug}|{side2}"),
            combined,
            tag=f" {sym} {slug} ARB-BATCH",
        )
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
            # TWAP CHAINLINK DE PREFERENCE (Steven 02/09, "polymarket resout sur
            # la TWAP officielle desormais, pas le spot instantane") : source
            # RTDS reelle de resolution, prioritaire sur le tick Binance brut.
            # Fenetre 30s d'abord (plus proche du spot tout en filtrant les
            # meches), 60s en repli si la 30s n'est pas encore fraiche, spot
            # Binance en dernier recours si RTDS est indisponible/pas encore
            # connecte -- jamais de blocage total sur ce calcul.
            _live_px = None
            if hasattr(self, "_ws"):
                _live_px = self._ws.twap(p["pair"], window_s=30)
                if _live_px is None:
                    _live_px = self._ws.twap(p["pair"], window_s=60)
                if _live_px is None:
                    _live_px = self._ws.spot_price(p["pair"])
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

        # TWAP-ORACLE (Steven 02/09) : INDEPENDANT de l'arb/favori Polymarket
        # ci-dessous -- tente a CHAQUE cycle, ne bloque rien d'autre. Doit
        # etre gate uniquement mode=="real" (fait a l'interieur de la
        # fonction) pour ne jamais tourner en paper.
        if mode == "real":
            try:
                self._try_twap_oracle(sym, m, p, quotes, outcomes, token_ids, mode, mk, slug)
            except Exception as e:
                self._tlog(f"twap_oracle_err_{sym}", f"💥 [TWAP-ORACLE] {sym} {slug} erreur: {e}")
        if ORACLE_ONLY_MODE:
            return False  # coupe l'ouverture arb/bothside/fav -- oracle deja tente ci-dessus
        # FILTRE PAR STRAT (Steven 05/08, "je vois des near-certain qui
        # attendent pas resolution") : cette fonction gere l'arb bothside/
        # swing exclusivement. legs_held pilote TOUT son comportement en
        # aval -- force_hedge, PARALLEL PATH, FIRST-LEG, et surtout
        # HEDGE-NEAR/FORCE-PAIR (les mecanismes de completion corriges plus
        # tot ce soir avec PAIR_COMPLETION_MAX_COMBINED). Sans ce filtre,
        # une jambe near-certain/fav/copy ouverte SEULE (volontairement, ce
        # n'est pas un echec d'arb) etait comptee comme "1 jambe en cours",
        # et ces mecanismes tentaient de la completer ou de la refermer
        # comme un orphelin -- observe on-chain : 100% des achats
        # near-certain revendus dans les 6-18 SECONDES suivant l'achat.
        legs_held = sum(
            1 for side in outcomes
            if (mk["open"].get(f"{slug}|{side}") or {}).get("strat") in ("bothside", "swing")
        )

        # EXCLUSIVITE PRE-OUVERTURE (Steven 06/08) : sur un symbole reserve,
        # ce chemin (arb both-side a l'ouverture, arb decale, FIRST-LEG,
        # HEDGE-NEAR...) n'ouvre RIEN. C'est lui qui ouvrait par-dessus la
        # pre-ouverture -- mesure sur doge-1785993000 : la pre-ouverture avait
        # solde proprement (-0.08$ de spread), puis ce chemin a ouvert 5s
        # apres l'ouverture et perdu -0.91$.
        # On ne coupe QUE l'ouverture : si des jambes sont deja tenues, on
        # laisse la fonction s'executer pour qu'elles restent gerees.
        if self._preopen_only(sym) and legs_held == 0:
            self._tlog(
                f"preopen_excl_{sym}",
                f"⏸️ [PRE-OUVERTURE] {sym} reserve a la pre-ouverture -> "
                f"aucune ouverture a l'ouverture du marche",
            )
            return False

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

        # ── FAVORITE DETECTION (Steven 05/08, "pas d'appel a fav_side en
        # risk free ! on cherche arb donc parfois aucun favoris !") : la
        # notion de favori (Binance/Polymarket) ne sert qu'aux strategies
        # directionnelles (hedge favori/underdog, FIRST-LEG). Un arb garanti
        # (2 jambes combinees < seuil) n'a besoin d'AUCUN favori -- il est
        # symetrique par construction, le sens ne compte pas. Sous risk-free,
        # on saute donc completement ce calcul (pas d'appel Binance/strike
        # inutile) : fav_side reste None, aucune preference forcee.
        if self._risk_free_on(sym):
            fav_side = None
            fav_side_ordering = None
        else:
            # PLUS DE BINANCE DU TOUT DANS CETTE DECISION (Steven 01/09,
            # "il n'achete jamais Down meme quand c'est obvious" -- persistait
            # meme apres avoir mis le marche en priorite, parce que
            # _fav_poly retombait a None des qu'UN SEUL cote manquait de prix
            # a l'instant du calcul, et _fav_binance (bruyant) reprenait la
            # main). Regle desormais simple et unique : le cote au-dessus de
            # 0.50$ est le favori. Binance ne decide plus jamais rien ici.
            _fav_prices = [(s, a) for s, (_, a, _) in zip(outcomes, [quotes.get(s, (None, None, None)) for s in outcomes]) if a is not None]
            fav_side = None
            if len(_fav_prices) == 2:
                fav_side = max(_fav_prices, key=lambda x: x[1])[0]
            elif len(_fav_prices) == 1 and _fav_prices[0][1] >= 0.50:
                fav_side = _fav_prices[0][0]
            fav_side_ordering = fav_side
            self._tlog(
                f"favdiag_{sym}",
                f"🔎 [FAV-DIAG] {sym} {slug} retenu={fav_side} prix_dispo={_fav_prices} "
                f"(marche seul, Binance retire de cette decision)",
            )
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
                    # PLANCHER DE PRIX SUR LE FAVORI (Steven 01/09, "c'est
                    # une blague ?" -- vu en reel : Down achete a 0.001$
                    # (1600 parts, -44%) et 0.01$ (100 parts, -95%) parce que
                    # le signal Binance designait ce cote "favori" alors que
                    # le MARCHE l'avait deja envoye vers zero. "Favori" sans
                    # accord du prix de marche n'en est pas un -- le marche a
                    # plus d'info que le signal spot seul en fin de fenetre.
                    if is_fav and ask_q < FAV_MIN_PRICE:
                        self._tlog(
                            f"favprice_low_{sym}",
                            f"🌫️ [BOTHSIDE-FAV] {sym} {side_q} designe favori mais "
                            f"prix marche {ask_q:.3f} < {FAV_MIN_PRICE} -> le marche "
                            f"n'est pas d'accord, on n'achete pas",
                        )
                        is_fav = False
                    if is_fav:
                        # FAVORITE-FIRST (Steven 28/07) : la favorite est achetee
                        # en 1er, meme si >0.52. max_entry=1.0 pour favorite.
                        first_target = (side_q, ask_q, True)
                    # PLUS DE REPLI SUR LE PERDANT (Steven 01/09, "on ne parie
                    # plus sur le perdant, on parie sur le gagnant"). Avant :
                    # si fav_side etait indetermine (spot/strike indispo), ce
                    # elif achetait quand meme le premier cote sous
                    # BOTH_SIDE_MAX_ENTRY (~0.52), souvent le cote pas cher/
                    # perdant par construction (ex: Down @ 0.09 vu en reel,
                    # perte quasi totale). Desormais : sans favori identifie,
                    # on n'achete rien du tout sur cette fenetre plutot que de
                    # parier a l'aveugle sur le cote bon marche.
            # FAVORI D'ABORD (Steven 04/08, "en achetant favori d'abord") : les
            # 2 jambes partent ensemble, mais l'ordre dans lequel on les envoie
            # decide laquelle a le plus de chances d'etre servie en premier si
            # le carnet ne peut pas tout absorber. Mesure de cette nuit : quand
            # une seule jambe se remplit, on garde un orphelin -- autant que ce
            # soit le FAVORI (donne gagnant par le marche) plutot qu'un cote au
            # hasard. Un orphelin favori gagne plus souvent qu'il ne perd ; un
            # orphelin underdog est perdant par construction.
            if fav_side_ordering and len(leg_data_immediate) == 2:
                leg_data_immediate.sort(key=lambda L: 0 if L[0] == fav_side_ordering else 1)
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
                        # VERROU VERIFIE (Steven 05/08) : is_risk_free n'est
                        # plus pose sur la seule foi que les 2 jambes sont
                        # remplies -- on controle que le pire cas couvre
                        # reellement le cout (voir _tag_pair_lock).
                        if _ok1 and _ok2:
                            self._tag_pair_lock(
                                mk["open"].get(f"{slug}|{leg_data_immediate[0][0]}"),
                                mk["open"].get(f"{slug}|{leg_data_immediate[1][0]}"),
                                combined_now,
                                tag=f" {sym} {slug} ARB-BYPASS",
                            )
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
                                    _held = _pos1["filled_shares"]
                                    _sold_uw = self._sell_orphan(
                                        _pos1["token_id"], _held,
                                        f" {sym} {slug} {_pos1['side']} ARB-BYPASS-UNWIND",
                                        entry_price=_pos1["entry_price"], symbol=sym,
                                        slug=slug, side=_pos1["side"],
                                    )
                                    # FIX (Steven 30/07) : le rameau real ne
                                    # supprimait jamais l'entree mk["open"]
                                    # apres l'avoir revendue -> position
                                    # "fantome" qui restait affichee comme
                                    # ouverte alors qu'elle etait deja soldee.
                                    #
                                    # FIX CRITIQUE (Steven 05/08, boucle de
                                    # churn trouvee on-chain) : le del etait
                                    # INCONDITIONNEL, meme quand la vente
                                    # ECHOUAIT (sold=0 : carnet sans bid, ordre
                                    # rejete...). Consequences en cascade :
                                    #  1) la position restait detenue on-chain
                                    #     mais disparaissait de l'etat du bot
                                    #     -> plus aucun TP/SL/suivi dessus ;
                                    #  2) _sell_orphan n'enregistre le trade que
                                    #     si sold>0 -> slug|side absent de
                                    #     mk["trades"] ET de mk["open"] -> la
                                    #     garde anti-doublon de _open_leg ne
                                    #     bloquait plus rien -> RE-ACHAT immediat
                                    #     de la meme jambe, re-echec, re-vente...
                                    # Observe on-chain sur btc-updown-5m-1785900300 :
                                    # 5 achats / 4 ventes sur la MEME jambe Up en
                                    # ~45s, prix qui s'effondre de 0.35 a 0.13,
                                    # 8.56$ achetes contre 4.94$ revendus (-3.62$)
                                    # + 5.74 parts fantomes invendues (= exactement
                                    # 29.04-23.30, verifie via data-api).
                                    # On ne supprime donc QUE ce qui est reellement
                                    # solde ; le reste reste gere.
                                    if _sold_uw >= _held - 0.01:
                                        del mk["open"][_key1]
                                    else:
                                        _rest = round(_held - _sold_uw, 2)
                                        _pos1["filled_shares"] = _rest
                                        _pos1["strat"] = "orphan"
                                        # ZERO JAMBE NUE : le reliquat n'est pas
                                        # une position a "gerer", c'est une jambe
                                        # non couverte -> a fermer, point.
                                        _pos1["must_close"] = True
                                        self._log(
                                            f"⚠️ [UNWIND-PARTIEL] {sym} {slug} {_pos1['side']} "
                                            f"{_sold_uw}/{_held} parts vendues -> {_rest} parts "
                                            f"marquees A FERMER (retentee chaque cycle)"
                                        )
                                    # Cooldown du slug : empeche le re-achat
                                    # immediat de la meme fenetre apres un
                                    # unwind (moteur de la boucle de churn).
                                    self._set_slug_cooldown(sym, slug, mk)
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
                    # PARI DIRECTIONNEL SUR LE FAVORI (Steven 05/08) : l'arb
                    # est impossible sur cette fenetre, mais si le favori est
                    # cher ET que Binance le donne gagnant NETTEMENT, on tente
                    # une petite mise directionnelle assumee, geree en TP/SL.
                    # Voir les constantes FAV_* pour ce que disent les donnees.
                    # NEAR-CERTAIN d'abord (seule strategie directionnelle
                    # validee par l'historique), FAV ensuite (desactive).
                    # ARB DECALE en premier : il vise TOT dans la fenetre
                    # (>=180s), la ou le near-certain vise la fin (<=120s)
                    # -- ils ne se marchent donc jamais dessus.
                    if self._try_twap_lock(sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
                        pass
                    elif self._try_split_maker(sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
                        pass
                    elif self._try_stagger_entry(sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
                        pass
                    elif self._try_overreaction(sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
                        pass
                    elif not self._try_near_certain(sym, m, p, quotes, outcomes, token_ids, mode, mk, slug):
                        self._try_favorite(sym, m, p, quotes, outcomes, token_ids, mode, mk, slug)
                    return legs_held > 0
                in_cd, cd_reason = self._in_cooldown(sym, slug, mk)
                if in_cd:
                    self._log(f"⏸️ [FIRST-LEG-COOLDOWN] {sym} {slug} -> {cd_reason}")
                    return legs_held > 0
                # PLANCHER DUR ANTI-ACHAT SOUS 0.50$ (Steven 01/09, "ajoute
                # un filtre anti achat under 50c"). Filet de securite final,
                # independant de fav_side (qui a deja bugue plusieurs fois
                # ce soir) -- peu importe comment ce prix a ete choisi, on ne
                # paye jamais moins de 0.50$ ici.
                if first_target[1] < FAV_MIN_PRICE:
                    self._tlog(
                        f"firstleg_floor_{sym}",
                        f"⛔ [FIRST-LEG-FLOOR] {sym} {slug} {first_target[0]} "
                        f"@ {first_target[1]:.3f} < {FAV_MIN_PRICE} -> refuse, "
                        f"jamais d'achat sous ce plancher",
                    )
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
                # BUG CORRIGE (Steven 07/08, "ETH a ouvert une paire a comb=1.010
                # en pensant payer 0.950"). L'ancien "COMBINED HISTORY GATE"
                # ecrasait `combined` par _comb_best (une valeur RECENTE mais
                # perimee) des que le prix ACTUEL depassait le seuil, puis
                # laissait passer la suite (sizing + post) sur cette valeur
                # fictive -- alors que `leg_data` (les prix REELS utilises pour
                # poster les ordres) restait, lui, sur le prix ACTUEL, moins bon.
                # Le bot achetait donc au prix reel tout en croyant avoir eu le
                # meilleur prix recent -> paires ouvertes a un combine garanti
                # perdant (ex. Up@0.49+Down@0.52=1.01 le 07/08 sur ETH), rattrapees
                # seulement en aval par _tag_pair_lock (qui, lui, recalcule
                # correctement depuis les VRAIS entry_price -- c'est ce qui a
                # empeche que la position soit traitee comme un arb garanti,
                # mais n'empechait pas l'entree elle-meme).
                #
                # `quotes` est fige UNE SEULE FOIS avant cette boucle de 3
                # tentatives (jamais rafraichi entre les essais) : la "seconde
                # chance" que ce garde-fou pretendait offrir n'existait donc pas
                # reellement -- chaque retry relisait le meme instantane. Le
                # supprimer ne fait perdre aucune vraie occasion de rattrapage,
                # ca supprime seulement la decision prise sur un prix qui
                # n'etait plus d'actualite.
                if combined > real_max:
                    self._tlog(
                        f"osc_reject_{sym}",
                        f"📊 [PRIX-TROP-HAUT] {sym} {slug} comb={combined:.3f} > {real_max:.3f} "
                        f"(meilleur recent {_comb_best:.3f}) -> abandon, prix actuel fait foi",
                    )
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
                    # VERROU VERIFIE (Steven 05/08) : cf. _tag_pair_lock --
                    # is_risk_free seulement si le pire cas couvre le cout.
                    if ok1 and ok2:
                        self._tag_pair_lock(
                            mk["open"].get(f"{slug}|{leg_data[0][0]}"),
                            mk["open"].get(f"{slug}|{leg_data[1][0]}"),
                            combined,
                            tag=f" {sym} {slug} ARB-PARALLEL",
                        )
                    if ok1 and not ok2:
                        # ATOMICITE (meme fix que ARB-BYPASS) : jambe seule -> revente immediate
                        _key1p = f"{slug}|{leg_data[0][0]}"
                        _pos1p = mk["open"].get(_key1p)
                        if _pos1p:
                            if mode == "real":
                                # Meme fix critique qu'ARB-BYPASS-UNWIND
                                # (Steven 05/08) : ne jamais supprimer une
                                # position dont la vente a echoue -> sinon
                                # jambe fantome on-chain + garde anti-doublon
                                # contournee -> boucle de re-achat/re-vente.
                                _heldp = _pos1p["filled_shares"]
                                _sold_p = self._sell_orphan(
                                    _pos1p["token_id"], _heldp,
                                    f" {sym} {slug} {_pos1p['side']} ARB-PARALLEL-UNWIND",
                                    entry_price=_pos1p["entry_price"], symbol=sym,
                                    slug=slug, side=_pos1p["side"],
                                )
                                if _sold_p >= _heldp - 0.01:
                                    del mk["open"][_key1p]
                                else:
                                    _restp = round(_heldp - _sold_p, 2)
                                    _pos1p["filled_shares"] = _restp
                                    _pos1p["strat"] = "orphan"
                                    _pos1p["must_close"] = True
                                    self._log(
                                        f"⚠️ [UNWIND-PARTIEL] {sym} {slug} {_pos1p['side']} "
                                        f"{_sold_p}/{_heldp} parts vendues -> {_restp} parts A FERMER"
                                    )
                                self._set_slug_cooldown(sym, slug, mk)
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
        # FILTRE PAR STRAT (Steven 05/08, "je vois des near-certain qui
        # attendent pas resolution") : ce pre-fill comptait TOUTE position
        # presente sur le slug, quelle que soit sa strategie d'origine -- une
        # jambe near-certain (achetee seule, volontairement, a 0.95-0.98)
        # etait donc prise pour une jambe d'arb en cours. Consequence directe
        # observee on-chain : 100% des achats near-certain revendus dans les
        # 6-18 SECONDES suivant l'achat, au meme prix (aucun profit/perte,
        # juste un aller-retour) -- le combined sequentiel (near-cert deja
        # tres cher + l'autre cote) depassait quasi-systematiquement le
        # plafond de couverture, et l'ATOMIC ARB GUARD marquait la jambe
        # "orpheline" -> must_close, alors qu'elle n'a jamais fait partie
        # d'une tentative d'arb. Ne compter ici que ce que CE mecanisme gere
        # lui-meme (bothside/swing) -- near-certain, fav et copy restent
        # geres exclusivement par leur propre logique (TP/SL normal).
        for side, token_id in zip(outcomes, token_ids):
            key = f"{slug}|{side}"
            _existing = mk["open"].get(key)
            if _existing and _existing.get("strat") in ("bothside", "swing"):
                filled_legs.append((side, token_id))
            elif _existing:
                # RETRAIT COMPLET (Steven 05/08) : selon l'ordre de outcomes,
                # une jambe etrangere (near-cert/fav/copy) pouvait se trouver
                # en position i=0 -> ne bloquait alors PAS l'ouverture de la
                # jambe opposee (le check "SKIP" plus bas ne saute que le
                # MEME cote), et bothside finissait par acheter l'autre cote
                # autour d'elle -- creant une paire non voulue, a un sizing
                # qui n'a jamais ete pense pour ca. Plus simple et plus sur :
                # des qu'un autre mecanisme a deja une jambe sur ce slug, on
                # laisse la fenetre entiere tranquille.
                self._tlog(
                    f"bothside_foreign_{sym}",
                    f"⏸️ [BOTHSIDE-SEQ] {sym} {slug} deja une jambe "
                    f"{_existing.get('strat')} dessus -> on n'y touche pas",
                )
                return legs_held > 0
        # ── GATE COMBINED SEQUENTIEL (Steven 05/08) ─────────────────────
        # TROU TROUVE EN PRODUCTION. Cette boucle-ci n'a JAMAIS controle le
        # combine : elle calcule _comb_sequential, s'en sert pour le sizing et
        # pour afficher l'edge, puis achete chaque jambe en ne verifiant que le
        # plafond de prix de CHAQUE jambe prise isolement. Deux prix
        # individuellement acceptables peuvent evidemment sommer au-dessus de
        # 1.00 -- et un pack Up+Down rapporte EXACTEMENT 1.00 a la resolution.
        # Trace reelle (prod, 07:00:13 UTC) :
        #   [BOTHSIDE][REEL][d=66] BTC Up ask=0.640 budget=4.00$ rempli=6.25
        #   [ENTRY] ... comb_ask=1.010 edge=0.0%
        #   -> paire achetee 5.98$ pour un payout garanti de 4.39$ = -1.59$
        # Le code affichait donc "edge=0.0%" et achetait quand meme. Les
        # plafonds poses plus tot ne couvraient que les chemins de COMPLETION
        # (FORCE-PAIR, ORPHAN-PAIR, HEDGE-NEAR), pas cette entree initiale.
        # PALIER (Steven 05/08) : une paire NEUVE doit etre un vrai verrou
        # (< 0.99). En revanche, si une jambe est DEJA tenue, la couvrir reste
        # preferable a la laisser nue jusqu'a PAIR_COMPLETION_HEDGE_MAX (1.03) :
        # perte bornee, et le TP/SL reste actif dessus. Au-dela, on refuse.
        _seq_cap = (
            PAIR_COMPLETION_HEDGE_MAX if filled_legs else PAIR_COMPLETION_MAX_COMBINED
        )
        if mode == "real" and _comb_sequential >= _seq_cap:
            self._tlog(
                f"seqcomb_{sym}",
                f"⛔ [BOTHSIDE-SEQ] {sym} {slug} comb={_comb_sequential:.3f} >= "
                f"{_seq_cap} ({'couverture' if filled_legs else 'entree neuve'}) "
                f"-> perte garantie, aucune jambe ouverte",
            )
            # Si UNE seule jambe est deja tenue, elle ne pourra plus etre
            # couverte a profit sur cette fenetre -> on la ferme (zero jambe
            # nue) plutot que d'attendre une amelioration qui, statistiquement,
            # n'arrive pas. Une paire deja complete (2 jambes) n'est pas touchee.
            if len(filled_legs) == 1:
                _held_pos = mk["open"].get(f"{slug}|{filled_legs[0][0]}")
                if _held_pos and not _held_pos.get("is_risk_free"):
                    _held_pos["strat"] = "orphan"
                    _held_pos["must_close"] = True
                    self._log(
                        f"⛔ [BOTHSIDE-SEQ] {sym} {slug} {filled_legs[0][0]} jambe seule "
                        f"non couvrable a profit -> marquee A FERMER"
                    )
            return legs_held > 0
        # ORDRE FAVORI D'ABORD (Steven 02/09, "poly renvoi dans l'ordre up/down
        # et notre bot prend le premier donc seulement up") : cette boucle ne
        # tente QUE l'element i=0 (voir le "if i > 0: break" plus bas, choix
        # volontaire du 19/08 pour ne jamais ouvrir de 2e jambe). Mais elle
        # itérait sur `outcomes` tel que renvoye par Polymarket, quasi
        # toujours ["Up", "Down"] dans cet ordre fixe -> i=0 etait TOUJOURS Up,
        # jamais Down, peu importe lequel etait reellement favori. Preuve en
        # prod : 50 entrees reelles sur ce chemin en ~22h, 50 Up, 0 Down.
        # fav_side_ordering existe deja et sert cet exact usage un peu plus
        # haut (tri de leg_data_immediate pour le chemin arb parallele) --
        # applique ici au chemin sequentiel qui en avait besoin aussi.
        _seq_order = list(zip(outcomes, token_ids))
        if fav_side:
            _seq_order.sort(key=lambda leg: 0 if leg[0] == fav_side else 1)
        for i, (side, token_id) in enumerate(_seq_order):
            # PLUS DE PAIRE (Steven 19/08, "on s'en fou de la jambe down ...
            # vu qu'on va tp directement la premiere leg ouvrir la deuxieme
            # n'a plus d'interet") : TP instantane universel rend le
            # hedging inutile -> une seule jambe par fenetre, jamais de 2e.
            if i > 0:
                break
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
            # ── PLAFOND PAR JAMBE DERIVE DE L'ECONOMIE DE LA PAIRE ──────
            # BUG RACINE (Steven 06/08, trouve en analysant la chute de 14$ a
            # 0.63$). Chaine complete :
            #   1. risk_free=True sur un symbole -> fav_side = None (change
            #      hier soir : "pas d'appel a fav_side en risk free") ;
            #   2. ligne au-dessus : `0.99 if side == fav_side else max_entry`
            #      -> avec fav_side None, AUCUN cote ne correspond, les DEUX
            #      jambes heritent de BOTH_SIDE_MAX_ENTRY = 0.52 ;
            #   3. une paire somme toujours a ~1.00 -> un cote est TOUJOURS
            #      au-dessus de 0.52 -> cette jambe ne peut JAMAIS etre
            #      achetee, quel que soit l'edge ;
            #   4. seule la jambe bon marche se remplit -> jambe nue
            #      systematique, tenue jusqu'a zero.
            # Mesure : 6 jambes seules sous 0.50, -10.02$ pour 12.32$ engages
            # (ROI -81.4%) = 74% de la perte de la session, pendant que les
            # arbs faisaient +19.5% et le near-certain +3.0%.
            # BOTH_SIDE_MAX_ENTRY (0.52) est un vestige de l'ANCIENNE
            # strategie hedge "acheter le cote pas cher". Pour un arb, seul le
            # COMBINE compte -- une paire 0.34/0.62 = 0.96 est excellente, et
            # l'ancien plafond la rendait impossible. Le gate BOTHSIDE-SEQ
            # au-dessus a DEJA valide le combine ; on derive donc le plafond
            # de la paire elle-meme au lieu d'une constante sans rapport.
            _other_ask = None
            for _os in outcomes:
                if _os != side:
                    _oq = quotes.get(_os)
                    _other_ask = _oq[1] if _oq else None
                    break
            if _other_ask is not None:
                _econ_cap = round(PAIR_COMPLETION_HEDGE_MAX - _other_ask, 3)
                if _econ_cap > _max_entry:
                    self._tlog(
                        f"econcap_{sym}",
                        f"🔓 [PAIRE-CAP] {sym} {slug} {side} plafond {_max_entry:.3f} "
                        f"-> {min(0.99, _econ_cap):.3f} (derive de la paire, l'autre "
                        f"cote est a {_other_ask:.3f} -- l'ancien plafond bloquait "
                        f"cette jambe et laissait l'autre a nu)",
                    )
                    _max_entry = min(0.99, _econ_cap)
            # ── PLAFOND DE LA 2e JAMBE ADOSSE AU PRIX REEL DE LA 1re ──────
            # (Steven 05/08) Le plafond etait FIXE (0.99 sur la jambe favorite,
            # max_entry sinon) et ne dependait PAS de ce qu'avait coute la
            # jambe 1. Les deux ordres etant separes de plusieurs secondes, le
            # prix derive entre-temps et la paire se referme au-dessus de 1.00
            # sans que rien ne s'y oppose. Cas reel : Up @ 0.410 puis, 6s plus
            # tard, Down @ 0.620 = 1.030, alors que le plafond de Down etait
            # 0.99 -- il passait donc largement.
            # Desormais le plafond de la 2e jambe = HEDGE_MAX - prix paye pour
            # la 1re : la paire ne peut plus depasser 1.03 par construction,
            # quelle que soit la derive. Et parts EGALES (le payout vaut 1$
            # par part : c'est min(parts) qui compte, pas les montants).
            _fp_shares = None
            if filled_legs:
                _l1 = mk["open"].get(f"{slug}|{filled_legs[0][0]}")
                _l1_px = _l1.get("entry_price") if _l1 else None
                _fp_shares = _l1.get("filled_shares") if _l1 else None
                if _l1_px:
                    _pair_cap = round(PAIR_COMPLETION_HEDGE_MAX - _l1_px, 3)
                    if _pair_cap < BOTH_SIDE_LEG_MIN:
                        # Jambe 1 trop chere : aucune 2e jambe ne peut ramener
                        # la paire sous le plafond -> on ne double pas la mise,
                        # on ferme la jambe nue.
                        _l1["strat"] = "orphan"
                        _l1["must_close"] = True
                        self._log(
                            f"⛔ [PAIRE-IMPOSSIBLE] {sym} {slug} jambe1 @ {_l1_px:.3f} "
                            f"-> plafond jambe2 {_pair_cap:.3f} < {BOTH_SIDE_LEG_MIN} : "
                            f"aucune couverture possible, jambe1 marquee A FERMER"
                        )
                        failed_legs.append((side, token_id))
                        continue
                    if _pair_cap < _max_entry:
                        self._log(
                            f"🔗 [PAIRE-CAP] {sym} {slug} {side} plafond {_max_entry:.3f} "
                            f"-> {_pair_cap:.3f} (jambe1 @ {_l1_px:.3f}, "
                            f"combine borne a {PAIR_COMPLETION_HEDGE_MAX})"
                        )
                        _max_entry = _pair_cap
            # Parts EGALES sur la 2e jambe (Steven 05/08) : c'est min(parts)
            # qui determine le payout du pire cas, donc des mises egales en $
            # sur des prix differents ne verrouillent rien. Cas reel : 4.976
            # parts Up contre 4.919 Down, pour 2.12$ et 3.13$.
            _leg_kwargs = (
                {"target_shares": round(_fp_shares, 2)}
                if _fp_shares
                else {"budget_usd": fav_budget if side == fav_side else leg_budget}
            )
            ok, _ = self._open_leg(
                sym,
                mode,
                m,
                p,
                side,
                token_id,
                _max_entry,
                tag,
                **_leg_kwargs,
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
        # pour que combined < PAIR_COMPLETION_MAX_COMBINED (0.99). Si ca echoue
        # La fenetre de 5min oscille: Leg2 finit par baisser a un moment.
        # HEDGE-NEAR-RESOLUTION rattrape tout a <30s si Leg2 n'est jamais
        # devenue assez pas chere. max_payable = 0.99 - fill_price (Steven 05/08 :
        # etait 1.02, soit une perte garantie -- on ne complete plus qu'un
        # verrou reel, cf. PAIR_COMPLETION_MAX_COMBINED).
        if filled_legs and failed_legs and secs_left > 5:
            for side, token_id in failed_legs:
                fill_price = None
                filled_side = None
                _leg1_shares = None
                for fs, _ in filled_legs:
                    leg_info = mk["open"].get(f"{slug}|{fs}")
                    if leg_info:
                        fill_price = leg_info.get("entry_price")
                        filled_side = fs
                        _leg1_shares = leg_info.get("filled_shares")
                        break
                if fill_price is not None:
                    max_payable = round(
                        max(0.05, FORCE_PAIR_MAX_COMBINED - fill_price), 3
                    )
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
                # PARTS EGALES, PAS DOLLARS EGAUX (Steven 05/08) : la 2e jambe
                # etait dimensionnee en BUDGET $ (fav_budget/leg_budget), donc
                # son nombre de parts dependait de son prix. Or le payout d'un
                # marche binaire vaut 1$ PAR PART : une paire 8 parts Up / 5
                # parts Down ne garantit que 5$, pas 8$. Le verrou d'un arb est
                # min(parts_up, parts_down) > cout_total -- il exige donc des
                # parts EGALES, pas des mises egales.
                # Mesure on-chain sur 27.9h : ecart median de 1.60x entre les 2
                # jambes, 67% des paires desequilibrees de plus de 20%, et
                # seulement 16 paires sur 78 reellement verrouillees (+11.23$)
                # contre 62 faux arbs (-41.79$). C'est LA cause du desequilibre.
                _fp_kwargs = (
                    {"target_shares": round(_leg1_shares, 2)}
                    if _leg1_shares
                    else {"budget_usd": fav_budget if side == fav_side else leg_budget}
                )
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
                        **_fp_kwargs,
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
                                    # FIX CRITIQUE (Steven 04/08, trouve via le
                                    # screenshot "31.9 positions" jamais liquidees) :
                                    # ordre (token, PRIX, parts) -- sell_shares et
                                    # sell_price etaient inverses, envoyant un
                                    # "prix" de plusieurs parts (invalide, >1$) ->
                                    # cette vente d'urgence echouait TOUJOURS
                                    # silencieusement, expliquant l'accumulation.
                                    #
                                    # FIX (Steven 05/08) : on passe par _sell_orphan
                                    # au lieu de sell_position brut. Deux raisons :
                                    #  1) sell_position par defaut poste un GTC
                                    #     PASSIF, qui peut ne jamais croiser -- ici
                                    #     on veut sortir, pas esperer un match ;
                                    #     _sell_orphan poste en FAK agressif ET
                                    #     VERIFIE le fill on-chain.
                                    #  2) le retour etait totalement ignore, puis la
                                    #     position etait pop() quoi qu'il arrive ->
                                    #     meme bug que les unwinds : jambe fantome
                                    #     detenue on-chain mais absente de l'etat.
                                    _fp_sold = 0.0
                                    try:
                                        _fp_sold = self._sell_orphan(
                                            sell_token, sell_shares,
                                            f" {sym} {slug} {sell_side} FORCE-PAIR-ECHEC",
                                            entry_price=sell_info.get("entry_price"),
                                            symbol=sym, slug=slug, side=sell_side,
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
                                    # ZERO JAMBE NUE (Steven 05/08) : ne retirer de
                                    # l'etat QUE ce qui est reellement solde. Sinon
                                    # la jambe reste marquee a fermer et sera
                                    # retentee a chaque cycle par _manage_orphans,
                                    # au lieu de devenir un pari directionnel
                                    # invisible (-32.98$ mesures sur 27.9h).
                                    if _fp_sold >= sell_shares - 0.01:
                                        mk["open"].pop(f"{slug}|{sell_side}", None)
                                    else:
                                        sell_info["filled_shares"] = round(
                                            sell_shares - _fp_sold, 2
                                        )
                                        sell_info["strat"] = "orphan"
                                        sell_info["must_close"] = True
                                        self._log(
                                            f"⚠️ [FORCE-PAIR] {sym} {slug} {sell_side} vente "
                                            f"{_fp_sold}/{sell_shares} -> reste "
                                            f"{sell_info['filled_shares']} parts MARQUEES A FERMER"
                                        )
        # ORPHAN FIX (Laguna XS 25/07) : quand legs_held==1 depuis un tick
        # precedent, filled_legs est VIDE (la jambe existante est skippee par
        # _open_leg). On retente l'achat de la 2e jambe chaque tick avec prix
        # frais (WS temps reel). ON VEND PAS — on attend que Leg2 baisse.
        # max_payable = 0.99 - fill_price (Steven 05/08). Si ca echoue, le tick
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
                            max_payable = round(
                        max(0.05, FORCE_PAIR_MAX_COMBINED - fill_price), 3
                    )
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
                                    # FIX (Steven 05/08) : 4e occurrence du meme
                                    # bug que les unwinds et FORCE-PAIR --
                                    # sell_position() en GTC PASSIF (peut ne
                                    # jamais croiser), retour ignore, puis pop()
                                    # inconditionnel -> jambe fantome detenue
                                    # on-chain mais absente de l'etat, et garde
                                    # anti-doublon contournee. On passe par
                                    # _sell_orphan (FAK agressif + verification
                                    # du fill on-chain) et on ne retire que ce
                                    # qui est reellement solde.
                                    _orph_sold = 0.0
                                    try:
                                        token_own = (
                                            mk["open"]
                                            .get(f"{slug}|{owned_side[0]}", {})
                                            .get("token_id")
                                        )
                                        if token_own:
                                            _orph_sold = self._sell_orphan(
                                                token_own,
                                                sell_shares_orph,
                                                f" {sym} {slug} {owned_side[0]} ORPHAN-PAIR-ECHEC",
                                                entry_price=sell_info_orph.get("entry_price"),
                                                symbol=sym, slug=slug, side=owned_side[0],
                                            )
                                    except Exception as e:
                                        self._log(f"⚠️ [ORPHAN] vente echouee: {e}")
                                    if _orph_sold >= sell_shares_orph - 0.01:
                                        mk["open"].pop(f"{slug}|{owned_side[0]}", None)
                                    else:
                                        sell_info_orph["filled_shares"] = round(
                                            sell_shares_orph - _orph_sold, 2
                                        )
                                        sell_info_orph["strat"] = "orphan"
                                        sell_info_orph["must_close"] = True
                                        self._log(
                                            f"⚠️ [ORPHAN] {sym} {slug} {owned_side[0]} vente "
                                            f"{_orph_sold}/{sell_shares_orph} -> reste "
                                            f"{sell_info_orph['filled_shares']} parts A FERMER"
                                        )
        # HEDGE NEAR-RESOLUTION (Laguna XS 24/07) : si < 30s et on tient 1 jambe,
        # on achete l'autre pour completer la paire.
        # REVISE (Steven 05/08) : le raisonnement d'origine ("meme a combined
        # 1.05 c'est mieux qu'un bet directionnel") ne resiste pas a la mesure.
        # Quand l'autre cote coute 0.95, completer et solder la jambe coutent la
        # MEME chose (marche efficient), mais completer immobilise en plus le
        # prix de la 2e jambe -- ce capital est precisement ce qui manque pour
        # prendre le vrai arb suivant. On ne complete donc plus qu'en dessous de
        # PAIR_COMPLETION_MAX_COMBINED ; au-dessus, la jambe part en must_close.
        HEDGE_NEAR_SECS = 30
        # DESACTIVE (Steven 01/09, "il a jete 1.60$ par la fenetre") : ce
        # mecanisme completait une jambe existante a N'IMPORTE QUEL PRIX --
        # PAIR_COMPLETION_MAX_COMBINED valait 99.0 depuis le soir meme
        # (desactive plus tot pour la logique "plus de paire" ailleurs),
        # donc plus aucun plafond ne le retenait. Vu en reel : cible 0.53$,
        # rempli a 0.01$ (l'ordre a balaye tout le carnet), sur une paire
        # qui garantissait deja une perte (comb=1.13). Coherent avec la
        # decision globale de la nuit : plus de paire du tout, une jambe
        # nue en fin de fenetre part en unwind normal (ZERO-JAMBE-NUE),
        # jamais complétée a l'aveugle.
        if False and legs_held == 1 and secs_left < HEDGE_NEAR_SECS and secs_left > 3:
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
                            # PLAFOND (Steven 05/08) : combined_h etait calcule
                            # et logge mais JAMAIS teste -> ce chemin achetait
                            # la 2e jambe a n'importe quel prix, y compris a
                            # perte garantie. Principal producteur des paires
                            # a combined effectif 1.20-2.00 mesurees on-chain.
                            if combined_h >= PAIR_COMPLETION_MAX_COMBINED:
                                _owned_pos = owned_info_h
                                if _owned_pos is not None:
                                    _owned_pos["strat"] = "orphan"
                                    _owned_pos["must_close"] = True
                                self._log(
                                    f"⛔ [HEDGE-NEAR] {sym} {slug} comb={combined_h:.3f} >= "
                                    f"{PAIR_COMPLETION_MAX_COMBINED} -> completer serait une perte "
                                    f"GARANTIE : jambe {owned_side_h[0]} marquee A FERMER"
                                )
                                continue
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
                                target_shares=(
                                    round(owned_info_h.get("filled_shares"), 2)
                                    if owned_info_h and owned_info_h.get("filled_shares")
                                    else None
                                ),
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
            # tiret plutot que underscore (Steven 02/09) : le classificateur
            # du journal DetailDesk (JournalTab.tsx) n'extrait que
            # [A-Z0-9-]+ entre crochets -- un underscore casse le match et
            # la ligne retombe dans "Autre" sans jamais etre categorisee.
            tag = f"[{pos['strat'].upper().replace('_', '-')}]" if pos.get("strat") else ""
            self._log(
                f"💹 [PRIX]{tag} {sym} {pos['slug']} {pos['side']} {cur:.3f} "
                f"(entree {entry:.3f}, {pnl_pct:+.1f}%)"
            )

    def _solder_excedent(self, sym):
        """Vend UNIQUEMENT les parts en trop d'une paire desequilibree.

        Extrait de _guard_both_side, qui n'etait appelee nulle part (code
        mort verifie par recherche exhaustive). On ne reprend QUE cette
        partie : le stop-loss par jambe que contient aussi cette fonction
        reste eteint -- mesure cette semaine, un SL sur MSF casse plus de
        verrous qu'il n'evite de pertes.

        Pourquoi c'est sur : sur une paire equilibree l'excedent vaut 0 et
        rien n'est vendu. Sur une paire desequilibree, l'excedent n'est
        couvert par RIEN (le gagnant paie 1$ par PART, donc seul min(parts)
        est protege) -- le vendre ameliore toujours le pire cas, que
        l'excedent soit du cote gagnant ou perdant. Verifie sur le cas reel
        7.69 contre 3.35 parts : pire cas 3.35$ -> 3.78$, sans jamais rien
        risquer. Couper la jambe perdante ENTIERE, en revanche, ferait
        tomber ce pire cas a 0.77$ -- c'est pour ca qu'on ne touche jamais a
        la partie appariee."""
        mk = self.state["markets"].get(sym) or {}
        for key, pos in list(mk.get("open", {}).items()):
            if not pos or pos.get("strat") != "bothside":
                continue
            _exc = pos.get("excedent_a_solder") or 0
            if _exc < MIN_SELL_SHARES:
                continue
            _n = round(min(_exc, pos.get("filled_shares") or 0), 2)
            if _n < MIN_SELL_SHARES:
                pos["excedent_a_solder"] = 0
                continue
            slug = pos.get("slug")
            _v = self._sell_orphan(
                pos["token_id"], _n,
                f" {sym} {slug} {pos.get('side')} EXCEDENT-NON-COUVERT",
                entry_price=pos.get("entry_price"), symbol=sym, slug=slug,
                side=pos.get("side"),
            )
            if _v > 0:
                pos["filled_shares"] = round((pos.get("filled_shares") or 0) - _v, 2)
                pos["cost"] = round((pos.get("cost") or 0) - _v * (pos.get("entry_price") or 0), 2)
                self._log(
                    f"⚖️ [EXCEDENT-SOLDE] {sym} {slug} {pos.get('side')} {_v:.2f} parts "
                    f"en trop vendues -> la paire revient a des parts egales"
                )
            pos["excedent_a_solder"] = round(max(0.0, _exc - _v), 2)
            self._save()

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
                # FILL VERIFIE (Steven 05/08) : avant, un sell_position() en GTC
                # PASSIF dont on ne testait que le `success` de l'API. Or un
                # ordre ACCEPTE n'est pas un ordre EXECUTE : un GTC pile au bid
                # peut ne jamais croiser. Le stop-loss enregistrait donc une
                # sortie et un PnL qui n'avaient pas eu lieu, tout en laissant
                # les parts detenues on-chain (l'inverse exact de la jambe
                # fantome : ici le bot se croit sorti alors qu'il est expose).
                # _sell_orphan poste en FAK agressif ET verifie le fill on-chain.
                # NB : appel HORS de self._order_lock -- _sell_orphan prend ce
                # meme lock non reentrant, l'imbriquer bloquerait le bot.
                # NE PAS CASSER LA COUVERTURE (Steven 05/08) : meme garde que
                # le SPREAD-EXIT. Couper la jambe perdante d'une paire encore
                # tenue retire precisement ce qui borne la perte -- mesure a
                # -8.60$ sur deux fenetres. Voir _hedge_would_break.
                # EXCEDENT NON COUVERT (marque par _tag_pair_lock, voir la-bas
                # pour la mesure). On le solde AVANT toute autre decision : ces
                # parts ne font pas partie de l'arb, les garder est un pari nu.
                _exc = pos.get("excedent_a_solder") or 0
                if _exc >= MIN_SELL_SHARES:
                    _v = self._sell_orphan(
                        pos["token_id"], round(min(_exc, pos.get("filled_shares") or 0), 2),
                        f" {sym} {slug} {pos['side']} EXCEDENT-NON-COUVERT",
                        entry_price=pos.get("entry_price"), symbol=sym, slug=slug,
                        side=pos.get("side"),
                    )
                    if _v > 0:
                        pos["filled_shares"] = round((pos["filled_shares"] or 0) - _v, 2)
                        pos["cost"] = round((pos.get("cost") or 0)
                                            - _v * (pos.get("entry_price") or 0), 2)
                        self._log(
                            f"⚖️ [DESEQUILIBRE] {sym} {slug} {pos['side']} {_v:.2f} parts "
                            f"en trop soldees -> la paire revient a des parts egales"
                        )
                    pos["excedent_a_solder"] = round(max(0.0, _exc - _v), 2)
                    self._save()
                    continue
                if self._hedge_would_break(mk, slug, pos.get("side"), cur, pos.get("entry_price")):
                    self._tlog(
                        f"slhedge_{key}",
                        f"🛡️ [SL] {sym} {slug} {pos['side']} coupe ANNULEE : la jambe "
                        f"opposee est encore tenue, la couverture borne deja la perte",
                    )
                    continue
                _sl_held = pos["filled_shares"]
                sold = self._sell_orphan(
                    pos["token_id"], _sl_held,
                    f" {sym} {slug} {pos['side']} STOP-LOSS",
                )
                mom_txt = f"{mom['fast_pct_s']:+.4f}%/s" if mom else "n/a"
                self._log(
                    f"🩹 [SL][REEL] {sym} {slug} {pos['side']} coupe @ {bid:.3f} "
                    f"(entree {pos['entry_price']:.3f}) mom={mom_txt} vendu={sold}/{_sl_held}"
                )
                if sold <= 0:
                    continue
                # MARQUEUR DIRECTIONNEL (Steven 05/08) : "des qu'on a fait un
                # SL, meme de 25%, ca devient directionnel". Exact -- toute
                # coupe reduit min(parts_up, parts_down), donc le payout du
                # pire cas : le verrou est deja entame. On le note sur la
                # fenetre pour autoriser le renfort de la jambe survivante
                # (_manage_reinforce). NB : une paire VRAIMENT verrouillee est
                # taggee is_risk_free donc EXEMPTE de SL -- si on arrive ici,
                # c'est par construction qu'il n'y avait pas de verrou a casser.
                mk.setdefault("sl_fired", {})[slug] = time.time()
                if sold < _sl_held - 0.01:
                    # Fill partiel : on comptabilise ce qui est reellement sorti
                    # et on garde le reste en position pour retenter au prochain
                    # cycle, au lieu de cloturer un trade a moitie execute.
                    pos["realized_pnl"] = round(
                        pos.get("realized_pnl", 0.0)
                        + sold * (bid - pos["entry_price"]),
                        3,
                    )
                    pos["filled_shares"] = round(_sl_held - sold, 2)
                    self._log(
                        f"⚠️ [SL][REEL] {sym} {slug} {pos['side']} fill partiel -> "
                        f"reste {pos['filled_shares']} parts, nouvelle tentative au prochain cycle"
                    )
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
            # Plancher de vente reel = MIN_SELL_SHARES (1 part), pas 5 : voir
            # le commentaire de MIN_SELL_SHARES. Avant, tout palier sous 5
            # parts se transformait en vente totale -> plus aucun etagement.
            if pos["mode"] == "real" and (
                sell_shares < MIN_SELL_SHARES
                or remaining - sell_shares < MIN_SELL_SHARES
            ):
                sell_shares = remaining
                frac = 1.0
            if pos["mode"] == "real":
                book = self._live.get_book_sync(pos["token_id"])
                bid = book["bids"][0][0] if book and book.get("bids") else None
                if bid is None or bid < target_price * 0.9:  # tampon anti-slippage
                    continue
                # FILL VERIFIE (Steven 05/08) : meme correction que le stop-loss
                # ci-dessus -- un GTC accepte n'est pas un GTC execute. Le
                # palier de TP creditait un gain jamais encaisse. _sell_orphan
                # poste en FAK agressif et verifie le fill on-chain ; on
                # comptabilise EXACTEMENT ce qui est sorti (appel hors du
                # _order_lock, qui n'est pas reentrant).
                sold = self._sell_orphan(
                    pos["token_id"], sell_shares,
                    f" {sym} {pos['slug']} {pos['side']} TP{stage + 1}",
                )
                if sold <= 0:
                    continue
                sell_shares = sold
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

    def _live_ask(self, token_id):
        """Prix d'ACHAT courant (meilleur ask du carnet) -- Steven 19/08,
        "si prix achat superieur au prix auquel on a achete = vente"."""
        try:
            book = self._live.get_book_sync(token_id)
            if book:
                asks = book.get("asks") or []
                if asks:
                    return round(asks[0][0], 3)
        except Exception:
            pass
        return None

    _POLY_STATUS_URLS = (
        "https://status.polymarket.com/v3/summary.json",
        "https://polymarket.instatus.com/summary.json",
    )
    _POLY_STATUS_EVERY_S = 60

    def _check_polymarket_status(self):
        """Lit la status page publique de Polymarket (Steven 19/08, "on
        aurait vu que ils sont en plein update") : les prix figes et le
        'trading is disabled' de ce soir venaient d'un incident reel cote
        Polymarket ('Issues with delayed open order read responses',
        cancel-only mode) -- ce log evite de le re-diagnostiquer a la main
        la prochaine fois."""
        now_t = time.time()
        if now_t - getattr(self, "_last_poly_status_check", 0) < self._POLY_STATUS_EVERY_S:
            return
        self._last_poly_status_check = now_t
        import requests as _rq

        data = None
        for url in self._POLY_STATUS_URLS:
            try:
                r = _rq.get(url, timeout=6)
                if r.status_code == 200:
                    data = r.json()
                    break
            except Exception:
                continue
        if not data:
            return
        status = data.get("page", {}).get("status", "UNKNOWN")
        incidents = data.get("activeIncidents", [])
        was_ok = getattr(self, "_last_poly_status_ok", True)
        if status != "UP" or incidents:
            self._last_poly_status_ok = False
            for inc in incidents or [{"name": status, "impact": "?", "status": "?"}]:
                self._log(
                    f"🚧 [POLYMARKET-STATUS] {status} -- {inc.get('name')} "
                    f"(impact={inc.get('impact')}, statut={inc.get('status')}) "
                    f"-> voir status.polymarket.com"
                )
        elif not was_ok:
            self._last_poly_status_ok = True
            self._log("✅ [POLYMARKET-STATUS] retour a UP, plus d'incident actif")

    def _gamma_market(self, slug):
        import requests as _rq
        try:
            r = _rq.get(
                "https://gamma-api.polymarket.com/markets",
                params={"slug": slug}, timeout=8,
            )
            d = r.json()
            return d[0] if isinstance(d, list) and d else None
        except Exception:
            return None

    MANUAL_SCAN_EVERY_S = 20

    def _adopt_manual_positions(self, sym):
        """Detecte les positions ouvertes A LA MAIN sur Polymarket (hors bot)
        et les ajoute a mk['open'] avec strat='manual' (Steven 19/08, "gerer
        meme mes trade manuel pour tp des que possible"). Le bot ne voit
        normalement QUE ce qu'il a lui-meme ouvert -- ceci comble ce trou en
        relisant l'activite on-chain du wallet, comme deja fait cette nuit
        pour auditer TWAP-SNIPER. Throttle a MANUAL_SCAN_EVERY_S : c'est un
        appel HTTP externe, pas question de le faire au rythme fast-exit
        (WS, ~1-2s)."""
        mk = self.state["markets"][sym]
        if mk.get("mode") != "real":
            return
        now_t = time.time()
        if now_t - mk.get("_last_manual_scan", 0) < self.MANUAL_SCAN_EVERY_S:
            return
        mk["_last_manual_scan"] = now_t
        funder = os.environ.get("POLY_FUNDER_ADDRESS", "")
        if not funder:
            return
        import requests as _rq
        try:
            r = _rq.get(
                "https://data-api.polymarket.com/activity",
                params={"user": funder, "limit": 100},
                timeout=8, headers={"User-Agent": "GHOST/3"},
            )
            raw = r.json() if r.status_code == 200 else []
        except Exception:
            return
        if not isinstance(raw, list):
            return
        prefix = f"{sym.lower()}-updown-5m-"
        legs = {}
        for a in raw:
            slug = a.get("slug") or ""
            if not slug.startswith(prefix) or a.get("type") != "TRADE":
                continue
            outcome = a.get("outcome")
            e = legs.setdefault((slug, outcome), {"buy_sh": 0.0, "buy_usd": 0.0, "sell_sh": 0.0})
            sz = float(a.get("size") or 0)
            usdc = float(a.get("usdcSize") or 0)
            if (a.get("side") or "").upper() == "BUY":
                e["buy_sh"] += sz
                e["buy_usd"] += usdc
            else:
                e["sell_sh"] += sz
        for (slug, outcome), e in legs.items():
            key = f"{slug}|{outcome}"
            if key in mk["open"]:
                continue
            if any(t.get("slug") == slug and t.get("side") == outcome for t in mk.get("trades", [])):
                continue
            remaining = round(e["buy_sh"] - e["sell_sh"], 3)
            if remaining < MIN_SELL_SHARES or e["buy_sh"] <= 0:
                continue
            avg = e["buy_usd"] / e["buy_sh"]
            if not (0.005 < avg < 0.995):
                continue
            m = self._gamma_market(slug)
            if not m:
                continue
            try:
                outcomes = json.loads(m.get("outcomes") or "[]")
                token_ids = json.loads(m.get("clobTokenIds") or "[]")
                token_id = token_ids[outcomes.index(outcome)]
            except Exception:
                continue
            try:
                start_ts = int(slug.rsplit("-", 1)[-1])
            except Exception:
                start_ts = int(now_t) - 150
            end_ts = start_ts + 300
            if now_t > end_ts:
                continue  # fenetre deja resolue, trop tard pour un TP
            mk["open"][key] = {
                "symbol": sym, "slug": slug, "side": outcome, "mode": "real",
                "strat": "manual", "token_id": token_id, "entry_price": round(avg, 4),
                "filled_shares": remaining, "cost": round(remaining * avg, 2),
                "start_ts": start_ts, "end_ts": end_ts,
                "opened_ts": now_t, "buffer": 0.0, "is_risk_free": False,
            }
            self._log(
                f"🖐️ [MANUEL] {sym} {slug} {outcome} {remaining} parts @ {avg:.3f} "
                f"detectee (hors bot) -> adoptee, TP instantane actif"
            )

    def _manage_pnl_tier_exits(self, sym):
        """TP/SL PALIERS PNL-BASED V3.2 : sort 25% a +25%, 25% a +50%,
        25% a +75%, laisse 25% runner avec trailing stop.
        Stop loss a -30% du prix d'entree.
        S'applique aux positions bothside/swing (pas orphan, pas ARB pur).
        Les orphans ont leur propre gestion dans _manage_orphans."""
        mk = self.state["markets"][sym]
        try:
            self._adopt_manual_positions(sym)
        except Exception as e:
            self._tlog(f"manual_adopt_err_{sym}", f"💥 [MANUEL] {sym} scan erreur: {e}")
        now = synced_now()
        for key, pos in list(mk["open"].items()):
            # "fav" ajoute (Steven 05/08) : le pari directionnel sur le favori
            # DOIT etre gere ici, c'est toute sa raison d'etre. Sans ca il
            # serait skippe et tiendrait jusqu'a resolution sans TP ni SL --
            # exactement le bug qu'on a corrige partout ailleurs aujourd'hui.
            if pos.get("strat") not in ("bothside", "swing", "fav", "nearcert", "copy", "manual", "overreact", "twaplock", "splitpair"):
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
            # ── JAMBE NUE BON MARCHE = BILLET DE LOTERIE (Steven 06/08) ──
            # La regle existait deja dans _manage_orphans, mais celui-ci ne
            # traite QUE strat=="orphan". Une jambe restee en strat=="bothside"
            # (paire dont la 2e jambe n'a jamais pu se remplir, et dont
            # l'unwind a echoue sans que le strat soit bascule) passe par ICI,
            # ou la regle etait absente -> elle etait tenue jusqu'a resolution.
            # Mesure sur la session : 6 jambes seules sous 0.50, -10.02$ pour
            # 12.32$ engages (ROI -81.4%), soit 74% de la perte totale de la
            # session -- alors que les arbs faisaient +19.5% et le near-certain
            # +3.0% sur la meme periode. 4 de ces 6 jambes n'ont JAMAIS ete
            # revendues. C'est le poste de perte n1, et de loin.
            if (
                pos.get("strat") == "bothside"
                and both_legs_held < 2
                and not pos.get("must_close")
            ):
                _nu_px = self._live_price(pos.get("token_id"), None, pos.get("side"))
                if _nu_px is not None and _nu_px < ORPHAN_KEEP_MIN_PRICE:
                    pos["strat"] = "orphan"
                    pos["must_close"] = True
                    self._tlog(
                        f"nakedcheap_{key}",
                        f"⛔ [JAMBE-NUE-BON-MARCHE] {sym} {slug} {pos.get('side')} "
                        f"@ {_nu_px:.3f} < {ORPHAN_KEEP_MIN_PRICE} et paire jamais completee "
                        f"-> billet de loterie, marquee A FERMER",
                    )
                    continue
            if both_legs_held >= 2 and not SPREAD_EXIT_DISABLED:
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
                                        # NE PAS CASSER LA COUVERTURE (Steven
                                        # 05/08) : c'est exactement ce chemin
                                        # qui a vendu la jambe Down d'ETH et de
                                        # SOL 10-18s apres l'achat, laissant la
                                        # jambe Up a nu -> -8.60$ sur deux
                                        # fenetres. Voir _hedge_would_break.
                                        if self._hedge_would_break(
                                            mk, slug, pos.get("side"),
                                            self._live_price(pos.get("token_id"), None, pos.get("side")),
                                            pos.get("entry_price"),
                                        ):
                                            self._tlog(
                                                f"spreadhedge_{key}",
                                                f"🛡️ [SPREAD-EXIT] {sym} {slug} {pos['side']} "
                                                f"vente ANNULEE : la jambe opposee est encore tenue, "
                                                f"couper celle-ci retirerait la couverture",
                                            )
                                            continue
                                        _shares = pos.get("filled_shares", 0)
                                        if _shares > 0:
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
            if shares <= 0:
                continue
            # RETRAIT DU SKIP MIN_ORDER_SIZE_SHARES (Steven 05/08, preuve
            # directe : "je peux vendre 25/50/75/100% quand je veux" via
            # l'UI Polymarket, + une ancienne version du bot faisait deja
            # du palier sur des trades a 1$/sous 5 parts). Ce plancher est
            # une regle d'ACHAT (taille minimum d'un nouvel ordre), pas de
            # VENTE (fermer une position existante n'a pas cette contrainte
            # cote Polymarket) -- le skip ici excluait A TORT toute gestion
            # TP/SL/trailing sur les positions sous 5 parts (bug trouve en
            # live : jambe a 2.78 parts, peak +168%, jamais prise). La
            # logique plus bas (ligne ~7354, "sell_shares < MIN -> vend
            # tout") gere deja correctement la vente en dessous du plancher.

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

            # ── MARKET MAKING ASYMETRIQUE : ordre de vente GTC pose des le
            # remplissage (Steven 19/08). Complement du TP au marche ci-dessous
            # -- si un acheteur presse croise ce prix, le spread est encaisse
            # sans meme attendre le prochain cycle de scan.
            if pos["mode"] == "real" and not pos.get("_spread_sell_posted") and entry > 0:
                _sc_price = min(0.99, round(entry + SPREAD_CAPTURE_PRICE, 2))
                try:
                    _sc_res = self._live.post_limit_sell(pos["token_id"], _sc_price, shares)
                    pos["_spread_sell_posted"] = True
                    if _sc_res.get("success"):
                        self._log(
                            f"📌 [SPREAD-CAPTURE] {sym} {slug} {pos['side']} ordre vente "
                            f"pose @ {_sc_price:.3f} (entree {entry:.3f}) {shares} parts"
                        )
                except Exception as e:
                    self._tlog(f"spreadcapture_err_{sym}", f"💥 [SPREAD-CAPTURE] {sym} erreur: {e}")

            # ── TP TRAILING : se fie au prix d'ACHAT courant, pas au mid
            # (Steven 19/08), et suit le pic plutot qu'un seuil fixe unique
            # (Steven 01/09, "ca monte 5.6.7.8 puis redescend a 7, on TP a
            # 7"). Arme des TP_TRAIL_ARM_PCT, vend au retracement de
            # TP_TRAIL_GIVEBACK_PCT depuis le pic ; TP_INSTANT_PCT reste un
            # plafond dur pour ne jamais attendre indefiniment.
            _tp_ask = self._live_ask(pos.get("token_id")) if pos["mode"] == "real" else cur
            _tp_pct = ((_tp_ask - entry) / entry) if _tp_ask is not None else None
            _tp_peak = pos.get("_tp_peak_pct", 0.0)
            if _tp_pct is not None and _tp_pct > _tp_peak:
                pos["_tp_peak_pct"] = _tp_peak = _tp_pct
            # PALIER FIXE EN PLUS DU TRAILING (Steven 01/09, "j'aurais du voir
            # 2 lignes de vente a ce stade" -- +52% sans jamais avoir
            # retrace, donc le trailing seul ne se declenchait JAMAIS : le pic
            # colle au prix courant quand ca monte sans redescendre). Les 2
            # conditions cohabitent : un palier fixe (25/50/75%) vend une
            # tranche meme sans repli, ET le retracement vend plus tot si un
            # repli survient avant le prochain palier.
            _tp_stage_now = pos.get("pnl_tp_stage", 0)
            _tp_next_target = (
                PNL_TP_TARGETS[_tp_stage_now] if _tp_stage_now < len(PNL_TP_TARGETS) else TP_INSTANT_PCT
            )
            # FIX (Steven 02/09, "je veux QUE du tp, j'avais ete clair") :
            # le trailing pouvait vendre EN DESSOUS du prix d'achat --
            # arme des +3% (TP_TRAIL_ARM_PCT), un repli brutal pouvait
            # redonner tout le gain ET plus avant que la verification
            # suivante ne rattrape, produisant une vente a PERTE etiquetee
            # "tp_instant" (confirme en prod : entry=0.750 exit=0.620,
            # pnl=-0.143$). Ajoute `_tp_pct > 0` : le trailing protege
            # toujours un gain existant, mais ne peut plus jamais vendre
            # sous le prix d'entree -- ce n'est plus un SL deguise.
            _tp_trigger = _tp_pct is not None and (
                _tp_pct >= TP_INSTANT_PCT
                or _tp_pct >= _tp_next_target
                or (_tp_peak >= TP_TRAIL_ARM_PCT
                    and _tp_pct <= _tp_peak * (1 - TP_TRAIL_GIVEBACK_PCT)
                    and _tp_pct > 0)
            )
            if _tp_trigger:
                exit_price = self._get_bid(pos) if pos["mode"] == "real" else cur
                if exit_price is None:
                    continue
                # ── VENTE EN ESCALIER (Steven 01/09, "pas oblige de tp a
                # 100%, on peut faire en escalier") : vend une fraction
                # (PNL_TP_FRACTIONS) au lieu de tout d'un coup -- le reste
                # continue d'etre suivi, un nouveau pic/retracement doit se
                # former pour la tranche suivante (peak reinitialise apres
                # chaque vente partielle).
                _tp_stage = pos.get("pnl_tp_stage", 0)
                _tp_target = round(init_shares * PNL_TP_FRACTIONS[min(_tp_stage, len(PNL_TP_FRACTIONS) - 1)], 2)
                _tp_target = min(_tp_target, shares)
                if shares - _tp_target < MIN_SELL_SHARES or _tp_stage >= len(PNL_TP_FRACTIONS) - 1:
                    _tp_target = shares  # derniere tranche ou reste sous le plancher -> tout vendre
                sold = _tp_target
                if pos["mode"] == "real":
                    sold = self._sell_orphan(
                        pos["token_id"], _tp_target, f" {sym} {slug} {pos['side']} TP-ESCALIER{_tp_stage + 1}"
                    )
                    if sold <= 0:
                        continue
                realized = round(sold * (exit_price - entry), 3)
                pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
                pos["filled_shares"] = round(shares - sold, 2)
                pos["pnl_tp_stage"] = _tp_stage + 1
                pos["_tp_peak_pct"] = _tp_pct  # nouveau cycle pour la tranche restante
                if pos["filled_shares"] < MIN_SELL_SHARES:
                    pnl = pos["realized_pnl"]
                    pos.update(win=pnl > 0, pnl=pnl, resolved_by="tp_instant", exit_price=round(exit_price, 3))
                    self._record_reversal(sym, pos, min_pct, pnl)
                    if pos["mode"] == "paper":
                        mk["paper_balance"] = round(mk["paper_balance"] + pnl, 3)
                    mk["trades"].append(pos)
                    del mk["open"][key]
                    self._record_trade_pnl(sym, pnl)
                    self._log(
                        f"⚡ [TP-ESCALIER] {sym} {slug} {pos['side']} @ entree {entry:.3f} "
                        f"ask={_tp_ask:.3f} -> DERNIERE tranche vendue @ {exit_price:.3f} pnl={pnl:+.3f}$"
                    )
                else:
                    self._log(
                        f"⚡ [TP-ESCALIER] {sym} {slug} {pos['side']} @ entree {entry:.3f} "
                        f"ask={_tp_ask:.3f} -> tranche {_tp_stage + 1} vendue ({sold} parts) "
                        f"@ {exit_price:.3f}, {pos['filled_shares']} parts restantes sous suivi"
                    )
                self._log_trade_exit(
                    sym, slug, pos["side"], "tp_instant", entry, exit_price,
                    realized, 0, 0, realized, now - pos.get("opened_ts", pos["start_ts"]),
                    "sold", "open",
                )
                continue

            # ── STOP LOSS DESACTIVE (Steven 19/08) ──────────────────────
            # Backtest sur donnees fraiches (235 series, sweep complet -10%
            # a -70%) : AVEC le TP instantane, le SL ne fait plus que couper
            # des positions en cours de retour a la moyenne avant qu'elles
            # n'atteignent le TP. Sans SL : moyenne +3.47%, capital compose
            # 100$->229.70$ sur 235 trades sequentiels. Avec SL -20% (celui
            # qui tournait avant) : moyenne -2.92%, 100$->28.01$ (quasi
            # ruine). Tous les seuils testes (-10% a -70%) sont negatifs,
            # -20% n'est pas un choix malheureux parmi d'autres -- c'est
            # STRUCTUREL : le TP instantane a change la nature du risque,
            # le vieux SL pense pour les paliers 25/50/75% ne s'applique
            # plus. PNL_SL_PCT desactive via TP_INSTANT_SL_DISABLED.
            effective_sl_pct = pos.get("rl_stop_pct", PNL_SL_PCT)
            _sl_breach = pnl_pct <= -effective_sl_pct and stage < len(PNL_TP_TARGETS)
            # CONFIRMATION ANTI-BRUIT (Steven 02/09, "il y a eu du bruit et il
            # a vendu" -- creux passager du carnet, remonte juste apres, mais
            # deja coupe a perte). Le seuil PNL_SL_PCT reste inchange (toujours
            # aussi serre), mais on exige desormais que la perte PERSISTE
            # SL_CONFIRM_S secondes sur des lectures successives avant de
            # vendre -- un simple wick d'une seconde ne declenche plus rien,
            # une vraie chute continue toujours aussi vite qu'avant.
            if not _sl_breach:
                pos.pop("_sl_breach_since", None)
                _sl_confirmed = False
            elif pos.get("_sl_breach_since") is None:
                pos["_sl_breach_since"] = now
                _sl_confirmed = False
            else:
                _sl_confirmed = (now - pos["_sl_breach_since"]) >= SL_CONFIRM_S
            if (not TP_INSTANT_SL_DISABLED) and _sl_confirmed:
                exit_price = self._get_bid(pos) if pos["mode"] == "real" else cur
                if exit_price is None:
                    continue
                # ── SL EN ESCALIER (Steven 01/09, "pas oblige de sl a 100%
                # non plus") : coupe la moitie d'abord, le reste seulement
                # si le prix continue de se degrader au 2e franchissement.
                _sl_stage = pos.get("sl_stage", 0)
                _sl_target = shares if _sl_stage >= 1 else round(shares / 2, 2)
                if shares - _sl_target < MIN_SELL_SHARES:
                    _sl_target = shares
                sold = _sl_target
                if pos["mode"] == "real":
                    sold = self._sell_orphan(
                        pos["token_id"], _sl_target, f" {sym} {slug} {pos['side']} SL-ESCALIER{_sl_stage + 1}"
                    )
                    if sold <= 0:
                        continue
                realized = round(sold * (exit_price - entry), 3)
                pos["realized_pnl"] = round(pos.get("realized_pnl", 0.0) + realized, 3)
                pos["filled_shares"] = round(shares - sold, 2)
                pos["sl_stage"] = _sl_stage + 1
                if pos["filled_shares"] < MIN_SELL_SHARES:
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
                        f"🛑 [SL-ESCALIER] {sym} {slug} {pos['side']} -{abs(pnl_pct) * 100:.1f}% "
                        f"-> DERNIERE tranche coupee @ {exit_price:.3f} pnl={pnl:+.3f}$ [{pos.get('tier', '?')}]"
                    )
                else:
                    self._log(
                        f"🛑 [SL-ESCALIER] {sym} {slug} {pos['side']} -{abs(pnl_pct) * 100:.1f}% "
                        f"-> tranche {_sl_stage + 1} coupee ({sold} parts) @ {exit_price:.3f}, "
                        f"{pos['filled_shares']} parts restantes sous suivi"
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
                    realized,
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
                    # Palier 25/50/75 respecte meme sur petites positions
                    # (Steven 05/08) : bascule "vend tout" seulement sous le
                    # plancher anti-poussiere reel, pas sous les 5 parts
                    # d'achat -- c'est ce qui empechait tout etagement.
                    if sell_shares < MIN_SELL_SHARES or shares - sell_shares < MIN_SELL_SHARES:
                        sell_shares = shares
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
                # Steven 02/09 ("je veux QUE du tp") : le "or pnl_pct <= 0" vendait
                # ce runner meme repasse sous l'entree -- un SL de fait, deguise en
                # trailing. Meme fix que le TP normal (8b0389f) et TP-INSTANT-ORPHAN :
                # ce tranche ne se vend plus jamais si elle n'est plus en gain.
                if pnl_pct > 0 and pnl_pct <= trail_floor_pct:
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
            # Idem RL exit manager (Steven 05/08) : plancher de vente reel.
            if sell_shares < MIN_SELL_SHARES or shares - sell_shares < MIN_SELL_SHARES:
                sell_shares = shares
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
                        # BUG CRITIQUE TROUVE (Steven 02/09, "il enquete sur
                        # les pertes") : "position absente de la liste" n'est
                        # PAS synonyme de "gagnee et redeemee" -- confirme sur
                        # 2 positions reelles (BTC+ETH 1788361500, achat
                        # TWAP-ORACLE @0.01) dont le prix observe s'est
                        # effondre a 0.001 (perdantes, quasi certaines) mais
                        # que ce code marquait quand meme won=True -> +495$
                        # de "gain" credite dans mk['trades'] qui n'a JAMAIS
                        # ete un vrai encaissement (le compteur PNL-REEL-
                        # ONCHAIN, source Polymarket independante, ne montre
                        # que quelques $ de redeems reels sur toute la
                        # session). L'API data-api semble aussi retirer les
                        # positions PERDANTES a valeur nulle de la liste, pas
                        # seulement les gagnantes reclamees -- l'hypothese du
                        # 25/07 ("les perdantes restent a curVal 0") est
                        # fausse dans ce cas. On utilise desormais le DERNIER
                        # PRIX REELLEMENT OBSERVE (price_log, deja collecte en
                        # continu pendant la detention) comme depatageur :
                        # >=0.5 -> probablement gagnee, <0.5 -> probablement
                        # perdue. Fallback sur l'ancien comportement (won=True)
                        # UNIQUEMENT si aucun historique de prix n'existe.
                        _hist = pos.get("price_log") or []
                        _last_px = _hist[-1]["price"] if _hist else None
                        if _last_px is not None:
                            won = _last_px >= 0.5
                        else:
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
            "steven_engine": self.steven_config(),
            "steven_stats": self.steven_stats(),
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
