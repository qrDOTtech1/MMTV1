"""ENGINEBTB3 (Steven 04/08) -- squelette, PAS branche sur le trading reel.

Structure par couches selon ENGINEBTB3_SPEC.txt (racine du projet) :
  market_data.py -> feeds, book, ingestion
  signals.py     -> detection edge, scoring
  execution.py   -> ordres, cancel/replace, signing
  risk.py        -> limites, kill-switch de cette couche
  review.py      -> post-trade review
  benchmark.py   -> comparaison perf vs traders/regions/strategies
  config.py      -> parametres, mode paper/live

STATUT ACTUEL : PAPER uniquement, AUCUNE logique reelle. Chaque module
est un stub explicite -- pas de fausse promesse de fonctionnalite.
Avant d'ajouter une vraie brique, lire ENGINEBTB3_SPEC.txt en entier.
"""

STATUS = "paper"
ACTIVE = False
"""tant que ACTIVE=False, aucun code de ce package ne doit jamais poster
d'ordre ni engager de capital, meme en mode paper -- ce flag est le seul
interrupteur global, verifie explicitement partout ou une action serait
prise (voir execution.py)."""
