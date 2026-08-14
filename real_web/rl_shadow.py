"""RL-SHADOW (Steven 14/08) : fait tourner l'agent DQN entraine EN OBSERVATEUR
sur chaque jambe seule MSF, sans jamais executer d'ordre a sa place. Log
uniquement, tag MMBOT -- c'est le "test reel" convenu : voir ce que l'agent
aurait decide face au marche reel, avant de lui laisser un centime.

ENTRAINEMENT : DQN, 1M pas, 3 graines independantes, sur 45h de carnet
6 symboles (128k ticks). Test decisif hors-echantillon (937 fenetres
jamais vues, glissement + latence modelises) : PnL net positif sur les
3 graines, profit factor 3.5-4.25 contre 2.94 pour l'heuristique fidele
du bot reel, drawdown 23-30$ contre 43$. Voir C:/.../MAKEMONEY/rl/ pour
le detail complet (dataset.py, env.py, decisive_test.py, multi_seed_test.py).

Zero dependance externe (ni torch ni numpy sur ce serveur) : le reseau
(2 couches cachees de 128, ReLU) est reexecute ici en Python pur a partir
des poids exportes dans rl_qnet_weights.json.
"""
import json
import math
import os

_POIDS = None
_ACTIONS = ("NOOP", "ENTER_UP", "ENTER_DOWN", "COMPLETE", "SELL_MARKET",
            "HOLD_TO_RESOLUTION")

FEE_VENTE = 0.062
CUTOFF_S_DEFAUT = 75


def _charge_poids():
    global _POIDS
    if _POIDS is not None:
        return _POIDS
    p = os.path.join(os.path.dirname(__file__), "rl_qnet_weights.json")
    try:
        with open(p, encoding="utf-8") as f:
            _POIDS = json.load(f)
    except Exception:
        _POIDS = False  # echec permanent, on ne retente pas a chaque appel
    return _POIDS


def _relu(v):
    return [x if x > 0 else 0.0 for x in v]


def _lin(x, W, b):
    return [sum(w * xi for w, xi in zip(row, x)) + bi for row, bi in zip(W, b)]


def _forward(obs):
    poids = _charge_poids()
    if not poids:
        return None
    x = obs
    for i, layer in enumerate(poids["layers"]):
        x = _lin(x, layer["W"], layer["b"])
        if i < len(poids["layers"]) - 1:
            x = _relu(x)
    return x  # 6 valeurs Q, une par action


def _logit(p):
    p = min(max(p, 1e-3), 1 - 1e-3)
    return math.log(p / (1 - p))


def observation(ub, ua, db, da, ubd, uad, dbd, dad, dt, cutoff_s,
                 side, entry, completed):
    """Reproduit EXACTEMENT l'encodage utilise a l'entrainement
    (rl/env.py::_observe). Tout defaut de prix est neutre a 0.5, toute
    profondeur absente est neutre a 0 -- fail-open, jamais bloquant."""
    ub = ub if ub is not None else 0.5
    ua = ua if ua is not None else 0.5
    db = db if db is not None else 0.5
    da = da if da is not None else 0.5
    ubd = math.log1p(ubd or 0.0)
    uad = math.log1p(uad or 0.0)
    dbd = math.log1p(dbd or 0.0)
    dad = math.log1p(dad or 0.0)
    dt_norm = dt / 300.0
    time_left = max(0.0, 300.0 - cutoff_s - dt) / 300.0
    pos_side = 0.0 if side is None else (1.0 if side == "Up" else -1.0)
    upnl = 0.0
    if side == "Up" and entry is not None:
        upnl = (ub - entry) - FEE_VENTE * min(ub, 1 - ub)
    elif side == "Down" and entry is not None:
        upnl = (db - entry) - FEE_VENTE * min(db, 1 - db)
    return [
        _logit(ub), _logit(ua), _logit(db), _logit(da),
        ubd, uad, dbd, dad, dt_norm, time_left,
        pos_side, entry or 0.0, upnl,
        1.0 if completed else 0.0,
    ]


def decide(obs):
    """Rend (action_str, q_values) ou (None, None) si les poids sont
    indisponibles (fail-open : n'affecte jamais le comportement reel)."""
    q = _forward(obs)
    if q is None:
        return None, None
    a = max(range(len(q)), key=lambda i: q[i])
    return _ACTIONS[a], q
