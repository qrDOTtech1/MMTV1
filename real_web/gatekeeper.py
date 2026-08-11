"""GATEKEEPER MSF -- apprentissage en mode OMBRE, Python pur.

Prédit, à l'ouverture d'une fenêtre, si elle va CROISER (les deux côtés
descendre à <= MAKER_OPEN_PRICE, donc verrou possible) ou finir en jambe
seule. C'est la seule décision qui compte économiquement -- mesuré sur 586
fenêtres réelles, frais de sortie inclus :
    verrou      : +0.300 $/part (garanti)
    jambe seule : -0.249 $/part (moyenne)
    ne rien poser : 0.00

AUCUNE DÉPENDANCE EXTERNE (ni numpy ni sklearn) : ce module tourne dans le
process du bot de trading, on n'y ajoute pas 200 Mo de librairies pour une
régression logistique de 40 lignes.

MODE OMBRE STRICT : ce module ne décide RIEN. Il apprend, se note, produit
une génération numérotée, et s'arrête là. Le branchement éventuel sur les
décisions de trading est une décision humaine séparée.
"""
import json
import math
import time
from collections import defaultdict

PRIX_MSF = 0.35
FENETRE_S = 300
DELAI_DECISION_S = 60

GAIN_VERROU = 0.300
COUT_JAMBE_SEULE = -0.249

MIN_FENETRES = 300
MIN_EVENEMENTS = 60
N_PLIS = 4

FEATURES = [
    "c0_ask", "c0_bid", "c0_spread", "c0_prof_bid", "c0_prof_ask", "c0_desequilibre",
    "c1_ask", "c1_bid", "c1_spread", "c1_prof_bid", "c1_prof_ask", "c1_desequilibre",
    "combine_ask", "heure_utc", "jour_sem", "danger",
]


# ── construction du jeu ────────────────────────────────────────────────────
def _open_ts(slug):
    try:
        return int(str(slug).rsplit("-", 1)[-1])
    except (ValueError, AttributeError):
        return None


def construire(rows, veille_seulement=True, duree="5m"):
    """Une ligne par fenêtre : caractéristiques à l'ouverture + étiquette.

    `duree` : le modèle est entraîné sur UNE durée à la fois. Toute
    l'économie ci-dessus (+0.300 / -0.249, prix de pose 0.35, fenêtre de
    300 s) est mesurée sur les marchés 5 minutes ; mélanger des fenêtres
    15m ou 4h y injecterait des exemples dont l'étiquette ne veut pas dire
    la même chose. Les autres durées sont collectées pour être analysées
    séparément, pas pour nourrir ce modèle-ci."""
    if veille_seulement:
        rows = [r for r in rows if r.get("source") == "veille"]
    if duree:
        # tolère les relevés d'avant l'ajout du champ (ils sont tous 5m)
        rows = [r for r in rows if r.get("duree", "5m") == duree]
    par_fen = defaultdict(list)
    for r in rows:
        op = _open_ts(r.get("slug"))
        if op is None:
            continue
        par_fen[(r.get("symbol"), r.get("slug"), op)].append(r)

    ex = []
    for (sym, slug, op), rs in par_fen.items():
        rs.sort(key=lambda r: r.get("ts", 0))
        cotes = sorted({r.get("side") for r in rs if r.get("side")})
        if len(cotes) < 2:
            continue
        touche = {}
        for c in cotes:
            asks = [r["ask_top"] for r in rs
                    if r.get("side") == c and r.get("ask_top") is not None
                    and r.get("ts", 0) <= op + FENETRE_S]
            touche[c] = bool(asks) and min(asks) <= PRIX_MSF
        if len(touche) < 2:
            continue
        y = 1 if all(touche.values()) else 0

        feats, ok = {}, True
        for i, c in enumerate(cotes[:2]):
            prem = next((r for r in rs if r.get("side") == c
                         and r.get("ts", 0) <= op + DELAI_DECISION_S), None)
            if prem is None:
                ok = False
                break
            p = "c%d" % i
            feats[p + "_ask"] = prem.get("ask_top")
            feats[p + "_bid"] = prem.get("bid_top")
            feats[p + "_spread"] = prem.get("spread")
            feats[p + "_prof_bid"] = prem.get("bid_depth_top3")
            feats[p + "_prof_ask"] = prem.get("ask_depth_top3")
            feats[p + "_desequilibre"] = prem.get("imbalance_bid_pct")
        if not ok:
            continue
        feats["heure_utc"] = rs[0].get("hour_utc")
        feats["jour_sem"] = rs[0].get("dow")
        feats["danger"] = rs[0].get("danger")
        if feats.get("c0_ask") is not None and feats.get("c1_ask") is not None:
            feats["combine_ask"] = round(feats["c0_ask"] + feats["c1_ask"], 4)
        ex.append(dict(symbol=sym, slug=slug, open_ts=op, y=y, **feats))
    ex.sort(key=lambda e: e["open_ts"])
    return ex


def matrice(ex):
    X, y = [], []
    for e in ex:
        ligne = [e.get(f) for f in FEATURES]
        if any(v is None for v in ligne):
            continue
        X.append([float(v) for v in ligne])
        y.append(int(e["y"]))
    return X, y


# ── régression logistique (descente de gradient, L2, classes pondérées) ────
def _standardise(X):
    n, d = len(X), len(X[0])
    moy = [sum(r[j] for r in X) / n for j in range(d)]
    ect = []
    for j in range(d):
        v = sum((r[j] - moy[j]) ** 2 for r in X) / max(1, n - 1)
        ect.append(math.sqrt(v) or 1.0)
    return moy, ect


def _applique(X, moy, ect):
    return [[(r[j] - moy[j]) / ect[j] for j in range(len(r))] for r in X]


def _sig(z):
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z)) if z < 700 else 1.0
    e = math.exp(z) if z > -700 else 0.0
    return e / (1.0 + e)


def entraine(X, y, iters=600, lr=0.15, l2=1e-3):
    """Retourne (poids, biais). Classes pondérées : sans ça le modèle
    apprend à toujours répondre la classe majoritaire quand le taux de
    croisement s'éloigne de 50%."""
    n, d = len(X), len(X[0])
    n1 = sum(y) or 1
    n0 = (n - sum(y)) or 1
    w1, w0 = n / (2.0 * n1), n / (2.0 * n0)
    w = [0.0] * d
    b = 0.0
    for _ in range(iters):
        gw = [0.0] * d
        gb = 0.0
        for i in range(n):
            p = _sig(sum(w[j] * X[i][j] for j in range(d)) + b)
            poids = w1 if y[i] == 1 else w0
            err = (p - y[i]) * poids
            for j in range(d):
                gw[j] += err * X[i][j]
            gb += err
        for j in range(d):
            w[j] -= lr * (gw[j] / n + l2 * w[j])
        b -= lr * (gb / n)
    return w, b


def predit(X, w, b):
    return [_sig(sum(w[j] * r[j] for j in range(len(r))) + b) for r in X]


def auc(y, p):
    pos = [pi for pi, yi in zip(p, y) if yi == 1]
    neg = [pi for pi, yi in zip(p, y) if yi == 0]
    if not pos or not neg:
        return float("nan")
    gagne = egal = 0
    for a in pos:
        for bb in neg:
            if a > bb:
                gagne += 1
            elif a == bb:
                egal += 1
    return (gagne + 0.5 * egal) / (len(pos) * len(neg))


def _pseudo_alea(graine, n):
    """Suite pseudo-aleatoire deterministe (pas de random global : ce module
    tourne dans le process du bot, on ne touche pas a son etat)."""
    out = []
    x = (graine * 1103515245 + 12345) & 0x7FFFFFFF
    for _ in range(n):
        x = (x * 1103515245 + 12345) & 0x7FFFFFFF
        out.append(x / 0x7FFFFFFF)
    return out


def gain(y, decision):
    """$/fenêtre d'une politique. decision=1 -> on pose, 0 -> on s'abstient."""
    if not y:
        return 0.0
    t = 0.0
    for yi, d in zip(y, decision):
        if d:
            t += GAIN_VERROU if yi == 1 else COUT_JAMBE_SEULE
    return t / len(y)


# ── évaluation en chaîne ───────────────────────────────────────────────────
def evalue(X, y, n_plis=N_PLIS):
    n = len(y)
    taille = n // (n_plis + 1)
    res = []
    if taille < 10:
        return res
    for k in range(1, n_plis + 1):
        i_tr, i_te = k * taille, min((k + 1) * taille, n)
        Xtr, ytr = X[:i_tr], y[:i_tr]
        Xte, yte = X[i_tr:i_te], y[i_tr:i_te]
        if len(set(ytr)) < 2 or len(yte) < 10:
            continue
        moy, ect = _standardise(Xtr)
        w, b = entraine(_applique(Xtr, moy, ect), ytr)
        ptr = predit(_applique(Xtr, moy, ect), w, b)
        pte = predit(_applique(Xte, moy, ect), w, b)
        # seuil calibré sur le TRAIN uniquement
        best_s, best_g = 0.5, -9.9
        s = 0.30
        while s <= 0.80:
            g = gain(ytr, [1 if q >= s else 0 for q in ptr])
            if g > best_g:
                best_g, best_s = g, s
            s += 0.05
        dec = [1 if q >= best_s else 0 for q in pte]
        part = sum(dec) / len(dec)
        # TEMOIN INDISPENSABLE (piege attrape au test de recette) : quand le
        # taux de croisement est sous le point mort (45.4% avec +0.300 /
        # -0.249), "poser sur tout" est PERDANT -- donc n'importe quel filtre
        # qui pose moins souvent le bat, sans rien avoir compris. On compare
        # donc aussi a un filtre ALEATOIRE posant la MEME proportion : c'est
        # la seule facon d'isoler une vraie discrimination d'une simple
        # reduction de volume.
        temoin = []
        for essai in range(24):
            rnd = _pseudo_alea(k * 1000 + essai, len(yte))
            seuil_r = sorted(rnd)[int(part * len(rnd))] if 0 < part < 1 else (1.1 if part == 0 else -0.1)
            temoin.append(gain(yte, [1 if q < seuil_r else 0 for q in rnd]))
        res.append({
            "pli": k, "n_test": len(yte), "auc": auc(yte, pte), "seuil": round(best_s, 2),
            "gain_filtre": round(gain(yte, dec), 4),
            "gain_sans": round(gain(yte, [1] * len(yte)), 4),
            "gain_alea": round(sum(temoin) / len(temoin), 4),
            "part_posee": round(part, 3),
        })
    return res


def cycle(rows, generation, force=False):
    """Produit UNE génération. Retourne un dict complet (jamais None) --
    même quand il n'y a pas assez de données, on veut une trace datée."""
    ex = construire(rows)
    X, y = matrice(ex)
    n = len(y)
    n1 = sum(y)
    n0 = n - n1
    span_h = round((ex[-1]["open_ts"] - ex[0]["open_ts"]) / 3600, 2) if len(ex) > 1 else 0.0
    par_sym = {}
    for e in ex:
        par_sym[e["symbol"]] = par_sym.get(e["symbol"], 0) + 1

    out = {
        "generation": generation,
        "ts": round(time.time(), 1),
        "n_fenetres": n, "n_croisantes": n1, "n_non_croisantes": n0,
        "couverture_h": span_h, "par_symbole": par_sym,
        "mode": "ombre",
    }
    assez = n >= MIN_FENETRES and n1 >= MIN_EVENEMENTS and n0 >= MIN_EVENEMENTS
    if not assez and not force:
        out["statut"] = "accumulation"
        out["manque"] = {
            "fenetres": max(0, MIN_FENETRES - n),
            "croisantes": max(0, MIN_EVENEMENTS - n1),
            "non_croisantes": max(0, MIN_EVENEMENTS - n0),
        }
        return out

    res = evalue(X, y)
    if not res:
        out["statut"] = "plis_insuffisants"
        return out
    nb = len(res)
    aucs = [r["auc"] for r in res if r["auc"] == r["auc"]]
    out["statut"] = "entraine_sur_donnees_insuffisantes" if not assez else "entraine"
    out["plis"] = res
    out["auc_moyen"] = round(sum(aucs) / len(aucs), 4) if aucs else None
    out["gain_filtre"] = round(sum(r["gain_filtre"] for r in res) / nb, 4)
    out["gain_sans"] = round(sum(r["gain_sans"] for r in res) / nb, 4)
    out["gain_alea"] = round(sum(r["gain_alea"] for r in res) / nb, 4)
    # VERDICT EXIGEANT (corrige apres un faux positif en recette) : il ne
    # suffit pas de battre "poser sur tout" -- quand le taux de croisement
    # est sous le point mort, s'abstenir au hasard y suffit deja. Le modele
    # doit battre le TEMOIN ALEATOIRE de meme volume, gagner de l'argent
    # dans l'absolu, et discriminer reellement (AUC).
    out["verdict"] = "gain" if (
        out["gain_filtre"] > out["gain_alea"]
        and out["gain_filtre"] > out["gain_sans"]
        and out["gain_filtre"] > 0
        and (out["auc_moyen"] or 0) >= 0.55
    ) else "pas_de_gain"
    # le modèle final est ré-entraîné sur TOUT, mais n'est utilisé par
    # personne tant que le verdict n'est pas validé sur plusieurs générations
    moy, ect = _standardise(X)
    w, b = entraine(_applique(X, moy, ect), y)
    out["modele"] = {"features": FEATURES, "moy": [round(v, 6) for v in moy],
                     "ect": [round(v, 6) for v in ect],
                     "poids": [round(v, 6) for v in w], "biais": round(b, 6)}
    return out
