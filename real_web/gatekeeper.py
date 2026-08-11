"""GATEKEEPER MSF -- apprentissage en mode OMBRE, Python pur.

LA QUESTION POSEE AU MODELE (reformulee le 11/08 apres mesure) :

    >> une jambe vient d'etre servie. L'autre va-t-elle suivre avant la
       fin de la fenetre, ou va-t-on rester avec une jambe seule ? <<

POURQUOI CETTE QUESTION ET PAS "faut-il poser ?" : poser ne coute RIEN --
un ordre non servi ne produit aucun evenement on-chain, donc aucun frais
(verifie). Ce qui coute, c'est d'etre servi d'UN SEUL cote. La decision
economiquement utile se prend donc au moment du premier remplissage, pas
a l'ouverture. Mesure sur 586 fenetres reelles, frais de sortie inclus :
    verrou      : +0.300 $/part (garanti)
    jambe seule : -0.249 $/part (moyenne)

La formulation precedente ("cette fenetre va-t-elle croiser ?") a ete
abandonnee pour deux raisons mesurees, pas theoriques :
  - a l'ouverture (t<=60s) l'AUC plafonnait a 0.47, soit le hasard ;
  - en observant plus longtemps l'AUC montait a 0.60, MAIS les
    caracteristiques encodaient alors une partie de l'etiquette (une
    marge <= 0 signifie "ce cote a deja touche"), et a t=180s MSF ne peut
    de toute facon quasiment plus poser. C'etait de la fuite, pas de la
    prediction.

JEU DE CARACTERISTIQUES VOLONTAIREMENT PETIT : 11 variables sur ~200
exemples etaient largement sur-parametrees. Mesure a protocole identique :
complet (11 var.) AUC 0.486 | resserre (4) 0.559 | minimal (2) 0.581.
On garde le resserre -- meilleur compromis entre pouvoir explicatif et
sur-ajustement, et on reevaluera quand le volume aura triple.

AUCUNE DEPENDANCE EXTERNE (ni numpy ni sklearn) : ce module tourne dans le
process du bot de trading, on n'y ajoute pas 200 Mo de librairies pour une
regression logistique de 40 lignes.

MODE OMBRE STRICT : ce module ne decide RIEN. Il apprend, se note, produit
une generation numerotee, et s'arrete la. Le branchement eventuel sur les
decisions de trading est une decision humaine separee.
"""
import json
import math
import time
from collections import defaultdict

PRIX_MSF = 0.35
FENETRE_S = 300

GAIN_VERROU = 0.300
COUT_JAMBE_SEULE = -0.249
# COUT REEL DE COUPER TOUT DE SUITE (Steven 11/08). Mesure sur 324 jambes
# seules reelles (tapes historiques, frais taker inclus) : vendre des le
# remplissage coute -0.0735 $/part en moyenne (mediane -0.065).
# CORRECTION MAJEURE : l'evaluation attribuait 0.00 a cette action, ce qui
# rendait l'abstention gratuite et faussait TOUTES les comparaisons -- un
# filtre qui coupait au hasard paraissait excellent. Ici la jambe est DEJA
# servie : ne rien faire n'existe pas, il faut choisir entre attendre et
# sortir, et les deux ont un cout.
COUT_COUPE_IMMEDIATE = -0.0735
# Point d'indifference : attendre vaut mieux que couper tant que
#   p*0.300 + (1-p)*(-0.249) > -0.0735  soit  p > 0.320.
# Le taux de base mesure est 37.6% -> attendre est le bon defaut, et le
# modele n'a de valeur que s'il sait reperer les cas sous 32%.
#   p*G + (1-p)*C_solo > C_coupe  <=>  p > (C_coupe - C_solo) / (G - C_solo)
SEUIL_INDIFFERENCE = round((COUT_COUPE_IMMEDIATE - COUT_JAMBE_SEULE)
                           / (GAIN_VERROU - COUT_JAMBE_SEULE), 4)

MIN_FENETRES = 300
MIN_EVENEMENTS = 60
N_PLIS = 4

# resserre : bat le jeu complet ET le minimal sur l'echantillon actuel
FEATURES = ["autre_marge", "autre_delta", "temps_restant", "t_premier_fill"]


# ── construction du jeu ────────────────────────────────────────────────────
def _open_ts(slug):
    try:
        return int(str(slug).rsplit("-", 1)[-1])
    except (ValueError, AttributeError):
        return None


def construire(rows, veille_seulement=True, duree="5m"):
    """Un exemple par fenetre OU une premiere jambe a ete servie.

    Caracteristiques : etat du carnet a l'instant EXACT de ce premier
    remplissage -- toute l'information est disponible a ce moment-la, aucune
    fuite depuis le futur.
    Etiquette : l'AUTRE cote finit-il par toucher lui aussi (verrou) ou
    reste-t-on avec une jambe seule (le cout) ?

    `duree` : le modele est entraine sur UNE duree a la fois. Toute
    l'economie (+0.300 / -0.249, pose a 0.35, fenetre 300 s) est mesuree
    sur le 5 minutes ; melanger du 15m ou du 4h y injecterait des exemples
    dont l'etiquette ne veut pas dire la meme chose."""
    if veille_seulement:
        rows = [r for r in rows if r.get("source") == "veille"]
    if duree:
        # tolere les releves d'avant l'ajout du champ (ils sont tous 5m)
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
        # premier instant ou chaque cote passe sous le prix de pose
        t_touche = {}
        for c in cotes:
            t = None
            for r in rs:
                if (r.get("side") == c and r.get("ask_top") is not None
                        and r["ask_top"] <= PRIX_MSF and r.get("ts", 0) <= op + FENETRE_S):
                    t = r["ts"]
                    break
            t_touche[c] = t
        servis = {c: t for c, t in t_touche.items() if t is not None}
        if not servis:
            continue                      # aucune jambe servie : rien a decider
        c_premier = min(servis, key=lambda c: servis[c])
        t0 = servis[c_premier]
        c_autre = [c for c in cotes if c != c_premier][0]
        t_autre = t_touche.get(c_autre)
        y = 1 if (t_autre is not None and t_autre >= t0) else 0

        # etat de l'AUTRE cote au moment du premier remplissage : c'est lui
        # qu'on attend, donc c'est lui qu'il faut decrire.
        hist = [r for r in rs if r.get("side") == c_autre and r.get("ts", 0) <= t0
                and r.get("ask_top") is not None]
        if not hist:
            continue
        der = hist[-1]
        ex.append({
            "symbol": sym, "slug": slug, "open_ts": op, "y": y,
            "autre_marge": round(der["ask_top"] - PRIX_MSF, 4),
            "autre_delta": round(der["ask_top"] - hist[0]["ask_top"], 4),
            "autre_ask": der.get("ask_top"),
            "autre_spread": der.get("spread"),
            "autre_prof_ask": der.get("ask_depth_top3"),
            "autre_desequilibre": der.get("imbalance_bid_pct"),
            "temps_restant": round(op + FENETRE_S - t0, 1),
            "t_premier_fill": round(t0 - op, 1),
            "danger": rs[0].get("danger"),
            "heure_utc": rs[0].get("hour_utc"),
        })
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
    """$/cas d'une politique, la jambe etant DEJA servie.
    decision=1 -> on ATTEND l'autre jambe ; 0 -> on COUPE tout de suite.
    Les deux ont un cout : ne rien faire n'existe pas a ce stade."""
    if not y:
        return 0.0
    t = 0.0
    for yi, d in zip(y, decision):
        if d:
            t += GAIN_VERROU if yi == 1 else COUT_JAMBE_SEULE
        else:
            t += COUT_COUPE_IMMEDIATE
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
            # les deux politiques simples : tout attendre (comportement
            # actuel du bot) et tout couper. Le modele doit battre LES DEUX.
            "gain_sans": round(gain(yte, [1] * len(yte)), 4),
            "gain_coupe_tout": round(gain(yte, [0] * len(yte)), 4),
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
    out["gain_coupe_tout"] = round(sum(r["gain_coupe_tout"] for r in res) / nb, 4)
    out["seuil_indifference"] = SEUIL_INDIFFERENCE
    # VERDICT EXIGEANT (corrige apres un faux positif en recette) : il ne
    # suffit pas de battre "poser sur tout" -- quand le taux de croisement
    # est sous le point mort, s'abstenir au hasard y suffit deja. Le modele
    # doit battre le TEMOIN ALEATOIRE de meme volume, gagner de l'argent
    # dans l'absolu, et discriminer reellement (AUC).
    # Le modele doit battre TOUTES les politiques simples : attendre a chaque
    # fois (comportement actuel), couper a chaque fois, et couper au hasard
    # du meme volume. Plus une AUC qui prouve une vraie discrimination.
    # On NE demande plus gain>0 dans l'absolu : la jambe est deja servie,
    # toutes les issues sont couteuses -- exiger un gain positif reviendrait
    # a exiger l'impossible. Ce qui compte est de PERDRE MOINS.
    out["verdict"] = "gain" if (
        out["gain_filtre"] > out["gain_alea"]
        and out["gain_filtre"] > out["gain_sans"]
        and out["gain_filtre"] > out["gain_coupe_tout"]
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
