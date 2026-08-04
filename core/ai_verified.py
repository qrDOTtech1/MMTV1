"""Sniper IA VÉRIFIÉ (idée Steven) : l'IA ne décide QUE sur des faits VÉRIFIABLES.

Elle lit des PREUVES — statut ESPN du match (décidé ou en cours) + news récentes —
et rend un SCORE DE CONFIANCE calibré, un résumé, et un drapeau 'grounded' (sa
décision s'appuie-t-elle sur un fait factuel ou une supposition ?).

Ce n'est PAS de la prédiction de marché (aucun edge là-dessus) : c'est de
l'AGRÉGATION de faits vérifiables, comme crypto_snipe/weather_snipe mais pour les
cas messy que le code en dur ne sait pas lire. On ne trade que si confiance haute
ET grounded, en mise plafonnée, et on MESURE sa calibration (tag 'ai_verified').
"""

import json
import re

import requests

from core.news_ai import _get_openrouter_key, search_news, MODELS, OPENROUTER_URL
from core.livescores import (get_inplay_games, get_decided_games,
                             match_polymarket_question, _norm)


_DECISION_PROMPT = """Tu es un analyste rigoureux. Décide UNIQUEMENT à partir des PREUVES VÉRIFIABLES ci-dessous — PAS de tes connaissances générales (qui peuvent être datées ou fausses).

QUESTION (marché binaire, YES = la réponse à la question est "oui") : {question}
PRIX ACTUEL DU MARCHÉ (probabilité implicite YES) : {price:.0f}%

PREUVES VÉRIFIABLES :
{ev_block}

Réponds STRICTEMENT en JSON, rien d'autre :
{{"side": "YES"|"NO", "confidence": <0.0 à 1.0>, "summary": "<1 phrase citant la preuve>", "grounded": true|false, "sl_pct": <0.05 à 0.40>}}

Règles :
- confidence HAUTE (>0.85) UNIQUEMENT si une preuve FACTUELLE tranche clairement (ex. ESPN dit le match fini + vainqueur connu).
- grounded=true SEULEMENT si ta décision s'appuie sur un fait des preuves (score, résultat officiel), pas une supposition.
- Si les preuves sont ambiguës ou hors-sujet : confidence basse et grounded=false.
- sl_pct = stop-loss que TU choisis : résultat DÉJÀ CERTAIN -> sl large (0.30-0.40, on tient). Incertain -> sl serré (0.08-0.15). C'est TA gestion du risque."""


def _ask_json(model: str, prompt: str, key: str) -> dict | None:
    """Appelle un modèle et renvoie le 1er objet JSON de sa réponse (ou None)."""
    try:
        r = requests.post(
            OPENROUTER_URL,
            headers={"Authorization": f"Bearer {key}"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.1},
            timeout=45,
        )
        if r.status_code != 200:
            return None
        content = r.json()["choices"][0]["message"]["content"]
        mt = re.search(r"\{.*\}", content, re.DOTALL)
        return json.loads(mt.group(0)) if mt else None
    except Exception:
        return None


def gather_evidence(question: str) -> list[str]:
    """Rassemble des faits VÉRIFIABLES sur la question (ESPN d'abord, puis news)."""
    ev = []
    try:
        mm = match_polymarket_question(question, get_decided_games())
        if mm:
            ev.append(f"ESPN (officiel) : match TERMINÉ — vainqueur {mm['winner']}, perdant {mm['loser']}.")
    except Exception:
        pass
    try:
        qn = _norm(question)
        for g in get_inplay_games():
            if _norm(g["p1"]) in qn and _norm(g["p2"]) in qn:
                ev.append(f"ESPN (en direct) : {g['p1']} {g['s1']} — {g['s2']} {g['p2']} (en cours).")
                break
    except Exception:
        pass
    try:
        for n in search_news(question, max_results=4):
            ev.append(f"News : {n['title']} — {(n.get('body') or '')[:150]}")
    except Exception:
        pass
    return ev


def evaluate(question: str, market_price_yes: float) -> dict | None:
    """L'IA lit les preuves et rend {side, confidence, summary, grounded, model}.
    None si pas de clé, pas de preuve, ou réponse inexploitable."""
    key = _get_openrouter_key()
    if not key:
        return None
    evidence = gather_evidence(question)
    if not evidence:
        return None
    ev_block = "\n".join(f"- {e}" for e in evidence)
    prompt = _DECISION_PROMPT.format(question=question, price=market_price_yes * 100, ev_block=ev_block)

    for model in MODELS:
        p = _ask_json(model, prompt, key)
        if not p:
            continue
        conf = 0.0
        try:
            conf = float(p.get("confidence", 0))
        except Exception:
            continue
        side = str(p.get("side", "")).upper()
        if side not in ("YES", "NO") or not (0.0 <= conf <= 1.0):
            continue
        try:
            sl = min(0.40, max(0.05, float(p.get("sl_pct", 0.15))))
        except Exception:
            sl = 0.15
        return {
            "side": side, "confidence": conf, "summary": p.get("summary", ""),
            "grounded": bool(p.get("grounded", False)), "sl_pct": sl,
            "model": model.split("/")[-1].replace(":free", ""), "n_evidence": len(evidence),
        }
    return None


def evaluate_consensus(question: str, market_price_yes: float, n_models: int = 3) -> dict | None:
    """② CONSENSUS MULTI-MODÈLES (idée Steven) : interroge les n_models premiers de
    la cascade et ne tranche QUE s'ils sont d'accord (même camp + tous grounded).
    Divergence = incertitude = None. Un ensemble qui tue l'hallucination d'un seul.
    Retourne {side, confidence, sl_pct, summary, models, n_agree} ou None."""
    key = _get_openrouter_key()
    if not key:
        return None
    evidence = gather_evidence(question)
    if not evidence:
        return None
    ev_block = "\n".join(f"- {e}" for e in evidence)
    prompt = _DECISION_PROMPT.format(question=question, price=market_price_yes * 100, ev_block=ev_block)

    votes = []
    for model in MODELS[:max(2, n_models)]:
        v = _ask_json(model, prompt, key)
        if not v:
            continue
        side = str(v.get("side", "")).upper()
        try:
            conf = float(v.get("confidence", 0))
        except Exception:
            continue
        if side not in ("YES", "NO") or not (0 <= conf <= 1) or not bool(v.get("grounded", False)):
            continue
        try:
            sl = min(0.40, max(0.05, float(v.get("sl_pct", 0.15))))
        except Exception:
            sl = 0.15
        votes.append({"side": side, "conf": conf, "sl": sl, "summary": v.get("summary", ""),
                      "model": model.split("/")[-1].replace(":free", "")})

    if len(votes) < 2:
        return None  # pas assez d'avis grounded pour un consensus
    yes = [v for v in votes if v["side"] == "YES"]
    no = [v for v in votes if v["side"] == "NO"]
    win = yes if len(yes) > len(no) else no if len(no) > len(yes) else None
    if not win or len(win) < 2:          # exige >=2 d'accord ET une majorité claire
        return None
    return {
        "side": win[0]["side"],
        "confidence": round(sum(v["conf"] for v in win) / len(win), 3),
        "sl_pct": min(v["sl"] for v in win),   # le stop le plus prudent
        "summary": win[0]["summary"],
        "models": [v["model"] for v in win],
        "n_agree": len(win),
        "n_votes": len(votes),
    }


def reevaluate(question: str, side: str, entry_price: float, cur_price: float) -> dict | None:
    """BOUCLE DE SURVEILLANCE (idée Steven) : l'IA re-regarde une position ouverte
    avec des PREUVES FRAÎCHES et décide HOLD ou EXIT. Retourne {action, confidence,
    summary} ou None. Utilisé périodiquement par le moteur pour gérer ses positions IA."""
    key = _get_openrouter_key()
    if not key:
        return None
    evidence = gather_evidence(question)
    if not evidence:
        return None
    ev_block = "\n".join(f"- {e}" for e in evidence)
    move = (cur_price - entry_price) / entry_price * 100 if entry_price else 0
    prompt = f"""Tu gères une position ouverte sur un marché de prédiction. Décide UNIQUEMENT sur les PREUVES VÉRIFIABLES ci-dessous.

QUESTION : {question}
TON PARI : {side} (entré à {entry_price:.2f}, prix actuel {cur_price:.2f}, soit {move:+.0f}%)

PREUVES FRAÎCHES :
{ev_block}

Faut-il GARDER ou SORTIR ? Réponds STRICTEMENT en JSON :
{{"action": "HOLD"|"EXIT", "confidence": <0.0 à 1.0>, "summary": "<1 phrase citant le score/temps>"}}

RÈGLE CLÉ — juge la RÉCUPÉRABILITÉ selon l'avancement du match :
- Déficit TÔT dans le match (début, beaucoup de temps/manches restant) = RÉCUPÉRABLE -> HOLD (ex: baseball 0-2 en 1re manche, 8 manches restantes).
- Déficit TARD (fin de match, peu de temps restant) et ton camp mené = PLIÉ -> EXIT (ex: 0-5 en 9e manche).
- Ton camp MÈNE ou a GAGNÉ -> HOLD.
- Preuves ambiguës / pas de score -> HOLD (on ne coupe pas dans le doute, le filet -25% protège)."""
    for model in MODELS:
        try:
            r = requests.post(OPENROUTER_URL, headers={"Authorization": f"Bearer {key}"},
                              json={"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.1},
                              timeout=45)
            if r.status_code != 200:
                continue
            content = r.json()["choices"][0]["message"]["content"]
            mt = re.search(r"\{.*\}", content, re.DOTALL)
            if not mt:
                continue
            p = json.loads(mt.group(0))
            act = str(p.get("action", "")).upper()
            if act not in ("HOLD", "EXIT"):
                continue
            return {"action": act, "confidence": float(p.get("confidence", 0) or 0),
                    "summary": p.get("summary", ""), "model": model.split("/")[-1].replace(":free", "")}
        except Exception:
            continue
    return None
