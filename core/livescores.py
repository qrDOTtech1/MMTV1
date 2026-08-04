"""
Scores en direct via l'API cachée d'ESPN — GRATUITE, sans clé, sans compte.
Vérifiée le 2026-07-13 : couvre NBA, NFL, foot (toutes ligues), tennis, MLB,
NHL, MMA. Renvoie le statut ("Final", "In Progress", "Scheduled") et le
vainqueur quand il est connu.

Usage : resolution-sniping. Un match "Final" (ou décidé de fait — gros écart
en fin de match) dont le marché Polymarket affiche encore le vainqueur en
dessous de 1.00 = achat à faible risque (on connaît déjà le résultat avant
que le marché ne finisse de le pricer ou ne se résolve).

PRUDENCE : ne jamais miser sur un statut incertain. On ne considère "décidé"
que si ESPN dit completed=True (match fini) OU écart décisif ET temps quasi
écoulé. Le doute = on s'abstient.
"""

import requests

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

# (sport, ligue) — les ligues couvrant le plus de marchés Polymarket sport.
SCOREBOARDS = [
    ("basketball", "nba"),
    ("soccer", "all"),
    ("tennis", "atp"),
    ("tennis", "wta"),
    ("baseball", "mlb"),
    ("hockey", "nhl"),
    ("mma", "ufc"),
]

_HEADERS = {"User-Agent": "Mozilla/5.0"}


def _competitor_name(c: dict) -> str:
    team = c.get("team", {})
    if team.get("displayName"):
        return team["displayName"]
    ath = c.get("athlete", {})
    return ath.get("displayName", "")


def get_decided_games() -> list[dict]:
    """Retourne les matchs DÉCIDÉS (résultat connu de fait) sur tous les
    scoreboards suivis. Chaque entrée : {winner, loser, status, sport,
    completed}. Un match n'est renvoyé que si un vainqueur est identifiable
    avec certitude."""
    decided = []
    for sport, league in SCOREBOARDS:
        try:
            r = requests.get(f"{ESPN_BASE}/{sport}/{league}/scoreboard",
                             timeout=10, headers=_HEADERS)
            if r.status_code != 200:
                continue
            events = r.json().get("events", [])
        except Exception:
            continue

        for e in events:
            comps = e.get("competitions", [{}])
            if not comps:
                continue
            comp = comps[0]
            status = comp.get("status", {}).get("type", {})
            completed = status.get("completed", False)
            state = status.get("state", "")  # "pre" | "in" | "post"
            competitors = comp.get("competitors", [])
            if len(competitors) != 2:
                continue

            # Vainqueur certain uniquement si le match est terminé (post/completed).
            # On NE devine PAS un vainqueur d'un match en cours ici — trop
            # risqué (un match peut basculer). Le sniping vise la fenêtre
            # entre "match fini" et "marché résolu".
            if not (completed or state == "post"):
                continue

            winner = next((c for c in competitors if c.get("winner")), None)
            if winner is None:
                continue
            loser = next((c for c in competitors if c is not winner), None)
            w_name = _competitor_name(winner)
            l_name = _competitor_name(loser) if loser else ""
            if not w_name:
                continue
            decided.append({
                "winner": w_name,
                "loser": l_name,
                "sport": sport,
                "status": status.get("description", "Final"),
                "completed": True,
            })
    return decided


_inplay_cache: list = [0.0, []]  # [ts, games] — cache court, le score bouge lentement à l'échelle d'un scan


def get_inplay_games() -> list[dict]:
    """Matchs EN COURS (state='in') avec le score actuel de chaque camp.
    Sert de filtre ANTI-SWING : quand une position sport plonge en prix, on
    regarde si notre joueur mène encore AU SCORE avant de couper (un prix qui
    swingue n'est pas un vrai effondrement tant que le tableau reste favorable).
    Chaque entrée : {p1, p2, s1, s2, sport} (p1/s1 = 1er compétiteur listé)."""
    import time as _t
    if _t.time() - _inplay_cache[0] < 15 and _inplay_cache[1]:
        return _inplay_cache[1]
    live = []
    for sport, league in SCOREBOARDS:
        try:
            r = requests.get(f"{ESPN_BASE}/{sport}/{league}/scoreboard",
                             timeout=8, headers=_HEADERS)
            if r.status_code != 200:
                continue
            events = r.json().get("events", [])
        except Exception:
            continue
        for e in events:
            comps = e.get("competitions", [{}])
            if not comps:
                continue
            comp = comps[0]
            state = comp.get("status", {}).get("type", {}).get("state", "")
            if state != "in":
                continue  # uniquement les matchs en cours
            cs = comp.get("competitors", [])
            if len(cs) != 2:
                continue
            def _score(c):
                try:
                    return float(c.get("score", 0))
                except (TypeError, ValueError):
                    return 0.0
            live.append({
                "p1": _competitor_name(cs[0]), "s1": _score(cs[0]),
                "p2": _competitor_name(cs[1]), "s2": _score(cs[1]),
                "sport": sport,
            })
    _inplay_cache[0] = _t.time()
    _inplay_cache[1] = live
    return live


def inplay_leader_status(question: str, we_hold_first: bool) -> str:
    """Notre camp mène-t-il encore AU SCORE dans ce match en cours ?
    Retourne 'leading' | 'trailing' | 'tied' | 'unknown'. we_hold_first = True
    si on détient le 1er joueur nommé dans la question (YES par convention)."""
    q = _norm(question)
    for g in get_inplay_games():
        n1, n2 = _norm(g["p1"]), _norm(g["p2"])
        if not (n1 and n2 and n1 in q and n2 in q):
            continue
        # score de NOTRE camp vs l'adversaire
        ours, theirs = (g["s1"], g["s2"]) if we_hold_first else (g["s2"], g["s1"])
        if ours > theirs:
            return "leading"
        if ours < theirs:
            return "trailing"
        return "tied"
    return "unknown"


def _norm(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())


def match_polymarket_question(question: str, decided: list[dict]) -> dict | None:
    """Associe une question Polymarket ('Team A vs Team B', 'Player A vs
    Player B') à un match décidé ESPN. Retourne {winner, side_hint} où
    side_hint indique si le vainqueur est mentionné en premier (YES) ou non.
    None si aucune correspondance fiable."""
    q = _norm(question)
    for g in decided:
        w = _norm(g["winner"])
        l = _norm(g["loser"])
        # Les DEUX noms doivent apparaître dans la question (évite les
        # correspondances partielles hasardeuses type "France" qui matche
        # plusieurs marchés).
        if w and l and w in q and l in q:
            return {"winner": g["winner"], "loser": g["loser"], "sport": g["sport"]}
    return None
