"""
Analyse IA gratuite pour paris directionnels — recherche DuckDuckGo (gratuit,
sans clé) + estimation de probabilité par Nemotron via OpenRouter (gratuit).

Principe : pour un marché Polymarket donné ("Will X happen by DATE?"), on
cherche les actualités les plus récentes, on les donne au LLM avec la
question exacte, et on lui demande une probabilité 0-100%. Si l'écart entre
cette estimation et le prix du marché est assez grand, c'est un signal —
PAS une vérité : les LLM peuvent halluciner, se tromper de date, ou juste
mal évaluer. D'où le seuil d'edge élevé et la taille de position faible tant
que la stratégie n'a pas fait ses preuves.
"""

import json
import re
import time
from pathlib import Path

import requests
from ddgs import DDGS

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
# Cascade de modèles GRATUITS : si l'un est épuisé (429 quota journalier) ou
# indispo, on passe au suivant. Chacun a son propre quota -> beaucoup plus de
# marge qu'un seul modèle (le nemotron seul était plafonné à 50 req/jour).
# TOUS les modèles gratuits OpenRouter disponibles (vérifiés le 2026-07-13),
# ordonnés du plus capable au moins capable. Chacun a son PROPRE quota
# journalier -> en cascade, ~20x plus de capacité (~1000 req/j) : l'IA n'est
# quasi plus jamais à court. Les modèles de code/safety sont en fin de liste
# (dernier recours), les généralistes forts en tête (meilleure estimation).
# Cascade de fallback CLASSÉE (Steven 19/07) : meilleur modèle en tête, on descend
# si 429/indispo. Top 10 sélectionné pour NOTRE tâche (lecture de faits + JSON fiable
# + raisonnement), inadaptés retirés (musique/vision/safety/coder/trop petits).
# 10 modèles gratuits en cascade = large marge vs les quotas quotidiens.
MODELS = [
    "nvidia/nemotron-3-super-120b-a12b:free",              # prouvé (test 0.98 grounded)
    "qwen/qwen3-next-80b-a3b-instruct:free",               # instruction + JSON
    "nvidia/nemotron-3-ultra-550b-a55b:free",              # le + capable
    "nousresearch/hermes-3-llama-3.1-405b:free",           # 405B, function-calling
    "meta-llama/llama-3.3-70b-instruct:free",              # fiable
    "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free",  # reasoning
    "google/gemma-4-31b-it:free",
    "openai/gpt-oss-20b:free",
    "nvidia/nemotron-3-nano-30b-a3b:free",
    "google/gemma-4-26b-a4b-it:free",
]
AUTH_JSON = Path.home() / ".local" / "share" / "opencode" / "auth.json"

_cache_key: list = [0.0, None]  # [ts, key]
CACHE_TTL = 3600


def _get_openrouter_key() -> str | None:
    now = time.time()
    if _cache_key[1] and now - _cache_key[0] < CACHE_TTL:
        return _cache_key[1]
    try:
        data = json.loads(AUTH_JSON.read_text(encoding="utf-8"))
        key = data.get("openrouter", {}).get("key")
        _cache_key[0] = now
        _cache_key[1] = key
        return key
    except Exception:
        return None


def get_rss_news(query: str, max_results: int = 6) -> list[dict]:
    """Google News RSS — gratuit, sans clé, TEMPS RÉEL (breaking news trié par
    fraîcheur). Meilleur que DuckDuckGo pour réagir vite sur du géopolitique
    qui bouge. Retourne [{title, body, url}] (body = date de publication)."""
    import requests
    from xml.etree import ElementTree as ET
    try:
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        root = ET.fromstring(r.content)
        out = []
        for it in root.findall(".//item")[:max_results]:
            title = (it.findtext("title") or "").strip()
            pub = (it.findtext("pubDate") or "")[:16]
            if title:
                out.append({"title": title, "body": f"({pub})", "url": it.findtext("link") or ""})
        return out
    except Exception:
        return []


def search_news(query: str, max_results: int = 5) -> list[dict]:
    """News pour l'IA : RSS temps réel (Google News) EN PREMIER (breaking),
    complété par DuckDuckGo. Combine fraîcheur et profondeur."""
    rss = get_rss_news(query, max_results=max_results)
    try:
        ddg = list(DDGS().text(query, max_results=max_results, timelimit="w"))
        ddg = [{"title": r.get("title", ""), "body": r.get("body", ""), "url": r.get("href", "")} for r in ddg]
    except Exception:
        ddg = []
    # RSS d'abord (plus frais), puis DDG, dédupliqué grossièrement par titre
    seen = set()
    merged = []
    for item in rss + ddg:
        key = item["title"][:40].lower()
        if key and key not in seen:
            seen.add(key)
            merged.append(item)
    return merged[: max_results * 2]


def estimate_probability(question: str, market_price: float, category: str = "") -> dict | None:
    """Cherche des news récentes sur la question, demande à Nemotron d'estimer
    P(YES) en %. Retourne {probability, confidence, reasoning, sources_used}
    ou None si la recherche/l'analyse échoue.

    Le prompt force le modèle à distinguer 'info trouvée' de 'connaissance
    générale' pour limiter les hallucinations sur des events très récents."""
    key = _get_openrouter_key()
    if not key:
        return None

    news = search_news(question, max_results=5)
    if not news:
        return None

    news_block = "\n\n".join(
        f"[{i+1}] {n['title']}\n{n['body']}" for i, n in enumerate(news)
    )

    prompt = f"""Tu es un analyste de marchés de prédiction. Voici une question d'un marché Polymarket et des actualités récentes trouvées sur le sujet.

QUESTION DU MARCHÉ : {question}
CATÉGORIE : {category or "générale"}
PRIX ACTUEL DU MARCHÉ (probabilité implicite YES) : {market_price*100:.0f}%

ACTUALITÉS RÉCENTES TROUVÉES :
{news_block}

Basé UNIQUEMENT sur les actualités ci-dessus (pas sur tes connaissances générales qui peuvent être datées), estime la probabilité réelle que la réponse soit "Oui" (YES).

Réponds STRICTEMENT en JSON, rien d'autre :
{{"probability": <0-100>, "confidence": "low"|"medium"|"high", "reasoning": "<1-2 phrases>"}}

confidence="low" si les actus ne parlent pas clairement du sujet exact ou sont ambiguës. Ne mets confidence="high" que si au moins 2 sources confirment clairement le même sens."""

    # Cascade : essaie chaque modèle gratuit jusqu'à ce qu'un réponde. Un 429
    # (quota épuisé) ou une indispo passe au suivant au lieu d'échouer.
    for model in MODELS:
        try:
            r = requests.post(
                OPENROUTER_URL,
                headers={"Authorization": f"Bearer {key}"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2},
                timeout=45,
            )
            if r.status_code != 200:
                continue  # 429/404/500 -> modèle suivant
            content = r.json()["choices"][0]["message"]["content"]
            match = re.search(r"\{.*\}", content, re.DOTALL)
            if not match:
                continue
            parsed = json.loads(match.group(0))
            prob = float(parsed.get("probability", 50))
            if not (0 <= prob <= 100):
                continue
            return {
                "probability": prob / 100.0,
                "confidence": parsed.get("confidence", "low"),
                "reasoning": parsed.get("reasoning", ""),
                "sources_used": len(news),
                "model": model.split("/")[-1].replace(":free", ""),
            }
        except Exception:
            continue
    return None
