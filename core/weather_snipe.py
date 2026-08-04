"""Resolution-sniping MÉTÉO — l'équivalent météo du sniper crypto.

Polymarket a une catégorie "Météo" : des marchés QUOTIDIENS "Will the highest
temperature in <Ville> be <N>°C on <date>?" (bucket EXACT par degré, un seul
gagne). Ils se résolvent sur la température réelle observée.

Source de vérité : Open-Meteo (gratuit, sans clé) — prévision du max du jour +
géocodage des villes. Près de la résolution (fin de journée locale) la prévision
est très fiable → on connaît quasi le bucket gagnant AVANT que le marché finisse
de le pricer. C'est un VRAI edge informationnel (comme Coinbase pour le crypto).

Stratégie prudente :
 - Achat NO sur un bucket clairement HORS de la fourchette du max prévu (le degré
   exact ne peut pas être celui-là) — quasi-sûr, haute fréquence.
 - Achat YES sur le bucket == round(max prévu) quand la fourchette est serrée
   (proche résolution) ET que le YES est sous-coté.
La marge exigée croît avec le temps restant (une prévision à J+2 est moins sûre).
"""

import re
import time

import requests

_geo_cache: dict = {}   # ville -> (lat, lon)
_fc_cache: dict = {}    # (lat,lon,unit) -> (ts, {date: max})

GEO_URL = "https://geocoding-api.open-meteo.com/v1/search"
FC_URL = "https://api.open-meteo.com/v1/forecast"


def parse_weather_market(question: str) -> dict | None:
    """Extrait (ville, seuil, unité, date) d'une question météo.
    Ex: 'Will the highest temperature in Shanghai be 36°C on July 18?'
        -> {city: 'Shanghai', threshold: 36, unit: 'C', date_str: 'July 18'}
    None si ce n'est pas un marché température reconnaissable."""
    q = question.strip()
    m = re.search(
        r"highest temperature in (.+?) be\s*(-?\d+)\s*°?\s*([CF])\b.*?on\s+([A-Za-z]+\s+\d+)",
        q, re.IGNORECASE)
    if not m:
        return None
    return {
        "city": m.group(1).strip(),
        "threshold": int(m.group(2)),
        "unit": m.group(3).upper(),
        "date_str": m.group(4).strip(),
    }


def _geocode(city: str):
    if city in _geo_cache:
        return _geo_cache[city]
    try:
        r = requests.get(GEO_URL, params={"name": city, "count": 1}, timeout=12)
        res = r.json().get("results") or []
        if not res:
            _geo_cache[city] = None
            return None
        latlon = (res[0]["latitude"], res[0]["longitude"])
        _geo_cache[city] = latlon
        return latlon
    except Exception:
        return None


def _forecast_maxes(lat: float, lon: float, unit: str) -> dict:
    """{date_iso: max_temp} sur quelques jours (caché 10 min)."""
    key = (round(lat, 2), round(lon, 2), unit)
    now = time.time()
    if key in _fc_cache and now - _fc_cache[key][0] < 600:
        return _fc_cache[key][1]
    try:
        u = "fahrenheit" if unit == "F" else "celsius"
        r = requests.get(FC_URL, params={
            "latitude": lat, "longitude": lon,
            "daily": "temperature_2m_max", "timezone": "auto",
            "temperature_unit": u, "forecast_days": 4,
        }, timeout=12)
        d = r.json().get("daily", {})
        out = dict(zip(d.get("time", []), d.get("temperature_2m_max", [])))
        _fc_cache[key] = (now, out)
        return out
    except Exception:
        return {}


def predicted_max(city: str, unit: str, target_date_iso: str | None) -> float | None:
    """Max prévu par Open-Meteo pour la ville à la date visée (ISO YYYY-MM-DD).
    Si la date n'est pas trouvée, prend le 1er jour dispo (aujourd'hui)."""
    latlon = _geocode(city)
    if not latlon:
        return None
    maxes = _forecast_maxes(latlon[0], latlon[1], unit)
    if not maxes:
        return None
    if target_date_iso and target_date_iso in maxes:
        return maxes[target_date_iso]
    # fallback : jour le plus proche dispo
    return next(iter(maxes.values()), None)


def evaluate(question: str, hours_to_resolution: float,
             target_date_iso: str | None = None,
             base_margin_c: float = 2.0) -> dict | None:
    """Décide si le résultat d'un bucket est quasi certain vu le max prévu.
    Retourne {winner_side: 'YES'|'NO', city, predicted, threshold, margin} ou None.

    Logique (bucket EXACT par degré) :
      - marge exigée = base + heures_restantes * 0.35 (prévision moins sûre loin).
      - NO quasi-sûr si le seuil est à > marge du max prévu (ce degré n'est pas le max).
      - YES quasi-sûr si round(max prévu) == seuil ET marge serrée (< 0.8, proche
        résolution) — sinon on ne parie pas le YES (risque d'erreur d'1 degré).
    """
    parsed = parse_weather_market(question)
    if not parsed:
        return None
    pred = predicted_max(parsed["city"], parsed["unit"], target_date_iso)
    if pred is None:
        return None
    thr = parsed["threshold"]
    required = base_margin_c + max(0.0, hours_to_resolution) * 0.35
    gap = abs(pred - thr)

    if gap >= required:
        # le degré exact ne peut pas être ce bucket -> NO quasi-sûr
        winner = "NO"
    elif round(pred) == thr and required <= 0.8:
        # proche résolution + prévision tombe pile sur ce bucket -> YES
        winner = "YES"
    else:
        return None  # zone incertaine (± autour du bucket) : on ne parie pas

    return {
        "winner_side": winner,
        "city": parsed["city"],
        "predicted": round(pred, 1),
        "threshold": thr,
        "unit": parsed["unit"],
        "required": round(required, 2),
        "gap": round(gap, 2),
    }
