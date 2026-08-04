"""Risk layer (stub). Voir ENGINEBTB3_SPEC.txt section 10. Rien
d'implemente -- valeurs par defaut toutes a zero (config.py), donc
toute tentative d'engagement serait de toute facon bloquee en amont."""


def check(symbol: str, size_usd: float) -> tuple[bool, str]:
    """Refuse toujours tant que ce n'est pas implemente -- stub."""
    return False, "risk layer non implemente (stub)"
