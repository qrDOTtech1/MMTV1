"""Market data layer (stub). Ingestion WS separee de l'execution -- voir
ENGINEBTB3_SPEC.txt section 1. Rien d'implemente : pas de connexion, pas de
buffer, pas de state cache. A construire quand ce module devient prioritaire."""


def get_snapshot(symbol: str) -> dict:
    """Retourne un etat vide -- stub. Ne fait AUCUN appel reseau."""
    return {"symbol": symbol, "implemented": False}
