"""Signal layer (stub). Doit produire action/direction/size/confidence/
urgency/expected_edge/expected_slippage/expected_fees -- voir
ENGINEBTB3_SPEC.txt section 2. Rien d'implemente."""


def evaluate(symbol: str) -> dict:
    """Retourne un signal neutre -- stub. Jamais de detection reelle ici."""
    return {"symbol": symbol, "action": "none", "implemented": False}
