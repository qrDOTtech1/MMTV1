"""Execution layer (stub). Voir ENGINEBTB3_SPEC.txt section 3-4. Rien
d'implemente -- AUCUN chemin de ce module ne doit jamais poster d'ordre
reel tant que enginebtb3.ACTIVE est False, verifie explicitement ici en
premiere ligne de toute fonction qui serait ajoutee plus tard."""

from . import ACTIVE


def submit_order(*args, **kwargs) -> dict:
    """Refuse TOUJOURS -- stub + garde-fou explicite (defense en profondeur,
    meme si ce module n'est appele par rien aujourd'hui)."""
    if not ACTIVE:
        return {"ok": False, "reason": "ENGINEBTB3 non actif (stub, aucune execution reelle)"}
    raise NotImplementedError("execution layer non implemente")
