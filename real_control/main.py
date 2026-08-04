"""Fenetre de controle du trading REEL. Ouvrir cette fenetre ne fait RIEN
financierement — seul le clic sur 'DEMARRER' (dans index.html) declenche
l'engine reel. Aucun --auto, aucun autostart."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import webview  # noqa: E402

from real_control.api import Api  # noqa: E402

ROOT = Path(__file__).parent


def main():
    api = Api()
    webview.create_window(
        "GHOST V3 — Contrôle Trading RÉEL",
        str(ROOT / "index.html"),
        js_api=api,
        width=880,
        height=800,
        background_color="#0e1117",
    )
    webview.start()


if __name__ == "__main__":
    main()
