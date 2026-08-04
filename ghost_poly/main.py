"""GHOST POLY — app autonome dédiée Polymarket (pywebview + WebView2)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import webview  # noqa: E402

from ghost_poly.api import Api  # noqa: E402

ROOT = Path(__file__).parent


def main():
    api = Api()

    # --auto (pilotage Kapitane) : la fenêtre s'ouvre ET le moteur démarre +
    # passe en LIVE tout seul, sans clic. Sans le flag, double-clic normal =
    # démarrage manuel via les boutons (sécurité par défaut conservée).
    if "--auto" in sys.argv:
        import threading, time

        def _autostart():
            time.sleep(2)  # laisse la fenêtre s'afficher avant de bosser
            r1 = api.start_engine()
            r2 = api.enable_live()
            print(f"[auto] start={r1} live={r2}", flush=True)

        threading.Thread(target=_autostart, daemon=True).start()

    webview.create_window(
        "GHOST POLY",
        str(ROOT / "index.html"),
        js_api=api,
        width=1150,
        height=780,
        background_color="#0d0f14",
    )
    webview.start()


if __name__ == "__main__":
    main()
