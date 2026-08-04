"""Pont JS <-> Python pour GHOST POLY (pywebview)."""

from ghost_poly.engine import PolyEngine


class Api:
    def __init__(self):
        self._engine = PolyEngine()

    def get_state(self):
        return self._engine.get_state()

    def start_engine(self):
        return self._engine.start()

    def stop_engine(self):
        return self._engine.stop()

    # ── LIVE ──

    def live_status(self):
        return self._engine.live_status()

    def setup_allowances(self):
        return self._engine.setup_allowances()

    def enable_live(self):
        return self._engine.enable_live()

    def disable_live(self):
        return self._engine.disable_live()
