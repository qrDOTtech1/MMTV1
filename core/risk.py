"""
Risk Manager — le gardien du capital.
- Plus AUCUN mécanisme automatique ne bloque les achats (ni disjoncteur par
  pertes consécutives, ni kill-switch journalier — les deux ont été retirés
  à la demande explicite : ils gelaient les entrées sur des critères jugés
  trop intrusifs, y compris suite à des actions manuelles). daily_pnl() reste
  calculé et affiché à titre purement informatif.
- La seule protection qui subsiste est PAR POSITION, pas globale : exits en
  paliers (TP1/TP2), stop suiveur, breakeven, stop-loss dur, timeout — elles
  s'appliquent à une position ouverte, jamais aux nouvelles entrées.
"""

import time
from datetime import datetime, timezone
from core.db import Session, Trade

# --- Échelle de sortie ---
# R:R corrigé : les gains réels observés sur ce type de token tournent autour
# de +1 à +3% avant retournement. Un stop à -30% pour viser +50% (jamais
# atteint) est structurellement perdant. On prend les gains tôt et on
# resserre le risque pour matcher la réalité des mouvements observés.
TP1_TRIGGER = 1.06      # +6% : premier palier, quasi toujours atteignable
TP1_FRACTION = 0.40     # vend 40% de la position
TP2_TRIGGER = 1.12      # +12% : si ça continue, sécurise encore
TP2_FRACTION = 0.35     # vend 35% du reste (donc ~25% de la position initiale)

HARD_STOP_LOSS = 0.88   # -12% (au lieu de -30%) : aligné sur la taille réelle des gains
BREAKEVEN_TRIGGER = 1.04   # dès +4% de pic, le reste ne peut plus finir en perte nette
TRAIL_TRIGGER = 1.08       # stop suiveur actif dès +8% de pic
TRAIL_PCT = 0.05           # suit le pic à -5% (resserré, pas -15%)
MAX_HOLD_HOURS = 6


class RiskManager:
    def __init__(self, capital_eth: float):
        self.capital_eth = capital_eth
        self._daily_cache: tuple[float, float] = (0.0, 0.0)  # (ts, pnl)

    # ── Autorisation d'achat ──

    def daily_pnl(self) -> float:
        now = time.time()
        if now - self._daily_cache[0] < 60:
            return self._daily_cache[1]
        midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        session = Session()
        trades = session.query(Trade).filter(
            Trade.timestamp >= midnight, Trade.pnl.isnot(None)
        ).all()
        pnl = sum(t.pnl for t in trades)
        session.close()
        self._daily_cache = (now, pnl)
        return pnl

    def allow_buy(self, strategy: str) -> tuple[bool, str]:
        """Toujours autorisé — plus aucun blocage automatique global. Les
        seuls refus d'achat viennent d'ailleurs (capital insuffisant, token
        risqué, pas de route de vente...), gérés dans base_strategy.buy()."""
        return True, ""

    def record_result(self, strategy: str, pnl: float, manual: bool = False):
        """manual=True pour les ventes déclenchées depuis l'UI (Close All,
        sell fraction...). Sert uniquement à invalider le cache de PnL du
        jour (affichage), aucun effet sur la capacité d'acheter."""
        self._daily_cache = (0.0, 0.0)  # invalide le cache

    # ── Exits en paliers ──

    def check_exit(self, pos: dict, current_price: float) -> dict | None:
        """Retourne {"action": "partial"|"full", "fraction": float, "reason": str}
        ou None pour garder la position telle quelle. Met à jour pos['peak_price']
        et les flags de palier déjà pris (_tp1_done/_tp2_done) en place."""
        entry = pos.get("entry_price", 0)
        if entry <= 0 or current_price <= 0:
            return None

        peak = max(pos.get("peak_price", entry), current_price)
        pos["peak_price"] = peak
        ratio = current_price / entry
        peak_ratio = peak / entry

        # --- Paliers de prise de gain : priorité sur tout le reste ---
        if not pos.get("_tp1_done") and ratio >= TP1_TRIGGER:
            pos["_tp1_done"] = True
            return {
                "action": "partial", "fraction": TP1_FRACTION,
                "reason": f"TP1 x{ratio:.3f} (+{(ratio-1)*100:.1f}%) — vend {TP1_FRACTION:.0%}",
            }
        if pos.get("_tp1_done") and not pos.get("_tp2_done") and ratio >= TP2_TRIGGER:
            pos["_tp2_done"] = True
            return {
                "action": "partial", "fraction": TP2_FRACTION,
                "reason": f"TP2 x{ratio:.3f} (+{(ratio-1)*100:.1f}%) — vend {TP2_FRACTION:.0%} du reste",
            }

        # --- Stop suiveur : une fois +8% de pic atteint, suit à -5% ---
        if peak_ratio >= TRAIL_TRIGGER and current_price <= peak * (1 - TRAIL_PCT):
            return {
                "action": "full", "fraction": 1.0,
                "reason": f"TRAIL (pic x{peak_ratio:.3f}, sortie x{ratio:.3f})",
            }

        # --- Breakeven : dès +4% de pic, le reste ne peut plus finir en perte ---
        if peak_ratio >= BREAKEVEN_TRIGGER and ratio <= 1.005:
            return {
                "action": "full", "fraction": 1.0,
                "reason": f"BREAKEVEN (pic x{peak_ratio:.3f}, sortie neutre)",
            }

        # --- Hard stop : -12%, aligné sur la taille réelle des gains observés ---
        if ratio <= HARD_STOP_LOSS:
            return {"action": "full", "fraction": 1.0, "reason": f"STOP-LOSS x{ratio:.3f}"}

        # --- Timeout ---
        if "entry_time" in pos:
            held_h = (datetime.now(timezone.utc) - pos["entry_time"]).total_seconds() / 3600
            if held_h >= MAX_HOLD_HOURS:
                return {
                    "action": "full", "fraction": 1.0,
                    "reason": f"TIMEOUT {held_h:.1f}h x{ratio:.3f}",
                }

        return None

    def status(self) -> str:
        """Purement informatif — n'implique plus aucun blocage."""
        pnl = self.daily_pnl()
        return f"jour: {pnl:+.5f}"
