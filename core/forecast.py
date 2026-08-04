"""
Prévision de PnL — projette le rythme de gain/perte réel sur 24h/7j/30j.

Pas de cache : chaque appel relit la DB et recalcule la pente à partir des
trades fermés les plus récents. C'est une "reprévision" continue — la
projection s'ajuste automatiquement à chaque nouveau trade, elle ne se fige
jamais sur un chiffre daté.
"""

from datetime import datetime, timezone
from core.db import Session, Trade

MIN_TRADES_FOR_FORECAST = 3
MIN_HOURS_FOR_FORECAST = 1.0    # sous 1h d'historique, une pente n'a aucun sens
                                 # statistique — 3 trades en 3 minutes donnait des
                                 # projections à +141 000% sur 30 jours
MIN_TRADES_GOOD_CONFIDENCE = 15
MIN_HOURS_GOOD_CONFIDENCE = 24


def _closed_trades(strategy: str | None = None) -> list[Trade]:
    session = Session()
    q = session.query(Trade).filter(Trade.pnl.isnot(None))
    if strategy:
        q = q.filter(Trade.strategy == strategy)
    trades = q.order_by(Trade.timestamp).all()
    session.close()
    return trades


def _as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _linear_rate_eth_per_hour(trades: list[Trade]) -> float:
    """Pente de régression linéaire (PnL cumulé vs temps écoulé), en ETH/heure.
    Plus robuste qu'un simple total/durée : lisse le bruit d'un gros trade isolé."""
    if len(trades) < 2:
        return 0.0

    t0 = _as_utc(trades[0].timestamp)
    xs, ys = [], []
    cum = 0.0
    for t in trades:
        cum += t.pnl
        hours = (_as_utc(t.timestamp) - t0).total_seconds() / 3600
        xs.append(hours)
        ys.append(cum)

    n = len(xs)
    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    sum_xx = sum(x * x for x in xs)
    denom = n * sum_xx - sum_x * sum_x
    if abs(denom) < 1e-9:
        return 0.0
    return (n * sum_xy - sum_x * sum_y) / denom


def forecast(strategy: str | None = None, capital_eth: float = 0.02) -> dict:
    """Retourne la projection de PnL. `available=False` avec une raison
    explicite si les données sont insuffisantes — jamais de faux chiffre."""
    trades = _closed_trades(strategy)
    n = len(trades)

    if n == 0:
        return {"available": False, "reason": "aucun trade fermé pour l'instant", "n_trades": 0}

    first_ts = _as_utc(trades[0].timestamp)
    elapsed_hours = max((datetime.now(timezone.utc) - first_ts).total_seconds() / 3600, 0.01)
    total_pnl = sum(t.pnl for t in trades)

    if n < MIN_TRADES_FOR_FORECAST:
        return {
            "available": False,
            "reason": f"{n} trade(s) fermé(s), minimum {MIN_TRADES_FOR_FORECAST} pour projeter",
            "n_trades": n,
            "total_pnl": total_pnl,
            "elapsed_hours": elapsed_hours,
        }

    if elapsed_hours < MIN_HOURS_FOR_FORECAST:
        return {
            "available": False,
            "reason": f"seulement {elapsed_hours*60:.0f} min d'historique, minimum {MIN_HOURS_FOR_FORECAST:.0f}h pour projeter",
            "n_trades": n,
            "total_pnl": total_pnl,
            "elapsed_hours": elapsed_hours,
        }

    rate_eth_day = _linear_rate_eth_per_hour(trades) * 24

    if n >= MIN_TRADES_GOOD_CONFIDENCE and elapsed_hours >= MIN_HOURS_GOOD_CONFIDENCE:
        confidence = "correcte"
    else:
        confidence = "faible (peu de données)"

    def pct(x: float) -> float:
        return (x / capital_eth * 100) if capital_eth > 0 else 0.0

    proj_24h = rate_eth_day * 1
    proj_7d = rate_eth_day * 7
    proj_30d = rate_eth_day * 30

    return {
        "available": True,
        "n_trades": n,
        "elapsed_hours": elapsed_hours,
        "total_pnl": total_pnl,
        "rate_eth_day": rate_eth_day,
        "confidence": confidence,
        "proj_24h_eth": proj_24h, "proj_24h_pct": pct(proj_24h),
        "proj_7d_eth": proj_7d, "proj_7d_pct": pct(proj_7d),
        "proj_30d_eth": proj_30d, "proj_30d_pct": pct(proj_30d),
    }
