"""
Méta-allocateur — « l'argent attire l'argent », littéralement.
Toutes les 30 min, le capital LIBRE est réalloué vers les stratégies
qui performent le mieux sur les 6 dernières heures (PnL réalisé).
Plancher 10% / plafond 50% par stratégie : personne ne meurt, personne ne rafle tout.
"""

from datetime import datetime, timezone, timedelta
from core.db import Session, Trade

WINDOW_HOURS = 6
MIN_SHARE = 0.10
MAX_SHARE = 0.50
REALLOC_INTERVAL_S = 1800


class MetaAllocator:
    def __init__(self, strategies: dict, tradable: list[str]):
        """strategies: {nom: instance} ; tradable: noms qui reçoivent du capital."""
        self.strategies = strategies
        self.tradable = tradable

    def rolling_pnl(self, name: str) -> float:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)
        session = Session()
        trades = session.query(Trade).filter(
            Trade.strategy == name,
            Trade.timestamp >= cutoff,
            Trade.pnl.isnot(None),
        ).all()
        pnl = sum(t.pnl for t in trades)
        session.close()
        return pnl

    def compute_weights(self) -> dict[str, float]:
        pnls = {n: self.rolling_pnl(n) for n in self.tradable}
        # Score = PnL positif uniquement (une strat perdante retombe au plancher)
        scores = {n: max(p, 0.0) for n, p in pnls.items()}
        total_score = sum(scores.values())

        n_strats = len(self.tradable)
        if total_score == 0:
            return {n: 1.0 / n_strats for n in self.tradable}

        # Répartition : plancher garanti + le reste au mérite
        floor_total = MIN_SHARE * n_strats
        merit_pool = 1.0 - floor_total
        weights = {
            n: MIN_SHARE + merit_pool * (scores[n] / total_score)
            for n in self.tradable
        }

        # Plafonnement : l'excédent au-dessus de MAX_SHARE est redistribué
        # aux stratégies non plafonnées (proportionnellement), itérativement.
        for _ in range(n_strats):
            excess = 0.0
            uncapped = []
            for n, w in weights.items():
                if w > MAX_SHARE:
                    excess += w - MAX_SHARE
                    weights[n] = MAX_SHARE
                elif w < MAX_SHARE:
                    uncapped.append(n)
            if excess <= 1e-12 or not uncapped:
                break
            share = excess / len(uncapped)
            for n in uncapped:
                weights[n] += share

        return weights

    def reallocate(self) -> dict[str, float]:
        """Redistribue le capital libre selon les poids. Retourne {nom: nouveau_budget}."""
        free_total = sum(self.strategies[n].paper_balance for n in self.tradable)
        if free_total <= 0:
            return {}
        weights = self.compute_weights()
        result = {}
        for n in self.tradable:
            new_budget = free_total * weights[n]
            self.strategies[n].paper_balance = new_budget
            result[n] = new_budget
        return result
