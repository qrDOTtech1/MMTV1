"""
Capital Management + Adaptive Compounding Engine.

Deux responsabilités :
1. RiskProfile — combien du capital on déploie (Conservative / Balanced /
   Aggressive Controlled), plafonds par stratégie, réserve gas incompressible.
2. AdaptiveCompoundingEngine — le multiplicateur de performance par stratégie,
   equity-based et NON-martingale : la taille monte quand l'equity RÉALISÉE
   monte, descend pendant une mauvaise séquence, jamais de doublement après
   perte ni de récupération forcée.

Toutes les composantes du sizing sont retournées dans un objet auditable
(SizingDecision) — rien n'est caché, chaque ordre peut logger le pourquoi.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from core.db import Session, Trade


# ─────────────────────────── Profils de risque ───────────────────────────

@dataclass(frozen=True)
class RiskProfile:
    name: str
    capital_use_min: float      # utilisation cible min (fraction de l'equity)
    capital_use_max: float
    max_positions: int
    pos_std_min_pct: float       # position standard, fraction de l'equity
    pos_std_max_pct: float
    pos_max_pct: float           # plafond absolu par token
    gas_reserve_pct: float       # réserve ETH/gas incompressible
    total_exposure_max_pct: float


PROFILES = {
    "conservative": RiskProfile(
        name="Conservative",
        capital_use_min=0.40, capital_use_max=0.60,
        max_positions=4,
        pos_std_min_pct=0.05, pos_std_max_pct=0.10,
        pos_max_pct=0.12,
        gas_reserve_pct=0.20,
        total_exposure_max_pct=0.60,
    ),
    "balanced": RiskProfile(
        name="Balanced",
        capital_use_min=0.65, capital_use_max=0.80,
        max_positions=6,
        pos_std_min_pct=0.10, pos_std_max_pct=0.15,
        pos_max_pct=0.16,
        gas_reserve_pct=0.15,
        total_exposure_max_pct=0.80,
    ),
    "aggressive": RiskProfile(
        name="Aggressive Controlled",
        capital_use_min=0.80, capital_use_max=0.90,
        max_positions=8,
        pos_std_min_pct=0.10, pos_std_max_pct=0.15,
        pos_max_pct=0.18,
        gas_reserve_pct=0.12,       # dans la fourchette 10-15%
        total_exposure_max_pct=0.88,
    ),
}

DEFAULT_PROFILE = "aggressive"

# Plafonds d'allocation par stratégie (fraction de l'equity)
STRATEGY_CAPS = {
    "sniper": 0.70,
    "arb": 0.30,
    "funding": 0.35,   # "momentum"
    "whale": 0.25,
}


# ────────────────── Régimes du multiplicateur adaptatif ──────────────────

# (label, valeur) — ordonné du plus défensif au plus agressif
REGIME_PROTECTION = ("PROTECTION", 0.50)
REGIME_DEFENSIVE = ("DEFENSIVE", 0.70)
REGIME_CAUTION = ("CAUTION", 0.85)
REGIME_NORMAL = ("NORMAL", 1.00)
REGIME_BOOST = ("BOOST", 1.10)
REGIME_STRONG = ("STRONG BOOST", 1.20)
REGIME_MAX = ("MAXIMUM", 1.25)

REGIME_LADDER = [REGIME_PROTECTION, REGIME_DEFENSIVE, REGIME_CAUTION, REGIME_NORMAL]


@dataclass
class StrategyMetrics:
    strategy: str
    closed_trades: int
    pnl_last15: float
    pnl_last30: float
    pnl_last50: float
    profit_factor_15: float
    profit_factor_30: float
    profit_factor_50: float
    consecutive_losses: int
    pnl_last5: float
    pnl_last10: float
    drawdown_pct: float          # depuis le high-water mark d'equity de la strat
    high_water_mark: float
    realized_equity: float       # equity réalisée nette de la stratégie
    is_live: bool


@dataclass
class SizingDecision:
    strategy: str
    equity_at_decision: float
    strategy_base_allocation_pct: float
    score_multiplier: float
    adaptive_performance_multiplier: float
    liquidity_multiplier: float
    risk_multiplier: float
    calculated_size: float
    capped_size: float
    regime: str
    reason: str
    max_acceptable_slippage_bps: int
    confidence: str
    reason_code: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def _profit_factor(pnls: list[float]) -> float:
    gains = sum(p for p in pnls if p > 0)
    losses = -sum(p for p in pnls if p < 0)
    if losses == 0:
        return 999.0 if gains > 0 else 1.0
    return gains / losses


def _consecutive_losses(pnls: list[float]) -> int:
    """pnls doit être ordonné du plus ancien au plus récent."""
    n = 0
    for p in reversed(pnls):
        if p < 0:
            n += 1
        else:
            break
    return n


class AdaptiveCompoundingEngine:
    """État persistant en mémoire : high-water mark par stratégie + régime
    courant + palier de recovery (nombre de trades gagnants depuis le dernier
    palier). Le high-water mark n'utilise QUE l'equity réalisée."""

    def __init__(self):
        self._hwm: dict[str, float] = {}          # high-water mark d'equity réalisée
        self._regime: dict[str, tuple] = {}        # régime courant par strat
        self._wins_since_step: dict[str, int] = {}  # trades gagnants depuis le dernier palier de recovery
        self._frozen = False                        # bouton admin : gèle à 1.00
        self._compounding_enabled = True            # bouton admin : désactive tout compounding

    # ── boutons admin ──
    def freeze(self, frozen: bool):
        self._frozen = frozen

    def set_compounding(self, enabled: bool):
        self._compounding_enabled = enabled

    @property
    def frozen(self) -> bool:
        return self._frozen

    @property
    def compounding_enabled(self) -> bool:
        return self._compounding_enabled

    def metrics(self, strategy: str, is_live: bool) -> StrategyMetrics:
        session = Session()
        # Trades clos = pnl non nul, dans le mode concerné (live vs paper)
        q = session.query(Trade).filter(
            Trade.strategy == strategy,
            Trade.pnl.isnot(None),
            Trade.simulated == (not is_live),
        ).order_by(Trade.timestamp)
        trades = q.all()
        session.close()

        pnls = [t.pnl for t in trades]
        realized = sum(pnls)

        # High-water mark sur equity réalisée cumulée
        cum = 0.0
        peak = 0.0
        for p in pnls:
            cum += p
            peak = max(peak, cum)
        hwm = max(self._hwm.get(strategy, 0.0), peak)
        self._hwm[strategy] = hwm

        drawdown_pct = ((hwm - realized) / hwm * 100) if hwm > 0 else 0.0

        last = lambda k: pnls[-k:] if len(pnls) >= 1 else []
        return StrategyMetrics(
            strategy=strategy,
            closed_trades=len(pnls),
            pnl_last15=sum(last(15)), pnl_last30=sum(last(30)), pnl_last50=sum(last(50)),
            profit_factor_15=_profit_factor(last(15)),
            profit_factor_30=_profit_factor(last(30)),
            profit_factor_50=_profit_factor(last(50)),
            consecutive_losses=_consecutive_losses(pnls),
            pnl_last5=sum(last(5)), pnl_last10=sum(last(10)),
            drawdown_pct=drawdown_pct, high_water_mark=hwm,
            realized_equity=realized, is_live=is_live,
        )

    def live_eligibility(self, strategy: str) -> dict:
        """Une stratégie ne devrait passer du capital réel que si elle a fait
        ses preuves EN PAPER selon les mêmes critères que le régime BOOST —
        pas juste "ça fait quelques jours qu'on regarde". Purement informatif :
        n'empêche rien tout seul, mais donne un avis chiffré avant d'activer
        le live sur une stratégie donnée."""
        m = self.metrics(strategy, is_live=False)  # toujours jugé sur l'historique paper
        reasons = []
        eligible = True

        if m.closed_trades < 15:
            eligible = False
            reasons.append(f"seulement {m.closed_trades} trades clos (mini 15)")
        if m.pnl_last15 <= 0 and m.closed_trades >= 15:
            eligible = False
            reasons.append("PnL net négatif ou nul sur les 15 derniers trades")
        if m.profit_factor_15 < 1.20 and m.closed_trades >= 15:
            eligible = False
            reasons.append(f"profit factor {m.profit_factor_15:.2f} < 1.20")
        if m.drawdown_pct >= 8:
            eligible = False
            reasons.append(f"drawdown {m.drawdown_pct:.1f}% >= 8%")

        return {
            "strategy": strategy, "eligible": eligible,
            "closed_trades": m.closed_trades,
            "profit_factor_15": round(m.profit_factor_15, 2),
            "pnl_last15": round(m.pnl_last15, 6),
            "drawdown_pct": round(m.drawdown_pct, 1),
            "reasons": reasons if reasons else ["critères BOOST validés en paper"],
        }

    def regime(self, strategy: str, is_live: bool, has_hard_risk_flag: bool = False) -> tuple:
        """Détermine le régime (label, multiplicateur) de la stratégie.
        Applique d'abord les conditions défensives (elles priment), puis les
        boosts (qui exigent un historique validé), puis la logique de recovery
        progressive."""
        # FORCE_BOOST=1 dans .env : Steven veut le mode boost en permanence —
        # le sizing reste au multiplicateur BOOST quel que soit l'historique,
        # et les régimes défensifs ne réduisent plus la taille. Demandé
        # explicitement (2026-07-11) en connaissance des pertes du jour.
        import os
        if os.environ.get("FORCE_BOOST") == "1":
            self._regime[strategy] = REGIME_BOOST
            return REGIME_BOOST

        if self._frozen or not self._compounding_enabled:
            self._regime[strategy] = REGIME_NORMAL
            return REGIME_NORMAL

        m = self.metrics(strategy, is_live)
        dd = m.drawdown_pct

        # ── Régimes défensifs (priment sur tout) ──
        if dd > 25:
            self._regime[strategy] = REGIME_PROTECTION
            return REGIME_PROTECTION  # kill switch géré côté risk/sizing
        if dd > 18 or has_hard_risk_flag:
            reg = REGIME_PROTECTION
            self._set_regime_with_recovery(strategy, reg)
            return self._regime[strategy]
        if m.consecutive_losses >= 3 or (10 <= dd <= 18) or (m.pnl_last10 < 0 and m.closed_trades >= 10):
            self._set_regime_with_recovery(strategy, REGIME_DEFENSIVE)
            return self._regime[strategy]
        if m.consecutive_losses >= 2 or (5 <= dd < 10) or (m.pnl_last5 < 0 and m.closed_trades >= 5):
            self._set_regime_with_recovery(strategy, REGIME_CAUTION)
            return self._regime[strategy]

        # ── Recovery : on ne remonte pas d'un coup à NORMAL depuis un régime bas ──
        current = self._regime.get(strategy, REGIME_NORMAL)
        if current in (REGIME_PROTECTION, REGIME_DEFENSIVE, REGIME_CAUTION):
            recovered = self._try_recover(strategy, m)
            if recovered != REGIME_NORMAL:
                return recovered

        # ── Boosts (exigent un historique validé par stratégie) ──
        # Arb reste non-boosté tant qu'il n'a pas d'edge net positif prouvé.
        if strategy == "arb" and m.realized_equity <= 0:
            self._regime[strategy] = REGIME_NORMAL
            return REGIME_NORMAL

        # Stratégie non validée (<15 trades) : plafonnée à NORMAL
        if m.closed_trades < 15:
            self._regime[strategy] = REGIME_NORMAL
            return REGIME_NORMAL

        if (m.is_live and m.closed_trades >= 50 and m.profit_factor_50 >= 1.60
                and m.drawdown_pct < 5 and not has_hard_risk_flag):
            self._regime[strategy] = REGIME_MAX
            return REGIME_MAX
        if (m.closed_trades >= 30 and m.pnl_last30 > 0 and m.profit_factor_30 >= 1.40
                and m.drawdown_pct < 5 and not has_hard_risk_flag):
            self._regime[strategy] = REGIME_STRONG
            return REGIME_STRONG
        if (m.closed_trades >= 15 and m.pnl_last15 > 0 and m.profit_factor_15 >= 1.20
                and m.drawdown_pct < 8 and not has_hard_risk_flag):
            self._regime[strategy] = REGIME_BOOST
            return REGIME_BOOST

        self._regime[strategy] = REGIME_NORMAL
        return REGIME_NORMAL

    def _set_regime_with_recovery(self, strategy: str, target: tuple):
        """Descendre est immédiat ; on réinitialise le compteur de recovery."""
        self._regime[strategy] = target
        self._wins_since_step[strategy] = 0

    def _try_recover(self, strategy: str, m: StrategyMetrics) -> tuple:
        """Remontée progressive : 0.50 -> 0.70 -> 0.85 -> 1.00, un palier à la
        fois, seulement après >= 5 trades clos ET positifs depuis le dernier
        palier."""
        current = self._regime.get(strategy, REGIME_NORMAL)
        idx = REGIME_LADDER.index(current) if current in REGIME_LADDER else len(REGIME_LADDER) - 1

        # On compte les trades depuis le dernier palier : approximé par pnl_last5 > 0
        # ET au moins 5 trades clos. Le compteur exact est réinitialisé à chaque
        # changement de palier.
        wins = self._wins_since_step.get(strategy, 0)
        # Incrément conservateur : si les 5 derniers sont nets positifs, on considère
        # le palier franchissable.
        if m.pnl_last5 > 0 and m.closed_trades >= 5:
            wins = 5
        self._wins_since_step[strategy] = wins

        if wins >= 5 and idx < len(REGIME_LADDER) - 1:
            new = REGIME_LADDER[idx + 1]
            self._regime[strategy] = new
            self._wins_since_step[strategy] = 0
            return new
        self._regime[strategy] = current
        return current


# ─────────────────────────── Fonction de sizing ───────────────────────────

def _score_multiplier(score: float, profile: RiskProfile) -> float:
    """Traduit un score de signal en fraction d'equity, dans la fourchette du
    profil. Score faible accepté -> bas de fourchette ; score élevé -> haut,
    jusqu'au plafond absolu par token."""
    # Normalise le score : 20 (seuil) -> 0, 80+ -> 1
    norm = max(0.0, min((score - 20) / 60, 1.0))
    lo = profile.pos_std_min_pct
    hi = profile.pos_std_max_pct
    base = lo + norm * (hi - lo)
    # Un signal exceptionnel (>= 80) peut viser le plafond absolu
    if score >= 80:
        base = profile.pos_max_pct * (0.85 + 0.15 * norm)
    return min(base, profile.pos_max_pct)


def calculate_position_size(
    strategy: str,
    score: float,
    equity: float,
    free_cash: float,
    open_exposure: float,
    open_positions: int,
    liquidity_usd: float,
    profile: RiskProfile,
    compounding: AdaptiveCompoundingEngine,
    is_live: bool,
    slippage_bps_estimate: int = 100,
    token_risk_ok: bool = True,
) -> SizingDecision:
    """Sizing explicable et pleinement audité. Toutes les limites sont
    appliquées APRÈS le calcul, jamais contournées par le compounding."""

    regime_label, perf_mult = compounding.regime(strategy, is_live, has_hard_risk_flag=not token_risk_ok)

    base_alloc_pct = _score_multiplier(score, profile)
    score_mult = 1.0  # déjà intégré dans base_alloc_pct (fourchette selon score)

    # Multiplicateur liquidité : réduit sur pool fine (protège du price impact)
    if liquidity_usd >= 100_000:
        liq_mult = 1.0
    elif liquidity_usd >= 30_000:
        liq_mult = 0.8
    elif liquidity_usd >= 15_000:
        liq_mult = 0.6
    else:
        liq_mult = 0.4

    # Multiplicateur risque : réduit si slippage estimé élevé
    if slippage_bps_estimate <= 100:
        risk_mult = 1.0
    elif slippage_bps_estimate <= 300:
        risk_mult = 0.75
    else:
        risk_mult = 0.5

    calculated = equity * base_alloc_pct * score_mult * perf_mult * liq_mult * risk_mult

    reasons = []
    reason_code = None

    # ── Application des limites (jamais contournées) ──
    capped = calculated

    # 1. Plafond absolu par token
    token_cap = equity * profile.pos_max_pct
    if capped > token_cap:
        capped = token_cap
        reasons.append(f"plafond token {profile.pos_max_pct:.0%} equity")
        reason_code = "TOKEN_CAP"

    # 2. Plafond d'allocation stratégie
    strat_cap = equity * STRATEGY_CAPS.get(strategy, 0.5)
    if capped > strat_cap:
        capped = strat_cap
        reasons.append(f"plafond strat {strategy} {STRATEGY_CAPS.get(strategy,0.5):.0%}")
        reason_code = "STRATEGY_CAP"

    # 3. Réserve gas incompressible + exposition totale max
    gas_reserve = equity * profile.gas_reserve_pct
    deployable = max(free_cash - gas_reserve, 0)
    exposure_room = equity * profile.total_exposure_max_pct - open_exposure
    room = min(deployable, exposure_room)
    if capped > room:
        capped = max(room, 0)
        reasons.append("réserve gas / exposition max")
        reason_code = "EXPOSURE_CAP"

    # 4. Kill switch drawdown / trop de positions / rejet risque
    m = compounding.metrics(strategy, is_live)
    if m.drawdown_pct >= 25:
        capped = 0.0
        reasons.append("KILL-SWITCH drawdown >= 25%")
        reason_code = "KILL_SWITCH"
    # Pas de blocage sur un compteur de positions arbitraire : la seule vraie
    # limite est le capital déployable, déjà appliquée ci-dessus (réserve gas +
    # plafond d'exposition). profile.max_positions reste affiché en UI à titre
    # indicatif, mais ne bloque plus un achat si du capital est encore libre.
    if not token_risk_ok:
        capped = 0.0
        reasons.append("token risqué / sellability")
        reason_code = "TOKEN_RISK"

    # Slippage acceptable ajusté à la liquidité
    max_slip = 300 if liquidity_usd >= 30_000 else 500

    confidence = "haute" if (perf_mult >= 1.10 and liq_mult == 1.0) else (
        "basse" if perf_mult <= 0.70 else "moyenne")

    return SizingDecision(
        strategy=strategy,
        equity_at_decision=round(equity, 6),
        strategy_base_allocation_pct=round(base_alloc_pct, 4),
        score_multiplier=round(score_mult, 3),
        adaptive_performance_multiplier=round(perf_mult, 3),
        liquidity_multiplier=round(liq_mult, 3),
        risk_multiplier=round(risk_mult, 3),
        calculated_size=round(calculated, 6),
        capped_size=round(max(capped, 0), 6),
        regime=regime_label,
        reason=" · ".join(reasons) if reasons else "aucune limite atteinte",
        max_acceptable_slippage_bps=max_slip,
        confidence=confidence,
        reason_code=reason_code,
    )
