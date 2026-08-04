"""
Moteur principal GHOST — orchestre toutes les stratégies en parallèle.
"""

import asyncio
from rich.console import Console
from rich.panel import Panel

from core.chain import Chain
from core.config import Config
from core.db import init_db, Session, Trade
from strategies.sniper import Sniper
from strategies.arb import Arb
from strategies.funding import Funding

console = Console()

PAPER_CAPITAL_ETH = 0.02  # ~50€ répartis entre stratégies


class Engine:
    def __init__(self, config: Config):
        self.config = config
        self.chain = Chain(config)
        init_db()

        self.sniper = Sniper(self.chain, config)
        self.arb = Arb(self.chain, config)
        self.funding = Funding(self.chain, config)

        # Allocation du capital paper
        self.sniper.paper_balance = PAPER_CAPITAL_ETH * 0.5   # 50% au sniper
        self.arb.paper_balance = PAPER_CAPITAL_ETH * 0.3       # 30% à l'arb
        self.funding.paper_balance = PAPER_CAPITAL_ETH * 0.2   # 20% au funding

    async def run(self):
        mode = "🔴 LIVE" if self.config.is_live else "📝 PAPER"
        balance = self.chain.get_eth_balance() if self.config.private_key else 0

        console.print(Panel(
            f"[bold]GHOST ENGINE[/]\n"
            f"Mode: {mode}\n"
            f"Chain: Base (ID {self.config.chain_id})\n"
            f"Wallet: {self.chain.address or 'N/A'}\n"
            f"Balance: {balance:.6f} ETH\n"
            f"Paper capital: {PAPER_CAPITAL_ETH} ETH\n"
            f"\nStratégies actives:\n"
            f"  🎯 Sniper  — {self.sniper.paper_balance:.4f} ETH\n"
            f"  ⚡ Arb     — {self.arb.paper_balance:.4f} ETH\n"
            f"  📊 Funding — {self.funding.paper_balance:.4f} ETH",
            title="👻 GHOST", border_style="cyan",
        ))

        await asyncio.gather(
            self.sniper.run(),
            self.arb.run(),
            self.funding.run(),
            self._status_loop(),
        )

    async def _status_loop(self):
        while True:
            await asyncio.sleep(30)
            session = Session()
            trades = session.query(Trade).count()
            buys = session.query(Trade).filter(Trade.action == "buy").count()
            signals = session.query(Trade).filter(Trade.action.like("%signal%")).count()
            session.close()

            console.print(
                f"[dim]── STATUS: {trades} trades | {buys} buys | "
                f"{signals} signals | sniper_pos={len(self.sniper.paper_positions)} "
                f"arb_opps={self.arb.opportunities_found} ──[/]"
            )
