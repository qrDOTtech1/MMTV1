"""
Scanner de nouveaux pools sur Base.
Surveille les événements PairCreated et filtre les tokens dangereux.
"""

import asyncio
import time
from dataclasses import dataclass, field
from web3 import Web3

from core.chain import Chain, WETH, UNISWAP_V2_FACTORY, UNISWAP_V2_FACTORY_ABI


@dataclass
class TokenScan:
    address: str
    symbol: str
    pair_address: str
    liquidity_eth: float
    decimals: int
    total_supply: int
    holder_count: int = 0
    is_honeypot: bool = False
    risk_score: float = 0.0
    price_eth: float = 0.0
    detected_at: float = field(default_factory=time.time)


class Scanner:
    def __init__(self, chain: Chain):
        self.chain = chain
        self.seen_pairs: set[str] = set()
        self.callbacks: list = []

    def on_new_token(self, callback):
        self.callbacks.append(callback)

    async def _check_honeypot(self, token_address: str) -> bool:
        try:
            out = self.chain.estimate_swap_out(0.0001, token_address)
            if out is None or out == 0:
                return True
            return False
        except Exception:
            return True

    def _check_liquidity(self, pair_address: str) -> float:
        try:
            r0, r1, token0 = self.chain.get_pair_reserves(pair_address)
            if token0.lower() == WETH.lower():
                return float(self.chain.w3.from_wei(r0, "ether"))
            else:
                return float(self.chain.w3.from_wei(r1, "ether"))
        except Exception:
            return 0.0

    def _compute_risk(self, scan: TokenScan) -> float:
        score = 0.0
        if scan.liquidity_eth < 0.5:
            score += 40
        elif scan.liquidity_eth < 2.0:
            score += 20
        if scan.is_honeypot:
            score += 50
        if scan.total_supply > 0:
            top_holder_pct = 0  # TODO: check top holder %
            if top_holder_pct > 50:
                score += 30
        return min(score, 100)

    async def scan_new_pairs(self):
        factory = self.chain.w3.eth.contract(
            address=UNISWAP_V2_FACTORY,
            abi=UNISWAP_V2_FACTORY_ABI,
        )

        latest = self.chain.w3.eth.block_number
        from_block = latest - 500  # ~15 min de blocs sur Base

        pair_filter = factory.events.PairCreated.create_filter(from_block=from_block)
        events = pair_filter.get_all_entries()

        for event in events:
            pair_addr = event["args"]["pair"]
            if pair_addr in self.seen_pairs:
                continue
            self.seen_pairs.add(pair_addr)

            token0 = event["args"]["token0"]
            token1 = event["args"]["token1"]
            token_addr = token1 if token0.lower() == WETH.lower() else token0

            if token_addr.lower() == WETH.lower():
                continue

            info = self.chain.get_token_info(token_addr)
            liquidity = self._check_liquidity(pair_addr)
            is_hp = await self._check_honeypot(token_addr)
            price = self.chain.get_token_price_eth(token_addr) or 0.0

            scan = TokenScan(
                address=token_addr,
                symbol=info["symbol"],
                pair_address=pair_addr,
                liquidity_eth=liquidity,
                decimals=info["decimals"],
                total_supply=info["total_supply"],
                is_honeypot=is_hp,
                price_eth=price,
            )
            scan.risk_score = self._compute_risk(scan)

            for cb in self.callbacks:
                await cb(scan)

    async def run(self, interval: float = 3.0):
        while True:
            try:
                await self.scan_new_pairs()
            except Exception as e:
                print(f"[SCANNER] Erreur: {e}")
            await asyncio.sleep(interval)
