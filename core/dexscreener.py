"""
DexScreener API — token discovery sur Base.
"""

import aiohttp
from dataclasses import dataclass

BASE_URL = "https://api.dexscreener.com"
WETH_BASE = "0x4200000000000000000000000000000000000006"


@dataclass
class DexToken:
    address: str
    symbol: str
    name: str
    pair_address: str
    price_usd: float
    price_eth: float
    liquidity_usd: float
    volume_24h: float
    price_change_5m: float
    price_change_1h: float
    price_change_6h: float
    price_change_24h: float
    txns_buys_5m: int
    txns_sells_5m: int
    created_at: float
    fdv: float
    url: str


async def _fetch(url: str) -> dict | list | None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception:
        pass
    return None


async def _get_eth_usd_safe() -> float:
    """Import différé pour éviter un cycle (geckoterminal utilise dexscreener
    en fallback pour son propre taux ETH/USD)."""
    try:
        from core.geckoterminal import get_eth_usd
        return await get_eth_usd()
    except Exception:
        return 1800.0  # fallback conservateur, proche de la réalité juillet 2026


MAX_SANE_PRICE_CHANGE_PCT = 5000.0  # au-delà : données API corrompues, pas un vrai pump


def _sane_pct(x) -> float:
    """DexScreener renvoie parfois des priceChange délirants (ex: 4.5e21%)
    sur des pairs à liquidité quasi nulle. Clamp au lieu de laisser polluer
    les stratégies de momentum avec du bruit numérique."""
    v = float(x or 0)
    if v != v or v in (float("inf"), float("-inf")):  # NaN/inf
        return 0.0
    if abs(v) > MAX_SANE_PRICE_CHANGE_PCT:
        return 0.0
    return v


def _parse_pair(pair: dict, eth_usd: float) -> DexToken | None:
    """eth_usd sert à dériver un price_eth fiable quand la pool n'est PAS
    cotée en WETH — bug réel corrigé : plusieurs tokens peuvent partager le
    même symbole (ex: plusieurs contrats "Claude" distincts sur Base), et même
    pour UN SEUL contrat, ses pools peuvent être cotées en USDC, WETH, ou
    autre. `priceNative` de DexScreener est le prix dans la devise de la
    pool — PAS toujours en ETH. Le traiter comme tel sans vérifier a provoqué
    une inflation de prix ~1800x (confondu prix en USDC avec prix en ETH) sur
    un token peu liquide, faussant tout le PnL calculé dessus."""
    try:
        if pair.get("chainId") != "base":
            return None
        base = pair.get("baseToken", {})
        quote = pair.get("quoteToken", {}) or {}
        pc = pair.get("priceChange", {})
        txns = pair.get("txns", {})
        m5 = txns.get("m5", {})

        price_usd = float(pair.get("priceUsd", 0) or 0)
        price_native = float(pair.get("priceNative", 0) or 0)
        quote_addr = (quote.get("address") or "").lower()

        if quote_addr == WETH_BASE.lower():
            price_eth = price_native  # pool cotée en WETH : priceNative EST le prix en ETH
        elif price_usd > 0 and eth_usd > 0:
            price_eth = price_usd / eth_usd  # dérivé du prix USD, fiable quel que soit le quote token
        else:
            price_eth = 0.0  # ni WETH ni prix USD exploitable : on ne devine pas

        return DexToken(
            address=base.get("address", ""),
            symbol=base.get("symbol", "?"),
            name=base.get("name", "?"),
            pair_address=pair.get("pairAddress", ""),
            price_usd=price_usd,
            price_eth=price_eth,
            liquidity_usd=float((pair.get("liquidity") or {}).get("usd", 0) or 0),
            volume_24h=float((pair.get("volume") or {}).get("h24", 0) or 0),
            price_change_5m=_sane_pct(pc.get("m5")),
            price_change_1h=_sane_pct(pc.get("h1")),
            price_change_6h=_sane_pct(pc.get("h6")),
            price_change_24h=_sane_pct(pc.get("h24")),
            txns_buys_5m=int(m5.get("buys", 0)),
            txns_sells_5m=int(m5.get("sells", 0)),
            created_at=float(pair.get("pairCreatedAt", 0) or 0) / 1000,
            fdv=float(pair.get("fdv", 0) or 0),
            url=pair.get("url", ""),
        )
    except Exception:
        return None


async def _fetch_many_tokens(addresses: list[str]) -> list[DexToken]:
    """Fetch les détails de plusieurs tokens en parallèle (au lieu de séquentiel)."""
    import asyncio
    tasks = [get_token_pairs(a) for a in addresses]
    results = []
    for pairs in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(pairs, list) and pairs:
            results.append(pairs[0])
    return results


def _base_addresses(data, limit: int) -> list[str]:
    seen = set()
    out = []
    for item in data or []:
        if item.get("chainId") != "base":
            continue
        addr = item.get("tokenAddress", "")
        if addr and addr not in seen:
            seen.add(addr)
            out.append(addr)
        if len(out) >= limit:
            break
    return out


async def get_new_pairs() -> list[DexToken]:
    data = await _fetch(f"{BASE_URL}/token-profiles/latest/v1")
    if not data or not isinstance(data, list):
        return []
    return await _fetch_many_tokens(_base_addresses(data, 15))


async def get_boosted_tokens() -> list[DexToken]:
    data = await _fetch(f"{BASE_URL}/token-boosts/top/v1")
    if not data or not isinstance(data, list):
        return []
    return await _fetch_many_tokens(_base_addresses(data, 10))


SCAN_QUERIES = [
    "DEGEN", "BRETT", "AERO", "VIRTUAL", "TOSHI",
    "MOCHI", "NORMIE", "BASED", "CHOMP", "SKI",
    "WETH", "cbBTC", "USDC", "AI", "PEPE",
    "DOG", "CAT", "MOON", "BABY", "TRUMP",
]

_trending_cache: list[DexToken] = []
_trending_ts: float = 0


async def get_trending_base() -> list[DexToken]:
    global _trending_cache, _trending_ts
    import asyncio
    import time
    if _trending_cache and (time.time() - _trending_ts) < 30:
        return _trending_cache

    eth_usd = await _get_eth_usd_safe()
    results = []
    seen = set()

    async def search_one(session, q):
        try:
            url = f"{BASE_URL}/latest/dex/search?q={q}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            pass
        return None

    async with aiohttp.ClientSession() as session:
        all_data = await asyncio.gather(*[search_one(session, q) for q in SCAN_QUERIES])

    for data in all_data:
        if not data:
            continue
        for pair in (data.get("pairs") or []):
            t = _parse_pair(pair, eth_usd)
            if t and t.address not in seen and t.volume_24h > 500:
                seen.add(t.address)
                results.append(t)

    results.sort(key=lambda x: -x.volume_24h)
    _trending_cache = results[:50]
    _trending_ts = time.time()
    return _trending_cache


async def get_token_pairs(address: str) -> list[DexToken]:
    data = await _fetch(f"{BASE_URL}/latest/dex/tokens/{address}")
    if not data:
        return []
    pairs = data.get("pairs") or (data if isinstance(data, list) else [])
    if not pairs:
        return []

    if address.lower() == WETH_BASE.lower():
        # geckoterminal.get_eth_usd() retombe sur get_token_pairs(WETH_BASE) en
        # dernier recours — repasser par _get_eth_usd_safe() ici créerait une
        # boucle infinie si GeckoTerminal est indisponible. price_eth n'est de
        # toute façon pas utilisé pour cette adresse sur ce chemin de secours
        # (seul price_usd est lu par l'appelant).
        eth_usd = 1.0
    else:
        eth_usd = await _get_eth_usd_safe()
    results = []
    for pair in pairs:
        t = _parse_pair(pair, eth_usd)
        if t:
            results.append(t)
    results.sort(key=lambda x: -x.liquidity_usd)
    return results
