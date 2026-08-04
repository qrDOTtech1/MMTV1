"""
GeckoTerminal API — la vraie source de découverte tokens Base.
Endpoint /new_pools : latence ~30-90s post-création (vs 5-30 min pour DexScreener).
Endpoint /trending_pools : momentum court terme.
Gratuit, sans clé, rate-limit 30 req/min.

DEX IDs Base vérifiés empiriquement (juillet 2026) : les VRAIS retournés par l'API.
"""

import time
import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import datetime

BASE_URL = "https://api.geckoterminal.com/api/v2"
NETWORK = "base"

# Whitelist DEX Base — vérifiée en live (curl /new_pools)
WHITELIST_DEX = {
    "uniswap-v2-base",
    "uniswap-v3-base",
    "uniswap-v4-base",
    "aerodrome-slipstream-v1",
    "aerodrome-v1",
    "baseswap",
    "sushiswap-v3-base",
}

# DEX qui NE PEUVENT PAS être achetés via le SwapRouter V2 de notre chain.py
# (Uniswap V4 utilise le PoolManager unique, V3 utilise SwapRouter02)
# En paper on peut simuler ; en live on doit rejeter.
NON_V2_DEX = {"uniswap-v3-base", "uniswap-v4-base", "aerodrome-slipstream-v1"}

WETH_BASE = "0x4200000000000000000000000000000000000006"


@dataclass
class GeckoPool:
    pool_address: str
    base_token_address: str
    base_token_symbol: str
    base_token_name: str
    dex_id: str
    price_usd: float
    price_change_m5: float
    price_change_h1: float
    price_change_h6: float
    price_change_h24: float
    volume_m5: float
    volume_h1: float
    volume_h24: float
    reserve_usd: float
    buys_m5: int
    sells_m5: int
    buys_h1: int
    sells_h1: int
    pool_created_at: float  # unix seconds
    fdv_usd: float

    @property
    def age_seconds(self) -> float:
        return time.time() - self.pool_created_at if self.pool_created_at > 0 else 1e9

    @property
    def age_minutes(self) -> float:
        return self.age_seconds / 60

    @property
    def is_v2_tradeable(self) -> bool:
        return self.dex_id not in NON_V2_DEX


_cache: dict[str, tuple[float, list[GeckoPool]]] = {}
CACHE_TTL = 25

# Prix ETH/USD partagé (pour convertir price_usd -> price_eth quand nécessaire)
_eth_usd_cache: tuple[float, float] = (0.0, 0.0)


def _f(x, default=0.0) -> float:
    try:
        return float(x) if x is not None else default
    except (TypeError, ValueError):
        return default


def _i(x, default=0) -> int:
    try:
        return int(x) if x is not None else default
    except (TypeError, ValueError):
        return default


def _parse_iso(ts: str) -> float:
    if not ts:
        return 0.0
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> dict | None:
    try:
        headers = {"User-Agent": "ghost-bot/1.0", "Accept": "application/json"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                return await r.json()
    except Exception:
        return None
    return None


def _extract_token(rel_key: str, pool_data: dict, included_by_id: dict) -> tuple[str, str, str]:
    rels = pool_data.get("relationships", {})
    ref = (rels.get(rel_key) or {}).get("data") or {}
    tok_id = ref.get("id", "")
    tok = included_by_id.get(tok_id, {})
    attrs = tok.get("attributes", {}) if tok else {}
    addr = attrs.get("address", "")
    if not addr and "_" in tok_id:
        addr = tok_id.split("_", 1)[-1]
    return addr, attrs.get("symbol", "?"), attrs.get("name", "?")


def _extract_dex_id(pool_data: dict) -> str:
    rels = pool_data.get("relationships", {})
    dex_ref = (rels.get("dex") or {}).get("data") or {}
    return dex_ref.get("id", "")


def _parse_pool(pool_data: dict, included_by_id: dict) -> GeckoPool | None:
    try:
        a = pool_data.get("attributes", {})
        base_addr, base_sym, base_name = _extract_token("base_token", pool_data, included_by_id)
        quote_addr, _, _ = _extract_token("quote_token", pool_data, included_by_id)

        # Si le base_token est WETH, on swap : c'est le quote qu'on veut trader
        if base_addr.lower() == WETH_BASE.lower() and quote_addr:
            base_addr, base_sym, base_name = _extract_token("quote_token", pool_data, included_by_id)

        if not base_addr:
            return None

        vol = a.get("volume_usd") or {}
        pc = a.get("price_change_percentage") or {}
        tx = a.get("transactions") or {}
        m5 = tx.get("m5") or {}
        h1 = tx.get("h1") or {}

        return GeckoPool(
            pool_address=a.get("address", ""),
            base_token_address=base_addr,
            base_token_symbol=base_sym,
            base_token_name=base_name,
            dex_id=_extract_dex_id(pool_data),
            price_usd=_f(a.get("base_token_price_usd")),
            price_change_m5=_f(pc.get("m5")),
            price_change_h1=_f(pc.get("h1")),
            price_change_h6=_f(pc.get("h6")),
            price_change_h24=_f(pc.get("h24")),
            volume_m5=_f(vol.get("m5")),
            volume_h1=_f(vol.get("h1")),
            volume_h24=_f(vol.get("h24")),
            reserve_usd=_f(a.get("reserve_in_usd")),
            buys_m5=_i(m5.get("buys")),
            sells_m5=_i(m5.get("sells")),
            buys_h1=_i(h1.get("buys")),
            sells_h1=_i(h1.get("sells")),
            pool_created_at=_parse_iso(a.get("pool_created_at", "")),
            fdv_usd=_f(a.get("fdv_usd")),
        )
    except Exception:
        return None


async def _fetch_pools(cache_key: str, path: str) -> list[GeckoPool]:
    now = time.time()
    if cache_key in _cache:
        ts, cached = _cache[cache_key]
        if now - ts < CACHE_TTL:
            return cached
    url = f"{BASE_URL}/networks/{NETWORK}/{path}"
    sep = "&" if "?" in path else "?"
    url = f"{url}{sep}include=base_token,quote_token,dex"
    async with aiohttp.ClientSession() as session:
        data = await _fetch_json(session, url)
    if not data:
        return []
    included_by_id = {it.get("id", ""): it for it in (data.get("included") or [])}
    pools = []
    for pd in (data.get("data") or []):
        p = _parse_pool(pd, included_by_id)
        # reserve_usd < 0 = pool cassé (V4 buggy), on ignore
        if p and p.base_token_address and p.reserve_usd > 0:
            pools.append(p)
    _cache[cache_key] = (now, pools)
    return pools


async def get_new_base_pools(min_reserve_usd: float = 5000) -> list[GeckoPool]:
    """Pools Base créées récemment, filtrées : DEX whitelist + reserve minimum."""
    pools = await _fetch_pools("new", "new_pools?page=1")
    return [
        p for p in pools
        if (not p.dex_id or p.dex_id in WHITELIST_DEX)
        and p.reserve_usd >= min_reserve_usd
    ]


async def get_trending_pools(duration: str = "1h") -> list[GeckoPool]:
    """Trending Base. duration in {5m, 1h, 6h, 24h}."""
    if duration not in {"5m", "1h", "6h", "24h"}:
        raise ValueError(f"duration invalide: {duration}")
    pools = await _fetch_pools(f"trending_{duration}", f"trending_pools?duration={duration}&page=1")
    return [p for p in pools if not p.dex_id or p.dex_id in WHITELIST_DEX]


async def get_top_pools() -> list[GeckoPool]:
    """Top pools par volume — inclus les blue chips liquides."""
    pools = await _fetch_pools("top", "pools?page=1")
    return [p for p in pools if not p.dex_id or p.dex_id in WHITELIST_DEX]


async def get_eth_usd() -> float:
    """Prix ETH/USD via WETH sur GeckoTerminal, caché 5 min (cet endpoint est
    vite rate-limité — 30 req/min partagées avec toutes les stratégies).
    Fallback DexScreener si Gecko échoue, avant le fallback fixe en dernier recours."""
    global _eth_usd_cache
    now = time.time()
    if _eth_usd_cache[1] > 0 and now - _eth_usd_cache[0] < 300:
        return _eth_usd_cache[1]

    url = f"{BASE_URL}/networks/{NETWORK}/tokens/{WETH_BASE}"
    for attempt in range(2):
        async with aiohttp.ClientSession() as session:
            data = await _fetch_json(session, url)
        if data:
            price = _f((data.get("data") or {}).get("attributes", {}).get("price_usd"))
            if price > 0:
                _eth_usd_cache = (now, price)
                return price
        await asyncio.sleep(1.5)

    # Fallback : DexScreener (source déjà utilisée ailleurs, indépendante du rate-limit Gecko)
    try:
        from core.dexscreener import get_token_pairs
        pairs = await get_token_pairs(WETH_BASE)
        if pairs:
            best = max(pairs, key=lambda p: p.liquidity_usd)
            if best.price_usd > 0:
                _eth_usd_cache = (now, best.price_usd)
                return best.price_usd
    except Exception:
        pass

    return _eth_usd_cache[1] or 1800  # dernier recours, proche de la réalité juillet 2026
