"""
Audit de sécurité token via GoPlus Labs (gratuit, sans clé).
Détecte honeypots, taxes cachées, owner malveillant, LP non lockée.
"""

import aiohttp
import time

GOPLUS_URL = "https://api.gopluslabs.io/api/v1/token_security/8453"

_cache: dict[str, tuple[float, tuple]] = {}
_raw_cache: dict[str, tuple[float, dict]] = {}  # données brutes GoPlus, pour les signaux dérivés
CACHE_TTL = 600  # 10 min


async def audit_token(address: str) -> tuple[bool, list[str]]:
    """Retourne (safe, raisons_de_refus). En cas d'API down: (True, ['audit indisponible'])
    — on laisse passer mais les autres filtres (liquidité, route de vente) restent actifs."""
    address = address.lower()

    cached = _cache.get(address)
    if cached and time.time() - cached[0] < CACHE_TTL:
        return cached[1]

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                GOPLUS_URL,
                params={"contract_addresses": address},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    return True, ["audit indisponible"]
                data = await resp.json()
    except Exception:
        return True, ["audit indisponible"]

    result = (data.get("result") or {}).get(address)
    if not result:
        # Token inconnu de GoPlus = trop récent/obscur pour être audité → refus
        verdict = (False, ["token inconnu des bases de sécurité"])
        _cache[address] = (time.time(), verdict)
        return verdict

    _raw_cache[address] = (time.time(), result)

    # GoPlus fait remonter hidden_owner/owner_change_balance comme une capacité
    # théorique du bytecode, même quand owner_address a été brûlé à 0x0 — ce qui
    # donne un faux positif systématique sur des tokens légitimes matures
    # (ex: VIRTUAL, 1M+ holders, coté Coinbase, owner_address=0x0).
    # On neutralise CE flag précis seulement si TROIS signaux de légitimité
    # convergent : ownership vraiment renoncée + code open-source + base de
    # holders établie. Un faux "renounce" avec un mécanisme caché ailleurs
    # (le vrai piège de rug) n'aura presque jamais 500+ holders distincts.
    owner_addr = (result.get("owner_address") or "").lower()
    owner_renounced = owner_addr.startswith("0x") and set(owner_addr[2:]) <= {"0"}
    is_open_source = result.get("is_open_source") == "1"
    try:
        holder_count = int(result.get("holder_count") or 0)
    except (ValueError, TypeError):
        holder_count = 0
    owner_flags_neutralized = owner_renounced and is_open_source and holder_count >= 500

    reasons = []

    def flag(key, label, can_be_neutralized=False):
        if result.get(key) == "1":
            if can_be_neutralized and owner_flags_neutralized:
                return
            reasons.append(label)

    flag("is_honeypot", "HONEYPOT confirmé")
    flag("cannot_sell_all", "vente totale impossible")
    flag("transfer_pausable", "transferts suspendables")
    flag("is_blacklisted", "blacklist active")
    flag("owner_change_balance", "owner peut modifier les balances", can_be_neutralized=True)
    flag("selfdestruct", "selfdestruct présent")
    flag("hidden_owner", "owner caché", can_be_neutralized=True)

    try:
        buy_tax = float(result.get("buy_tax") or 0)
        sell_tax = float(result.get("sell_tax") or 0)
        if buy_tax > 0.10:
            reasons.append(f"taxe achat {buy_tax:.0%}")
        if sell_tax > 0.10:
            reasons.append(f"taxe vente {sell_tax:.0%}")
    except (ValueError, TypeError):
        pass

    # Proxy + owner non renoncé = code modifiable à tout moment
    if result.get("is_proxy") == "1" and result.get("can_take_back_ownership") == "1":
        reasons.append("proxy avec owner récupérable")

    verdict = (len(reasons) == 0, reasons)
    _cache[address] = (time.time(), verdict)
    return verdict


def holder_accumulation_signal(address: str) -> tuple[float, str | None]:
    """Signal de concentration des holders — dérivé des mêmes données GoPlus
    déjà récupérées par audit_token() (aucun appel réseau supplémentaire, aucune
    clé d'API en plus). Proxy honnête pour du "smart money" sans vrai tracking
    de wallets historiques (ça, ça demanderait un indexeur payant type Basescan
    Pro/Alchemy) : repère une accumulation par des wallets individuels distincts
    des holders LP, ni trop concentrée (whale unique) ni inexistante (personne
    n'accumule). Retourne (bonus_score, raison) — bonus=0 si pas de données
    ou rien de notable."""
    address = address.lower()
    cached = _raw_cache.get(address)
    if not cached:
        return 0.0, None
    result = cached[1]

    holders = result.get("holders") or []
    try:
        holder_count = int(result.get("holder_count") or 0)
    except (ValueError, TypeError):
        holder_count = 0

    # Somme des % détenus par des wallets individuels (pas des contrats/LP)
    wallet_holders_pct = sum(
        float(h.get("percent") or 0) * 100
        for h in holders
        if h.get("is_contract") == 0
    )
    top_wallet_pct = max(
        (float(h.get("percent") or 0) * 100 for h in holders if h.get("is_contract") == 0),
        default=0.0,
    )

    if wallet_holders_pct <= 0:
        return 0.0, None

    # Whale unique dominant = risque de dump, pas un signal d'accumulation saine
    if top_wallet_pct > 15:
        return -5.0, f"1 wallet detient {top_wallet_pct:.1f}% (risque dump)"

    bonus = 0.0
    reasons = []

    if 3.0 <= wallet_holders_pct <= 30.0:
        bonus += 8.0
        reasons.append(f"wallets top10 {wallet_holders_pct:.1f}%")

    if holder_count >= 1000:
        bonus += 6.0
        reasons.append(f"{holder_count:,} holders")
    elif holder_count < 50:
        bonus -= 6.0
        reasons.append(f"seulement {holder_count} holders")

    if not reasons:
        return 0.0, None
    return bonus, ", ".join(reasons)
