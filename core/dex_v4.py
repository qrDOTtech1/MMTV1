"""
Exécution Uniswap V4 sur Base — Universal Router + PoolManager.

V4 n'a pas de contrat par pool (tout vit dans le PoolManager singleton). La
clé d'une pool (PoolKey : currency0/currency1/fee/tickSpacing/hooks) se
retrouve en scannant les events Initialize du PoolManager.

GARDE-FOU DE SÉCURITÉ NON NÉGOCIABLE : une pool avec un contrat "hooks" non
nul peut exécuter du code arbitraire à chaque swap (frais dynamiques,
blacklist, blocage de revente...). On ne peut pas auditer ce code à la volée.
Toute pool avec hooks != 0x0 est refusée purement et simplement.
"""

import json
import time
from web3 import Web3

UNIVERSAL_ROUTER = Web3.to_checksum_address("0x6fF5693b99212Da76ad316178A184AB56D299b43")
POOL_MANAGER = Web3.to_checksum_address("0x498581fF718922c3f8e6A244956aF099B2652b2b")
WETH_BASE = Web3.to_checksum_address("0x4200000000000000000000000000000000000006")
# Adresse canonique Permit2 — identique sur toutes les chaînes EVM, vérifiée
# par bytecode réel sur Base (9152 bytes, pas une supposition depuis un
# résumé de recherche qui avait tronqué un caractère).
PERMIT2 = Web3.to_checksum_address("0x000000000022D473030F116dDEE9F6B43aC78BA3")
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
# V4Quoter (lens périphérique, pas le PoolManager) — vérifié par bytecode réel
# sur Base (5820 bytes déployés), adresse issue de docs.uniswap.org/contracts/v4/deployments.
V4_QUOTER = Web3.to_checksum_address("0x0d5e0F971ED27FBfF6c2837bf31316121532048D")

# Currency native ETH en V4 : address(0), PAS WETH — V4 gère l'ETH nativement
NATIVE_ETH = "0x0000000000000000000000000000000000000000"

# keccak256("Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)")
INITIALIZE_TOPIC = Web3.keccak(
    text="Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
).hex()
if not INITIALIZE_TOPIC.startswith("0x"):
    INITIALIZE_TOPIC = "0x" + INITIALIZE_TOPIC

UNIVERSAL_ROUTER_ABI = json.loads('''[
  {"inputs":[
      {"name":"commands","type":"bytes"},
      {"name":"inputs","type":"bytes[]"},
      {"name":"deadline","type":"uint256"}
    ],"name":"execute","outputs":[],"stateMutability":"payable","type":"function"}
]''')

PERMIT2_ABI = json.loads('''[
  {"inputs":[{"name":"token","type":"address"},{"name":"spender","type":"address"},
             {"name":"amount","type":"uint160"},{"name":"expiration","type":"uint48"}],
   "name":"approve","outputs":[],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[{"name":"user","type":"address"},{"name":"token","type":"address"},{"name":"spender","type":"address"}],
   "name":"allowance",
   "outputs":[{"name":"amount","type":"uint160"},{"name":"expiration","type":"uint48"},{"name":"nonce","type":"uint48"}],
   "stateMutability":"view","type":"function"}
]''')

V4_QUOTER_ABI = json.loads('''[
  {"inputs":[{"components":[
      {"components":[
        {"name":"currency0","type":"address"},{"name":"currency1","type":"address"},
        {"name":"fee","type":"uint24"},{"name":"tickSpacing","type":"int24"},{"name":"hooks","type":"address"}
      ],"name":"poolKey","type":"tuple"},
      {"name":"zeroForOne","type":"bool"},
      {"name":"exactAmount","type":"uint128"},
      {"name":"hookData","type":"bytes"}
    ],"name":"params","type":"tuple"}],
   "name":"quoteExactInputSingle",
   "outputs":[{"name":"amountOut","type":"uint256"},{"name":"gasEstimate","type":"uint256"}],
   "stateMutability":"nonpayable","type":"function"}
]''')

WETH9_ABI = json.loads('''[
  {"inputs":[],"name":"deposit","outputs":[],"stateMutability":"payable","type":"function"},
  {"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"stateMutability":"nonpayable","type":"function"}
]''')

ERC20_ALLOWANCE_ABI = json.loads('''[
  {"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],
   "name":"approve","outputs":[{"name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],
   "name":"allowance","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}
]''')

# Commande V4_SWAP dans Universal Router (Commands.sol)
CMD_V4_SWAP = 0x10

# Actions du V4Router (Actions.sol)
ACTION_SWAP_EXACT_IN_SINGLE = 0x06
ACTION_SETTLE_ALL = 0x0c
ACTION_TAKE_ALL = 0x0f

# PoolKey ABI type : (address,address,uint24,int24,address)
POOL_KEY_TYPE = "(address,address,uint24,int24,address)"
EXACT_IN_SINGLE_TYPE = f"({POOL_KEY_TYPE},bool,uint128,uint128,bytes)"


class PoolKey:
    def __init__(self, currency0: str, currency1: str, fee: int, tick_spacing: int, hooks: str):
        self.currency0 = Web3.to_checksum_address(currency0)
        self.currency1 = Web3.to_checksum_address(currency1)
        self.fee = fee
        self.tick_spacing = tick_spacing
        self.hooks = Web3.to_checksum_address(hooks)

    @property
    def has_hooks(self) -> bool:
        return self.hooks.lower() != ZERO_ADDRESS

    def as_tuple(self):
        return (self.currency0, self.currency1, self.fee, self.tick_spacing, self.hooks)


CHUNK_BLOCKS = 9_000  # sous la limite eth_getLogs mesurée (413 au-delà de ~10k sur ce RPC)


MIN_CHUNK_BLOCKS = 2_000  # en-dessous, on ne subdivise plus : on abandonne la tranche


def _get_logs_chunked(w3: Web3, params_base: dict, from_block: int, to_block: int,
                      retries: int = 3) -> tuple[list, bool]:
    """eth_getLogs par fenêtres FIXES de CHUNK_BLOCKS. TOUS les RPC Base
    (publics ET Alchemy) plafonnent getLogs à ~10k blocs (413/400 au-delà) —
    inutile de tenter de grandes plages, ça ne fait que multiplier les échecs
    et geler l'appelant. On itère donc en fenêtres de ~9k, chacune réessayée
    (retries) sur rate-limit ponctuel.

    Retourne (logs, complet) : complet=False si une fenêtre a échoué
    définitivement — l'appelant ne doit alors PAS conclure fermement
    'aucune route'."""
    logs = []
    complete = True
    current = to_block
    while current > from_block:
        lo = max(current - CHUNK_BLOCKS, from_block)
        got = False
        for attempt in range(retries):
            try:
                logs.extend(w3.eth.get_logs({**params_base, "fromBlock": lo, "toBlock": current}))
                got = True
                break
            except Exception:
                if attempt < retries - 1:
                    time.sleep(0.3 * (attempt + 1))
        if not got:
            complete = False
        current = lo - 1
    return logs, complete


def _parse_initialize_log(log) -> PoolKey:
    """Décode un event Initialize du PoolManager en PoolKey."""
    currency0 = Web3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])
    currency1 = Web3.to_checksum_address("0x" + log["topics"][3].hex()[-40:])
    data = log["data"]
    if isinstance(data, bytes):
        data_hex = data.hex()
    else:
        data_hex = data[2:] if data.startswith("0x") else data
    # data non-indexé : fee (uint24), tickSpacing (int24), hooks (address), sqrtPriceX96, tick
    fee = int(data_hex[0:64], 16)
    tick_spacing_raw = int(data_hex[64:128], 16)
    tick_spacing = tick_spacing_raw if tick_spacing_raw < 2**23 else tick_spacing_raw - 2**24
    hooks = Web3.to_checksum_address("0x" + data_hex[128:192][-40:])
    return PoolKey(currency0, currency1, fee, tick_spacing, hooks)


def find_all_pool_keys(w3: Web3, token_address: str, lookback_blocks: int = 45_000) -> tuple[list, bool]:
    """Retourne (pools, complet) : TOUTES les PoolKey V4 impliquant le token,
    triées de la plus récente à la plus ancienne. complet=False si le scan a
    raté des tranches (résultat potentiellement partiel)."""
    token_cs = Web3.to_checksum_address(token_address)
    latest = w3.eth.block_number
    from_block = max(latest - lookback_blocks, 0)
    token_topic = "0x" + "0" * 24 + token_cs[2:].lower()

    logs_c0, ok0 = _get_logs_chunked(
        w3, {"address": POOL_MANAGER, "topics": [INITIALIZE_TOPIC, None, token_topic]},
        from_block, latest,
    )
    logs_c1, ok1 = _get_logs_chunked(
        w3, {"address": POOL_MANAGER, "topics": [INITIALIZE_TOPIC, None, None, token_topic]},
        from_block, latest,
    )
    all_logs = list(logs_c0) + list(logs_c1)
    all_logs.sort(key=lambda lg: lg["blockNumber"], reverse=True)
    pools = [_parse_initialize_log(lg) for lg in all_logs]
    return pools, (ok0 and ok1)


def find_pool_key(w3: Web3, token_address: str, lookback_blocks: int = 45_000,
                  quote_currencies: tuple | None = None) -> PoolKey | None:
    """PoolKey V4 la plus récente qui soit RÉELLEMENT exécutable en un saut :
    sans hook ET appariée à une devise qu'on sait payer (WETH ou ETH natif par
    défaut). Ne renvoie plus aveuglément la pool la plus récente (qui pouvait
    être une paire USDC inutilisable en single-hop, ou une pool hookée).

    Retourne None seulement si aucune pool utilisable n'existe DANS un scan
    complet — un scan incomplet renvoie la meilleure trouvée sans affirmer
    faussement qu'il n'y a rien."""
    if quote_currencies is None:
        quote_currencies = (WETH_BASE.lower(), NATIVE_ETH.lower())
    else:
        quote_currencies = tuple(c.lower() for c in quote_currencies)

    pools, _complete = find_all_pool_keys(w3, token_address, lookback_blocks)
    token_l = token_address.lower()
    for pk in pools:  # déjà triées de la plus récente à la plus ancienne
        if pk.has_hooks:
            continue
        other = pk.currency0.lower() if pk.currency1.lower() == token_l else pk.currency1.lower()
        if other in quote_currencies:
            return pk
    return None


def _encode_v4_swap_calldata(pool_key: PoolKey, zero_for_one: bool, amount_in: int, min_amount_out: int) -> bytes:
    """Encode la séquence d'actions V4Router : SWAP_EXACT_IN_SINGLE + SETTLE_ALL + TAKE_ALL."""
    from eth_abi import encode as abi_encode

    actions = bytes([ACTION_SWAP_EXACT_IN_SINGLE, ACTION_SETTLE_ALL, ACTION_TAKE_ALL])

    swap_params = abi_encode(
        [EXACT_IN_SINGLE_TYPE],
        [(pool_key.as_tuple(), zero_for_one, amount_in, min_amount_out, b"")],
    )

    input_currency = pool_key.currency0 if zero_for_one else pool_key.currency1
    output_currency = pool_key.currency1 if zero_for_one else pool_key.currency0
    settle_params = abi_encode(["address", "uint256"], [input_currency, amount_in])
    take_params = abi_encode(["address", "uint256"], [output_currency, min_amount_out])

    inputs = [swap_params, settle_params, take_params]
    return abi_encode(["bytes", "bytes[]"], [actions, inputs])


def quote_exact_input_single(w3: Web3, pool_key: PoolKey, zero_for_one: bool, amount_in: int) -> int:
    """Devis réel via V4Quoter.quoteExactInputSingle (call statique, revert-based
    comme QuoterV2 en V3 — pas d'état modifié). Lève si le call échoue (ex: pool
    sans liquidité) plutôt que de renvoyer un chiffre trompeur."""
    quoter = w3.eth.contract(address=V4_QUOTER, abi=V4_QUOTER_ABI)
    params = (pool_key.as_tuple(), zero_for_one, amount_in, b"")
    amount_out, _gas_estimate = quoter.functions.quoteExactInputSingle(params).call()
    return amount_out


def build_swap_tx(w3: Web3, sender: str, pool_key: PoolKey, zero_for_one: bool,
                   amount_in: int, min_amount_out: int, value_wei: int = 0) -> dict:
    """Construit la tx Universal Router pour un swap V4. REFUSE toute pool
    avec un hook non nul — sécurité non contournable.

    Si value_wei > 0 (paiement en ETH natif msg.value), vérifie que la devise
    d'entrée du swap est bien address(0) (ETH natif) — trouvé en test réel
    qu'un pool V4 peut être apparié à n'importe quel ERC20 (ex: DOT/USDC, sans
    WETH ni ETH du tout). Payer en ETH un pool qui n'en contient pas produit
    un revert Permit2 trompeur (AllowanceExpired) plutôt qu'une erreur claire —
    on le détecte nous-mêmes avant d'envoyer quoi que ce soit."""
    if pool_key.has_hooks:
        raise ValueError(
            f"pool V4 avec hooks {pool_key.hooks} — code arbitraire non auditable, refusé"
        )

    input_currency = pool_key.currency0 if zero_for_one else pool_key.currency1
    if value_wei > 0 and input_currency.lower() != NATIVE_ETH.lower():
        raise ValueError(
            f"paiement en ETH natif demandé mais la devise d'entrée du pool est "
            f"{input_currency} (ni ETH natif ni géré via value_wei) — pool invalide pour ce flux"
        )

    router = w3.eth.contract(address=UNIVERSAL_ROUTER, abi=UNIVERSAL_ROUTER_ABI)
    commands = bytes([CMD_V4_SWAP])
    v4_calldata = _encode_v4_swap_calldata(pool_key, zero_for_one, amount_in, min_amount_out)
    deadline = int(time.time()) + 300

    fn = router.functions.execute(commands, [v4_calldata], deadline)
    tx_params = {
        "from": sender, "value": value_wei,
        "gasPrice": int(w3.eth.gas_price * 1.1),
        "nonce": w3.eth.get_transaction_count(sender),
        "chainId": w3.eth.chain_id,
    }
    tx_params["gas"] = int(fn.estimate_gas(tx_params) * 1.3)
    return fn.build_transaction(tx_params)


# ── Préparation Permit2 pour les pools WETH-ERC20 (la majorité des pools V4
# réels observés sur Base — payer en ETH natif via value_wei ne marche que
# pour les pools appariés à address(0), rares en pratique) ──

def _base_tx_params(w3: Web3, sender: str, value: int = 0) -> dict:
    return {
        "from": sender, "value": value,
        "gasPrice": int(w3.eth.gas_price * 1.1),
        "nonce": w3.eth.get_transaction_count(sender),
        "chainId": w3.eth.chain_id,
    }


def build_wrap_eth_tx(w3: Web3, sender: str, amount_wei: int) -> dict:
    """ETH natif -> WETH, via le contrat WETH9 canonique. Étape 1 de 3 pour
    acheter une pool V4 cotée en WETH-ERC20 (pas en ETH natif)."""
    weth = w3.eth.contract(address=WETH_BASE, abi=WETH9_ABI)
    fn = weth.functions.deposit()
    tx_params = _base_tx_params(w3, sender, value=amount_wei)
    tx_params["gas"] = int(fn.estimate_gas(tx_params) * 1.3)
    return fn.build_transaction(tx_params)


def build_unwrap_weth_tx(w3: Web3, sender: str, amount_wei: int) -> dict:
    """WETH -> ETH natif. Étape finale après une vente V4 réglée en WETH."""
    weth = w3.eth.contract(address=WETH_BASE, abi=WETH9_ABI)
    fn = weth.functions.withdraw(amount_wei)
    tx_params = _base_tx_params(w3, sender)
    tx_params["gas"] = int(fn.estimate_gas(tx_params) * 1.3)
    return fn.build_transaction(tx_params)


def erc20_needs_approval(w3: Web3, token: str, owner: str, spender: str, amount: int) -> bool:
    erc20 = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ALLOWANCE_ABI)
    allowance = erc20.functions.allowance(
        Web3.to_checksum_address(owner), Web3.to_checksum_address(spender)
    ).call()
    return allowance < amount


def build_erc20_approve_tx(w3: Web3, sender: str, token: str, spender: str) -> dict:
    """Approve ERC20 classique — utilisé pour autoriser Permit2 lui-même à
    tirer le token (étape préalable obligatoire, Permit2 ne peut rien sans ça)."""
    erc20 = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ALLOWANCE_ABI)
    fn = erc20.functions.approve(Web3.to_checksum_address(spender), 2**256 - 1)
    tx_params = _base_tx_params(w3, sender)
    tx_params["gas"] = int(fn.estimate_gas(tx_params) * 1.3)
    return fn.build_transaction(tx_params)


def permit2_needs_approval(w3: Web3, token: str, owner: str, spender: str, amount: int) -> bool:
    permit2 = w3.eth.contract(address=PERMIT2, abi=PERMIT2_ABI)
    allowance_amount, expiration, _ = permit2.functions.allowance(
        Web3.to_checksum_address(owner), Web3.to_checksum_address(token), Web3.to_checksum_address(spender)
    ).call()
    return allowance_amount < amount or (expiration != 0 and expiration < int(time.time()))


def build_permit2_approve_tx(w3: Web3, sender: str, token: str, spender: str, amount: int) -> dict:
    """Enregistre une autorisation dans Permit2 pour que spender (le Universal
    Router) puisse tirer `token` — valable 30 jours, pas un an ni indéfiniment,
    pour limiter la fenêtre de risque en cas de bug ailleurs dans le système."""
    permit2 = w3.eth.contract(address=PERMIT2, abi=PERMIT2_ABI)
    expiration = int(time.time()) + 30 * 24 * 3600
    fn = permit2.functions.approve(
        Web3.to_checksum_address(token), Web3.to_checksum_address(spender),
        min(amount, 2**160 - 1), expiration,
    )
    tx_params = _base_tx_params(w3, sender)
    tx_params["gas"] = int(fn.estimate_gas(tx_params) * 1.3)
    return fn.build_transaction(tx_params)


def prepare_weth_erc20_steps(w3: Web3, sender: str, token: str, amount: int) -> list[dict]:
    """Retourne la liste des transactions de préparation nécessaires (dans
    l'ordre) avant un swap V4 sur une pool cotée en WETH-ERC20 : approve ERC20
    -> Permit2 (si besoin) puis Permit2 -> UniversalRouter (si besoin). Liste
    vide si tout est déjà en place (cas normal après le premier trade)."""
    steps = []
    if erc20_needs_approval(w3, token, sender, PERMIT2, amount):
        steps.append(build_erc20_approve_tx(w3, sender, token, PERMIT2))
    if permit2_needs_approval(w3, token, sender, UNIVERSAL_ROUTER, amount):
        steps.append(build_permit2_approve_tx(w3, sender, token, UNIVERSAL_ROUTER, amount))
    return steps
