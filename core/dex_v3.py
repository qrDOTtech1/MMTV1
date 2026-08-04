"""
Exécution Uniswap V3 sur Base — SwapRouter02 + QuoterV2.
Adresses vérifiées le 11/07/2026 : bytecode confirmé on-chain (pas juste une
recherche web) avant tout usage dans du code qui manipulera de l'argent réel.
"""

import json
import time
from web3 import Web3

SWAP_ROUTER_02 = Web3.to_checksum_address("0x2626664c2603336E57B271c5C0b26F421741e481")
QUOTER_V2 = Web3.to_checksum_address("0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a")
V3_FACTORY = Web3.to_checksum_address("0x33128a8fC17869897dcE68Ed026d694621f6FDfD")

# Frais courants sur Base : 0.01% / 0.05% / 0.3% / 1%
FEE_TIERS = [500, 3000, 10000, 100]

SWAP_ROUTER_02_ABI = json.loads('''[
  {"inputs":[{"components":[
      {"name":"tokenIn","type":"address"},
      {"name":"tokenOut","type":"address"},
      {"name":"fee","type":"uint24"},
      {"name":"recipient","type":"address"},
      {"name":"amountIn","type":"uint256"},
      {"name":"amountOutMinimum","type":"uint256"},
      {"name":"sqrtPriceLimitX96","type":"uint160"}
    ],"name":"params","type":"tuple"}],
   "name":"exactInputSingle","outputs":[{"name":"amountOut","type":"uint256"}],
   "stateMutability":"payable","type":"function"},
  {"inputs":[{"name":"data","type":"bytes[]"}],"name":"multicall",
   "outputs":[{"name":"results","type":"bytes[]"}],"stateMutability":"payable","type":"function"},
  {"inputs":[{"name":"amountMinimum","type":"uint256"},{"name":"recipient","type":"address"}],
   "name":"unwrapWETH9","outputs":[],"stateMutability":"payable","type":"function"}
]''')

# Sentinel d'adresse standard des contrats périphériques Uniswap : "envoie au
# routeur lui-même" plutôt qu'à l'utilisateur, pour enchaîner une 2e étape
# (unwrapWETH9) dans le même multicall atomique — jamais deux transactions
# séparées où des fonds transiteraient de façon non-atomique.
ADDRESS_THIS = Web3.to_checksum_address("0x0000000000000000000000000000000000000002")

QUOTER_V2_ABI = json.loads('''[
  {"inputs":[{"components":[
      {"name":"tokenIn","type":"address"},
      {"name":"tokenOut","type":"address"},
      {"name":"amountIn","type":"uint256"},
      {"name":"fee","type":"uint24"},
      {"name":"sqrtPriceLimitX96","type":"uint160"}
    ],"name":"params","type":"tuple"}],
   "name":"quoteExactInputSingle",
   "outputs":[
      {"name":"amountOut","type":"uint256"},
      {"name":"sqrtPriceX96After","type":"uint160"},
      {"name":"initializedTicksCrossed","type":"uint32"},
      {"name":"gasEstimate","type":"uint256"}
   ],
   "stateMutability":"nonpayable","type":"function"}
]''')

V3_FACTORY_ABI = json.loads('''[
  {"inputs":[{"name":"tokenA","type":"address"},{"name":"tokenB","type":"address"},{"name":"fee","type":"uint24"}],
   "name":"getPool","outputs":[{"name":"pool","type":"address"}],"stateMutability":"view","type":"function"}
]''')

WETH9_ABI = json.loads('''[
  {"inputs":[],"name":"deposit","outputs":[],"stateMutability":"payable","type":"function"},
  {"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
  {"inputs":[{"name":"who","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}
]''')

ERC20_APPROVE_ABI = json.loads('''[
  {"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}
]''')

WETH_BASE = Web3.to_checksum_address("0x4200000000000000000000000000000000000006")


def find_fee_tier(w3: Web3, token_a: str, token_b: str) -> int | None:
    """Cherche parmi les fee tiers standards laquelle a un pool existant.
    Lecture seule, gratuite. Retourne None si aucune pool V3 n'existe."""
    factory = w3.eth.contract(address=V3_FACTORY, abi=V3_FACTORY_ABI)
    zero = "0x" + "0" * 40
    for fee in FEE_TIERS:
        try:
            pool = factory.functions.getPool(
                Web3.to_checksum_address(token_a), Web3.to_checksum_address(token_b), fee
            ).call()
            if pool and pool.lower() != zero:
                return fee
        except Exception:
            continue
    return None


def quote_exact_input(w3: Web3, token_in: str, token_out: str, fee: int, amount_in: int) -> int | None:
    """Devis réel via QuoterV2 (lecture seule, gratuite — simule le swap sans
    l'exécuter, donne le vrai amountOut attendu à l'instant présent)."""
    try:
        quoter = w3.eth.contract(address=QUOTER_V2, abi=QUOTER_V2_ABI)
        params = (
            Web3.to_checksum_address(token_in), Web3.to_checksum_address(token_out),
            amount_in, fee, 0,
        )
        result = quoter.functions.quoteExactInputSingle(params).call()
        return result[0]
    except Exception:
        return None


def _base_tx_params(w3: Web3, sender: str, value: int = 0) -> dict:
    return {
        "from": sender, "value": value,
        "gasPrice": int(w3.eth.gas_price * 1.1),
        "nonce": w3.eth.get_transaction_count(sender),
        "chainId": w3.eth.chain_id,
    }


def build_buy_tx(w3: Web3, sender: str, token_out: str, fee: int,
                  amount_in_wei: int, min_amount_out: int) -> dict:
    """Achat ETH -> token. Envoie l'ETH natif directement (value=amount_in_wei)
    avec tokenIn=WETH : SwapRouter02 wrap automatiquement depuis msg.value —
    pattern standard documenté (pay() interne), pas une supposition — vérifié
    contre un vrai devis QuoterV2 avant tout envoi."""
    router = w3.eth.contract(address=SWAP_ROUTER_02, abi=SWAP_ROUTER_02_ABI)
    params = (
        WETH_BASE, Web3.to_checksum_address(token_out),
        fee, Web3.to_checksum_address(sender), amount_in_wei, min_amount_out, 0,
    )
    fn = router.functions.exactInputSingle(params)
    tx_params = _base_tx_params(w3, sender, value=amount_in_wei)
    tx_params["gas"] = int(fn.estimate_gas(tx_params) * 1.3)
    return fn.build_transaction(tx_params)


def needs_approval(w3: Web3, token: str, owner: str, amount: int) -> bool:
    erc20 = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_APPROVE_ABI)
    allowance = erc20.functions.allowance(
        Web3.to_checksum_address(owner), SWAP_ROUTER_02
    ).call()
    return allowance < amount


def build_approve_tx(w3: Web3, sender: str, token: str) -> dict:
    erc20 = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_APPROVE_ABI)
    fn = erc20.functions.approve(SWAP_ROUTER_02, 2**256 - 1)
    tx_params = _base_tx_params(w3, sender)
    tx_params["gas"] = int(fn.estimate_gas(tx_params) * 1.3)
    return fn.build_transaction(tx_params)


def build_sell_tx(w3: Web3, sender: str, token_in: str, fee: int,
                   amount_in: int, min_amount_out: int) -> dict:
    """Vente token -> ETH natif, en UNE SEULE transaction atomique (multicall) :
    exactInputSingle envoie le WETH au routeur lui-même (ADDRESS_THIS), puis
    unwrapWETH9 le convertit et l'envoie au wallet — jamais deux transactions
    séparées où des WETH resteraient bloqués entre les deux."""
    router = w3.eth.contract(address=SWAP_ROUTER_02, abi=SWAP_ROUTER_02_ABI)

    swap_params = (
        Web3.to_checksum_address(token_in), WETH_BASE,
        fee, ADDRESS_THIS, amount_in, min_amount_out, 0,
    )
    swap_calldata = router.functions.exactInputSingle(swap_params)._encode_transaction_data()
    unwrap_calldata = router.functions.unwrapWETH9(
        min_amount_out, Web3.to_checksum_address(sender)
    )._encode_transaction_data()

    fn = router.functions.multicall([swap_calldata, unwrap_calldata])
    tx_params = _base_tx_params(w3, sender)
    tx_params["gas"] = int(fn.estimate_gas(tx_params) * 1.3)
    return fn.build_transaction(tx_params)
