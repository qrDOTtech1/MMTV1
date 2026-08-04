"""
Interface Web3 avec Base L2.
Gère la connexion, les prix, et l'exécution de swaps.
Multi-RPC avec rotation automatique sur rate limit.
"""

import json
import time
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from core.config import Config

BASE_RPCS = [
    "https://mainnet.base.org",
    "https://base.drpc.org",
    "https://base-rpc.publicnode.com",
]

ERC20_ABI = json.loads('[{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},{"constant":true,"inputs":[{"name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":false,"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"type":"function"}]')

UNISWAP_V2_PAIR_ABI = json.loads('[{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"name":"_reserve0","type":"uint112"},{"name":"_reserve1","type":"uint112"},{"name":"_blockTimestampLast","type":"uint32"}],"type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"}]')

UNISWAP_V2_FACTORY_ABI = json.loads('[{"constant":true,"inputs":[{"name":"tokenA","type":"address"},{"name":"tokenB","type":"address"}],"name":"getPair","outputs":[{"name":"pair","type":"address"}],"type":"function"},{"anonymous":false,"inputs":[{"indexed":true,"name":"token0","type":"address"},{"indexed":true,"name":"token1","type":"address"},{"indexed":false,"name":"pair","type":"address"},{"indexed":false,"name":"","type":"uint256"}],"name":"PairCreated","type":"event"}]')

AERODROME_FACTORY_ABI = json.loads('[{"inputs":[{"name":"tokenA","type":"address"},{"name":"tokenB","type":"address"},{"name":"stable","type":"bool"}],"name":"getPool","outputs":[{"name":"","type":"address"}],"type":"function"}]')

UNISWAP_V2_ROUTER_ABI = json.loads('[{"inputs":[{"name":"amountIn","type":"uint256"},{"name":"amountOutMin","type":"uint256"},{"name":"path","type":"address[]"},{"name":"to","type":"address"},{"name":"deadline","type":"uint256"}],"name":"swapExactETHForTokens","outputs":[{"name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"name":"amountIn","type":"uint256"},{"name":"amountOutMin","type":"uint256"},{"name":"path","type":"address[]"},{"name":"to","type":"address"},{"name":"deadline","type":"uint256"}],"name":"swapExactTokensForETH","outputs":[{"name":"amounts","type":"uint256[]"}],"type":"function"},{"inputs":[{"name":"amountOutMin","type":"uint256"},{"name":"path","type":"address[]"},{"name":"to","type":"address"},{"name":"deadline","type":"uint256"}],"name":"swapExactETHForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"name":"amountIn","type":"uint256"},{"name":"amountOutMin","type":"uint256"},{"name":"path","type":"address[]"},{"name":"to","type":"address"},{"name":"deadline","type":"uint256"}],"name":"swapExactTokensForETHSupportingFeeOnTransferTokens","outputs":[],"type":"function"},{"inputs":[{"name":"amountIn","type":"uint256"},{"name":"path","type":"address[]"}],"name":"getAmountsOut","outputs":[{"name":"amounts","type":"uint256[]"}],"type":"function"}]')

ERC20_ALLOWANCE_ABI = json.loads('[{"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"type":"function"}]')

# --- ABIs Uniswap V3 (SwapRouter02 + QuoterV2 + Factory) ---
UNISWAP_V3_ROUTER_ABI = json.loads('''[
  {"inputs":[{"components":[{"name":"tokenIn","type":"address"},{"name":"tokenOut","type":"address"},{"name":"fee","type":"uint24"},{"name":"recipient","type":"address"},{"name":"amountIn","type":"uint256"},{"name":"amountOutMinimum","type":"uint256"},{"name":"sqrtPriceLimitX96","type":"uint160"}],"name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},
  {"inputs":[],"name":"refundETH","outputs":[],"stateMutability":"payable","type":"function"},
  {"inputs":[{"name":"amountMinimum","type":"uint256"},{"name":"recipient","type":"address"}],"name":"unwrapWETH9","outputs":[],"stateMutability":"payable","type":"function"}
]''')

UNISWAP_V3_QUOTER_ABI = json.loads('''[
  {"inputs":[{"components":[{"name":"tokenIn","type":"address"},{"name":"tokenOut","type":"address"},{"name":"amountIn","type":"uint256"},{"name":"fee","type":"uint24"},{"name":"sqrtPriceLimitX96","type":"uint160"}],"name":"params","type":"tuple"}],"name":"quoteExactInputSingle","outputs":[{"name":"amountOut","type":"uint256"},{"name":"sqrtPriceX96After","type":"uint160"},{"name":"initializedTicksCrossed","type":"uint32"},{"name":"gasEstimate","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}
]''')

UNISWAP_V3_FACTORY_ABI = json.loads('''[
  {"inputs":[{"name":"tokenA","type":"address"},{"name":"tokenB","type":"address"},{"name":"fee","type":"uint24"}],"name":"getPool","outputs":[{"name":"pool","type":"address"}],"stateMutability":"view","type":"function"}
]''')

V3_FEE_TIERS = [500, 3000, 10000, 100]  # ordre de probabilité décroissante sur Base

# L'exécution V4 (PoolKey réelle via events, ABIs Universal Router/Permit2,
# constantes de commandes/actions) vit dans core/dex_v4.py — pas dupliquée
# ici, pour n'avoir qu'une seule source de vérité sur du code qui manipule
# de l'argent réel.

WETH = Web3.to_checksum_address("0x4200000000000000000000000000000000000006")
UNISWAP_V2_FACTORY = Web3.to_checksum_address("0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6")
UNISWAP_V2_ROUTER = Web3.to_checksum_address("0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24")
AERODROME_FACTORY = Web3.to_checksum_address("0x420DD381b31aEf6683db6B902084cB0FFECe40Da")
AERODROME_ROUTER = Web3.to_checksum_address("0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43")

UNISWAP_V3_FACTORY = Web3.to_checksum_address("0x33128a8fC17869897dcE68Ed026d694621f6FDfD")
UNISWAP_V3_ROUTER = Web3.to_checksum_address("0x2626664c2603336E57B271c5C0b26F421741e481")
UNISWAP_V3_QUOTER = Web3.to_checksum_address("0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a")


class Chain:
    def __init__(self, config: Config):
        self.config = config
        # Le RPC du .env (ex: endpoint Alchemy dédié) passe en tête de la
        # rotation : en cas d'erreur passagère on essaie les publics, puis on
        # REVIENT dessus au tour suivant au lieu de rester sur les publics.
        self._rpcs = [config.rpc_url] + [u for u in BASE_RPCS if u != config.rpc_url]
        self._rpc_index = 0
        self._last_call = 0.0
        self._min_interval = 0.2
        self._init_web3(self._rpcs[0])

        if config.private_key:
            self.account = self.w3.eth.account.from_key(config.private_key)
            self.address = self.account.address
        else:
            self.account = None
            self.address = None

    def _init_web3(self, rpc_url: str):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 15}))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.factory = self.w3.eth.contract(address=UNISWAP_V2_FACTORY, abi=UNISWAP_V2_FACTORY_ABI)
        self.router = self.w3.eth.contract(address=UNISWAP_V2_ROUTER, abi=UNISWAP_V2_ROUTER_ABI)

        self.v3_factory = self.w3.eth.contract(address=UNISWAP_V3_FACTORY, abi=UNISWAP_V3_FACTORY_ABI)
        self.v3_router = self.w3.eth.contract(address=UNISWAP_V3_ROUTER, abi=UNISWAP_V3_ROUTER_ABI)
        self.v3_quoter = self.w3.eth.contract(address=UNISWAP_V3_QUOTER, abi=UNISWAP_V3_QUOTER_ABI)

        # RPC DÉDIÉ au scan d'events V4 (eth_getLogs). Certains endpoints
        # dédiés (Alchemy) REJETTENT ces requêtes getLogs par topic, ce qui
        # faisait exploser la bisection en centaines de sous-requêtes qui
        # échouent → sniper gelé. On force donc un RPC public connu pour les
        # accepter, uniquement pour la découverte de pools V4. Le reste
        # (solde, devis, envoi de tx) reste sur self.w3 (Alchemy).
        log_rpc = next((u for u in BASE_RPCS if "alchemy" not in u and "1rpc" not in u),
                       "https://mainnet.base.org")
        self.log_w3 = Web3(Web3.HTTPProvider(log_rpc, request_kwargs={"timeout": 20}))
        self.log_w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    def _throttle(self):
        elapsed = time.time() - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call = time.time()

    def _rotate_rpc(self):
        self._rpc_index = (self._rpc_index + 1) % len(self._rpcs)
        self._init_web3(self._rpcs[self._rpc_index])

    def safe_call(self, fn, retries=3):
        for i in range(retries):
            self._throttle()
            try:
                return fn()
            except Exception as e:
                err = str(e)
                # -32001/"usage limit" : quota atteint chez certains RPC publics
                # (ex: 1rpc.io) — même traitement qu'un 429 : on tourne.
                if ("429" in err or "Too Many" in err or "521" in err or "502" in err
                        or "503" in err or "-32001" in err or "usage limit" in err.lower()
                        or "rate limit" in err.lower()):
                    self._rotate_rpc()
                    time.sleep(1.5)
                elif i == retries - 1:
                    raise
        return None

    def get_eth_balance(self) -> float:
        if not self.address:
            return 0.0
        bal = self.safe_call(lambda: self.w3.eth.get_balance(self.address))
        return float(self.w3.from_wei(bal, "ether"))

    def get_token_info(self, token_address: str) -> dict:
        token = self.w3.eth.contract(
            address=Web3.to_checksum_address(token_address), abi=ERC20_ABI,
        )
        try:
            symbol = self.safe_call(lambda: token.functions.symbol().call())
            decimals = self.safe_call(lambda: token.functions.decimals().call())
            total_supply = self.safe_call(lambda: token.functions.totalSupply().call())
            return {"symbol": symbol, "decimals": decimals, "total_supply": total_supply}
        except Exception:
            return {"symbol": "???", "decimals": 18, "total_supply": 0}

    def get_pair_reserves(self, pair_address: str) -> tuple:
        pair = self.w3.eth.contract(
            address=Web3.to_checksum_address(pair_address), abi=UNISWAP_V2_PAIR_ABI,
        )
        reserves = self.safe_call(lambda: pair.functions.getReserves().call())
        token0 = self.safe_call(lambda: pair.functions.token0().call())
        return reserves[0], reserves[1], token0

    def get_token_price_eth(self, token_address: str) -> float | None:
        token_address = Web3.to_checksum_address(token_address)
        pair_address = self.safe_call(
            lambda: self.factory.functions.getPair(token_address, WETH).call()
        )
        if pair_address == "0x" + "0" * 40:
            return None
        r0, r1, token0 = self.get_pair_reserves(pair_address)
        if r0 == 0 or r1 == 0:
            return None
        if token0.lower() == token_address.lower():
            return r1 / r0
        else:
            return r0 / r1

    def estimate_swap_out(self, amount_in_eth: float, token_address: str) -> int | None:
        try:
            amount_in = self.w3.to_wei(amount_in_eth, "ether")
            path = [WETH, Web3.to_checksum_address(token_address)]
            return self.safe_call(
                lambda: self.router.functions.getAmountsOut(amount_in, path).call()
            )[1]
        except Exception:
            return None

    # ── Exécution réelle (mode live) ──

    def get_token_balance(self, token_address: str) -> int:
        """Balance brute (wei du token) du wallet."""
        token = self.w3.eth.contract(
            address=Web3.to_checksum_address(token_address), abi=ERC20_ABI,
        )
        return self.safe_call(lambda: token.functions.balanceOf(self.address).call()) or 0

    def _sign_send_wait(self, tx: dict, timeout: int = 90) -> dict:
        """Signe, envoie, attend le receipt. Lève si la tx échoue on-chain."""
        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
        if receipt["status"] != 1:
            raise RuntimeError(f"tx revert on-chain: {tx_hash.hex()}")
        return receipt

    def _base_tx_params(self, value: int = 0) -> dict:
        return {
            "from": self.address,
            "value": value,
            "gasPrice": int(self.w3.eth.gas_price * 1.1),
            "nonce": self.w3.eth.get_transaction_count(self.address),
            "chainId": self.config.chain_id,
        }

    def execute_buy(self, amount_eth: float, token_address: str, slippage: float = 0.10) -> dict:
        """Achat réel ETH -> token. Retourne {tx_hash, tokens_received, gas_eth}."""
        if not self.account:
            raise RuntimeError("pas de clé privée configurée")
        token_cs = Web3.to_checksum_address(token_address)
        amount_in = self.w3.to_wei(amount_eth, "ether")
        path = [WETH, token_cs]

        amounts_out = self.safe_call(
            lambda: self.router.functions.getAmountsOut(amount_in, path).call()
        )
        if not amounts_out or amounts_out[1] == 0:
            raise RuntimeError("pas de route de swap (token pas sur Uniswap V2)")
        min_out = int(amounts_out[1] * (1 - slippage))
        deadline = int(time.time()) + 300

        balance_before = self.get_token_balance(token_cs)

        # Variante fee-on-transfer : compatible avec les tokens taxés (la plupart des memecoins)
        fn = self.router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
            min_out, path, self.address, deadline
        )
        params = self._base_tx_params(value=amount_in)
        params["gas"] = int(fn.estimate_gas({**params}) * 1.3)
        receipt = self._sign_send_wait(fn.build_transaction(params))

        tokens_received = self.get_token_balance(token_cs) - balance_before
        gas_eth = float(self.w3.from_wei(
            receipt["gasUsed"] * receipt.get("effectiveGasPrice", params["gasPrice"]), "ether"
        ))
        return {
            "tx_hash": receipt["transactionHash"].hex(),
            "tokens_received": tokens_received,
            "gas_eth": gas_eth,
        }

    def _ensure_approval(self, token_address: str, amount: int):
        token_cs = Web3.to_checksum_address(token_address)
        token = self.w3.eth.contract(address=token_cs, abi=ERC20_ABI + ERC20_ALLOWANCE_ABI)
        allowance = self.safe_call(
            lambda: token.functions.allowance(self.address, UNISWAP_V2_ROUTER).call()
        ) or 0
        if allowance >= amount:
            return
        fn = token.functions.approve(UNISWAP_V2_ROUTER, 2**256 - 1)
        params = self._base_tx_params()
        params["gas"] = int(fn.estimate_gas({**params}) * 1.3)
        self._sign_send_wait(fn.build_transaction(params))

    def _estimate_gas_retry(self, fn, params: dict, tries: int = 4) -> int:
        """estimate_gas avec retries. Les nœuds Alchemy sont load-balancés et
        certains ont un état en retard de quelques blocs : juste après un
        approve, la simulation peut tomber sur un nœud qui ne le voit pas
        encore -> revert TRANSFER_FROM_FAILED fantôme. On réessaie avec
        backoff avant de conclure à un vrai échec (vécu : vente VIRTUAL
        impossible pendant des minutes alors que tout était correct)."""
        last_err = None
        for attempt in range(tries):
            try:
                return fn.estimate_gas({**params})
            except Exception as e:
                last_err = e
                if attempt < tries - 1:
                    time.sleep(1.5 * (attempt + 1))
        raise last_err

    def execute_sell(self, token_address: str, fraction: float = 1.0, slippage: float = 0.15) -> dict:
        """Vente réelle d'une fraction du solde du token -> ETH (1.0 = tout).
        Retourne {tx_hash, eth_received, gas_eth}."""
        if not self.account:
            raise RuntimeError("pas de clé privée configurée")
        fraction = max(0.0, min(fraction, 1.0))
        token_cs = Web3.to_checksum_address(token_address)
        full_balance = self.get_token_balance(token_cs)
        # Arithmétique ENTIÈRE obligatoire : int(balance * 1.0) passe par un
        # float 64 bits (~15 chiffres) et peut ARRONDIR AU-DESSUS du solde réel
        # sur un token 18 décimales -> transferFrom revert (TRANSFER_FROM_FAILED).
        # Bug réel : vente 100% de VIRTUAL impossible, +249 wei fantômes.
        amount_in = full_balance if fraction >= 1.0 else (full_balance * int(fraction * 10_000)) // 10_000
        if amount_in == 0:
            raise RuntimeError("solde token nul, rien à vendre")

        self._ensure_approval(token_cs, amount_in)

        path = [token_cs, WETH]
        amounts_out = self.safe_call(
            lambda: self.router.functions.getAmountsOut(amount_in, path).call()
        )
        if not amounts_out or amounts_out[1] == 0:
            raise RuntimeError("pas de route de vente")
        min_out = int(amounts_out[1] * (1 - slippage))
        deadline = int(time.time()) + 300

        eth_before = self.w3.eth.get_balance(self.address)

        fn = self.router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
            amount_in, min_out, path, self.address, deadline
        )
        params = self._base_tx_params()
        params["gas"] = int(self._estimate_gas_retry(fn, params) * 1.3)
        receipt = self._sign_send_wait(fn.build_transaction(params))

        gas_wei = receipt["gasUsed"] * receipt.get("effectiveGasPrice", params["gasPrice"])
        eth_received = float(self.w3.from_wei(
            self.w3.eth.get_balance(self.address) - eth_before + gas_wei, "ether"
        ))
        gas_eth = float(self.w3.from_wei(gas_wei, "ether"))
        return {
            "tx_hash": receipt["transactionHash"].hex(),
            "eth_received": eth_received,
            "gas_eth": gas_eth,
        }

    def can_sell_on_v2(self, token_address: str) -> bool:
        """Vérifie qu'une route de vente existe sur Uniswap V2 (pré-requis pour acheter en live)."""
        try:
            token_cs = Web3.to_checksum_address(token_address)
            amounts = self.safe_call(
                lambda: self.router.functions.getAmountsOut(
                    10**9, [token_cs, WETH]
                ).call()
            )
            return bool(amounts and amounts[1] > 0)
        except Exception:
            return False

    # ── Uniswap V3 ──

    def find_v3_fee_tier(self, token_address: str) -> int | None:
        """Sonde les fee tiers standards pour trouver la pool V3 token/WETH.
        Retourne le fee (en centièmes de bps, ex 3000 = 0.3%) ou None."""
        token_cs = Web3.to_checksum_address(token_address)
        zero = "0x" + "0" * 40
        for fee in V3_FEE_TIERS:
            try:
                pool = self.safe_call(
                    lambda f=fee: self.v3_factory.functions.getPool(token_cs, WETH, f).call()
                )
                if pool and pool != zero:
                    return fee
            except Exception:
                continue
        return None

    def v3_quote_exact_in(self, token_in: str, token_out: str, amount_in: int, fee: int) -> int | None:
        """Devis V3 via QuoterV2 (appel en lecture seule, aucun coût)."""
        try:
            params = (Web3.to_checksum_address(token_in), Web3.to_checksum_address(token_out),
                      amount_in, fee, 0)
            result = self.safe_call(
                lambda: self.v3_quoter.functions.quoteExactInputSingle(params).call()
            )
            return result[0] if result else None
        except Exception:
            return None

    def execute_buy_v3(self, amount_eth: float, token_address: str, fee: int, slippage: float = 0.10) -> dict:
        """Achat réel ETH -> token via Uniswap V3 SwapRouter02. Devis validé par
        QuoterV2, puis simulé (eth_call, gratuit) avant tout envoi réel."""
        if not self.account:
            raise RuntimeError("pas de clé privée configurée")
        token_cs = Web3.to_checksum_address(token_address)
        amount_in = self.w3.to_wei(amount_eth, "ether")

        quoted_out = self.v3_quote_exact_in(WETH, token_cs, amount_in, fee)
        if not quoted_out or quoted_out == 0:
            raise RuntimeError(f"pas de devis V3 exploitable (fee={fee})")
        min_out = int(quoted_out * (1 - slippage))

        # ExactInputSingleParams V3 : (tokenIn, tokenOut, fee, recipient, amountIn, amountOutMinimum, sqrtPriceLimitX96)
        params = (WETH, token_cs, fee, self.address, amount_in, min_out, 0)

        fn = self.v3_router.functions.exactInputSingle(params)
        base_params = self._base_tx_params(value=amount_in)

        # Simulation gratuite avant tout envoi réel : si ça revert ici, aucune
        # transaction n'est envoyée (pas de gas gaspillé, pas de tx échouée).
        try:
            fn.call(base_params)
        except Exception as e:
            raise RuntimeError(f"simulation V3 échouée (pas de liquidité suffisante ?) : {e}")

        balance_before = self.get_token_balance(token_cs)
        base_params["gas"] = int(fn.estimate_gas(base_params) * 1.3)
        receipt = self._sign_send_wait(fn.build_transaction(base_params))

        tokens_received = self.get_token_balance(token_cs) - balance_before
        gas_eth = float(self.w3.from_wei(
            receipt["gasUsed"] * receipt.get("effectiveGasPrice", base_params["gasPrice"]), "ether"
        ))
        return {"tx_hash": receipt["transactionHash"].hex(), "tokens_received": tokens_received, "gas_eth": gas_eth}

    def _ensure_approval_v3(self, token_address: str, amount: int):
        token_cs = Web3.to_checksum_address(token_address)
        token = self.w3.eth.contract(address=token_cs, abi=ERC20_ABI + ERC20_ALLOWANCE_ABI)
        allowance = self.safe_call(
            lambda: token.functions.allowance(self.address, UNISWAP_V3_ROUTER).call()
        ) or 0
        if allowance >= amount:
            return
        fn = token.functions.approve(UNISWAP_V3_ROUTER, 2**256 - 1)
        params = self._base_tx_params()
        params["gas"] = int(fn.estimate_gas({**params}) * 1.3)
        self._sign_send_wait(fn.build_transaction(params))

    def execute_sell_v3(self, token_address: str, fee: int, fraction: float = 1.0, slippage: float = 0.15) -> dict:
        """Vente réelle d'une fraction du solde du token -> ETH via V3."""
        if not self.account:
            raise RuntimeError("pas de clé privée configurée")
        fraction = max(0.0, min(fraction, 1.0))
        token_cs = Web3.to_checksum_address(token_address)
        full_balance = self.get_token_balance(token_cs)
        # Arithmétique ENTIÈRE obligatoire : int(balance * 1.0) passe par un
        # float 64 bits (~15 chiffres) et peut ARRONDIR AU-DESSUS du solde réel
        # sur un token 18 décimales -> transferFrom revert (TRANSFER_FROM_FAILED).
        # Bug réel : vente 100% de VIRTUAL impossible, +249 wei fantômes.
        amount_in = full_balance if fraction >= 1.0 else (full_balance * int(fraction * 10_000)) // 10_000
        if amount_in == 0:
            raise RuntimeError("solde token nul, rien à vendre")

        self._ensure_approval_v3(token_cs, amount_in)

        quoted_out = self.v3_quote_exact_in(token_cs, WETH, amount_in, fee)
        if not quoted_out or quoted_out == 0:
            raise RuntimeError(f"pas de devis V3 exploitable en vente (fee={fee})")
        min_out = int(quoted_out * (1 - slippage))

        # recipient = router lui-même : le WETH reçu doit être unwrap en ETH natif
        # via unwrapWETH9 dans le même bloc (pattern standard SwapRouter02).
        params = (token_cs, WETH, fee, self.address, amount_in, min_out, 0)
        fn = self.v3_router.functions.exactInputSingle(params)
        base_params = self._base_tx_params()

        try:
            fn.call(base_params)
        except Exception as e:
            raise RuntimeError(f"simulation V3 échouée : {e}")

        eth_before = self.w3.eth.get_balance(self.address)
        base_params["gas"] = int(fn.estimate_gas(base_params) * 1.3)
        receipt = self._sign_send_wait(fn.build_transaction(base_params))

        gas_wei = receipt["gasUsed"] * receipt.get("effectiveGasPrice", base_params["gasPrice"])
        eth_received = float(self.w3.from_wei(
            self.w3.eth.get_balance(self.address) - eth_before + gas_wei, "ether"
        ))
        gas_eth = float(self.w3.from_wei(gas_wei, "ether"))
        return {"tx_hash": receipt["transactionHash"].hex(), "eth_received": eth_received, "gas_eth": gas_eth}

    # ── Uniswap V4 (Universal Router) ──
    # Délègue à core/dex_v4.py, qui lit la VRAIE PoolKey via les events
    # Initialize du PoolManager (pas une devinette de fee/tickSpacing) et gère
    # les deux cas réels observés sur Base : ETH natif (rare) et WETH-ERC20 via
    # Permit2 (la grande majorité des pools V4 constatés en pratique — un
    # premier design ici qui ne gérait que l'ETH natif aurait échoué sur
    # ~100% des pools V4 réellement rencontrés).

    def find_v4_pool(self, token_address: str):
        """Retourne la PoolKey réelle (core.dex_v4.PoolKey) ou None. Refuse
        déjà les hooks non nuls au niveau de find_pool_key/build_swap_tx.
        Le scan d'events passe par self.log_w3 (RPC public), pas Alchemy."""
        from core.dex_v4 import find_pool_key
        return find_pool_key(self.log_w3, token_address)

    def execute_buy_v4(self, amount_eth: float, token_address: str, slippage: float = 0.12) -> dict:
        """Achat réel ETH -> token via V4 Universal Router. Gère les deux cas :
        pool cotée ETH natif (paiement direct par value) ou WETH-ERC20 (wrap +
        double approbation Permit2 au préalable, healthchecks avant chaque
        étape pour ne rien approuver deux fois inutilement)."""
        from core.dex_v4 import (
            find_pool_key, build_swap_tx, build_wrap_eth_tx, prepare_weth_erc20_steps,
            quote_exact_input_single, NATIVE_ETH, WETH_BASE as V4_WETH,
        )
        if not self.account:
            raise RuntimeError("pas de clé privée configurée")
        token_cs = Web3.to_checksum_address(token_address)
        amount_in = self.w3.to_wei(amount_eth, "ether")

        pool_key = find_pool_key(self.log_w3, token_cs)
        if pool_key is None:
            raise RuntimeError("aucune pool V4 trouvée pour ce token (ou hors fenêtre de recherche)")
        if pool_key.has_hooks:
            raise RuntimeError(f"pool V4 avec hooks {pool_key.hooks} — refusé (code non auditable)")

        zero_for_one = pool_key.currency0.lower() == token_cs.lower()
        zero_for_one = not zero_for_one  # on ACHÈTE le token : il doit être la sortie, pas l'entrée
        input_currency = pool_key.currency0 if zero_for_one else pool_key.currency1
        is_native = input_currency.lower() == NATIVE_ETH.lower()

        if not is_native:
            # Pool cotée WETH-ERC20 : wrap puis double approbation Permit2 si besoin
            wrap_tx = build_wrap_eth_tx(self.w3, self.address, amount_in)
            self._sign_send_wait(wrap_tx)
            for step_tx in prepare_weth_erc20_steps(self.w3, self.address, V4_WETH, amount_in):
                self._sign_send_wait(step_tx)

        try:
            expected_out = quote_exact_input_single(self.w3, pool_key, zero_for_one, amount_in)
        except Exception as e:
            raise RuntimeError(f"devis V4 impossible (pool sans liquidité ?) : {e}")
        min_out = int(expected_out * (1 - slippage))
        value_wei = amount_in if is_native else 0

        try:
            tx = build_swap_tx(self.w3, self.address, pool_key, zero_for_one, amount_in, min_out, value_wei)
        except Exception as e:
            raise RuntimeError(f"construction tx V4 échouée : {e}")

        balance_before = self.get_token_balance(token_cs)
        receipt = self._sign_send_wait(tx)

        tokens_received = self.get_token_balance(token_cs) - balance_before
        gas_eth = float(self.w3.from_wei(
            receipt["gasUsed"] * receipt.get("effectiveGasPrice", tx["gasPrice"]), "ether"
        ))
        return {"tx_hash": receipt["transactionHash"].hex(), "tokens_received": tokens_received, "gas_eth": gas_eth}

    def execute_sell_v4(self, token_address: str, fraction: float = 1.0, slippage: float = 0.15) -> dict:
        """Vente réelle d'une fraction du solde du token -> ETH via V4. Le
        produit de vente (WETH si pool WETH-ERC20) est unwrap en ETH natif
        dans une étape séparée après le swap."""
        from core.dex_v4 import (
            find_pool_key, build_swap_tx, build_unwrap_weth_tx, prepare_weth_erc20_steps,
            quote_exact_input_single, NATIVE_ETH, WETH_BASE as V4_WETH,
        )
        if not self.account:
            raise RuntimeError("pas de clé privée configurée")
        fraction = max(0.0, min(fraction, 1.0))
        token_cs = Web3.to_checksum_address(token_address)
        full_balance = self.get_token_balance(token_cs)
        # Arithmétique ENTIÈRE obligatoire : int(balance * 1.0) passe par un
        # float 64 bits (~15 chiffres) et peut ARRONDIR AU-DESSUS du solde réel
        # sur un token 18 décimales -> transferFrom revert (TRANSFER_FROM_FAILED).
        # Bug réel : vente 100% de VIRTUAL impossible, +249 wei fantômes.
        amount_in = full_balance if fraction >= 1.0 else (full_balance * int(fraction * 10_000)) // 10_000
        if amount_in == 0:
            raise RuntimeError("solde token nul, rien à vendre")

        pool_key = find_pool_key(self.log_w3, token_cs)
        if pool_key is None:
            raise RuntimeError("aucune pool V4 trouvée pour ce token")
        if pool_key.has_hooks:
            raise RuntimeError(f"pool V4 avec hooks {pool_key.hooks} — refusé (code non auditable)")

        zero_for_one = pool_key.currency0.lower() == token_cs.lower()
        output_currency = pool_key.currency1 if zero_for_one else pool_key.currency0
        is_native_out = output_currency.lower() == NATIVE_ETH.lower()

        for step_tx in prepare_weth_erc20_steps(self.w3, self.address, token_cs, amount_in):
            self._sign_send_wait(step_tx)

        try:
            expected_out = quote_exact_input_single(self.w3, pool_key, zero_for_one, amount_in)
        except Exception as e:
            raise RuntimeError(f"devis V4 impossible (pool sans liquidité ?) : {e}")
        min_out = int(expected_out * (1 - slippage))
        try:
            tx = build_swap_tx(self.w3, self.address, pool_key, zero_for_one, amount_in, min_out, value_wei=0)
        except Exception as e:
            raise RuntimeError(f"construction tx V4 échouée : {e}")

        eth_before = self.w3.eth.get_balance(self.address)
        weth_before = self.get_token_balance(V4_WETH) if not is_native_out else 0
        receipt = self._sign_send_wait(tx)
        gas_wei = receipt["gasUsed"] * receipt.get("effectiveGasPrice", tx["gasPrice"])

        if not is_native_out:
            # Le produit de vente est en WETH : unwrap en ETH natif
            weth_received = self.get_token_balance(V4_WETH) - weth_before
            if weth_received > 0:
                unwrap_tx = build_unwrap_weth_tx(self.w3, self.address, weth_received)
                unwrap_receipt = self._sign_send_wait(unwrap_tx)
                gas_wei += unwrap_receipt["gasUsed"] * unwrap_receipt.get("effectiveGasPrice", unwrap_tx["gasPrice"])

        eth_received = float(self.w3.from_wei(
            self.w3.eth.get_balance(self.address) - eth_before + gas_wei, "ether"
        ))
        gas_eth = float(self.w3.from_wei(gas_wei, "ether"))
        return {"tx_hash": receipt["transactionHash"].hex(), "eth_received": eth_received, "gas_eth": gas_eth}

    def find_v4_route(self, token_address: str):
        """Compat : retourne (fee, tick_spacing) si une pool V4 exploitable
        (sans hook) existe, sinon None."""
        pool_key = self.find_v4_pool(token_address)
        if pool_key is None or pool_key.has_hooks:
            return None
        return (pool_key.fee, pool_key.tick_spacing)

    # ── Routage unifié : essaie V2 puis V3 puis V4 ──

    def quote_route_buy(self, route: dict, amount_eth: float, token_address: str) -> int | None:
        """Devis réel d'achat sur la route donnée : combien de tokens (unités
        brutes) recevrait-on pour amount_eth ? Lecture seule, aucun coût.
        Retourne None si le devis échoue — l'appelant doit alors REFUSER le
        trade, pas le tenter à l'aveugle."""
        token_cs = Web3.to_checksum_address(token_address)
        amount_in = self.w3.to_wei(amount_eth, "ether")
        try:
            if route["dex"] == "v2":
                amounts = self.safe_call(
                    lambda: self.router.functions.getAmountsOut(amount_in, [WETH, token_cs]).call()
                )
                return amounts[-1] if amounts else None
            if route["dex"] == "v3":
                return self.v3_quote_exact_in(WETH, token_cs, amount_in, route["fee"])
            if route["dex"] == "v4":
                from core.dex_v4 import quote_exact_input_single
                pool_key = self.find_v4_pool(token_cs)
                if pool_key is None or pool_key.has_hooks:
                    return None
                # on achète le token : l'entrée est l'autre currency de la pool
                zero_for_one = pool_key.currency0.lower() != token_cs.lower()
                return quote_exact_input_single(self.w3, pool_key, zero_for_one, amount_in)
        except Exception:
            return None
        return None

    def check_price_sanity(self, route: dict, amount_eth: float, token_address: str,
                            displayed_price_eth: float, tolerance: float = 0.35) -> tuple[bool, str]:
        """Garde-fou anti-pool-piège : compare le prix RÉEL d'exécution (devis
        on-chain de la route) au prix affiché par la source de données
        (DexScreener/Gecko). Le trade GITLAWB a payé ~5 000 000x le prix
        affiché dans une pool V2 leurre quasi vide — perte sèche, revente à
        zéro. Un devis réel avant achat rend ce piège impossible.

        Retourne (ok, raison). tolerance=0.35 laisse passer l'impact prix
        normal d'une petite pool, mais bloque tout écart au-delà de ±35%."""
        if displayed_price_eth <= 0:
            return False, "prix affiché nul ou négatif"
        tokens_out = self.quote_route_buy(route, amount_eth, token_address)
        if not tokens_out or tokens_out <= 0:
            return False, "devis réel impossible sur la route (pool vide ?)"
        try:
            decimals = self.get_token_info(token_address).get("decimals", 18)
        except Exception:
            decimals = 18
        implied_price = amount_eth / (tokens_out / 10 ** decimals)
        ratio = implied_price / displayed_price_eth
        if ratio > 1 + tolerance:
            return False, f"prix réel {ratio:.1f}x plus cher que l'affiché (pool piège ?)"
        if ratio < 1 / (1 + tolerance):
            return False, f"prix réel {1/ratio:.1f}x moins cher que l'affiché (données corrompues ?)"
        return True, f"écart prix réel/affiché {100*(ratio-1):+.1f}%"

    def find_best_route(self, token_address: str) -> dict:
        """Détermine quel DEX peut exécuter ce token, dans l'ordre V2 -> V3 -> V4
        (du plus simple/fiable au plus complexe). Retourne {"dex": "v2"|"v3"|"v4"|None, ...}."""
        if self.can_sell_on_v2(token_address):
            return {"dex": "v2"}
        fee = self.find_v3_fee_tier(token_address)
        if fee is not None:
            return {"dex": "v3", "fee": fee}
        v4_route = self.find_v4_route(token_address)
        if v4_route is not None:
            fee, tick_spacing = v4_route
            return {"dex": "v4", "fee": fee, "tick_spacing": tick_spacing}
        return {"dex": None}
