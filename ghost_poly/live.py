"""
GHOST POLY — exécution RÉELLE sur Polymarket (Polygon + CLOB).

Historique de mise au point (2026-07-12), pour ne pas refaire les mêmes
détours : Polymarket a changé de protocole d'ordre côté serveur récemment.
- Le client publié `py-clob-client` (PyPI, dernière version 0.34.6) envoie un
  format d'ordre que le serveur rejette désormais ("invalid order version").
  Le remplacement officiel est le package séparé `py-clob-client-v2`.
- Se connecter via MetaMask sur le site Polymarket déploie un WALLET PROXY
  (smart contract) distinct de l'EOA — c'est LUI qui détient les fonds/positions
  et doit être passé en tant que `funder`. L'adresse se trouve sur le site :
  profil -> "Adresse développeur" (visible aussi via
  https://data-api.polymarket.com/value?user=<PROXY> qui renvoie sa valeur
  de positions réelle).
- Le signature_type qui fonctionne pour ce flux "deposit wallet" est
  POLY_1271 (valeur 3) — POLY_GNOSIS_SAFE (valeur 2, le type "classique" pour
  wallet proxy) donne "maker address not allowed, please use the deposit
  wallet flow".
- Taille minimale d'ordre CLOB observée : 5 parts (pas d'unité USD fixe, ça
  dépend du prix — prix x taille doit rester dans le solde disponible ET
  taille >= 5).

Prouvé en conditions réelles : ordre posté (status LIVE, visible via
get_open_orders() = vu par le compte Polymarket réel), puis annulé proprement.

Adresses de contrats vérifiées par bytecode on-chain le 2026-07-12 (voir docs
https://docs.polymarket.com/resources/contracts) :
  USDC.e            0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 (6 déc.)
  CTF ERC1155       0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
  CTF Exchange      0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
  NegRisk Exchange  0xC5d563A36AE78145C45a50134d48A1215220f80a
  NegRisk Adapter   0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from web3 import Web3

POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://1rpc.io/matic",
]
CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
MIN_ORDER_SIZE_SHARES = 5.0

USDC_E = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
CTF = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
CTF_EXCHANGE = Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
NEGRISK_EXCHANGE = Web3.to_checksum_address(
    "0xC5d563A36AE78145C45a50134d48A1215220f80a"
)
NEGRISK_ADAPTER = Web3.to_checksum_address("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296")
SPENDERS = [CTF_EXCHANGE, NEGRISK_EXCHANGE, NEGRISK_ADAPTER]

ERC20_ABI = json.loads(
    '[{"constant":true,"inputs":[{"name":"o","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"},'
    '{"constant":true,"inputs":[{"name":"o","type":"address"},{"name":"s","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"type":"function"},'
    '{"constant":false,"inputs":[{"name":"s","type":"address"},{"name":"a","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"type":"function"}]'
)
ERC1155_ABI = json.loads(
    '[{"inputs":[{"name":"account","type":"address"},{"name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"name":"","type":"bool"}],"stateMutability":"view","type":"function"},'
    '{"inputs":[{"name":"operator","type":"address"},{"name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"stateMutability":"nonpayable","type":"function"}]'
)

# CTF redeemPositions — signature standard Gnosis Conditional Tokens.
CTF_REDEEM_ABI = json.loads(
    '[{"inputs":[{"name":"collateralToken","type":"address"},{"name":"parentCollectionId","type":"bytes32"},'
    '{"name":"conditionId","type":"bytes32"},{"name":"indexSets","type":"uint256[]"}],'
    '"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}]'
)


def _w3() -> Web3:
    last = None
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 20}))
            w3.eth.chain_id  # probe
            return w3
        except Exception as e:
            last = e
    raise RuntimeError(f"aucun RPC Polygon joignable: {last}")


class PolyLive:
    """Toute l'exécution réelle. Instanciée seulement si PRIVATE_KEY et
    POLY_FUNDER_ADDRESS sont présentes dans le .env."""

    def __init__(self, private_key: str, funder_address: str):
        if not private_key:
            raise RuntimeError("PRIVATE_KEY absente du .env")
        if not funder_address:
            raise RuntimeError(
                "POLY_FUNDER_ADDRESS absente du .env — c'est l'adresse du "
                "wallet PROXY Polymarket (profil du site -> 'Adresse "
                "développeur'), pas ton adresse MetaMask."
            )
        self.w3 = _w3()
        self.account = self.w3.eth.account.from_key(private_key)
        self.address = self.account.address  # EOA signataire
        self.funder = Web3.to_checksum_address(
            funder_address
        )  # wallet qui détient les fonds
        self._pk = private_key
        self._client = None

    # ── état / prérequis ──

    def get_cash_usdc_fast(self) -> float | None:
        """SOLDE USDC SEUL (Steven 30/07, latence detection->achat) : status()
        fait 3 appels reseau EN SEQUENCE (gas POL, solde USDC, valeur proxy
        data-api avec timeout 10s) alors que _read_cash(max_age=0) - appele a
        CHAQUE detection d'arb pour forcer une lecture fraiche - n'utilise QUE
        le solde USDC. Les 2 autres appels (gas, proxy value) ne servent a
        rien dans le chemin de decision d'arb et pouvaient a eux seuls couter
        plusieurs secondes entre detection et achat. Un seul appel reseau ici."""
        try:
            from py_clob_client_v2 import BalanceAllowanceParams, AssetType

            c = self.client()
            bal = c.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            return round(int(bal.get("balance", 0)) / 1e6, 4)
        except Exception:
            return None

    def status(self) -> dict:
        pol = float(self.w3.from_wei(self.w3.eth.get_balance(self.address), "ether"))

        # Le vrai solde tradable n'est PAS une lecture ERC20 naïve sur le
        # proxy (testé : renvoie 0 alors que le compte a des fonds réels) —
        # Polymarket gère la custody différemment en interne. La seule
        # source de vérité fiable est LEUR endpoint balance-allowance, celui
        # que le serveur d'ordres consulte lui-même. Les allowances internes
        # (CTF Exchange, NegRisk Exchange/Adapter) sont déjà gérées par
        # Polymarket côté proxy — rien à approuver de notre part.
        cash_usdc = None
        try:
            from py_clob_client_v2 import BalanceAllowanceParams, AssetType

            c = self.client()
            bal = c.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            cash_usdc = int(bal.get("balance", 0)) / 1e6
        except Exception as e:
            self._status_error = str(e)[:150]

        try:
            r = requests.get(
                f"https://data-api.polymarket.com/value?user={self.funder}", timeout=10
            )
            proxy_value = (r.json() or [{}])[0].get("value", 0)
        except Exception:
            proxy_value = None

        ready = (cash_usdc or 0) >= (
            MIN_ORDER_SIZE_SHARES * 0.02
        )  # marge : au moins de quoi couvrir un ordre min à bas prix
        return {
            "signer_eoa": self.address,
            "funder_proxy": self.funder,
            "pol_gas_eoa": round(pol, 4),
            "cash_usdc": round(cash_usdc, 4) if cash_usdc is not None else None,
            "proxy_position_value": proxy_value,
            "ready": ready,
        }

    def setup_allowances(self) -> list[str]:
        """Approuve depuis l'EOA (utile seulement si tu tradais depuis l'EOA
        directement, ce qui n'est pas le flux utilisé ici — le funder proxy
        est déjà configuré par Polymarket lui-même). Laissé pour compat UI."""
        usdc = self.w3.eth.contract(address=USDC_E, abi=ERC20_ABI)
        ctf = self.w3.eth.contract(address=CTF, abi=ERC1155_ABI)
        txs = []
        nonce = self.w3.eth.get_transaction_count(self.address)
        for sp in SPENDERS:
            if usdc.functions.allowance(self.address, sp).call() < 10**12:
                fn = usdc.functions.approve(sp, 2**256 - 1)
                txs.append(self._send(fn, nonce))
                nonce += 1
            if not ctf.functions.isApprovedForAll(self.address, sp).call():
                fn = ctf.functions.setApprovalForAll(sp, True)
                txs.append(self._send(fn, nonce))
                nonce += 1
        return txs

    def _send(self, fn, nonce: int) -> str:
        params = {
            "from": self.address,
            "nonce": nonce,
            "gasPrice": int(self.w3.eth.gas_price * 1.2),
            "chainId": CHAIN_ID,
        }
        params["gas"] = int(fn.estimate_gas({"from": self.address}) * 1.3)
        signed = self.account.sign_transaction(fn.build_transaction(params))
        h = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = self.w3.eth.wait_for_transaction_receipt(h, timeout=120)
        if receipt["status"] != 1:
            raise RuntimeError(f"tx approbation revert: {h.hex()}")
        return h.hex()

    # ── client CLOB v2 (le v1/py-clob-client est obsolète côté serveur) ──

    def client(self):
        if self._client is None:
            from py_clob_client_v2.client import ClobClient
            from py_clob_client_v2 import SignatureTypeV2

            c = ClobClient(
                CLOB_HOST,
                key=self._pk,
                chain_id=CHAIN_ID,
                signature_type=int(SignatureTypeV2.POLY_1271),
                funder=self.funder,
            )
            creds = c.create_or_derive_api_key()
            c.set_api_creds(creds)
            self._client = c
        return self._client

    def ws_auth(self) -> dict:
        """Creds L2 (api_key/secret/passphrase) pour le canal WS "user" (Steven
        30/07, "on a WS aussi" - fills pousses en direct au lieu du polling
        REST position_size, qui attend le reglement on-chain custody)."""
        c = self.client()
        creds = c.creds
        return {
            "apiKey": creds.api_key,
            "secret": creds.api_secret,
            "passphrase": creds.api_passphrase,
        }

    def real_pnl_since(self, since_ts: float) -> dict:
        """VERITE TERRAIN (Steven 04/08) : PnL reel calcule depuis l'activite
        on-chain publiee par Polymarket, PAS depuis nos compteurs internes.
        Motif : le 04/08 le dashboard annoncait +28.84$ alors que l'activite
        reelle donnait -22.40$ sur la meme periode (ecart ~51$) -> toute
        decision prise sur le compteur interne etait faussee. Ici on
        additionne betement ce qui est SORTI (achats) et ce qui est RENTRE
        (ventes + redeems). Aucune interpretation, aucun etat local."""
        try:
            r = requests.get(
                "https://data-api.polymarket.com/activity",
                params={"user": self.funder, "limit": 500},
                timeout=10,
            )
            evts = r.json() or []
        except Exception as e:
            return {"ok": False, "error": str(e)[:120]}
        buy = sell = redeem = 0.0
        nb = ns = nr = 0
        for e in evts:
            if e.get("timestamp", 0) < since_ts:
                continue
            t = e.get("type")
            a = float(e.get("usdcSize") or 0)
            if t == "REDEEM":
                redeem += a
                nr += 1
            elif t == "TRADE":
                if e.get("side") == "BUY":
                    buy += a
                    nb += 1
                else:
                    sell += a
                    ns += 1
        return {
            "ok": True,
            "buy_usd": round(buy, 2),
            "sell_usd": round(sell, 2),
            "redeem_usd": round(redeem, 2),
            "net_usd": round(redeem + sell - buy, 2),
            "n_buys": nb,
            "n_sells": ns,
            "n_redeems": nr,
        }

    def redeem_resolved(self) -> int:
        """Réclame (redeem) les positions gagnantes de marchés déjà résolus,
        pour recréditer l'USDC au wallet — capital recyclé plus vite.

        On passe par la data-API pour lister les positions 'redeemable' avec
        de la valeur (>0 = on a gagné), puis CTF.redeemPositions(). Chaque
        redeem est d'abord validé par estimate_gas (dry-run) : si ça ne passe
        pas, on saute — jamais d'envoi à l'aveugle sur du money-code.
        Retourne le nombre de positions réclamées."""
        import requests

        try:
            r = requests.get(
                f"https://data-api.polymarket.com/positions?user={self.funder}&redeemable=true",
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            red = [
                p
                for p in r.json()
                if (p.get("currentValue", 0) or 0) > 0.01 and p.get("conditionId")
            ]
        except Exception:
            return 0
        if not red:
            return 0

        ctf = self.w3.eth.contract(address=CTF, abi=CTF_REDEEM_ABI)
        USDC = USDC_E
        ZERO32 = b"\x00" * 32
        claimed = 0
        for p in red:
            try:
                cond = p["conditionId"]
                cond_b = (
                    bytes.fromhex(cond[2:])
                    if cond.startswith("0x")
                    else bytes.fromhex(cond)
                )
                # marché binaire : index sets [1, 2] couvre les 2 issues
                fn = ctf.functions.redeemPositions(USDC, ZERO32, cond_b, [1, 2])
                fn.estimate_gas({"from": self.address})  # dry-run : lève si invalide
                params = {
                    "from": self.address,
                    "nonce": self.w3.eth.get_transaction_count(self.address),
                    "gasPrice": int(self.w3.eth.gas_price * 1.2),
                    "chainId": CHAIN_ID,
                }
                params["gas"] = int(fn.estimate_gas({"from": self.address}) * 1.3)
                signed = self.account.sign_transaction(fn.build_transaction(params))
                rcpt = self.w3.eth.wait_for_transaction_receipt(
                    self.w3.eth.send_raw_transaction(signed.raw_transaction),
                    timeout=120,
                )
                if rcpt["status"] == 1:
                    claimed += 1
            except Exception:
                continue  # ce redeem échoue (negRisk, déjà réclamé...) — on saute
        return claimed

    def settled_outcome(self, slug: str, token_id: str) -> dict:
        """Résolution AUTORITATIVE via Polymarket (data-api), PAS via Binance.

        Découverte du 21/07 : le calcul Binance strike-vs-close ne matche PAS la
        résolution reelle de Polymarket (un trade Binance='Down' a ete paye 'Up'
        par Polymarket). La seule verite = ce que Polymarket a settle.

        Cherche notre position par slug+asset ; quand elle est resolue
        (redeemable / curPrice tranche a 0 ou 1), renvoie gagne/perdu + la vraie
        valeur. Retourne {resolved: bool, won: bool|None, cur_value, cash_pnl}."""
        import requests

        try:
            r = requests.get(
                f"https://data-api.polymarket.com/positions?user={self.funder}",
                timeout=12,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            for p in r.json():
                if p.get("slug") == slug and str(p.get("asset")) == str(token_id):
                    cur_price = float(p.get("curPrice", 0) or 0)
                    cur_val = float(p.get("currentValue", 0) or 0)
                    resolved = (
                        p.get("redeemable") or cur_price <= 0.02 or cur_price >= 0.98
                    )
                    if not resolved:
                        return {
                            "resolved": False,
                            "found": True,
                            "won": None,
                            "cur_value": cur_val,
                            "cash_pnl": float(p.get("cashPnl", 0) or 0),
                        }
                    won = cur_price >= 0.5 or cur_val > 0.01
                    return {
                        "resolved": True,
                        "found": True,
                        "won": won,
                        "cur_value": cur_val,
                        "cash_pnl": float(p.get("cashPnl", 0) or 0),
                    }
            # BUG FIX (25/07) : distinguer "API a renvoye des positions mais la
            # notre manque" (= gagnee + redeemee) vs "API a renvoye une liste
            # vide" (= rate-limit / timeout / error). Avant : les 2 cas
            # retournait found=False et _resolve_market marquait won=True pour
            # LES DEUX jambes d'une paire ARB -> double-credit. Seule une
            # liste NON-VIDE justifie l'hypothese "redeemee = gagnee".
            positions = r.json()
            if not positions:
                # Liste vide = API ne repond pas correctement -> NE PAS
                # conclure a won=True, le caller doit garder en attente.
                return {
                    "resolved": False,
                    "found": False,
                    "won": None,
                    "cur_value": 0.0,
                    "cash_pnl": 0.0,
                    "api_empty": True,
                }
            # Liste non-vide mais notre position absente = gagnee + redeemee
            # (les gagnantes disparaissent, les perdantes restent a curVal 0).
            return {
                "resolved": False,
                "found": False,
                "won": None,
                "cur_value": 0.0,
                "cash_pnl": 0.0,
                "api_empty": False,
            }
        except Exception:
            pass
        return {
            "resolved": False,
            "found": None,
            "won": None,
            "cur_value": 0.0,
            "cash_pnl": 0.0,
            "api_empty": True,
        }

    def position_size_sure(self, token_id: str, attempts: int = 3) -> float:
        """position_size() avec RETRY (Steven 04/08). position_size() retourne
        -1.0 quand la lecture echoue, en disant explicitement "indetermine, ne
        PAS conclure a 0" -- mais TOUS les appelants faisaient `x if x>=0 else
        0.0`, donc concluaient a 0. Consequence mesuree : si la lecture rate
        alors qu'on detient deja des parts, le fill mesure (after - 0) vaut
        stock_existant + vrai_fill -> on croit avoir rempli 11.63 parts au lieu
        de 5, puis on "reequilibre" en vendant 6.63 VRAIES parts a perte
        (trace : BTC 1785804900, -1.724$). Explique aussi le desequilibre
        systematique (258 paires sur 323 a >5% d'ecart). Retourne -1.0 si
        vraiment indetermine apres retries : l'appelant DOIT alors renoncer,
        pas supposer 0."""
        for i in range(attempts):
            v = self.position_size(token_id)
            if v >= 0:
                return v
            if i < attempts - 1:
                time.sleep(0.15)
        return -1.0

    def position_size(self, token_id: str) -> float:
        """Nombre de parts RÉELLEMENT détenues on-chain pour un outcome-token.
        Source de vérité pour vendre : la DB dérive (positions résolues/vendues
        toujours marquées 'open', ou jamais remplies) — vendre selon la DB
        échoue en boucle (400) sur des parts qu'on ne détient pas."""
        from py_clob_client_v2 import BalanceAllowanceParams, AssetType

        try:
            c = self.client()
            bal = c.get_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL, token_id=token_id
                )
            )
            return int(bal.get("balance", 0)) / 1e6
        except Exception:
            return -1.0  # -1 = indéterminé (ne pas conclure à 0)

    def cancel_stale_orders(self) -> int:
        """Annule tout ordre encore ouvert (non rempli) — les ordres GTC de
        cette stratégie sont censés se remplir immédiatement (prix pris au
        meilleur ask). Un ordre encore là au prochain scan (~30s+) veut dire
        qu'il n'a pas matché et bloque du capital pour rien. Retourne le
        nombre annulé."""
        c = self.client()
        oo = c.get_open_orders()
        if not oo:
            return 0
        ids = [o["id"] for o in oo]
        c.cancel_orders(ids)
        return len(ids)

    # ── market making (Steven 23/07) : ordres MAKER (GTC, non-marketables),
    # postes loin du meilleur prix pour capturer le spread au lieu de le payer.
    # Distinct de execute_directional/sell_position (marketable, prix agressif).

    def post_limit_buy(self, token_id: str, price: float, size: float) -> dict:
        """Ordre BID maker : GTC au prix demande (PAS agressif -> ne doit
        jamais traverser le book, sinon ce n'est plus du market making mais
        du taking). L'appelant est responsable de verifier bid < best_ask
        avant d'appeler ceci. Retourne {success, order_id} pour permettre le
        cancel individuel plus tard (cancel_order)."""
        from py_clob_client_v2 import OrderArgsV2, OrderType

        c = self.client()
        args = OrderArgsV2(
            token_id=token_id, price=round(price, 2), size=round(size, 2), side="BUY"
        )
        try:
            resp = c.post_order(c.create_order(args), OrderType.GTC)
            oid = resp.get("orderID") or resp.get("order_id") or resp.get("id")
            return {
                "success": bool(oid) or resp.get("success", True) is not False,
                "order_id": oid,
                "raw": resp,
            }
        except Exception as e:
            return {"success": False, "error": str(e)[:150], "order_id": None}

    def post_limit_sell(self, token_id: str, price: float, size: float) -> dict:
        """Ordre ASK maker : symmetrique de post_limit_buy, cote SELL."""
        from py_clob_client_v2 import OrderArgsV2, OrderType

        c = self.client()
        args = OrderArgsV2(
            token_id=token_id, price=round(price, 2), size=round(size, 2), side="SELL"
        )
        try:
            resp = c.post_order(c.create_order(args), OrderType.GTC)
            oid = resp.get("orderID") or resp.get("order_id") or resp.get("id")
            return {
                "success": bool(oid) or resp.get("success", True) is not False,
                "order_id": oid,
                "raw": resp,
            }
        except Exception as e:
            return {"success": False, "error": str(e)[:150], "order_id": None}

    def cancel_order(self, order_id: str) -> bool:
        """Annule UN ordre precis (par opposition a cancel_stale_orders qui
        annule TOUT) -> necessaire pour un market maker qui gere plusieurs
        quotes actives simultanement sur des marches differents."""
        if not order_id:
            return False
        try:
            c = self.client()
            c.cancel_orders([order_id])
            return True
        except Exception:
            return False

    def get_open_orders_list(self) -> list:
        """Liste des ordres GTC encore ouverts (non remplis), lecture seule."""
        try:
            c = self.client()
            return c.get_open_orders() or []
        except Exception:
            return []

    # ── ordre directionnel simple (momentum / IA) ──

    def sell_position(self, token_id: str, price: float, size: float, aggressive: bool = False) -> dict:
        """Vend size parts détenues, au prix donné. FALLBACK NEG-RISK : comme
        l'achat, les marchés multi-issues (World Cup...) rejettent l'ordre standard
        -> on retente en neg_risk=True pour pouvoir SORTIR de ces positions.
        `aggressive` (Steven 30/07, "orphelin evitable ?") : par defaut GTC au
        prix donne = ordre MAKER passif, peut rester ouvert longtemps sans
        jamais matcher (observe : unwind d'orphelin qui reste a 0/N vendues
        apres le delai de verif, parce qu'un GTC pile au bid n'a aucune
        garantie de croiser). Pour une SORTIE (stop-loss, unwind, orphelin),
        la vitesse d'execution prime sur le prix -> aggressive=True poste en
        FAK legerement SOUS le bid (garanti de prendre la liquidite dispo
        tout de suite, ou annule net), au lieu d'attendre un match qui peut
        ne jamais venir."""
        from py_clob_client_v2 import OrderArgsV2, OrderType
        from py_clob_client_v2.clob_types import PartialCreateOrderOptions

        c = self.client()
        order_type = OrderType.FAK if aggressive else OrderType.GTC
        sell_price = round(max(0.01, price - 0.02), 2) if aggressive else price
        args = OrderArgsV2(token_id=token_id, price=sell_price, size=size, side="SELL")
        try:
            resp = c.post_order(c.create_order(args), order_type)
            if resp and resp.get("success", True) is not False:
                return resp
            std_err = str(resp)
        except Exception as e:
            std_err = str(e)
        try:
            signed = c.create_order(args, PartialCreateOrderOptions(neg_risk=True))
            return c.post_order(signed, order_type)
        except Exception as e:
            return {
                "success": False,
                "error": f"std:{std_err[:60]} | negrisk:{str(e)[:60]}",
            }

    def execute_directional(self, token_id: str, price: float, size: float) -> dict:
        """Un seul ordre BUY, GTC proche du marché. Utilisé pour les paris
        directionnels (momentum, estimation IA) — pas de couverture
        symétrique comme l'arb, donc pas de rollback à gérer : soit l'ordre
        passe, soit il reste en carnet (annulable par l'appelant).

        FALLBACK NEG-RISK : les marchés multi-issues (FIFA World Cup winner,
        meilleur buteur...) sont 'negRisk' et rejettent l'ordre CLOB standard.
        Si le premier essai échoue, on retente en mode neg_risk=True -> débloque
        toute la catégorie foot-futures (notre edge) sans threader un flag partout."""
        from py_clob_client_v2 import OrderArgsV2, OrderType
        from py_clob_client_v2.clob_types import PartialCreateOrderOptions

        size = max(size, MIN_ORDER_SIZE_SHARES)
        c = self.client()
        args = OrderArgsV2(token_id=token_id, price=price, size=size, side="BUY")
        # 1) ordre standard
        try:
            resp = c.post_order(c.create_order(args), OrderType.GTC)
            if resp and resp.get("success", True) is not False:
                return resp
            std_err = str(resp)
        except Exception as e:
            std_err = str(e)
        # 2) fallback NEG-RISK
        try:
            signed = c.create_order(args, PartialCreateOrderOptions(neg_risk=True))
            resp = c.post_order(signed, OrderType.GTC)
            return resp
        except Exception as e:
            return {
                "success": False,
                "error": f"std:{std_err[:80]} | negrisk:{str(e)[:80]}",
            }

    # ── snipe marketable (V3 BTC/ETH up-down) ──

    def get_book_sync(self, token_id: str, attempts: int = 3) -> dict | None:
        """Carnet d'ordres temps réel (bids/asks triés). Version synchrone
        dédiée au sniper V3 (le moteur V3 n'est pas async).

        RETRY (21/07) : un hoquet reseau renvoyait None, indistinguable d'un vrai
        carnet vide -> loggue "ask=None" et on ratait le trade alors qu'il y avait
        peut-etre des vendeurs. On retente donc rapidement (timeout court) avant
        de conclure. `error` distingue desormais panne reseau vs carnet vraiment
        vide, pour que les logs disent la verite."""
        last_exc = None
        for i in range(attempts):
            try:
                r = requests.get(
                    f"{CLOB_HOST}/book", params={"token_id": token_id}, timeout=3
                )
                data = r.json()
                bids = sorted(
                    (
                        (float(b["price"]), float(b["size"]))
                        for b in data.get("bids", [])
                    ),
                    key=lambda x: -x[0],
                )
                asks = sorted(
                    (
                        (float(a["price"]), float(a["size"]))
                        for a in data.get("asks", [])
                    ),
                    key=lambda x: x[0],
                )
                return {"bids": bids, "asks": asks, "error": None}
            except Exception as e:
                last_exc = e
                if i < attempts - 1:
                    time.sleep(0.25)
        return {"bids": [], "asks": [], "error": f"reseau: {str(last_exc)[:80]}"}

    def _wait_for_fill(
        self, token_id: str, before: float, timeout: float = 8.0
    ) -> float:
        """Attend que le fill se propage cote custody Polymarket, en interrogeant
        position_size plusieurs fois (le solde on-chain ne se met PAS a jour en
        1.2s -> c'est ce qui a fait rater la detection du 2e trade reel). Retourne
        le nombre de parts REELLEMENT acquises (0 si vraiment rien apres timeout)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(0.6)
            after = self.position_size(token_id)
            if after >= 0 and after - before > 0.001:
                return round(after - before, 2)
        return 0.0

    def snipe_buy(
        self,
        token_id: str,
        max_price: float,
        target_usd: float,
        price_buffer: float | None = None,
    ) -> dict:
        """Achat MARKETABLE immédiat pour le sniper V3.

        Corrige DEUX bugs des tests reels :
          - 1er test : ordre GTC posE au dernier prix, jamais rempli, faux WIN.
          - 2e test : un ordre FAK avait REELLEMENT rempli (5 parts) mais la verif
            (position_size apres 1.2s) n'avait pas vu le fill -> bot croyait "0 part"
            et a meme retente -> comptabilite fausse, garde-fous defaits.

        Corrige :
          1) lit le carnet EN DIRECT, prend le MEILLEUR ASK (vrai prix vendeur),
          2) refuse si l'ask dEpasse max_price (edge disparu / trop cher),
          3) UN SEUL ordre FAK (Fill-And-Kill) : jamais de re-tir a l'aveugle qui
             pourrait double-executer sans qu'on le sache,
          4) verif de fill ROBUSTE via _wait_for_fill (polling jusqu'a 8s) -> ne
             conclut JAMAIS "non rempli" avant que le solde ait eu le temps de
             se propager.
        Retourne {filled_shares, ask, avg_cost}. filled_shares > 0 = vrai trade
        confirme on-chain."""
        from py_clob_client_v2 import OrderArgsV2, OrderType

        c = self.client()
        book = self.get_book_sync(token_id)
        if not book or not book.get("asks"):
            why = (book or {}).get("error") or "aucun ask (pas de vendeur)"
            return {"success": False, "error": why, "filled_shares": 0.0, "ask": None}

        # PROFONDEUR : on ne se limite plus au SEUL meilleur niveau. Si le premier
        # ask est minuscule (souvent le cas en fin de fenetre), on agrege les
        # niveaux suivants tant qu'ils restent <= max_price -> beaucoup moins
        # d'echecs "pas assez de vendeurs au meilleur prix".
        usable = [(px, sz) for px, sz in book["asks"] if px <= max_price]
        if not usable:
            best = book["asks"][0][0]
            return {
                "success": False,
                "error": f"ask {best:.3f} > max {max_price:.3f}",
                "filled_shares": 0.0,
                "ask": best,
            }
        ask_price = usable[0][0]
        ask_size = sum(
            sz for _, sz in usable
        )  # profondeur totale achetable sous le plafond

        # PRIX D'ORDRE avec petit TAMPON (+0.02) au-dessus du meilleur ask, plafonne
        # a max_price : entre le moment ou on lit le carnet et l'arrivee de l'ordre
        # (~1-3s), le vendeur au meilleur ask a souvent disparu -> un FAK au prix
        # EXACT ne matche plus ("no orders found"). Un ordre marketable se remplit
        # au MEILLEUR prix dispo (pas a notre limite) -> on ne surpaie pas, on
        # capture juste plus de fills.
        best_ask = round(ask_price, 2)
        # la limite doit couvrir TOUTE la profondeur qu'on veut consommer (sinon on
        # ne matche que le 1er niveau), sans jamais depasser max_price.
        deepest_usable = round(usable[-1][0], 2)
        # tampon ELARGISSABLE (21/07) : en toute fin de fenetre le prix peut sauter
        # de plusieurs centimes entre la lecture du carnet et l'arrivee de l'ordre
        # (vu 0.83 -> 0.92 en <1s). L'appelant peut demander un tampon plus large
        # pour ces cas precis (price_buffer), sinon le tampon standard +0.02 reste.
        buf = price_buffer if price_buffer is not None else 0.02
        limit_price = min(
            round(max_price, 2), max(round(best_ask + buf, 2), deepest_usable)
        )
        # BUG CORRIGE (22/07, trouvaille Steven) : l'ancien calcul divisait
        # target_usd par le SEUL meilleur ask (usable[0]) pour obtenir le nombre
        # de parts, puis laissait l'ordre consommer TOUTE la profondeur agregee
        # (ask_size, plusieurs niveaux de prix). Si le meilleur niveau etait une
        # miette a prix tres bas (ex: 1 part a 0.02$ alors que le reste du carnet
        # est a 0.14-0.15), wanted_shares explosait (target/0.02) et l'ordre
        # consommait bien plus de profondeur que prevu -> depense reelle jusqu'a
        # 8x le budget (trade BTC 1$ prevu -> 8.34$ depenses). On calcule desormais
        # le nombre de parts en MARCHANT niveau par niveau et en s'arretant des
        # que le budget cible est atteint (cout REEL plafonne a target_usd),
        # au lieu de deviner a partir d'un seul prix qui peut etre un outlier.
        remaining_usd, shares = target_usd, 0.0
        for px, sz in usable:
            if remaining_usd <= 0:
                break
            take_here = min(sz, remaining_usd / px)
            shares += take_here
            remaining_usd -= take_here * px
        take = max(MIN_ORDER_SIZE_SHARES, min(shares, ask_size))
        take = float(int(round(take)))
        take = max(MIN_ORDER_SIZE_SHARES, take)
        before = self.position_size(token_id)
        before = before if before >= 0 else 0.0

        args = OrderArgsV2(token_id=token_id, price=limit_price, size=take, side="BUY")
        try:
            resp = c.post_order(c.create_order(args), OrderType.FAK)
        except Exception as e:
            # meme si post_order leve, un fill partiel a PU passer avant l'erreur
            # -> on verifie quand meme le solde avant de conclure "rien"
            filled = self._wait_for_fill(token_id, before, timeout=4.0)
            if filled > 0:
                return {
                    "success": True,
                    "resp": {"note": "fill malgre exception"},
                    "filled_shares": filled,
                    "ask": round(ask_price, 4),
                    "avg_cost": round(ask_price, 4),
                    "spent_est": round(filled * ask_price, 2),
                }
            return {
                "success": False,
                "error": str(e)[:150],
                "filled_shares": 0.0,
                "ask": ask_price,
            }

        filled = self._wait_for_fill(token_id, before, timeout=8.0)
        if filled > 0:
            return {
                "success": True,
                "resp": resp,
                "filled_shares": filled,
                "ask": round(ask_price, 4),
                "avg_cost": round(ask_price, 4),
                "spent_est": round(filled * ask_price, 2),
            }

        return {
            "success": False,
            "error": "FAK poste mais 0 part apres verif on-chain (8s)",
            "filled_shares": 0.0,
            "ask": ask_price,
        }

    def snipe_buy_market(
        self, token_id: str, max_price: float, target_usd: float
    ) -> dict:
        """Achat via ORDRE MARKET reel (Steven 22/07), dimensionne en DOLLARS et
        non en parts -> contourne le plancher de 5 parts des ordres LIMIT/FAK
        (cf. snipe_buy). C'est le chemin qu'utilise l'interface Polymarket
        elle-meme pour permettre des mises minuscules (1$ observe par Steven).

        MarketOrderArgsV2.amount = montant EN DOLLARS a depenser (pas un nombre
        de parts) -> le nombre de parts est calcule par l'exchange lui-meme a
        partir du prix. `price` sert de PLAFOND (on ne paie jamais plus cher
        que max_price) : marketable si en dessous, ne matche rien au-dela.
        FAK (pas FOK) : accepte un fill partiel plutot que tout ou rien, meme
        philosophie que snipe_buy.

        Retourne {filled_shares, ask, avg_cost, spent_est} comme snipe_buy pour
        rester interchangeable cote appelant."""
        from py_clob_client_v2 import MarketOrderArgsV2, OrderType

        c = self.client()
        book = self.get_book_sync(token_id)
        if not book or not book.get("asks"):
            why = (book or {}).get("error") or "aucun ask (pas de vendeur)"
            return {"success": False, "error": why, "filled_shares": 0.0, "ask": None}
        ask_price = book["asks"][0][0]
        if ask_price > max_price:
            return {
                "success": False,
                "error": f"ask {ask_price:.3f} > max {max_price:.3f}",
                "filled_shares": 0.0,
                "ask": ask_price,
            }

        before = self.position_size(token_id)
        before = before if before >= 0 else 0.0
        amount = max(
            1.0, round(target_usd, 2)
        )  # 1$ = plancher Polymarket pour les market BUY orders

        args = MarketOrderArgsV2(
            token_id=token_id,
            amount=amount,
            side="BUY",
            price=round(max_price, 2),
            order_type=OrderType.FAK,
        )
        try:
            resp = c.post_order(c.create_market_order(args), OrderType.FAK)
        except Exception as e:
            filled = self._wait_for_fill(token_id, before, timeout=4.0)
            if filled > 0:
                return {
                    "success": True,
                    "resp": {"note": "fill malgre exception"},
                    "filled_shares": filled,
                    "ask": round(ask_price, 4),
                    "avg_cost": round(ask_price, 4),
                    "spent_est": round(filled * ask_price, 2),
                }
            return {
                "success": False,
                "error": str(e)[:150],
                "filled_shares": 0.0,
                "ask": ask_price,
            }

        filled = self._wait_for_fill(token_id, before, timeout=8.0)
        if filled > 0:
            return {
                "success": True,
                "resp": resp,
                "filled_shares": filled,
                "ask": round(ask_price, 4),
                "avg_cost": round(ask_price, 4),
                "spent_est": round(filled * ask_price, 2),
            }

        return {
            "success": False,
            "error": "market FAK poste mais 0 part apres verif on-chain (8s)",
            "filled_shares": 0.0,
            "ask": ask_price,
        }

    def snipe_buy_limit_exact(
        self, token_id: str, exact_price: float, shares: float, timeout: float = 4.0
    ) -> dict:
        """ACHAT SANS SLIPPAGE (Steven 30/07, arb risk-free) : GTC pose PILE au
        prix demande (pas de tampon comme snipe_buy/snipe_buy_market qui
        acceptent de payer plus cher) -> soit rempli EXACTEMENT a ce prix, soit
        PAS DU TOUT (jamais un prix pire). Pour un arb garanti, un non-fill ne
        coute rien (aucun capital engage) ; un fill a prix degrade casserait
        l'edge calcule -> ceci est strictement preferable a un ordre market
        pour CE cas d'usage precis (pas pour du snipe directionnel classique
        ou rater le fill coute l'opportunite entiere).
        Retourne {filled_shares, ask, avg_cost, spent_est} comme les autres snipe_*."""
        before = self.position_size(token_id)
        before = before if before >= 0 else 0.0
        res = self.post_limit_buy(token_id, exact_price, shares)
        if not res.get("success"):
            return {
                "success": False,
                "error": res.get("error", "post_limit_buy echec"),
                "filled_shares": 0.0,
                "ask": exact_price,
            }
        order_id = res.get("order_id")
        filled = self._wait_for_fill(token_id, before, timeout=timeout)
        if filled > 0:
            return {
                "success": True,
                "resp": res.get("raw"),
                "filled_shares": filled,
                "ask": round(exact_price, 4),
                "avg_cost": round(exact_price, 4),
                "spent_est": round(filled * exact_price, 2),
            }
        # pas rempli dans le delai -> annule l'ordre GTC en attente, rien engage
        if order_id:
            self.cancel_order(order_id)
        return {
            "success": False,
            "error": "GTC exact non rempli dans le delai -> annule, 0 engage",
            "filled_shares": 0.0,
            "ask": exact_price,
        }

    def preflight_leg(self, token_id: str, max_price: float, min_depth: float) -> dict:
        """CHECK SANS POSTER (Steven 23/07) : lecture fraiche du carnet pour
        verifier AVANT tout envoi d'ordre qu'une jambe est encore prenable
        (ask <= max_price) ET profonde (taille dispo au best ask >= min_depth).
        Objectif : ne JAMAIS acheter une jambe puis devoir la revendre en
        catastrophe si l'AUTRE jambe echoue -> on verifie les 2 jambes AVANT
        de poster quoi que ce soit (aucun capital engage si l'une des 2 rate)."""
        book = self.get_book_sync(token_id)
        if not book or not book.get("asks"):
            return {
                "ok": False,
                "ask": None,
                "depth": 0.0,
                "error": (book or {}).get("error") or "aucun ask",
            }
        ask_price, ask_size = book["asks"][0]
        if ask_price > max_price:
            return {
                "ok": False,
                "ask": ask_price,
                "depth": ask_size,
                "error": f"ask {ask_price:.3f} > max {max_price:.3f}",
            }
        # PROFONDEUR CUMULEE (Steven 04/08) : on ne comptait que la taille du
        # MEILLEUR ask, alors que l'ordre envoye est un GTC pose au CAP
        # (prix + marge) -> il se remplit contre TOUS les niveaux jusqu'a ce
        # cap, pas seulement le premier. Mesurer un seul niveau sous-estime la
        # liquidite reelle et rejette des paires parfaitement executables
        # (trace : DOGE 1785810900 rejete pour "profondeur 6.0 < min 6.5" alors
        # que les niveaux suivants restaient sous le cap). On somme donc les
        # tailles de tous les niveaux dont le prix <= max_price.
        depth = 0.0
        for lvl in book["asks"]:
            try:
                lvl_px, lvl_sz = lvl[0], lvl[1]
            except Exception:
                continue
            if lvl_px > max_price:
                break
            depth += lvl_sz
        if depth < min_depth:
            return {
                "ok": False,
                "ask": ask_price,
                "depth": depth,
                "error": f"profondeur {depth:.1f} < min {min_depth:.1f} parts (cumul jusqu'a {max_price:.3f})",
            }
        return {"ok": True, "ask": ask_price, "depth": depth, "error": None}

    def post_market_order(
        self, token_id: str, max_price: float, target_usd: float
    ) -> dict:
        """PHASE 1 d'un achat parallelise (Steven 22/07) : POSTE l'ordre market
        FAK sans attendre le fill, et retourne un HANDLE {before, ask, posted}.
        Sert a poster les 2 jambes d'un arb quasi-simultanement (on ne bloque
        plus 8s sur le fill de la 1re avant de poster la 2e). Le fill se confirme
        ensuite via confirm_fill(). NE PAS utiliser hors du flux both-side."""
        from py_clob_client_v2 import MarketOrderArgsV2, OrderType

        c = self.client()
        book = self.get_book_sync(token_id)
        if not book or not book.get("asks"):
            return {
                "posted": False,
                "error": (book or {}).get("error") or "aucun ask",
                "before": 0.0,
                "ask": None,
            }
        ask_price = book["asks"][0][0]
        if ask_price > max_price:
            return {
                "posted": False,
                "error": f"ask {ask_price:.3f} > max {max_price:.3f}",
                "before": 0.0,
                "ask": ask_price,
            }
        before = self.position_size(token_id)
        before = before if before >= 0 else 0.0
        amount = max(1.0, round(target_usd, 2))
        args = MarketOrderArgsV2(
            token_id=token_id,
            amount=amount,
            side="BUY",
            price=round(max_price, 2),
            order_type=OrderType.FAK,
        )
        try:
            c.post_order(c.create_market_order(args), OrderType.FAK)
            return {
                "posted": True,
                "before": before,
                "ask": ask_price,
                "token_id": token_id,
            }
        except Exception as e:
            return {
                "posted": False,
                "error": str(e)[:150],
                "before": before,
                "ask": ask_price,
                "token_id": token_id,
            }

    def confirm_fill(self, token_id: str, before: float, timeout: float = 8.0) -> float:
        """PHASE 2 : nombre de parts REELLEMENT acquises depuis `before` (polling
        on-chain). A appeler apres post_market_order, en parallele pour les 2 jambes."""
        return self._wait_for_fill(token_id, before, timeout=timeout)

    def _resign_via_rust(self, order, exchange_address: str, chain_id: int = 137):
        """Re-signe un ordre DEJA construit par Python (montants, tick size,
        fees -- toute la logique metier -- restent 100% Python, zero risque
        de dupliquer ce calcul) via le service Rust local (Steven 04/08,
        "je veux tester ce que rust nous fait gagner"). Remplace UNIQUEMENT
        order.signature si Rust repond a temps avec une signature valide.
        Timeout court + tout echec => l'ordre garde la signature Python
        (deja valide) -- ce n'est jamais un blocage, juste une tentative.
        Verifie : signature Rust et Python BYTE-IDENTIQUES pour le meme
        ordre, EOA (struct V2 simple) ET POLY_1271 (wrapper TypedDataSign) --
        voir enginebtb3_rust/BENCHMARK_RESULTS.md.
        GARDE-FOU : ce compte reel resout signatureType=3 (POLY_1271, wallet
        intelligent), pas 0 (EOA) -- confirme en inspectant un vrai ordre
        construit par c.create_order(). Les deux schemas (0=EOA, 3=POLY_1271)
        sont maintenant implementes cote Rust (poly1271.rs, traduction mot
        pour mot de ExchangeOrderBuilderV2._build_poly_1271_order_signature).
        Tout autre type (1=proxy, 2=gnosis-safe) n'est PAS couvert -> no-op,
        fallback Python automatique, comme pour tout echec du service Rust."""
        if int(order.signatureType) not in (0, 3):
            return order, None
        url = os.environ.get("RUST_SIGN_URL", "http://127.0.0.1:9931/sign")
        try:
            payload = {
                "maker": order.maker,
                "signer": order.signer,
                "token_id": str(order.tokenId),
                "maker_amount": str(order.makerAmount),
                "taker_amount": str(order.takerAmount),
                "side": int(order.side),
                "signature_type": int(order.signatureType),
                "timestamp": str(order.timestamp),
                "metadata": order.metadata,
                "builder": order.builder,
                "salt": str(order.salt),
                "chain_id": chain_id,
                "exchange": exchange_address,
            }
            r = requests.post(url, json=payload, timeout=0.3)
            if r.status_code != 200:
                return order, None
            sig = r.json().get("signature")
            if not sig:
                return order, None
            order.signature = sig
            return order, r.json().get("sign_us")
        except Exception:
            return order, None  # jamais fatal -- Python a deja signe correctement

    def post_limit_pair_no_slippage(
        self, tid1: str, price1: float, size1: float, tid2: str, price2: float, size2: float
    ) -> dict:
        """UN SEUL ENVOI RESEAU pour les 2 jambes (Steven 30/07, "en un seul
        envoi demander plusieurs achats") : la lib CLOB expose post_orders()
        qui poste une LISTE d'ordres signes en une seule requete HTTP, au lieu
        de 2 requetes concurrentes (post_limit_order_handle x2, chacune son
        propre aller-retour reseau). Ca n'elimine pas le risque de prix (le
        matching cote Polymarket reste independant par ordre), mais ca
        supprime l'ecart de latence CLIENT entre les 2 legs (connexion TCP,
        serialisation, etc. faits une seule fois pour les 2). GTC exact-price
        (comme post_limit_buy) : soit rempli pile au prix, soit annule."""
        from py_clob_client_v2 import OrderArgsV2, OrderType
        from py_clob_client_v2.clob_types import PostOrdersV2Args

        c = self.client()
        # CHRONO FIN (Steven 04/08, "qu'est-ce que t'as oublie pour etre + rapide")
        # : le chrono externe mesurait post_lui_meme=2000-3500ms en moyenne sur
        # 39 arbs reels, alors qu'un benchmark isole de create_order() donnait
        # ~330ms -> ecart jamais explique. On decoupe ici les 3 etapes internes
        # (baseline, signature, soumission) pour savoir laquelle mange le temps
        # au lieu de re-deviner.
        _tt0 = time.time()
        # BASELINE FIABLE (Steven 04/08) : voir position_size_sure(). Une
        # baseline fausse (0 au lieu du stock reel) gonfle le fill mesure et
        # declenche une revente d'excedent FANTOME sur des parts bien reelles.
        # Ici on RENONCE plutot que de poster a l'aveugle : un arb rate ne
        # coute rien, un arb sur baseline fausse coute du capital reel.
        # EN PARALLELE (Steven 04/08, "faut etre + rapide") : ces 2 lectures
        # on-chain etaient SEQUENTIELLES sur le chemin critique, juste avant
        # le post -> j'avais moi-meme ajoute ~2 allers-retours reseau en
        # serie en corrigeant la baseline. En parallele on paie le temps
        # d'UNE lecture au lieu de deux, sans rien perdre en fiabilite.
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=2) as _ex:
            _f1 = _ex.submit(self.position_size_sure, tid1)
            _f2 = _ex.submit(self.position_size_sure, tid2)
            before1 = _f1.result()
            before2 = _f2.result()
        _tt1 = time.time()
        if before1 < 0 or before2 < 0:
            return {
                "success": False,
                "error": "position de depart indeterminee (lecture on-chain KO) -> abandon, aucun ordre poste",
                "legs": [],
            }
        try:
            # EN PARALLELE (Steven 04/08, mesure chrono) : create_order() prend
            # ~330ms chacun (il contient des lectures reseau : tick_size,
            # neg_risk, version) et etait appele DEUX FOIS EN SERIE juste avant
            # le post -> ~660ms de latence pure sur le chemin critique, alors
            # que les 2 signatures sont totalement independantes.
            with ThreadPoolExecutor(max_workers=2) as _ex2:
                _o1 = _ex2.submit(
                    c.create_order,
                    OrderArgsV2(token_id=tid1, price=round(price1, 2), size=round(size1, 2), side="BUY"),
                )
                _o2 = _ex2.submit(
                    c.create_order,
                    OrderArgsV2(token_id=tid2, price=round(price2, 2), size=round(size2, 2), side="BUY"),
                )
                o1 = _o1.result()
                o2 = _o2.result()
            _tt2 = time.time()
            # RE-SIGNATURE RUST (Steven 04/08, "je veux tester ce que rust nous
            # fait gagner") : o1/o2 sont deja des ordres COMPLETS et VALIDES
            # (Python a calcule montants/tick/fees et signe) -> on tente
            # seulement de remplacer la signature par celle du service Rust
            # local, EN PARALLELE pour ne pas serialiser les 2 jambes. Timeout
            # 300ms, echec silencieux -> garde la signature Python (deja bonne).
            from py_clob_client_v2.config import get_contract_config

            _cfg = get_contract_config(137)
            try:
                _neg1 = c.get_neg_risk(tid1)
                _neg2 = c.get_neg_risk(tid2)
            except Exception:
                _neg1 = _neg2 = False
            _exch1 = _cfg.neg_risk_exchange_v2 if _neg1 else _cfg.exchange_v2
            _exch2 = _cfg.neg_risk_exchange_v2 if _neg2 else _cfg.exchange_v2
            with ThreadPoolExecutor(max_workers=2) as _ex3:
                _r1 = _ex3.submit(self._resign_via_rust, o1, _exch1)
                _r2 = _ex3.submit(self._resign_via_rust, o2, _exch2)
                o1, _rust_us1 = _r1.result()
                o2, _rust_us2 = _r2.result()
            _tt2b = time.time()
            results = c.post_orders(
                [
                    PostOrdersV2Args(order=o1, orderType=OrderType.GTC),
                    PostOrdersV2Args(order=o2, orderType=OrderType.GTC),
                ]
            )
            _tt3 = time.time()
            _timing = {
                "baseline_ms": round((_tt1 - _tt0) * 1000),
                "signature_ms": round((_tt2 - _tt1) * 1000),
                "rust_resign_ms": round((_tt2b - _tt2) * 1000),
                "rust_used": bool(_rust_us1 and _rust_us2),
                "post_orders_ms": round((_tt3 - _tt2b) * 1000),
            }
        except Exception as e:
            return {"success": False, "error": str(e)[:200], "legs": []}
        legs = []
        for tid, before, res in ((tid1, before1, results[0]), (tid2, before2, results[1])):
            oid = res.get("orderID") or res.get("order_id") or res.get("id")
            legs.append({
                "token_id": tid,
                "before": before,
                "success": bool(oid) or res.get("success", True) is not False,
                "order_id": oid,
            })
        return {"success": True, "legs": legs, "timing": _timing}

    def post_limit_order_handle(self, token_id: str, price: float, size: float) -> dict:
        """SANS SLIPPAGE (Steven 30/07, arb risk-free reel) : equivalent de
        post_market_order mais en GTC exact-price -> soit rempli PILE au prix
        demande, soit pas du tout (jamais paye plus cher). Meme forme de retour
        (before/posted/order_id) pour s'inserer dans le flux parallele existant
        (post -> confirm_fill -> cancel si pas rempli)."""
        # BASELINE FIABLE (Steven 04/08) : cf. position_size_sure(). Renoncer
        # vaut mieux que poster avec un stock de depart suppose a tort nul.
        before = self.position_size_sure(token_id)
        if before < 0:
            return {
                "posted": False,
                "error": "position de depart indeterminee -> abandon (pas d'ordre poste)",
                "before": 0.0,
                "ask": price,
                "token_id": token_id,
            }
        res = self.post_limit_buy(token_id, price, size)
        if not res.get("success"):
            return {
                "posted": False,
                "error": res.get("error", "post_limit_buy echec"),
                "before": before,
                "ask": price,
                "token_id": token_id,
            }
        return {
            "posted": True,
            "before": before,
            "ask": price,
            "order_id": res.get("order_id"),
            "token_id": token_id,
        }

    # ── exécution d'un arb ──

    def execute_arb(
        self, token_yes: str, token_no: str, ask_yes: float, ask_no: float, size: float
    ) -> dict:
        """Achète size parts de YES et de NO en ordres GTC au ask affiché
        (le CLOB de Polymarket n'expose pas de FOK marché fiable sur ce
        client — GTC à un prix agressif proche de l'ask a le même effet
        pratique : rempli immédiatement s'il reste du volume au prix, sinon
        reste en carnet et doit être annulé si la 2e jambe échoue)."""
        from py_clob_client_v2 import OrderArgsV2, OrderType

        size = max(size, MIN_ORDER_SIZE_SHARES)
        c = self.client()
        results = {"legs": []}
        posted_ids = []
        for token_id, price, label in [
            (token_yes, ask_yes, "YES"),
            (token_no, ask_no, "NO"),
        ]:
            try:
                args = OrderArgsV2(
                    token_id=token_id, price=price, size=size, side="BUY"
                )
                signed = c.create_order(args)
                resp = c.post_order(signed, OrderType.GTC)
            except Exception as e:
                resp = {"success": False, "error": str(e)}
            results["legs"].append({"side": label, "resp": resp})
            if resp.get("success"):
                posted_ids.append(resp.get("orderID"))
            else:
                results["error"] = f"jambe {label} refusée: {resp}"
                # la couverture est cassée si une jambe est passée et l'autre non —
                # on annule ce qui a été posté pour ne pas rester exposé sans hedge
                if posted_ids:
                    try:
                        c.cancel_orders(posted_ids)
                        results["rolled_back"] = True
                    except Exception as e2:
                        results["rollback_error"] = str(e2)
                break
        return results
