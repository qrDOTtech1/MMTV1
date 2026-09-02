"""FLUX TEMPS REEL WebSocket (Steven 23/07) — remplace le polling REST (~1s de
latence) par des flux pousses (<100ms). C'EST le vrai levier : l'audit a montre
que l'arb crypto s'evapore en 1s et que le MM payait le spread faute de voir le
carnet en direct. Deux flux :

  1. BINANCE bookTicker : best bid/ask spot temps reel pour les 5 paires.
  2. POLYMARKET CLOB market : carnet (best bid/ask) temps reel par token, via
     un snapshot 'book' puis des deltas 'price_change'.

Thread-safe. Reconnexion auto. Chaque valeur est horodatee -> l'appelant peut
juger la fraicheur (data_age) et retomber sur le REST si le flux est stale.
Aucun ordre n'est jamais passe ici : lecture seule.

ARB STREAM (Steven 26/07) : au lieu de scanner les asks toutes les 30s via REST,
on s'abonne aux order books de TOUS les marches actifs. A chaque update WS, on
recalcule le combined ask en temps reel. Si combined <= threshold -> callback push
instantane (< 100ms de latency vs ~15s en moyenne avant)."""

import json
import threading
import time
import logging

from websocket import (
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
    create_connection,
)

BINANCE_WS = "wss://stream.binance.com:9443/stream"
POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
# RTDS CHAINLINK TWAP (Steven 02/09, "polymarket resout desormais sur une TWAP
# chainlink 30/60s, pas le spot instantane -- il faut suivre CA, pas Binance
# tick par tick") : Polymarket relaie en direct la TWAP officielle Chainlink
# (celle qui determine reellement l'issue du marche) via ce flux, sans
# credentials. Doc : docs.polymarket.com/market-data/chainlink-twap.
TWAP_WS = "wss://ws-live-data.polymarket.com"
TWAP_SYM_TO_SUB = {
    "BTC": "btc/usd", "ETH": "eth/usd", "SOL": "sol/usd",
    "XRP": "xrp/usd", "DOGE": "doge/usd", "BNB": "bnb/usd",
}
PAIRS = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
    "DOGE": "dogeusdt",
    "BNB": "bnbusdt",
}
STALE_S = 3.0  # au-dela, une valeur est jugee perimee -> l'appelant fallback REST
TWAP_STALE_S = 10.0  # cadence de publication Chainlink non documentee precisement
# -> tolerance plus large que le spot Binance pour ne pas rejeter une TWAP
# valide juste parce qu'elle publie un peu moins souvent qu'un bookTicker.

_log = logging.getLogger("ws_feed")


class WSFeed:
    def __init__(self):
        self._lock = threading.Lock()
        # Binance : sym -> (bid, ask, ts)
        self._spot = {}
        # RTDS Chainlink TWAP : sym -> (value, ts), un dict par fenetre
        self._twap30 = {}
        self._twap60 = {}
        # Polymarket : token_id -> {"bids": {px:sz}, "asks": {px:sz}, "ts": ts}
        self._books = {}
        self._poly_wanted = set()  # tokens a suivre (mis a jour par le trader)
        self._poly_ws = None
        self._poly_sub = set()  # tokens deja abonnes sur la connexion courante
        self._started = False

        # ── ARB STREAM : registre de marches + callback push ──
        # slug -> {"outcomes": [...], "token_ids": [...], "question": "...",
        #          "volume24hr": float, "ts": float}
        self._arb_markets = {}
        # index inversé : token_id -> slug (pour check O(1) par message WS)
        self._token_to_slug = {}
        # callable(slug, outcomes, tids, a0, a1, combined, meta) -> None
        self._arb_callback = None
        self._arb_threshold = 0.95  # combined <= ce seuil -> fire
        # debounce: slug -> dernier timestamp ou le callback a ete fire
        self._arb_last_fired = {}
        self._arb_debounce_s = 30  # min secondes entre 2 signals pour meme slug
        self._arb_stats = {"signals": 0, "fired": 0, "debounced": 0}

        # ── CANAL USER (Steven 30/07, "on a WS aussi") : fills pousses en
        # direct par Polymarket au lieu d'attendre le polling REST
        # position_size (qui attend le reglement on-chain custody, source
        # confirmee du delai detection->achat). token_id -> parts remplies
        # cumulees observees via WS depuis le dernier reset_fill_tracking().
        self._fills = {}
        self._user_auth = None
        self._user_started = False
        self._user_log = None  # callback(str) -> None, branche par le trader
        self._user_msg_count = 0

    def start_user_channel(self, auth: dict, log_fn=None):
        """auth = {"apiKey","secret","passphrase"} (PolyLive.ws_auth()).
        `log_fn` (Steven 30/07, "dedie des log") : callback vers le _log() du
        trader -> visible dans /api/log (le logging Python standard de ce
        module n'ecrit PAS dans le meme fichier que le dashboard lit).
        Idempotent : ne relance pas si deja demarre."""
        self._user_auth = auth
        if log_fn:
            self._user_log = log_fn
        if self._user_started:
            return
        self._user_started = True
        threading.Thread(target=self._poly_user_loop, daemon=True).start()

    def reset_fill_tracking(self, token_id: str):
        """A appeler juste AVANT de poster un ordre sur ce token, pour que
        fill_since() ne compte pas un fill d'un ordre precedent."""
        with self._lock:
            self._fills.pop(token_id, None)

    def fill_since(self, token_id: str) -> float:
        """Parts remplies vues via WS depuis le dernier reset_fill_tracking()
        sur ce token. 0.0 si rien vu (pas forcement 0 reel -> combiner avec le
        fallback REST position_size, WS n'est qu'une acceleration, jamais
        l'unique source de verite sur du capital reel)."""
        with self._lock:
            return self._fills.get(token_id, 0.0)

    def _poly_user_loop(self):
        while True:
            try:
                if not self._user_auth:
                    time.sleep(0.5)
                    continue
                ws = create_connection(POLY_WS, timeout=10)
                ws.send(json.dumps({"type": "user", "auth": self._user_auth}))
                ws.settimeout(5.0)
                if self._user_log:
                    self._user_log("🔌 [WS-USER] canal fills connecte, en attente de messages")
                while True:
                    try:
                        raw = ws.recv()
                    except Exception:
                        continue
                    if not raw:
                        continue
                    try:
                        msgs = json.loads(raw)
                    except Exception:
                        continue
                    if not isinstance(msgs, list):
                        msgs = [msgs]
                    for msg in msgs:
                        self._apply_user_msg(msg)
            except Exception as e:
                if self._user_log:
                    self._user_log(f"⚠️ [WS-USER] connexion perdue/echec ({str(e)[:80]}) -> reconnexion")
                time.sleep(0.5)

    def _apply_user_msg(self, msg):
        """Schema WS "user" non verifie hors-ligne (pas de doc bundlee ici) ->
        on essaie plusieurs noms de champs plausibles (trade/order event,
        asset_id/token_id, size/matched_amount) sans jamais lever. Si rien ne
        matche, aucun effet (le fallback REST position_size reste actif et
        prioritaire) - defensif par construction, jamais une fausse confirmation."""
        try:
            with self._lock:
                self._user_msg_count += 1
                first = self._user_msg_count == 1
            if first and self._user_log:
                self._user_log(f"📩 [WS-USER] 1er message recu, schema brut : {json.dumps(msg)[:200]}")
            tid = msg.get("asset_id") or msg.get("token_id") or msg.get("market")
            size = (
                msg.get("size")
                or msg.get("matched_amount")
                or msg.get("filled_size")
                or msg.get("amount")
            )
            et = msg.get("event_type") or msg.get("type")
            if not tid or size is None or et not in ("trade", "order", "fill", None):
                return
            with self._lock:
                self._fills[tid] = self._fills.get(tid, 0.0) + float(size)
        except Exception:
            pass

    # ── demarrage ──
    def start(self):
        if self._started:
            return
        self._started = True
        threading.Thread(target=self._binance_loop, daemon=True).start()
        threading.Thread(target=self._poly_loop, daemon=True).start()
        threading.Thread(target=self._twap_loop, daemon=True).start()

    # ── lecture (ce que le trader appelle) ──
    def spot(self, pair_or_sym):
        """(bid, ask, mid) spot Binance temps reel, ou None si absent/stale."""
        sym = pair_or_sym.replace("USDT", "") if "USDT" in pair_or_sym else pair_or_sym
        with self._lock:
            v = self._spot.get(sym)
        if not v or time.time() - v[2] > STALE_S:
            return None
        bid, ask, _ = v
        return bid, ask, round((bid + ask) / 2, 6)

    def spot_price(self, pair):
        """Prix mid seul (compat _binance_price), None si stale."""
        s = self.spot(pair)
        return s[2] if s else None

    def twap(self, pair_or_sym, window_s=30):
        """TWAP Chainlink officielle (30 ou 60s), source REELLE de resolution
        Polymarket -- a preferer au spot Binance instantane des que fraiche.
        None si le flux RTDS n'a pas encore de valeur recente pour cette
        paire (l'appelant doit alors retomber sur spot_price/_binance_price)."""
        sym = pair_or_sym.replace("USDT", "") if "USDT" in pair_or_sym else pair_or_sym
        sym = sym.upper()
        store = self._twap30 if window_s == 30 else self._twap60
        with self._lock:
            v = store.get(sym)
        if not v or time.time() - v[1] > TWAP_STALE_S:
            return None
        return v[0]

    def book(self, token_id):
        """(best_bid, best_ask, ts) temps reel d'un token Polymarket, ou None."""
        with self._lock:
            b = self._books.get(token_id)
            if not b or time.time() - b["ts"] > STALE_S:
                return None
            bids, asks = b["bids"], b["asks"]
            best_bid = max(bids) if bids else None
            best_ask = min(asks) if asks else None
        return best_bid, best_ask, b["ts"]

    def book_depth(self, token_id):
        """(best_bid, best_bid_size, best_ask, best_ask_size, ts) — Steven 23/07,
        pour verifier une VRAIE liquidite 2 faces avant d'engager du capital (pas
        juste un carnet fantome/abandonne). None si absent/stale."""
        with self._lock:
            b = self._books.get(token_id)
            if not b or time.time() - b["ts"] > STALE_S:
                return None
            bids, asks = b["bids"], b["asks"]
            if not bids or not asks:
                return None
            best_bid = max(bids)
            best_ask = min(asks)
        return best_bid, bids[best_bid], best_ask, asks[best_ask], b["ts"]

    def ask_depth_upto(self, token_id, max_price):
        """(best_ask, profondeur_cumulee_jusqu_a_max_price, ts) ou None.
        Steven 04/08 : book_depth() ne renvoie que la taille du MEILLEUR ask,
        alors qu'un ordre pose au cap se remplit contre TOUS les niveaux
        jusqu'a ce cap -> mesurer un seul niveau sous-estime la liquidite et
        fait rejeter des paires executables. Le carnet complet est deja en
        memoire ici, autant s'en servir. Aucun appel reseau."""
        with self._lock:
            b = self._books.get(token_id)
            if not b or time.time() - b["ts"] > STALE_S:
                return None
            asks = b["asks"]
            if not asks:
                return None
            best_ask = min(asks)
            depth = sum(sz for px, sz in asks.items() if px <= max_price)
            ts = b["ts"]
        return best_ask, depth, ts

    def want_tokens(self, token_ids):
        """Declare les tokens a suivre (le trader appelle ca a chaque tick avec
        les tokens des fenetres actives). Les nouveaux sont abonnes au prochain
        cycle de la boucle Polymarket."""
        with self._lock:
            for t in token_ids:
                if t:
                    self._poly_wanted.add(t)

    def stats(self):
        with self._lock:
            fresh_spot = sum(
                1 for v in self._spot.values() if time.time() - v[2] <= STALE_S
            )
            fresh_books = sum(
                1 for b in self._books.values() if time.time() - b["ts"] <= STALE_S
            )
            fresh_twap30 = sum(
                1 for v in self._twap30.values() if time.time() - v[1] <= TWAP_STALE_S
            )
            fresh_twap60 = sum(
                1 for v in self._twap60.values() if time.time() - v[1] <= TWAP_STALE_S
            )
            return {
                "spot_fresh": fresh_spot,
                "spot_total": len(self._spot),
                "books_fresh": fresh_books,
                "books_total": len(self._books),
                "twap30_fresh": fresh_twap30,
                "twap60_fresh": fresh_twap60,
                "poly_wanted": len(self._poly_wanted),
                "arb_markets": len(self._arb_markets),
                "arb_signals": self._arb_stats["signals"],
                "arb_fired": self._arb_stats["fired"],
                "arb_debounced": self._arb_stats["debounced"],
            }

    # ── ARB STREAM : enregistrement de marches + callback push ──

    def set_arb_callback(self, callback, threshold=0.95, debounce_s=30):
        """Enregistre le callback ARB appele quand combined <= threshold.
        callback(slug, outcomes, tids, a0, a1, combined, meta)"""
        with self._lock:
            self._arb_callback = callback
            self._arb_threshold = threshold
            self._arb_debounce_s = debounce_s
        _log.info(
            f"[ARB-STREAM] callback registre, threshold={threshold}, debounce={debounce_s}s"
        )

    def register_market(self, slug, outcomes, token_ids, question="", volume24hr=0.0):
        """Enregistre un marche binaire pour le suivi ARB temps reel.
        Les tokens seront abonnes au WS automatiquement."""
        with self._lock:
            # skip si deja enregistre (evite re-subscribe WS inutile)
            if slug in self._arb_markets:
                return
            self._arb_markets[slug] = {
                "outcomes": outcomes,
                "token_ids": token_ids,
                "question": question,
                "volume24hr": volume24hr,
                "ts": time.time(),
            }
            for tid in token_ids:
                if tid:
                    self._poly_wanted.add(tid)
                    self._token_to_slug[tid] = slug

    def unregister_market(self, slug):
        """Retire un marche du suivi ARB."""
        with self._lock:
            meta = self._arb_markets.pop(slug, None)
            if meta:
                for tid in meta.get("token_ids", []):
                    self._token_to_slug.pop(tid, None)
            self._arb_last_fired.pop(slug, None)

    def get_arb_markets(self):
        """Retourne la liste des marches ARB suivis."""
        with self._lock:
            return dict(self._arb_markets)

    def _check_arb_signal(self, tid, now):
        """Appele apres chaque update WS. Si le token modifie appartient a un
        marche ARB registre, recalcule le combined et retourne le signal si
        combined <= threshold + debounce OK. Sinon None. O(1) via index inversé."""
        parent_slug = self._token_to_slug.get(tid)
        if parent_slug is None:
            return None
        parent_meta = self._arb_markets.get(parent_slug)
        if parent_meta is None:
            return None

        tids = parent_meta["token_ids"]
        asks = []
        for t in tids:
            b = self._books.get(t)
            if not b or not b["asks"]:
                return None
            asks.append(min(b["asks"]))

        if len(asks) != 2:
            return None

        a0, a1 = asks
        combined = a0 + a1
        self._arb_stats["signals"] += 1

        if combined > self._arb_threshold:
            return None

        last = self._arb_last_fired.get(parent_slug, 0)
        if now - last < self._arb_debounce_s:
            self._arb_stats["debounced"] += 1
            return None

        self._arb_last_fired[parent_slug] = now
        self._arb_stats["fired"] += 1
        return (
            parent_slug,
            parent_meta["outcomes"],
            tids,
            a0,
            a1,
            combined,
            parent_meta,
        )

    # ── Binance : un seul stream combine pour les 5 paires ──
    def _binance_loop(self):
        streams = "/".join(f"{p}@bookTicker" for p in PAIRS.values())
        url = f"{BINANCE_WS}?streams={streams}"
        pair_to_sym = {p.upper(): s for s, p in PAIRS.items()}
        while True:
            try:
                ws = create_connection(url, timeout=10)
                while True:
                    msg = json.loads(ws.recv())
                    d = msg.get("data", msg)
                    sym = pair_to_sym.get(d.get("s", "").upper())
                    if sym and "b" in d and "a" in d:
                        with self._lock:
                            self._spot[sym] = (
                                float(d["b"]),
                                float(d["a"]),
                                time.time(),
                            )
            except Exception:
                time.sleep(1.0)  # reconnexion

    # ── RTDS Chainlink TWAP (Steven 02/09) ──────────────────────────────
    def _twap_loop(self):
        """Flux RTDS Polymarket : relaie la TWAP Chainlink officielle 30s et
        60s -- c'est CETTE valeur, pas le spot Binance instantane, qui
        determine la resolution reelle du marche depuis leur mise a jour du
        moteur d'execution. Pas de credentials, pas d'historique/replay
        (docs.polymarket.com/market-data/chainlink-twap) -> reconnexion
        simple en cas de coupure, on repart en flux avant seulement."""
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_twap_thirty", "type": "update"},
                {"topic": "crypto_prices_twap_sixty", "type": "update"},
            ],
        }
        while True:
            try:
                ws = create_connection(TWAP_WS, timeout=10)
                ws.send(json.dumps(sub_msg))
                ws.settimeout(5.0)
                last_ping = time.time()
                last_msg = time.time()
                while True:
                    # maintien de connexion : PING texte toutes les 5s (exige par RTDS)
                    if time.time() - last_ping >= 5.0:
                        ws.send("PING")
                        last_ping = time.time()
                    if time.time() - last_msg > 20.0:
                        raise ConnectionError("flux TWAP silencieux 20s")
                    try:
                        raw = ws.recv()
                        last_msg = time.time()
                    except WebSocketTimeoutException:
                        continue
                    except (WebSocketConnectionClosedException, OSError) as e:
                        raise ConnectionError(f"connexion TWAP fermee: {e}")
                    if not raw or raw in ("PONG", "PING"):
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    self._apply_twap(msg)
            except Exception:
                time.sleep(1.0)

    def _apply_twap(self, msg):
        topic = msg.get("topic")
        if topic not in ("crypto_prices_twap_thirty", "crypto_prices_twap_sixty"):
            return
        payload = msg.get("payload") or {}
        symbol = (payload.get("symbol") or "").upper()  # "BTC/USD"
        value = payload.get("value")
        if not symbol or value is None:
            return
        sym = symbol.split("/")[0]
        now = time.time()
        with self._lock:
            if topic == "crypto_prices_twap_thirty":
                self._twap30[sym] = (float(value), now)
            else:
                self._twap60[sym] = (float(value), now)

    # ── Polymarket : carnet temps reel, re-subscribe dynamique ──
    def _poly_loop(self):
        while True:
            try:
                with self._lock:
                    wanted = list(self._poly_wanted)
                if not wanted:
                    time.sleep(0.5)
                    continue
                ws = create_connection(POLY_WS, timeout=10)
                ws.send(json.dumps({"assets_ids": wanted, "type": "market"}))
                self._poly_sub = set(wanted)
                ws.settimeout(5.0)
                last_check = time.time()
                last_msg = time.time()
                while True:
                    # re-subscribe si de nouveaux tokens sont demandes
                    if time.time() - last_check > 2.0:
                        last_check = time.time()
                        with self._lock:
                            new = self._poly_wanted - self._poly_sub
                        if new:
                            raise ConnectionError(
                                "resub"
                            )  # relance avec la liste complete
                    # CHIEN DE GARDE (Steven 06/08) : si plus AUCUN message
                    # n'arrive pendant 20s alors qu'on suit des marches 5min
                    # tres actifs, la connexion est morte meme si l'objet ws
                    # ne le signale pas encore -> on force la reconnexion.
                    if time.time() - last_msg > 20.0:
                        raise ConnectionError("flux silencieux 20s")
                    try:
                        raw = ws.recv()
                        last_msg = time.time()
                    except WebSocketTimeoutException:
                        continue  # timeout normal -> reboucle (permet le re-check)
                    except (WebSocketConnectionClosedException, OSError) as e:
                        # BUG CORRIGE (Steven 06/08, "on peut etre plus
                        # rapide ?") : l'ancien `except Exception: continue`
                        # attrapait AUSSI la connexion fermee -> la boucle
                        # tournait a l'infini sans jamais se reconnecter.
                        # Symptome mesure en prod : books_total=34 mais
                        # books_fresh=0 -- les carnets recus une seule fois a
                        # la connexion, plus aucune mise a jour ensuite, donc
                        # TOUTES les lectures de prix retombaient sur le REST
                        # (~1s de retard) au lieu du WS (<100ms).
                        raise ConnectionError(f"connexion fermee: {e}")
                    if not raw:
                        continue
                    msgs = json.loads(raw)
                    if not isinstance(msgs, list):
                        msgs = [msgs]
                    for msg in msgs:
                        self._apply_poly(msg)
            except Exception:
                time.sleep(0.5)

    def _apply_poly(self, msg):
        et = msg.get("event_type")
        tid = msg.get("asset_id")
        if not tid:
            return
        now = time.time()
        signal = None
        with self._lock:
            b = self._books.setdefault(tid, {"bids": {}, "asks": {}, "ts": now})
            if et == "book":
                b["bids"] = {
                    float(x["price"]): float(x["size"]) for x in msg.get("bids", [])
                }
                b["asks"] = {
                    float(x["price"]): float(x["size"]) for x in msg.get("asks", [])
                }
                b["ts"] = now
            elif et == "price_change":
                for ch in msg.get("changes", []):
                    px = float(ch["price"])
                    sz = float(ch["size"])
                    side = "bids" if ch.get("side") == "BUY" else "asks"
                    if sz <= 0:
                        b[side].pop(px, None)
                    else:
                        b[side][px] = sz
                b["ts"] = now
            # ARB STREAM : check apres chaque update
            if self._arb_callback and (et in ("book", "price_change")):
                signal = self._check_arb_signal(tid, now)
        # Fire callback HORS lock pour eviter deadlock
        if signal:
            slug, outcomes, tids, a0, a1, combined, meta = signal
            try:
                self._arb_callback(slug, outcomes, tids, a0, a1, combined, meta)
            except Exception as e:
                _log.error(f"[ARB-STREAM] callback error: {e}")


# singleton partage
_feed = None
_feed_lock = threading.Lock()


def get_feed():
    global _feed
    with _feed_lock:
        if _feed is None:
            _feed = WSFeed()
            _feed.start()
        return _feed
