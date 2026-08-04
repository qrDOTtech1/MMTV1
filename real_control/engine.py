"""GHOST V3 — moteur de trading REEL, controle par bouton (Steven doit cliquer
'Demarrer' lui-meme : cette classe ne lance AUCUN ordre tant que start() n'a
pas ete appele explicitement depuis l'interface).

Garde-fous non modifiables sans accord de Steven :
  - REAL_STAKE_USD = 1.0 (mise ciblee, cout reel >= 5 parts imposees par le CLOB)
  - MAX_REAL_TRADES = 5 (arret automatique)
  - STOP_AFTER_CONSEC_LOSSES = 2 (arret automatique)
"""
import json
import os
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

import sys
sys.path.insert(0, str(ROOT))

from core.btc_updown import find_active_markets, parse_updown_market, evaluate, _strike_at
from paper_snipe import outcome_price, size_stake
from ghost_poly.live import PolyLive, MIN_ORDER_SIZE_SHARES

LOG_FILE = ROOT / "data" / "ghost_v3_real.log"
STATE_FILE = ROOT / "data" / "real_state.json"

REAL_STAKE_USD = 1.0
MAX_REAL_TRADES = 5
STOP_AFTER_CONSEC_LOSSES = 2
MAX_ENTRY_PRICE = 0.97   # ne jamais acheter au-dessus de 97c : trop peu d'upside vs risque de fill
                          # (assoupli de 0.95->0.97 le 21/07 : le book bouge vite en fin de fenetre,
                          #  0.95 ratait trop de fills legitimes a edge encore correct)
POLL_SECS = 1
RESOLVE_BUFFER = 8


def _now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]


class RealEngine:
    def __init__(self):
        self._thread = None
        self._running = threading.Event()
        self._live = None
        self._init_error = None
        self.state = self._load_state()

    def _log(self, msg):
        line = f"[{_now()}] {msg}"
        print(line, flush=True)
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def _load_state(self):
        if STATE_FILE.exists():
            try:
                return json.loads(STATE_FILE.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"trades": [], "open": {}, "consec_losses": 0, "stopped": False, "stop_reason": None}

    def _save_state(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(self.state, indent=2), encoding="utf-8")

    # ── API exposee au bouton ──

    def is_running(self):
        return self._thread is not None and self._thread.is_alive()

    def precheck(self):
        """Lecture seule : verifie credentials + solde AVANT que Steven clique.
        N'envoie aucun ordre."""
        pk = os.environ.get("PRIVATE_KEY", "")
        funder = os.environ.get("POLY_FUNDER_ADDRESS", "")
        if not pk or not funder:
            return {"ok": False, "message": "PRIVATE_KEY ou POLY_FUNDER_ADDRESS absente du .env"}
        # la creation d'api-key CLOB rate PARFOIS (transitoire) -> retry, et si
        # on a deja lu un solde valide avant, on ne bloque pas le bouton pour un
        # simple hoquet reseau (on renvoie le dernier solde connu bon).
        last_err = ""
        for _ in range(3):
            try:
                live = PolyLive(pk, funder)
                st = live.status()
                cash = st.get("cash_usdc")
                if cash is not None:
                    self._last_good_cash = cash
                    ready = cash >= 0.1
                    return {"ok": ready, "cash_usdc": cash,
                            "message": "pret" if ready else "solde insuffisant"}
                last_err = "lecture solde vide"
            except Exception as e:
                last_err = str(e)[:150]
            time.sleep(0.6)
        if getattr(self, "_last_good_cash", None) is not None:
            return {"ok": self._last_good_cash >= 0.1, "cash_usdc": self._last_good_cash,
                    "message": f"solde (cache) — lecture live instable: {last_err}"}
        return {"ok": False, "message": f"lecture solde echouee: {last_err}"}

    def start(self):
        """Appele UNIQUEMENT par le clic du bouton cote UI. Refuse si deja lance
        ou si la session precedente s'est arretee sur un garde-fou (protection
        anti double-clic / anti reprise automatique)."""
        if self.is_running():
            return {"ok": False, "message": "deja en cours"}
        self.state = self._load_state()
        if self.state.get("stopped"):
            return {"ok": False, "message": f"session precedente arretee ({self.state.get('stop_reason')}) "
                                             f"- supprime data/real_state.json pour repartir a zero"}
        pk = os.environ.get("PRIVATE_KEY", "")
        funder = os.environ.get("POLY_FUNDER_ADDRESS", "")
        st, last_err = None, ""
        for _ in range(3):
            try:
                self._live = PolyLive(pk, funder)
                st = self._live.status()
                if st.get("cash_usdc") is not None:
                    break
            except Exception as e:
                last_err = str(e)[:150]
            time.sleep(0.6)
        cash = (st or {}).get("cash_usdc")
        if cash is None:
            cash = getattr(self, "_last_good_cash", None)
        if cash is None:
            return {"ok": False, "message": f"lecture solde echouee: {last_err}"}
        if cash < 0.1:
            return {"ok": False, "message": "solde insuffisant"}
        self._last_good_cash = cash

        self._log("=" * 70)
        self._log(f"=== DEMARRAGE REEL (declenche par clic utilisateur) === "
                   f"mise={REAL_STAKE_USD}$ max_trades={MAX_REAL_TRADES} "
                   f"stop_apres={STOP_AFTER_CONSEC_LOSSES}_pertes | cash={cash}$")
        self._running.set()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return {"ok": True, "message": "demarre"}

    def stop(self):
        self._running.clear()
        self._log("=== ARRET MANUEL demande depuis l'interface ===")
        return {"ok": True}

    # ── boucle interne ──

    def _loop(self):
        scan_n = 0
        while self._running.is_set() and not self.state["stopped"]:
            scan_n += 1
            try:
                n_markets = self._try_snipe()
                self._resolve_open()
                self._save_state()
                if scan_n % 30 == 0:  # heartbeat ~30s : preuve que la boucle est vivante
                    self._log(f"💓 vivant scan#{scan_n} | {n_markets or 0} marches surveilles | "
                              f"{len(self.state['trades'])}/{MAX_REAL_TRADES} trades reels | "
                              f"{len(self.state['open'])} position(s) ouverte(s)")
            except Exception as e:
                self._log(f"💥 erreur boucle: {e}\n{traceback.format_exc()}")
            time.sleep(POLL_SECS)
        self._running.clear()

    def _size_from_stake(self, entry_price, target_usd):
        shares = max(MIN_ORDER_SIZE_SHARES, target_usd / entry_price if entry_price > 0 else MIN_ORDER_SIZE_SHARES)
        return round(shares, 2)

    def _token_id_for_side(self, market, side):
        outcomes = json.loads(market.get("outcomes") or "[]")
        token_ids = json.loads(market.get("clobTokenIds") or "[]")
        return token_ids[outcomes.index(side)]

    def _real_outcome(self, pair, start_ts, end_ts):
        strike = _strike_at(pair, start_ts)
        close = _strike_at(pair, end_ts)
        if strike is None or close is None:
            return None
        return ("Up" if close > strike else "Down"), strike, close

    def _try_snipe(self):
        if len(self.state["trades"]) >= MAX_REAL_TRADES:
            self.state["stopped"] = True
            self.state["stop_reason"] = f"limite de {MAX_REAL_TRADES} trades reels atteinte"
            self._log(f"🛑 {self.state['stop_reason']}")
            return 0
        markets = find_active_markets()
        for m in markets:
            p = parse_updown_market(m)
            if not p:
                continue
            slug = m.get("slug")
            if slug in self.state["open"] or any(t["slug"] == slug for t in self.state["trades"]):
                continue
            sig = evaluate(m)
            if not sig:
                continue

            t0 = time.time()
            budget_usd = min(size_stake(sig), REAL_STAKE_USD)
            token_id = self._token_id_for_side(m, sig["side"])

            self._log(f"🎯 SIGNAL {sig['symbol']} {slug} cote={sig['side']} "
                       f"budget={budget_usd:.2f}$ buffer={sig['buffer']:+.2f} "
                       f"marge={sig['margin']:.2f} | {sig['seconds_left']}s restantes")

            t1 = time.time()
            try:
                # achat MARKETABLE : prend le vrai ask EN DIRECT (pas de prix perime),
                # FAK, retry si le book bouge, verifie le fill reel
                res = self._live.snipe_buy(token_id, MAX_ENTRY_PRICE, budget_usd)
            except Exception as e:
                t2 = time.time()
                self._log(f"💥 ERREUR ordre {slug}: {e} | t1->exception={t2-t1:.3f}s")
                continue
            t2 = time.time()

            filled = res.get("filled_shares", 0.0)
            self._log(f"📨 REPONSE {slug} rempli={filled} parts @ ask={res.get('ask')} "
                       f"depense~{res.get('spent_est')}$ "
                       f"err={res.get('error','')} | "
                       f"prep={t1-t0:.3f}s ordre+verif={t2-t1:.3f}s total={t2-t0:.3f}s")

            if filled <= 0:
                # RIEN n'a été rempli -> pas de position fantôme, on ne compte pas de trade
                self._log(f"⚠️ {slug} : ordre NON rempli (0 part) -> aucune position ouverte, on continue")
                continue

            avg_cost = res.get("avg_cost") or res.get("ask") or 0.0
            self.state["open"][slug] = {
                **p, "slug": slug, "side": sig["side"], "entry_price": avg_cost,
                "filled_shares": filled, "real_cost": round(filled * avg_cost, 2),
                "buffer": sig["buffer"], "margin": sig["margin"], "opened_ts": t0,
                "token_id": token_id,
                "timing": {"t0_signal": t0, "t1_order_sent": t1, "t2_response": t2,
                           "prep_secs": round(t1 - t0, 3), "order_secs": round(t2 - t1, 3),
                           "total_secs": round(t2 - t0, 3)},
            }
            self._log(f"✅ POSITION RÉELLE OUVERTE {slug} : {filled} parts @ {avg_cost:.3f} "
                       f"= {round(filled*avg_cost,2)}$ engagés")
            return len(markets)
        return len(markets)

    def _resolve_open(self):
        now = time.time()
        still_open = {}
        for slug, pos in self.state["open"].items():
            if now < pos["end_ts"] + RESOLVE_BUFFER:
                still_open[slug] = pos
                continue
            res = self._real_outcome(pos["pair"], pos["start_ts"], pos["end_ts"])
            if res is None:
                still_open[slug] = pos
                continue
            actual_side, strike, close = res
            win = actual_side == pos["side"]
            filled = pos.get("filled_shares", 0.0)
            cost = pos.get("real_cost", 0.0)
            # P&L RÉEL : si gagné, chaque part vaut 1$ (payout) -> gain = parts - cout ;
            # si perdu, la part vaut 0 -> perte = -cout
            pnl = round((filled * 1.0 - cost) if win else -cost, 3)
            pos.update(actual_side=actual_side, strike_final=strike, close_final=close,
                       win=win, pnl=pnl)
            self.state["consec_losses"] = 0 if win else self.state["consec_losses"] + 1
            self.state["trades"].append(pos)
            icon = "✅ WIN " if win else "❌ LOSS"
            self._log(f"{icon} RESOLU {slug} predit={pos['side']} reel={actual_side} "
                       f"strike={strike:.2f} close={close:.2f} pnl={pnl:+.3f}$ "
                       f"| pertes_consec={self.state['consec_losses']}")
            # réclame le gain on-chain pour recycler l'USDC (best-effort, non bloquant si échoue)
            if win:
                try:
                    n = self._live.redeem_resolved()
                    if n:
                        self._log(f"💰 {n} position(s) gagnante(s) réclamée(s) (USDC recrédité)")
                except Exception as e:
                    self._log(f"⚠️ redeem non effectué (réessai plus tard): {str(e)[:80]}")
            if self.state["consec_losses"] >= STOP_AFTER_CONSEC_LOSSES:
                self.state["stopped"] = True
                self.state["stop_reason"] = f"{STOP_AFTER_CONSEC_LOSSES} pertes reelles consecutives"
                self._log(f"🛑 ARRET AUTOMATIQUE : {self.state['stop_reason']}")
        self.state["open"] = still_open
