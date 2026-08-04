"""Pont JS <-> Python. Le moteur reel ne demarre JAMAIS tout seul : seul
start_engine() (appele par le clic du bouton cote HTML) declenche le trading.

Le reste (statut compte, courbe prix vs strike, carnet, embed, horloge de
cycle) est en LECTURE SEULE et peut tourner en continu sans aucun effet
financier."""
import json
import threading
import time
from pathlib import Path

import requests

from real_control.engine import RealEngine
from core.btc_updown import find_active_markets, parse_updown_market, _strike_at, _binance_price, synced_now, clock_offset

BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
PAIRS = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT",
         "XRP": "XRPUSDT", "DOGE": "DOGEUSDT"}


def _backfill_binance(sym):
    """Remplit l'historique avec les vrais prix Binance de la derniere heure
    (klines 1s -> ~detail seconde) pour que la courbe soit PLEINE des le
    demarrage, pas vide a gauche. Appele une seule fois par symbole."""
    pair = PAIRS[sym]
    out = []
    try:
        # 1s de resolution, max 1000 pts (~16min) pour le zoom fin ; puis 1min
        # pour completer jusqu'a 1h. On combine les deux.
        r = requests.get(BINANCE_KLINES, params={"symbol": pair, "interval": "1m", "limit": 60}, timeout=8)
        for k in r.json():
            out.append({"ts": k[0] / 1000.0, "price": float(k[4])})  # close de chaque minute
        r2 = requests.get(BINANCE_KLINES, params={"symbol": pair, "interval": "1s", "limit": 900}, timeout=8)
        for k in r2.json():
            out.append({"ts": k[0] / 1000.0, "price": float(k[4])})  # close seconde (detail recent)
    except Exception:
        return
    out.sort(key=lambda p: p["ts"])
    # dedoublonne par timestamp (les 1s recouvrent la derniere minute)
    seen, merged = set(), []
    for p in out:
        key = int(p["ts"])
        if key in seen:
            continue
        seen.add(key)
        merged.append(p)
    with _sample_lock:
        # fusionne avec les points live deja arrives, garde l'ordre chronologique
        existing = {int(p["ts"]): p for p in _history[sym]}
        for p in merged:
            existing.setdefault(int(p["ts"]), p)
        _history[sym] = sorted(existing.values(), key=lambda p: p["ts"])

ROOT = Path(__file__).parent.parent
LOG_FILE = ROOT / "data" / "ghost_v3_real.log"

MAX_HISTORY_SECS = 3600  # 1h de retention pour le zoom des courbes
SAMPLE_DEBOUNCE = 0.25   # pywebview execute chaque appel API dans son propre thread ->
                         # courbe BTC/ETH + horloge peuvent appeler _sample() en meme
                         # temps -> lock + debounce pour eviter les points en desordre
                         # chronologique (le "zigzag" observe sur les courbes)

# symboles suivis : source unique de verite (ajout SOL/XRP/DOGE le 21/07 —
# ces dicts etaient cables en dur sur BTC/ETH, d'ou l'absence de courbes)
CURVE_SYMS = ("BTC", "ETH", "SOL", "XRP", "DOGE")

# historique glissant continu par symbole (traverse plusieurs fenetres de 5min)
_history = {s: [] for s in CURVE_SYMS}          # [{ts, price}, ...]
_strike_segments = {s: [] for s in CURVE_SYMS}  # [{start_ts, end_ts, strike, slug}, ...]
_seen_slugs = set()                             # pour compter les cycles de 5min analyses
_sample_lock = threading.Lock()
_last_sample_ts = 0.0
_last_active = {s: None for s in CURVE_SYMS}    # dernier slug/infos actifs (pour l'horloge)


_bg_started = False


def _bg_sampler():
    """Echantillonne prix/strike en TACHE DE FOND (~1s). Les endpoints HTTP ne
    font PLUS d'appel reseau : ils lisent juste le cache -> reponses instantanees,
    plus de saturation des threads du serveur dev (cause du blocage observe)."""
    api = Api.__new__(Api)  # instance legere juste pour _sample_locked
    while True:
        try:
            with _sample_lock:
                # BUG CORRIGE (21/07) : ce module (dashboard/horloge) utilisait
                # l'horloge locale (time.time()), PAS la version synchronisee sur
                # le serveur Polymarket (synced_now(), ajoutee dans core/btc_updown.py
                # apres decouverte d'un decalage de 6-7s). Le moteur de trading
                # avait deja le fix, mais pas l'affichage du dashboard -> Steven
                # continuait de voir un ecart entre notre horloge et Polymarket.
                api._sample_locked(synced_now())
        except Exception:
            pass
        time.sleep(1.0)


class Api:
    def __init__(self):
        global _bg_started
        self._engine = RealEngine()
        for sym in CURVE_SYMS:
            threading.Thread(target=_backfill_binance, args=(sym,), daemon=True).start()
        if not _bg_started:
            _bg_started = True
            threading.Thread(target=_bg_sampler, daemon=True).start()

    # ── controle (le SEUL point qui trade, sur clic) ──

    def precheck(self):
        return self._engine.precheck()

    def start_engine(self):
        return self._engine.start()

    def stop_engine(self):
        return self._engine.stop()

    # ── lecture seule ──

    def get_status(self):
        st = self._engine.state
        pnl_total = round(sum(t.get("pnl", 0.0) for t in st.get("trades", [])), 3)
        return {
            "running": self._engine.is_running(),
            "stopped": st.get("stopped", False),
            "stop_reason": st.get("stop_reason"),
            "trades_done": len(st.get("trades", [])),
            "trades": st.get("trades", []),
            "open": list(st.get("open", {}).values()),
            "consec_losses": st.get("consec_losses", 0),
            "pnl_total": pnl_total,
            "cycles_analyzed": len(_seen_slugs),
        }

    def get_account(self):
        """Solde USDC reel (lecture seule via l'endpoint balance Polymarket)."""
        try:
            pc = self._engine.precheck()
            return {"cash_usdc": pc.get("cash_usdc"), "ready": pc.get("ok"), "message": pc.get("message")}
        except Exception as e:
            return {"cash_usdc": None, "ready": False, "message": str(e)[:150]}

    def _sample(self):
        """Echantillonne prix Binance live + strike pour BTC et ETH, alimente
        l'historique glissant continu (traverse les fenetres) et compte les
        cycles de 5min vus. Aucun effet de bord financier.

        pywebview execute chaque appel API dans SON PROPRE thread : la courbe
        BTC, la courbe ETH et l'horloge appellent toutes _sample() en parallele,
        ce qui pouvait entrelacer des points hors ordre chronologique dans
        l'historique (le "zigzag" observe sur les courbes). Lock + debounce
        -> un seul echantillonnage reel toutes les SAMPLE_DEBOUNCE secondes,
        les appels concurrents lisent juste le resultat deja pret."""
        global _last_sample_ts
        with _sample_lock:
            now = time.time()
            if now - _last_sample_ts < SAMPLE_DEBOUNCE:
                return
            _last_sample_ts = now
            self._sample_locked(now)

    def _sample_locked(self, now):
        for m in find_active_markets():
            p = parse_updown_market(m)
            if not p:
                continue
            secs_left = p["end_ts"] - now
            if secs_left < -8 or secs_left > 320:
                continue
            strike = _strike_at(p["pair"], p["start_ts"])
            price = _binance_price(p["pair"])
            if strike is None or price is None:
                continue
            sym, slug = p["symbol"], m.get("slug")
            _seen_slugs.add(slug)
            _history[sym].append({"ts": now, "price": price})
            segs = _strike_segments[sym]
            if not segs or segs[-1]["slug"] != slug:
                segs.append({"start_ts": p["start_ts"], "end_ts": p["end_ts"], "strike": strike, "slug": slug})
            _last_active[sym] = {"slug": slug, "start_ts": p["start_ts"], "end_ts": p["end_ts"],
                                  "strike": strike, "price": price, "seconds_left": round(secs_left)}
        # purge > 1h pour ne pas grossir indefiniment
        for sym in CURVE_SYMS:
            cutoff = now - MAX_HISTORY_SECS
            _history[sym] = [pt for pt in _history[sym] if pt["ts"] >= cutoff]
            _strike_segments[sym] = [s for s in _strike_segments[sym] if s["end_ts"] >= cutoff]
            if _last_active[sym] and now - _last_active[sym]["end_ts"] > 20:
                _last_active[sym] = None  # plus de fenetre active recente -> horloge en attente

    def get_price_curve(self, range_secs=300):
        """Historique glissant (borne a range_secs) par symbole, + segments de
        strike (une fenetre peut avoir un strike different de la precedente)
        -> permet de zoomer 5min/30min/1h en gardant un strike exact par period.
        Lit le cache alimente par _bg_sampler (pas d'appel reseau ici)."""
        now = time.time()
        out = []
        for sym in CURVE_SYMS:
            cutoff = now - range_secs
            with _sample_lock:
                pts = sorted((pt for pt in _history[sym] if pt["ts"] >= cutoff), key=lambda p: p["ts"])
                segs = sorted((s for s in _strike_segments[sym] if s["end_ts"] >= cutoff), key=lambda s: s["start_ts"])
            out.append({
                "symbol": sym,
                "points": pts,
                "strikes": segs,
                "active": _last_active[sym],
            })
        return out

    def get_cycle_clock(self):
        """Position dans le cycle de 5min courant (pour l'horloge), + zone de
        decision du bot. Lit le cache (_bg_sampler), pas d'appel reseau ici.
        Detail par symbole + decalage d'horloge server ajoutes (Steven 22/07)
        pour une horloge plus informative sur le dashboard."""
        # prend la fenetre BTC ou ETH la plus proche de sa fin (les deux sont
        # alignees sur les memes bornes de 5min, donc une seule horloge suffit)
        actives = [a for a in _last_active.values() if a]
        per_symbol = {sym: (a and {"slug": a["slug"], "seconds_left": a["seconds_left"],
                                    "strike": a["strike"], "price": a["price"]})
                      for sym, a in _last_active.items()}
        offset_ms = round(clock_offset() * 1000, 1)
        if not actives:
            return {"active": False, "clock_offset_ms": offset_ms, "per_symbol": per_symbol,
                    "cycles_analyzed": len(_seen_slugs), "server_ts": synced_now()}
        a = min(actives, key=lambda x: x["seconds_left"])
        elapsed = 300 - a["seconds_left"]
        return {
            "active": True,
            "elapsed_secs": max(0, min(300, elapsed)),
            "seconds_left": a["seconds_left"],
            "decision_zone": {"from_secs_left": 90, "to_secs_left": 6},
            "cycles_analyzed": len(_seen_slugs),
            "clock_offset_ms": offset_ms,
            "per_symbol": per_symbol,
            "server_ts": synced_now(),
        }

    def log_client_error(self, msg):
        """Pont debug : remonte les erreurs JS cote navigateur dans le log
        python, pour pouvoir diagnostiquer sans acces aux devtools."""
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[JS-ERROR] {msg}\n")
        except Exception:
            pass

    def get_log_tail(self, n=50):
        try:
            return LOG_FILE.read_text(encoding="utf-8").splitlines()[-n:]
        except Exception:
            return []
