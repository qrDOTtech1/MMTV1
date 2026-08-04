"""GHOST POLY — base SQLite dédiée (indépendante de ghost.db)."""

import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "ghost_poly.db"


def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with _conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            kind TEXT NOT NULL,            -- 'arb' | 'mispricing'
            question TEXT,
            ask_yes REAL, ask_no REAL,
            total_cost REAL, edge_pct REAL,
            max_size REAL, max_profit_usd REAL,
            volume_24h REAL
        );
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            question TEXT,
            side TEXT,                     -- 'ARB' (YES+NO simultanés)
            cost_usd REAL,                 -- dépense totale
            payout_usd REAL,               -- 1.00 x taille (garanti à résolution)
            profit_usd REAL,
            size_shares REAL,
            end_date TEXT,
            status TEXT DEFAULT 'locked'   -- locked (attend résolution) | resolved
        );
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            markets INTEGER, arbs INTEGER
        );
        CREATE TABLE IF NOT EXISTS directional_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            strategy TEXT NOT NULL,         -- 'momentum' | 'ai_news'
            question TEXT,
            side TEXT,                      -- 'YES' | 'NO'
            price REAL,
            cost_usd REAL,
            size_shares REAL,
            reasoning TEXT,
            live INTEGER DEFAULT 0,
            token_id TEXT,
            status TEXT DEFAULT 'open',     -- 'open' | 'closed'
            exit_price REAL,
            pnl_usd REAL,
            conviction REAL                 -- 0..1, fixe TP/SL dynamiques à la sortie
        );
        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            total REAL,                     -- valeur totale (cash + positions)
            cash REAL,                      -- USDC libre
            pos_value REAL,                 -- valeur des positions ouvertes
            n_positions INTEGER             -- nb de positions ouvertes
        );
        CREATE INDEX IF NOT EXISTS idx_snap_ts ON portfolio_snapshots(ts);
        CREATE TABLE IF NOT EXISTS onchain_trades (
            tx_hash TEXT,
            asset TEXT,                     -- token_id de l'outcome
            side TEXT,                      -- BUY | SELL (ou type pour REDEEM)
            ts REAL,
            type TEXT,                      -- TRADE | REDEEM
            title TEXT,
            outcome TEXT,                   -- Yes | No
            price REAL,
            usdc_size REAL,
            shares REAL,
            condition_id TEXT,
            source TEXT,                    -- 'bot' | 'manual' (Steven a agi lui-même)
            PRIMARY KEY (tx_hash, asset, side)
        );
        CREATE INDEX IF NOT EXISTS idx_oc_ts ON onchain_trades(ts);
        CREATE TABLE IF NOT EXISTS momentum_shadow (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL, question TEXT, categorie TEXT,
            move_pts REAL,          -- ampleur du mouvement détecté (points de %)
            price0 REAL,            -- prix au signal (ask, entrée hypothétique)
            token_id TEXT,
            checked INTEGER DEFAULT 0,
            price1 REAL,            -- prix ~5min après (bid, sortie hypothétique)
            net_pts REAL,           -- (price1 - price0)*100 = gain net simulé après spread
            continued INTEGER       -- 1 si le mouvement a continué dans le même sens
        );
        CREATE INDEX IF NOT EXISTS idx_mom_checked ON momentum_shadow(checked, ts);
        """)
        _migrate(c)


def _migrate(c: sqlite3.Connection):
    """CREATE TABLE IF NOT EXISTS ne modifie pas une table déjà existante —
    ajoute ici toute colonne introduite après la création initiale, pour que
    la base d'un utilisateur qui tournait déjà avant un ajout de schéma ne
    plante pas au démarrage (vécu : 'no such column: token_id')."""
    existing = {r[1] for r in c.execute("PRAGMA table_info(directional_trades)").fetchall()}
    for name, coltype in [
        ("token_id", "TEXT"), ("status", "TEXT DEFAULT 'open'"),
        ("exit_price", "REAL"), ("pnl_usd", "REAL"), ("conviction", "REAL"),
        ("partial_taken", "INTEGER DEFAULT 0"), ("peak_price", "REAL"),
        ("last_price", "REAL"),
        # COPY v2 : traçage pour mesurer slippage/délai/perf par wallet
        ("src_wallet", "TEXT"), ("src_price", "REAL"), ("src_ts", "REAL"),
    ]:
        if name not in existing:
            c.execute(f"ALTER TABLE directional_trades ADD COLUMN {name} {coltype}")


def log_scan(markets: int, arbs: int):
    with _conn() as c:
        c.execute("INSERT INTO scans (ts, markets, arbs) VALUES (?,?,?)", (time.time(), markets, arbs))


def log_opportunity(kind: str, o: dict):
    with _conn() as c:
        c.execute(
            "INSERT INTO opportunities (ts, kind, question, ask_yes, ask_no, total_cost, edge_pct, max_size, max_profit_usd, volume_24h) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (time.time(), kind, o.get("question"), o.get("ask_yes"), o.get("ask_no"),
             o.get("total_cost"), o.get("edge_pct"), o.get("max_size_shares"),
             o.get("max_profit_usd"), o.get("volume_24h")),
        )


def log_paper_trade(question: str, cost: float, payout: float, size: float, end_date: str | None):
    with _conn() as c:
        c.execute(
            "INSERT INTO paper_trades (ts, question, side, cost_usd, payout_usd, profit_usd, size_shares, end_date) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (time.time(), question, "ARB", cost, payout, payout - cost, size, end_date),
        )


def log_directional_trade(strategy: str, question: str, side: str, price: float,
                          cost: float, size: float, reasoning: str, live: bool,
                          token_id: str = None, conviction: float = 0.5,
                          src_wallet: str = None, src_price: float = None, src_ts: float = None):
    with _conn() as c:
        # last_price initialisé au PRIX D'ENTRÉE : si la position se résout entre
        # deux scans (avant qu'update_last_price passe), on infère quand même
        # l'issue depuis l'entrée (>0.5 -> gagné, <0.5 -> perdu) au lieu de
        # compter 0. Sans ça, la mesure de perf restait aveugle (tout à 0).
        c.execute(
            "INSERT INTO directional_trades (ts, strategy, question, side, price, cost_usd, size_shares, reasoning, live, token_id, conviction, last_price, src_wallet, src_price, src_ts) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (time.time(), strategy, question, side, price, cost, size, reasoning, 1 if live else 0, token_id, conviction, price, src_wallet, src_price, src_ts),
        )


def recent_directional_trades(limit: int = 50) -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM directional_trades ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]


STATS_SINCE_FILE = DB_PATH.parent / "stats_since.txt"


def get_stats_since() -> float:
    """Timestamp de remise à zéro des stats : on ne compte que les trades de
    la VERSION ACTUELLE (après les corrections momentum/esport/whipsaw), pour
    que les chiffres reflètent ce que fait le bot maintenant, pas les bugs
    passés déjà corrigés."""
    try:
        return float(STATS_SINCE_FILE.read_text().strip())
    except Exception:
        return 0.0


def reset_stats_now(ts: float):
    """Fixe le point de départ des stats à maintenant."""
    STATS_SINCE_FILE.write_text(str(ts))


def performance_stats() -> dict:
    """Bilan réalisé de la VERSION ACTUELLE : PnL total, gagnants/perdants,
    taux de réussite, détail par stratégie. Ne compte que les trades LIVE
    clôturés APRÈS le point de remise à zéro, et exclut 'momentum' (désactivé)."""
    since = get_stats_since()
    with _conn() as c:
        rows = c.execute(
            "SELECT strategy, pnl_usd FROM directional_trades "
            "WHERE live=1 AND status='closed' AND pnl_usd IS NOT NULL "
            "AND ts >= ? AND strategy != 'momentum'",
            (since,)
        ).fetchall()
    total = 0.0
    wins = 0
    losses = 0
    by_strat: dict = {}
    for r in rows:
        pnl = r["pnl_usd"] or 0
        total += pnl
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        s = by_strat.setdefault(r["strategy"], {"pnl": 0.0, "n": 0, "wins": 0})
        s["pnl"] += pnl
        s["n"] += 1
        if pnl > 0:
            s["wins"] += 1
    n = wins + losses
    return {
        "realized_pnl": round(total, 2),
        "trades_closed": len(rows),
        "wins": wins,
        "losses": losses,
        "win_rate": round(100 * wins / n, 0) if n else 0,
        "by_strategy": {k: {"pnl": round(v["pnl"], 2), "n": v["n"],
                            "win_rate": round(100 * v["wins"] / v["n"], 0) if v["n"] else 0}
                        for k, v in by_strat.items()},
    }


def open_live_directional_positions() -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM directional_trades WHERE live=1 AND status='open' ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]


def open_exposure_on_question(question: str) -> float:
    """Somme des coûts déjà engagés (positions live encore ouvertes) sur
    cette question précise — empêche l'empilement : plusieurs signaux
    momentum/IA successifs sur le MÊME marché ne doivent pas dépasser le
    plafond par position, ils doivent le PARTAGER. Bug réel observé : 3
    entrées momentum distinctes sur 'Bitcoin above $62,000?' ont totalisé
    7.73$ alors que le plafond voulu était 6$."""
    with _conn() as c:
        row = c.execute(
            "SELECT COALESCE(SUM(cost_usd),0) s FROM directional_trades "
            "WHERE live=1 AND status='open' AND question=?", (question,)
        ).fetchone()
        return row["s"]


def reopen_held_but_closed(held_asset_ids) -> int:
    """RÉCONCILIATION (idée Steven 'DB=réalité') : ré-ouvre toute position marquée
    'closed' alors que le token est ENCORE DÉTENU on-chain (bug orpheline récurrent
    - snapshot/RPC qui lague ferme à tort une position tenue). Appelé à chaque
    refresh compte : la DB se recale en permanence sur la vérité. Retourne le nb
    ré-ouvert."""
    import time as _t
    ids = [str(a) for a in held_asset_ids if a]
    if not ids:
        return 0
    with _conn() as c:
        # Ne PAS ré-ouvrir un token qu'on vient de VENDRE volontairement : après
        # une vente, la balance on-chain met du temps à tomber à 0 (settlement),
        # donc il apparaît encore 'détenu' — le ré-ouvrir crée une boucle de
        # churn (ré-ouvert -> re-vendu/re-acheté l'autre camp = both-sides !).
        recent_sold = {
            r["asset"] for r in c.execute(
                "SELECT DISTINCT asset FROM onchain_trades WHERE side='SELL' AND ts >= ?",
                (_t.time() - 600,)
            ).fetchall()
        }
        to_reopen = [i for i in ids if i not in recent_sold]
        if not to_reopen:
            return 0
        ph = ",".join("?" * len(to_reopen))
        cur = c.execute(
            f"UPDATE directional_trades SET status='open', pnl_usd=NULL "
            f"WHERE live=1 AND status='closed' AND token_id IN ({ph})",
            to_reopen,
        )
        return cur.rowcount


def upsert_onchain_trade(tx_hash, asset, side, ts, typ, title, outcome, price,
                         usdc_size, shares, condition_id, source):
    """Insère un event on-chain (source de vérité). INSERT OR IGNORE sur la clé
    (tx_hash, asset, side) — re-syncable sans doublon."""
    with _conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO onchain_trades "
            "(tx_hash, asset, side, ts, type, title, outcome, price, usdc_size, shares, condition_id, source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (tx_hash, asset, side, ts, typ, title, outcome, price, usdc_size, shares, condition_id, source),
        )


def onchain_bot_tokens(within_s: float = 172800) -> list:
    """(token_id, ts) des trades LOGGÉS par le bot récemment — sert à taguer un
    event on-chain 'bot' vs 'manual' (Steven)."""
    import time as _t
    with _conn() as c:
        rows = c.execute(
            "SELECT token_id, ts FROM directional_trades WHERE token_id IS NOT NULL AND ts >= ?",
            (_t.time() - within_s,)
        ).fetchall()
        return [(r["token_id"], r["ts"]) for r in rows]


def recent_onchain_trades(limit: int = 60) -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM onchain_trades ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]


def ai_calibration_report() -> dict:
    """① CALIBRATION IA (idée Steven) : quand l'IA dit X% de confiance, gagne-t-elle
    vraiment ~X% ? Lit les trades ai_verified clôturés (conviction = confiance prédite,
    pnl_usd = résultat). Retourne le win-rate réel, la confiance moyenne prédite, le net,
    et le nombre de clôtures — pour piloter automatiquement la mise/le seuil de l'IA."""
    with _conn() as c:
        rows = c.execute(
            "SELECT conviction, pnl_usd, cost_usd FROM directional_trades "
            "WHERE strategy='ai_verified' AND status='closed' AND conviction IS NOT NULL"
        ).fetchall()
    n = len(rows)
    if n == 0:
        return {"n": 0, "win_rate": 0.0, "avg_conf": 0.0, "net": 0.0, "cost": 0.0, "roi": 0.0}
    wins = sum(1 for r in rows if (r["pnl_usd"] or 0) > 0.02)
    net = sum((r["pnl_usd"] or 0) for r in rows)
    cost = sum((r["cost_usd"] or 0) for r in rows)
    avg_conf = sum((r["conviction"] or 0) for r in rows) / n
    return {"n": n, "win_rate": round(wins / n, 3), "avg_conf": round(avg_conf, 3),
            "net": round(net, 2), "cost": round(cost, 2),
            "roi": round(net / cost, 3) if cost > 0 else 0.0}


def onchain_realized_pnl(since: float = 0.0) -> dict:
    """PnL RÉALISÉ calculé depuis la vérité on-chain : par marché, proceeds
    (SELL+REDEEM) - cost (BUY). Ne compte que les positions CLÔTURÉES (qui ont
    des proceeds). Ventile bot vs manuel. C'est LE bilan fiable.

    since>0 : ne compte que les marchés dont l'ACHAT date d'après ce timestamp
    -> mesure du bot PROPRE (après les fixes), non polluée par les vieux bugs."""
    with _conn() as c:
        rows = c.execute(
            "SELECT title, asset, side, type, usdc_size, source, ts, condition_id FROM onchain_trades "
            "WHERE ts >= ? OR ? = 0",
            (since, since)
        ).fetchall()
    mk: dict = {}
    for r in rows:
        # GROUPÉ PAR conditionId (identifiant du MARCHÉ) : un REDEEM a un asset VIDE
        # mais le même conditionId que l'achat -> sinon achat et gain-par-résolution
        # ne s'apparient jamais et les victoires near-certain disparaissent (fix 2026-07-17).
        key = r["condition_id"] or (r["title"], r["asset"])
        m = mk.setdefault(key, {"buy": 0.0, "proceeds": 0.0, "source": r["source"]})
        if r["type"] == "REDEEM" or r["side"] == "SELL":
            m["proceeds"] += r["usdc_size"] or 0
        elif r["side"] == "BUY":
            m["buy"] += r["usdc_size"] or 0
        if r["source"] == "manual":
            m["source"] = "manual"  # si au moins une jambe manuelle, marque manuel
    total = 0.0; bot = 0.0; manual = 0.0; n_closed = 0; wins = 0
    for key, m in mk.items():
        if m["buy"] <= 0 or m["proceeds"] <= 0:
            continue  # position pas encore clôturée
        pnl = m["proceeds"] - m["buy"]
        total += pnl; n_closed += 1
        if pnl > 0.02:
            wins += 1
        if m["source"] == "manual":
            manual += pnl
        else:
            bot += pnl
    return {
        "realized_total": round(total, 2),
        "bot_pnl": round(bot, 2),
        "manual_pnl": round(manual, 2),
        "closed": n_closed,
        "win_rate": round(100 * wins / n_closed, 0) if n_closed else 0,
    }


def record_portfolio_snapshot(total: float, cash: float, pos_value: float, n_positions: int,
                              min_interval_s: float = 300):
    """Enregistre un point de l'historique du portefeuille (courbe du solde,
    style appli bancaire). Throttlé : au moins min_interval_s entre 2 points
    pour ne pas gonfler la base (1 point / 5 min suffit pour une belle courbe)."""
    import time as _t
    with _conn() as c:
        last = c.execute("SELECT ts FROM portfolio_snapshots ORDER BY id DESC LIMIT 1").fetchone()
        if last and _t.time() - last["ts"] < min_interval_s:
            return
        c.execute(
            "INSERT INTO portfolio_snapshots (ts, total, cash, pos_value, n_positions) VALUES (?,?,?,?,?)",
            (_t.time(), round(total, 2), round(cash, 2), round(pos_value, 2), n_positions),
        )


def portfolio_history(limit: int = 500) -> list[dict]:
    """Historique du solde (du plus ancien au plus récent) pour tracer la courbe."""
    with _conn() as c:
        rows = c.execute(
            "SELECT ts, total, cash, pos_value, n_positions FROM portfolio_snapshots "
            "ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in reversed(rows)]


def portfolio_summary() -> dict:
    """Résumé style appli bancaire : solde actuel, point de départ, variation
    absolue et %, plus haut/plus bas, sur toute la période enregistrée."""
    hist = portfolio_history(1000)
    if not hist:
        return {"current": 0, "start": 0, "change": 0, "change_pct": 0, "high": 0, "low": 0, "points": 0}
    totals = [h["total"] for h in hist if h["total"] is not None]
    if not totals:
        return {"current": 0, "start": 0, "change": 0, "change_pct": 0, "high": 0, "low": 0, "points": 0}
    start, current = totals[0], totals[-1]
    return {
        "current": round(current, 2),
        "start": round(start, 2),
        "change": round(current - start, 2),
        "change_pct": round(100 * (current - start) / start, 1) if start else 0,
        "high": round(max(totals), 2),
        "low": round(min(totals), 2),
        "points": len(totals),
        "since_ts": hist[0]["ts"],
    }


def recently_lost_on(question: str, within_s: float = 21600) -> bool:
    """A-t-on clôturé une position PERDANTE sur ce marché récemment (6h défaut) ?
    Bloque le re-pari IA d'une thèse déjà infirmée par le marché (vécu : NO sur
    'Israeli parliament' perdu 3× d'affilée en re-entrant après chaque stop)."""
    import time as _t
    with _conn() as c:
        row = c.execute(
            "SELECT 1 FROM directional_trades WHERE question=? AND status='closed' "
            "AND pnl_usd IS NOT NULL AND pnl_usd < 0 AND ts >= ? LIMIT 1",
            (question, _t.time() - within_s),
        ).fetchone()
        return row is not None


def open_exposure_on_wallet(wallet: str) -> float:
    """Somme des coûts engagés (positions copy live encore ouvertes) sur ce wallet
    source — pour plafonner l'exposition par wallet (point ferme Perplexity)."""
    with _conn() as c:
        row = c.execute(
            "SELECT COALESCE(SUM(cost_usd),0) s FROM directional_trades "
            "WHERE live=1 AND status='open' AND strategy='copy' AND src_wallet=?", (wallet,)
        ).fetchone()
        return row["s"]


def log_momentum_signal(question, categorie, move_pts, price0, token_id):
    """Shadow logger (Perplexity) : enregistre un signal de momentum SANS trader.
    Anti-doublon : pas 2x le même token dans les 10 min."""
    import time as _t
    with _conn() as c:
        dup = c.execute("SELECT 1 FROM momentum_shadow WHERE token_id=? AND ts>=? LIMIT 1",
                        (token_id, _t.time() - 600)).fetchone()
        if dup:
            return
        c.execute("INSERT INTO momentum_shadow (ts, question, categorie, move_pts, price0, token_id) "
                  "VALUES (?,?,?,?,?,?)", (_t.time(), question, categorie, move_pts, price0, token_id))


def momentum_signals_to_check(min_age_s: float = 300) -> list[dict]:
    """Signaux à vérifier (~5min après) pour mesurer la continuation."""
    import time as _t
    with _conn() as c:
        rows = c.execute("SELECT id, token_id, price0 FROM momentum_shadow "
                         "WHERE checked=0 AND ts <= ? LIMIT 20", (_t.time() - min_age_s,)).fetchall()
        return [dict(r) for r in rows]


def record_momentum_check(sig_id, price1):
    with _conn() as c:
        row = c.execute("SELECT price0 FROM momentum_shadow WHERE id=?", (sig_id,)).fetchone()
        if not row:
            return
        net = round((price1 - row["price0"]) * 100, 2)
        c.execute("UPDATE momentum_shadow SET checked=1, price1=?, net_pts=?, continued=? WHERE id=?",
                  (price1, net, 1 if net > 0 else 0, sig_id))


def momentum_shadow_report() -> dict:
    """Bilan du shadow logger : par catégorie, le momentum CONTINUE-t-il (net
    positif après spread) ? Révèle où un momentum tradable pourrait exister."""
    with _conn() as c:
        rows = c.execute("SELECT categorie, net_pts, continued FROM momentum_shadow WHERE checked=1").fetchall()
    cats = {}
    for r in rows:
        d = cats.setdefault(r["categorie"], {"n": 0, "cont": 0, "net_sum": 0.0})
        d["n"] += 1; d["cont"] += r["continued"] or 0; d["net_sum"] += r["net_pts"] or 0
    return {"par_categorie": sorted(
        [{"categorie": k, "signaux": v["n"], "continuation_pct": round(100 * v["cont"] / v["n"]) if v["n"] else 0,
          "net_moyen_pts": round(v["net_sum"] / v["n"], 2) if v["n"] else 0} for k, v in cats.items()],
        key=lambda x: -x["net_moyen_pts"])}


def category_report(since: float = 0.0) -> dict:
    """Rapport par CATÉGORIE sur la VÉRITÉ on-chain (demande Perplexity : mesurer
    chaque famille SÉPARÉMENT, net de tous les coûts réels). Par marché clôturé :
    pnl = proceeds(SELL+REDEEM) - cost(BUY) — donc DÉJÀ net de spread/frais/slippage
    réels. Ne mélange jamais les catégories. Seul le 'bot' compte (pas le manuel)."""
    import re
    with _conn() as c:
        rows = c.execute(
            "SELECT title, asset, side, type, usdc_size, source, ts FROM onchain_trades "
            "WHERE ts >= ? OR ? = 0", (since, since)
        ).fetchall()

    def categorize(t: str) -> str:
        tl = (t or "").lower()
        if any(k in tl for k in ["bitcoin", "ethereum", "solana", "crypto", " btc", " eth"]):
            return "crypto"
        if any(k in tl for k in ["itf", "atp", "wta", "tennis", " open:", "roland",
                                 "wimbledon", "challenger"]):
            return "tennis"
        if any(k in tl for k in ["counter", "dota", "cs2", "league of legends", "valorant", "esport"]):
            return "esport"
        if any(k in tl for k in ["odi", "t20", "cricket", "test series"]):
            return "cricket"
        if any(k in tl for k in ["nba", "wnba", "basket", "storm", "mystics", "aces", " sky", "summer league"]):
            return "basket"
        # FOOT élargi (76% du volume whales = 'Team to Advance'/'X vs. Y'/O-U raté avant)
        if any(k in tl for k in ["fifa", "world cup", "premier league", "champions",
                                 "la liga", "serie a", "bundesliga", "ligue 1", "soccer",
                                 "football", "win on", "team to advance", "to advance",
                                 " advance", "end in a draw", "o/u", "total corners", " vs. "]):
            return "foot"
        if any(k in tl for k in ["parliament", "election", "president", "trump", "iran",
                                 "israel", "war", "court", "minister", "government"]):
            return "politique"
        if any(k in tl for k in ["tweet", "musk", "weather", "temperature"]):
            return "novelty"
        return "autre"

    mk: dict = {}
    for r in rows:
        if r["source"] != "bot":
            continue
        k = (r["title"], r["asset"])
        m = mk.setdefault(k, {"buy": 0.0, "proc": 0.0, "title": r["title"]})
        if r["type"] == "REDEEM" or r["side"] == "SELL":
            m["proc"] += r["usdc_size"] or 0
        elif r["side"] == "BUY":
            m["buy"] += r["usdc_size"] or 0

    cats: dict = {}
    for k, m in mk.items():
        if m["buy"] <= 0 or m["proc"] <= 0:
            continue  # pas clôturé
        pnl = m["proc"] - m["buy"]
        cat = categorize(m["title"])
        d = cats.setdefault(cat, {"pnl": 0.0, "n": 0, "wins": 0, "cost": 0.0})
        d["pnl"] += pnl; d["n"] += 1; d["cost"] += m["buy"]
        if pnl > 0.02:
            d["wins"] += 1
    out = []
    for cat, d in cats.items():
        out.append({"categorie": cat, "pnl_net": round(d["pnl"], 2), "clotures": d["n"],
                    "win_rate": round(100 * d["wins"] / d["n"]) if d["n"] else 0,
                    "roi_pct": round(100 * d["pnl"] / d["cost"]) if d["cost"] else 0,
                    "investi": round(d["cost"], 2),
                    "statut": ("PROUVÉ+" if d["pnl"] > 1 and d["n"] >= 15 else
                               "PROMETTEUR" if d["pnl"] > 0 else
                               "PROUVÉ-" if d["n"] >= 15 else "à mesurer")})
    return {"par_categorie": sorted(out, key=lambda x: -x["pnl_net"])}


def copy_v2_report() -> dict:
    """Rapport COPY v2 (demande Perplexity) : mesure incontestable par wallet et
    global — PnL, slippage (prix source vs notre fill), délai, nb trades, expo,
    win-rate. Sert à VALIDER que copier un wallet vert est réellement rentable
    POUR NOUS après exécution réelle (un wallet rapide/arb n'est pas copiable)."""
    with _conn() as c:
        rows = c.execute(
            "SELECT src_wallet, reasoning, price, src_price, ts, src_ts, cost_usd, "
            "pnl_usd, status FROM directional_trades "
            "WHERE strategy='copy' AND live=1 AND src_wallet IS NOT NULL ORDER BY id DESC"
        ).fetchall()
    by_w: dict = {}
    g = {"n": 0, "closed": 0, "pnl": 0.0, "wins": 0, "slip_sum": 0.0, "slip_n": 0,
         "delay_sum": 0.0, "delay_n": 0, "cost": 0.0}
    for r in rows:
        w = r["src_wallet"]
        name = (r["reasoning"] or "")[6:26]  # "copie NAME (..."
        d = by_w.setdefault(w, {"name": name, "n": 0, "closed": 0, "pnl": 0.0,
                                "wins": 0, "slip_sum": 0.0, "slip_n": 0,
                                "delay_sum": 0.0, "delay_n": 0, "cost": 0.0})
        d["n"] += 1; g["n"] += 1; d["cost"] += r["cost_usd"] or 0; g["cost"] += r["cost_usd"] or 0
        if r["src_price"] and r["price"]:
            slip = r["price"] - r["src_price"]  # >0 = on a payé + cher que la source
            d["slip_sum"] += slip; d["slip_n"] += 1; g["slip_sum"] += slip; g["slip_n"] += 1
        if r["src_ts"] and r["ts"]:
            dl = r["ts"] - r["src_ts"]  # délai entre le trade source et notre fill
            if 0 <= dl < 3600:
                d["delay_sum"] += dl; d["delay_n"] += 1; g["delay_sum"] += dl; g["delay_n"] += 1
        if r["status"] == "closed" and r["pnl_usd"] is not None:
            d["closed"] += 1; g["closed"] += 1; d["pnl"] += r["pnl_usd"]; g["pnl"] += r["pnl_usd"]
            if r["pnl_usd"] > 0.02:
                d["wins"] += 1; g["wins"] += 1
    def fmt(d):
        return {"name": d.get("name", "GLOBAL"), "trades": d["n"], "closed": d["closed"],
                "pnl": round(d["pnl"], 2),
                "win_rate": round(100 * d["wins"] / d["closed"]) if d["closed"] else None,
                "slippage_moy": round(d["slip_sum"] / d["slip_n"], 3) if d["slip_n"] else None,
                "delai_moy_s": round(d["delay_sum"] / d["delay_n"]) if d["delay_n"] else None,
                "expo": round(d["cost"], 2)}
    wallets = sorted((fmt(d) | {"wallet": w[:10]} for w, d in by_w.items()),
                     key=lambda x: (x["pnl"] if x["pnl"] else 0), reverse=True)
    return {"global": fmt(g), "par_wallet": wallets}


def close_directional_trade(trade_id: int, exit_price: float, pnl_usd: float):
    with _conn() as c:
        c.execute(
            "UPDATE directional_trades SET status='closed', exit_price=?, pnl_usd=? WHERE id=?",
            (exit_price, pnl_usd, trade_id),
        )


def update_peak_price(trade_id: int, peak: float):
    with _conn() as c:
        c.execute("UPDATE directional_trades SET peak_price=? WHERE id=?", (peak, trade_id))


def update_last_price(trade_id: int, last: float):
    """Mémorise le dernier prix observé (côté qu'on détient) — sert à déduire
    l'issue à la résolution : prix→1 = notre camp a gagné, prix→0 = perdu."""
    with _conn() as c:
        c.execute("UPDATE directional_trades SET last_price=? WHERE id=?", (last, trade_id))


def mark_partial_taken(trade_id: int, remaining_size: float, realized_pnl: float):
    """Take-profit escalier : moitié vendue, on note la sortie partielle et la
    taille restante (le reste continue de courir)."""
    with _conn() as c:
        c.execute(
            "UPDATE directional_trades SET partial_taken=1, size_shares=?, pnl_usd=COALESCE(pnl_usd,0)+? WHERE id=?",
            (remaining_size, realized_pnl, trade_id),
        )


def get_stats() -> dict:
    with _conn() as c:
        scans = c.execute("SELECT COUNT(*) n, COALESCE(SUM(arbs),0) a FROM scans").fetchone()
        opps = c.execute("SELECT COUNT(*) n FROM opportunities WHERE kind='arb'").fetchone()
        trades = c.execute(
            "SELECT COUNT(*) n, COALESCE(SUM(cost_usd),0) cost, COALESCE(SUM(profit_usd),0) profit "
            "FROM paper_trades").fetchone()
        last_scan = c.execute("SELECT ts, markets FROM scans ORDER BY id DESC LIMIT 1").fetchone()
        dir_trades = c.execute(
            "SELECT COUNT(*) n, COALESCE(SUM(cost_usd),0) cost, "
            "SUM(CASE WHEN live=1 THEN 1 ELSE 0 END) n_live "
            "FROM directional_trades").fetchone()
        return {
            "scans": scans["n"],
            "arbs_seen": opps["n"],
            "paper_trades": trades["n"],
            "paper_invested": round(trades["cost"], 2),
            "paper_profit_locked": round(trades["profit"], 2),
            "directional_trades": dir_trades["n"],
            "directional_invested": round(dir_trades["cost"], 2),
            "directional_live": dir_trades["n_live"] or 0,
            "last_scan_ts": last_scan["ts"] if last_scan else None,
            "last_scan_markets": last_scan["markets"] if last_scan else 0,
        }


def recent_opportunities(limit: int = 50) -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM opportunities ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]


def recent_paper_trades(limit: int = 50) -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM paper_trades ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
