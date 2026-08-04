"""Config ENGINEBTB3 (Steven 04/08). Rien de ceci n'est encore consomme par
le trader reel -- module autonome, prevu pour rester isole tant que
ACTIVE=False dans __init__.py."""

MODE = "paper"  # "paper" | "live" -- live interdit tant que ACTIVE=False
MARKETS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]  # crypto 5min, meme univers que GHOST/MMTRADE
WEATHER_MARKETS = []  # pas encore scope -- voir ENGINEBTB3_SPEC.txt section "MARCHE"

MAX_POSITION_SIZE_USD = 0.0  # 0 = aucune position possible, garde-fou explicite
MAX_DAILY_LOSS_USD = 0.0
