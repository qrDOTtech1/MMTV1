from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "ghost.db"
engine = create_engine(f"sqlite:///{DB_PATH}")
Session = sessionmaker(bind=engine)
Base = declarative_base()


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    strategy = Column(String)
    token_address = Column(String)
    token_symbol = Column(String)
    action = Column(String)  # buy / sell
    amount_eth = Column(Float)
    price = Column(Float)
    simulated = Column(Boolean, default=True)
    pnl = Column(Float, nullable=True)
    notes = Column(String, nullable=True)


class Position(Base):
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True)
    token_address = Column(String, index=True)  # PAS unique : 2 strats peuvent tenir le même token
    token_symbol = Column(String)
    strategy = Column(String)
    entry_price = Column(Float)
    amount = Column(Float)
    entry_time = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    simulated = Column(Boolean, default=True)


class Stats(Base):
    __tablename__ = "stats"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    strategy = Column(String)
    total_trades = Column(Integer, default=0)
    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    total_pnl = Column(Float, default=0.0)
    balance = Column(Float, default=0.0)


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(engine)
