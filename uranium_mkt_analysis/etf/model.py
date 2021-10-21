from sqlalchemy import create_engine
from sqlalchemy import Column, ForeignKey, BigInteger, Date, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from ..constants import DB_NAME

engine = create_engine('sqlite:///{}?check_same_thread=False'.format(DB_NAME), pool_pre_ping=True)
Base = declarative_base(bind=engine)
Session = scoped_session(sessionmaker(bind=engine, autoflush=False))


def session():
    s = sessionmaker(bind=engine)
    return s()


class ETFHoldings(Base):
    __tablename__ = 'etf_holdings'
    hdate = Column(Date, nullable=False, primary_key=True)
    fund = Column(String, nullable=False, primary_key=True)
    ticker = Column(String, primary_key=True)
    name = Column(String, nullable=False, primary_key=True)
    shares = Column(BigInteger, primary_key=True, default=0)
    mv = Column(Float, nullable=False, default=0)
    pct_of_nav = Column(Float, nullable=False, default=0)


class ETFTradingData(Base):
    __tablename__ = 'etf_trading_data'
    datadate = Column(Date, nullable=False, primary_key=True)
    etf = Column(String, nullable=False, primary_key=True)
    shs_outstanding = Column(BigInteger)
    net_assets = Column(BigInteger)
    volume = Column(BigInteger)
