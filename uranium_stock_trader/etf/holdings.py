import pandas as pd
import yfinance as yf
import sqlite3
from functools import lru_cache
from ..constants import DB_NAME
from ..utils import wrap_list


@lru_cache
def connection():
    return sqlite3.connect(DB_NAME)


def construct_query(tickers, start_date=None, end_date=None, etfs=None):
    date_filter = ''
    if start_date and end_date:
        date_filter = f'h.hdate BETWEEN date("{start_date}") and date("{end_date}")'

    etf_filter = ''
    if etfs:
        etfs_str = ",".join([f'"{etf}"' for etf in etfs])
        etfs_str = f'({etfs_str})'
        etf_filter = f'h.fund IN {etfs_str}'

    ticker_filter = ''
    if tickers:
        tickers_str = ",".join([f'"{ticker}"' for ticker in tickers])
        tickers_str = f'({tickers_str})'
        ticker_filter = f'h.ticker IN {tickers_str}'

    all_filters = f'{date_filter} AND {etf_filter} AND {ticker_filter}'
    all_filters_list = [filt for filt in all_filters.split(' AND ') if filt]
    query_filters = ' AND '.join(all_filters_list)
    query = 'SELECT * FROM etf_holdings h'
    query = f'{query} WHERE {query_filters}' if query_filters else query
    return query


def get_ticker_holding(tickers, start_date=None, end_date=None, etfs=None):
    tickers = wrap_list(tickers)
    etfs = wrap_list(etfs)
    query = construct_query(tickers, start_date, end_date, etfs)
    df = pd.read_sql_query(query, connection())
    return df
