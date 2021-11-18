import datetime

import pandas as pd
import yfinance as yf
import sqlite3
from functools import lru_cache
from ..constants import DB_NAME
from ..utils import wrap_list


@lru_cache()
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
    df = pd.read_sql_query(query, connection(), parse_dates={'hdate': '%Y-%m-%d'})
    df = df.rename(columns={'shares': 'shares_held'})
    return df


def _get_start_and_end_date(start_date, end_date, all_dates):
    start_date = start_date or pd.Timestamp(min(all_dates)).to_pydatetime()
    end_date = end_date or pd.Timestamp(max(all_dates)).to_pydatetime()
    end_date = end_date + datetime.timedelta(days=1)
    return start_date, end_date


def shares_traded_in_etf_vs_mkt(ticker, start_date=None, end_date=None, etfs=None):
    etfs = wrap_list(etfs)
    ticker_holding = get_ticker_holding(ticker, start_date, end_date, etfs)
    ticker_holding = ticker_holding.sort_values('hdate')
    ticker_holding['shares_held_delta'] = ticker_holding['shares_held'].diff()
    ticker_holding['shares_held_delta_abs'] = ticker_holding['shares_held_delta'].abs()
    if start_date is None or end_date is None:
        all_dates = ticker_holding['hdate'].unique()
        start_date, end_date = _get_start_and_end_date(start_date, end_date, all_dates)
    yf_ticker = yf.Ticker(ticker)
    yf_start_date = start_date - datetime.timedelta(days=130)
    ticker_hist = yf_ticker.history(start=yf_start_date, end=end_date)
    ticker_hist['3MAvgVol'] = ticker_hist['Volume'].rolling(window=90).mean()
    ticker_overall = pd.merge(ticker_hist, ticker_holding, left_index=True, right_on='hdate')
    ticker_overall['pct_of_3M_vol'] = ticker_overall['shares_held_delta'] / ticker_overall['3MAvgVol']
    ticker_overall['pct_of_3M_vol_abs'] = ticker_overall['shares_held_delta_abs'] / ticker_overall['3MAvgVol']
    ticker_overall = ticker_overall[['hdate', 'fund', 'ticker', 'mv', 'shares_held', 'shares_held_delta',
                                     'shares_held_delta_abs', '3MAvgVol', 'pct_of_3M_vol', 'pct_of_3M_vol_abs',
                                     'pct_of_nav']]
    return ticker_overall
