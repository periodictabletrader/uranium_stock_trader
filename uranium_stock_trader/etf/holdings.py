import datetime
import sqlite3
from functools import lru_cache

import matplotlib.ticker as mticker
import pandas as pd
import seaborn as sns
import yfinance as yf
from matplotlib import pyplot as plt

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
    df = df.rename(columns={'shares': 'Shares_held'})
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
    ticker_holding['Shares_held_change'] = ticker_holding['Shares_held'].diff()
    ticker_holding['Shares_held_change_abs'] = ticker_holding['Shares_held_change'].abs()
    ticker_holding['Shares_held_change_pct'] = ticker_holding['Shares_held_change'] / ticker_holding['Shares_held']
    if start_date is None or end_date is None:
        all_dates = ticker_holding['hdate'].unique()
        start_date, end_date = _get_start_and_end_date(start_date, end_date, all_dates)
    yf_ticker = yf.Ticker(ticker)
    yf_start_date = start_date - datetime.timedelta(days=130)
    ticker_hist = yf_ticker.history(start=yf_start_date, end=end_date)
    ticker_hist['Price_change'] = ticker_hist['Close'] - ticker_hist['Open']
    ticker_hist['Price_change_pct'] = ticker_hist['Price_change'] / ticker_hist['Open']
    ticker_hist = ticker_hist.rename(columns={'Close': 'Price'})
    ticker_hist['3MAvgVol'] = ticker_hist['Volume'].rolling(window=90).mean()
    ticker_overall = pd.merge(ticker_hist, ticker_holding, left_index=True, right_on='hdate')
    ticker_overall['pct_of_3M_vol'] = ticker_overall['Shares_held_change_abs'] / ticker_overall['3MAvgVol']
    ticker_overall['pct_of_Volume'] = ticker_overall['Shares_held_change_abs'] / ticker_overall['Volume']
    ticker_overall = ticker_overall[['hdate', 'fund', 'ticker', 'Price', 'Price_change', 'Price_change_pct', 'mv',
                                     'Shares_held', 'Shares_held_change', 'Shares_held_change_abs',
                                     'Shares_held_change_pct', 'Volume', 'pct_of_Volume', '3MAvgVol', 'pct_of_3M_vol',
                                     'pct_of_nav']]
    return ticker_overall


def plot_etf_activity_vs_mkt_volume(ticker, start_date=None, end_date=None, etfs=None):
    etfs = wrap_list(etfs)
    analysis = shares_traded_in_etf_vs_mkt(ticker, start_date, end_date, etfs)
    melted_df = pd.melt(analysis, id_vars=['hdate', 'ticker'], value_vars=['Shares_held_change_abs', 'Volume'])
    combined_df = pd.merge(melted_df, analysis, left_on='hdate', right_on='hdate')
    combined_df['Date'] = combined_df['hdate'].apply(lambda dt: pd.Timestamp(dt).to_pydatetime().strftime('%Y-%m-%d'))

    etfs_str = f'{", ".join(etfs)}' if etfs else 'ETF'
    fig, ax1 = plt.subplots(figsize=(20, 12))
    fig.autofmt_xdate()
    ax1.set_title(f'{ticker} - {etfs_str} activity vs Price', fontsize=16)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: format(int(x), ',')))
    ax1 = sns.barplot(x='Date', y='value', data=combined_df, hue='variable')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('# Shares')
    plt.xticks(rotation=90)
    plt.legend(title='')

    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Price', fontsize=16, color=color)
    ax2 = sns.lineplot(x='Date', y='Price', data=combined_df, sort=False, color=color)
    ax2.tick_params(axis='y', color=color)
    plt.show()
