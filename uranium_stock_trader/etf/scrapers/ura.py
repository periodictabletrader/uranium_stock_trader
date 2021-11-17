import datetime
import re
from io import StringIO

import pandas as pd
import requests

from .base import Scraper
from ..model import engine
from ..ref_data import TICKER_MAP


class URAScraper(Scraper):

    def __init__(self):
        url = 'https://www.globalxetfs.com/funds/ura/?download_full_holdings=true'
        date_selector = '#fund > section > div.container-fluid > div.row.header > div.col-sm-7 > h3 > small'
        date_fmt = '%m/%d/%Y'
        super().__init__(url, date_selector, date_fmt)
        self.resp = requests.get(url)

    @property
    def as_of_date(self):
        if self.resp.status_code == 200:
            resp_lines = self.resp.text.split('\n')
            date_line = resp_lines[1]
            self._as_of_date = self.parse_date(date_line)
        return self._as_of_date

    def parse_date(self, date_txt):
        date_strs = re.findall('\d{2}/\d{2}/\d{4}', date_txt)
        if date_strs:
            date_str = date_strs[0]
            return datetime.datetime.strptime(date_str, self.date_fmt).date()

    def scrape(self):
        resp_text = self.resp.text
        holdings_strs = resp_text.split('\n')
        holdings_str = '\n'.join(holdings_strs[2:-2])
        holdings_df = pd.read_csv(StringIO(holdings_str))
        holdings_df['hdate'] = self.as_of_date
        holdings_df['fund'] = 'URA'
        holdings_df['ticker'] = holdings_df['Ticker'].apply(lambda tikr: TICKER_MAP.get(tikr) or 'N/A')
        holdings_df['name'] = holdings_df['Name']
        holdings_df['shares'] = holdings_df['Shares Held']
        holdings_df['mv'] = holdings_df['Market Value ($)']
        holdings_df['pct_of_nav'] = holdings_df['% of Net Assets'] / 100
        etf_holdings_df = holdings_df[['hdate', 'fund', 'ticker', 'name', 'shares', 'mv', 'pct_of_nav', ]]
        etf_holdings_df.to_sql('etf_holdings', con=engine, if_exists='append', index=False)
        return etf_holdings_df
