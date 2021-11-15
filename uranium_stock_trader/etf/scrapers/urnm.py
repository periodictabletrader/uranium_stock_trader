import datetime
import re

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from uranium_stock_trader.constants import CHROME_DRIVER_PATH, DEFAULT_DOWNLOAD_PATH
from .base import Scraper
from ..model import engine
from ..ref_data import TICKER_MAP


class URNMScraper(Scraper):

    def __init__(self, url=None, date_selector=None, date_fmt=None):
        url = 'https://urnmetf.com/urnm'
        date_selector = '#fund > section > div.container-fluid > div.row.header > div.col-sm-7 > h3 > small'
        date_fmt = '%m/%d/%Y'
        self.date_xpath = '/html/body/div/div/div/div[2]/div[5]/div/div/div/h2'
        super().__init__(url, date_selector, date_fmt)
        self.resp = requests.get(url)
        self.driver = self._create_driver()
        self._prime_webdriver()

    @property
    def as_of_date(self):
        date_elem = self.driver.find_element(By.XPATH, self.date_xpath)
        date_string = date_elem.text
        self._as_of_date = self.parse_date(date_string)
        return self._as_of_date

    def parse_date(self, date_txt):
        date_strs = re.findall('\d{2}/\d{2}/\d{4}', date_txt)
        if date_strs:
            date_str = date_strs[0]
            return datetime.datetime.strptime(date_str, self.date_fmt).date()

    def _prime_webdriver(self):
        self.driver.get('https://urnmetf.com/urnm')
        wait = WebDriverWait(self.driver, 10)
        view_all_btn_xpath = '//*[@id="mcjs"]/div[5]/div/div/div/div/div/button[1]'
        wait.until(EC.element_to_be_clickable((By.XPATH, view_all_btn_xpath)))
        download_btn_xpath = '/html/body/div/div/div/div[2]/div[5]/div/div/div/div/div/button[2]'
        wait.until(EC.element_to_be_clickable((By.XPATH, download_btn_xpath)))

    def _create_driver(self):
        service = Service(executable_path=CHROME_DRIVER_PATH)
        # options = ChromiumOptions()
        options = webdriver.ChromeOptions()
        options.headless = False
        options.add_argument("--headless")
        prefs = {'download.default_directory': DEFAULT_DOWNLOAD_PATH}
        options.add_experimental_option('prefs', prefs)
        driver = webdriver.Chrome(service=service, chrome_options=options)
        return driver

    def download_file(self):
        download_btn_xpath = '/html/body/div/div/div/div[2]/div[5]/div/div/div/div/div/button[2]'
        download_btn = self.driver.find_element(By.XPATH, download_btn_xpath)
        download_btn.click()

    def scrape(self):
        self.download_file()
        date_str = datetime.date.today().strftime('%m-%d-%Y')
        file_name = f'{DEFAULT_DOWNLOAD_PATH}/urnm-holdings-{date_str}.csv'
        raw_df = pd.read_csv(file_name)
        etf_holdings_df = self.parse_scraped_df(raw_df)
        etf_holdings_df.to_sql('etf_holdings', con=engine, if_exists='append', index=False)
        return etf_holdings_df

    def parse_scraped_df(self, holdings_df, as_of_date=None):
        as_of_date = as_of_date or self.as_of_date
        holdings_df['hdate'] = as_of_date
        holdings_df['fund'] = 'URNM'
        holdings_df['ticker'] = holdings_df['TICKER'].apply(lambda tikr: TICKER_MAP.get(tikr) or 'N/A')
        holdings_df['name'] = holdings_df['COMPANY NAME']
        holdings_df['shares'] = holdings_df['SHARES']
        holdings_df['mv'] = holdings_df['MARKET VALUE']
        holdings_df['pct_of_nav'] = holdings_df['% OF NET ASSET VALUES']
        return holdings_df[['hdate', 'fund', 'ticker', 'name', 'shares', 'mv', 'pct_of_nav', ]]
