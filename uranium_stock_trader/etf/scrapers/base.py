import datetime
import re
import requests
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from io import BytesIO, StringIO
import pandas as pd


class Scraper(ABC):

    def __init__(self, url, date_selector, date_fmt):
        self.url = url
        self._soup = None
        self._as_of_date = None
        self.date_selector = date_selector
        self.date_fmt = date_fmt

    @property
    @abstractmethod
    def as_of_date(self):
        pass

    @abstractmethod
    def scrape(self):
        pass
