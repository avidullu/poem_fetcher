import datetime
import urllib
import urllib3
from bs4 import BeautifulSoup
import sqlite3
import argparse
import sys
import logging

class UrlDb:
    conn = None
    def __init__(self, db_path):
        try:
          self.conn = sqlite3.connect(db_path)
        except sqlite3.OperationalError as e:
            print (e)
            sys.exit("DB connection error. Aborting")

    def print_tables(self):
        c = self.conn.cursor()
        try:
          c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        except sqlite3.OperationalError as e:
            print(e)
            sys.exit("Reading from DB failed. Aborting");
        tables = c.fetchall()
        [print(table) for table in tables]

    def add_url(self, url):
        curr = self.conn.cursor()
        now_time = datetime.datetime.now().isoformat()
        logging.debug("Inserting url: ", url, type(url), now_time, type(now_time))
        try:
          curr.execute("insert into seen_urls values(?, ?);",
                       (url, now_time))
          self.conn.commit()
        except sqlite3.OperationalError as e:
            print(e)
            print("Writing to DB failed.")

    def remove_url(self, url):
        curr = self.conn.cursor()
        logging.debug("Removing url.", url, type(url))
        try:
          curr.execute("delete from seen_urls where url = (?);", (url,))
          self.conn.commit()
        except sqlite3.OperationalError as e:
            print(e)
            print("Removing from DB failed.")

class UrlCrawler:
    def __init__(self, url, base_domain):
        pass
    def fetch(self):
        pass
    def get_url(self):
        pass
    def get_crawl_time(self):
        pass

# Use BeautifulSoup to parse and extract information from a fetched page
class Parser:
    data = None
    def __init__(self, data):
        self.data = data
    def find_all(self, tag):
        pass

class UrlProcessor:
    crawler = None
    db = None
    parser = None
    def __init__(self, url, url_db, parser):
        self.crawler = UrlCrawler()
        self.db = url_db
        self.parser = parser

    def sanitized_url(self):
        pass
    def get_all_urls(self):
        pass
    def find_all(self, tag):
        pass

class CrawlDriver:
    db = None
    base_url = None
    def __init__(self, db, base_url):
        self.db = db
        self.base_url = base_url
    def run(self):
        pass
    def _get_urls_from_table(self, num_to_fetch=10):
        pass
    def _mark_url_processed(self, url):
        pass

def main():
    base_domain = 'http://www.kavitakosh.org'
    db_path = 'kavita_kosh.db'
    print("We are starting to crawl ", base_domain)
    print("We are using", db_path, " as the db to store our information.")
    db = UrlDb(db_path)
    driver = CrawlDriver(db, base_domain)
    driver.run()

if __name__ == "__main__":
    main()
