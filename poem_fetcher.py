import argparse
import sys
import logging

from crawler.crawler import UrlCrawler
from db.url_db import UrlDb
from parser.parser import Parser

class CrawlDriver:
    _db = None
    _base_url = None
    _crawler = None
    _parser = None
    def __init__(self, db, base_url):
        self._db = db
        self._base_url = base_url
        self._crawler = UrlCrawler(base_url)
        self._parser = Parser()
    def run(self):
        # We start with the _base_url as the base crawl point and go from there
        # Steps in the flow
        # a. Start from a base url
        # b. Fetch the url, update it in the Db
        # c. Get all the links from the page and insert all of them
        #    in the db (which do not already exist and matches the base domain)
        # d. Read the db and fetch more
        if not self._db.exists(self._base_url):
            self._db.add_url(self._base_url)
        if not self._crawler.fetch(self._base_url):
            logging.error("Aborting. Could not fetch base url: ", self._base_url)
            sys.exit(1)
        if self._crawler.get_contents() == False:
            logging.error("Aborting. Found empty content in base url: ", self._base_url)
            sys.exit(1)
        self._parser.set_data(self._crawler.get_contents())
        a_tags = self._parser.find_all("a")
        href_text = {}
        for a_tag in a_tags:
            if a_tag.has_attr('href') and self._crawler.is_from_base_domain(a_tag['href']):
                href_text[a_tag['href']] = a_tag.get_text()
            else:
                if a_tag.has_attr('href'):
                        print(a_tag['href'])
        print("Total number of links extracted: ", len(a_tags))
        print("Total number of links from domain extracted: ", len(href_text))
        for link in href_text.keys():
            self._db.add_url(self._crawler.canonicalize_url(link))


    def _get_urls_from_table(self, num_to_fetch=10):
        pass
    def _mark_url_processed(self, url):
        pass

def main():
    base_domain = 'http://kavitakosh.org'
    db_path = 'kavita_kosh.db'
    print("We are starting to crawl ", base_domain)
    print("We are using", db_path, " as the db to store our information.")
    db = UrlDb(db_path)
    driver = CrawlDriver(db, base_domain)
    driver.run()

if __name__ == "__main__":
    main()
