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
        self._crawler = UrlCrawler(base_url)
        self._parser = Parser()
        self._base_url = base_url

    def run(self):
        max_urls_to_process = 10000
        print("Total URLs in the DB.", self._db.get_total())
        urls = [self._base_url]
        while len(urls) > 0:
            for u in urls:
                self._init_db(u, False)
                if self._db.get_total() > max_urls_to_process:
                    return
            print("Total URLs in the DB.", self._db.get_total())
            urls = self._get_urls_from_table(1000000)

    def _init_db(self, base_url, skip_if_present=False):
        print("Processing url: ", base_url)
        # We start with the base_url as the base crawl point and go from there
        # Steps in the flow
        # a. Start from a base url
        # b. Fetch the url, update it in the Db
        # c. Get all the links from the page and insert all of them
        #    in the db (which do not already exist and matches the base domain)
        # d. Read the db and fetch more
        if not self._db.exists(base_url):
            self._db.add_url(base_url)
        elif skip_if_present == True:
            logging.info("URL already present. Skipping.")
            return

        if not self._crawler.fetch(base_url):
            logging.error("Aborting. Could not fetch base url: ", base_url)
            sys.exit(1)
        if self._crawler.get_contents() == '':
            logging.error("Aborting. Found empty content in base url: ", base_url)
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
            canonicalized_link = self._crawler.canonicalize_url(link)
            if not self._db.exists(canonicalized_link):
                self._db.add_url(canonicalized_link)

        print("Total URLs in the DB.", self._db.get_total())

    def _get_urls_from_table(self, num_to_fetch=100):
        return self._db.read(max_to_fetch=num_to_fetch)

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
