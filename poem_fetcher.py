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
    _max_urls_to_process = 0
    _urls_processed = 0
    _only_base_domain_urls = False

    def __init__(self, db, flags):
        self._db = db
        self._crawler = UrlCrawler(flags['base_domain'])
        self._parser = Parser()
        self._base_url = flags['base_domain']
        self._max_urls_to_process = flags['max_urls_to_process']
        self._only_base_domain_urls = flags['only_include_base_domain_urls']

    def run(self):
        logging.info("Total URLs in the DB: %d", self._db.get_total())
        urls = [self._base_url]
        while len(
                urls) > 0 and self._urls_processed < self._max_urls_to_process:
            for u in urls:
                self._init_db(u, False)
            urls = self._get_urls_from_table(1000000)
        logging.info("Total URLs in the DB: %d", self._db.get_total())

    def _init_db(self, base_url, skip_if_present=False):
        if self._urls_processed > self._max_urls_to_process:
            logging.debug("Max number of URLs processed. Skipping.")
            return
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
            return 0

        logging.debug("Processing url: %s", base_url)

        if not self._crawler.fetch(base_url):
            logging.error("Aborting. Could not fetch base url: %s", base_url)
            sys.exit(1)
        if self._crawler.get_contents() == '':
            logging.error("Aborting. Found empty content in base url: %s ",
                          base_url)
            sys.exit(1)

        self._parser.set_data(self._crawler.get_contents())
        a_tags = self._parser.find_all("a")
        href_text = {}
        for a_tag in a_tags:
            if a_tag.has_attr('href') and self._crawler.is_from_base_domain(
                    a_tag['href']):
                href_text[a_tag['href']] = a_tag.get_text()
        logging.debug("Total number of links extracted: %d", len(a_tags))
        logging.debug("Total number of links from domain extracted: %d",
                      len(href_text))
        num_new = 0
        for link in href_text.keys():
            canonicalized_link = self._crawler.canonicalize_url(link)
            if not self._db.exists(
                    canonicalized_link
            ) and self._urls_processed < self._max_urls_to_process and (
                    self._only_base_domain_urls == False
                    or self._crawler.is_from_base_domain(canonicalized_link)):
                self._db.add_url(canonicalized_link)
                num_new += 1
                self._urls_processed += 1
        logging.info("Number of new URLs found: %s", num_new)
        return num_new

    def _get_urls_from_table(self, num_to_fetch=100):
        return self._db.read(max_to_fetch=num_to_fetch)

    def _mark_url_processed(self, url):
        pass


# Gets a disctionary of all flags parsed.
def SetupLogger(flags):
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s '
                        '[%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=flags['log'])


# Returns a dict of all parsed flags.
def ProcessArgs():
    parser = argparse.ArgumentParser(
        description='Crawl and process hindi poems')
    parser.add_argument('--max_urls_to_process',
                        help='Maximum number of urls to visit',
                        type=int,
                        required=True)
    parser.add_argument('--db_path',
                        help='Path to the DB',
                        type=str,
                        default='kavita_kosh2.db',
                        required=False)
    parser.add_argument('--base_domain',
                        help='Base domain to start crawl',
                        type=str,
                        default='http://kavitakosh.org',
                        required=False)
    parser.add_argument('--log',
                        help='logging level',
                        type=str,
                        default="WARNING")  # NOTSET
    parser.add_argument(
        '--only_include_base_domain_urls',
        help='Only crawl and process URLs from the base domain.',
        type=bool,
        default=True,
        required=False)
    args = parser.parse_args()
    return vars(args)


def main():
    flags = ProcessArgs()
    print("All flags passed.")
    [print(f) for f in flags.items()]
    SetupLogger(flags)
    print("We are starting to crawl ", flags['base_domain'])
    print("We are using", flags['db_path'],
          " as the db to store our information.")
    db = UrlDb(flags['db_path'])
    driver = CrawlDriver(db, flags)
    driver.run()


if __name__ == "__main__":
    main()
