import argparse
import hashlib
import logging
import random
import sys

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
    _content_fetched_urls = 0
    _only_base_domain_urls = False

    def __init__(self, db, flags):
        self._db = db
        self._crawler = UrlCrawler(flags['base_domain'])
        self._parser = Parser()
        self._base_url = flags['base_domain']
        self._max_urls_to_process = flags['max_urls_to_process']
        self._only_base_domain_urls = flags['only_include_base_domain_urls']

    # We start with the base_url as the base crawl point and go from there
    # Steps in the flow
    # a. Start from a base url
    # b. Fetch the url, update it in the Db
    # c. Get all the links from the page and insert all of them
    #        in the db (which do not already exist and matches the base domain)
    # d. Read the db and fetch more
    def run(self):
        logging.info("Total URLs in the DB: %d", self._db.get_total_seen())
        urls = [self._base_url]
        if not self._db.is_seen(self._base_url):
            self._db.add_seen_url(self._base_url)
        while len(
                urls) > 0 and self._urls_processed < self._max_urls_to_process:
            for u in urls:
                if self._urls_processed > self._max_urls_to_process:
                    logging.debug("Max number of URLs processed. Skipping.")
                    break
                self._process_url(u)
            # TODO: Should not fetch this high number of URLs.
            urls = self._get_seen_urls(100000)
            random.shuffle(urls)
        logging.info("Total URLs in the DB: %d", self._db.get_total_seen())

    def _process_url(self, url):
        url = self._crawler.canonicalize_url(url)
        logging.info("Processing url: %s", url)

        # TODO: This should check for the time when this was crawled eg. is_recently_crawled(url)
        if not self._should_crawl_url(url) or self._db.is_crawled(url):
            logging.debug("Url crawled or invalid. Skipping.")
            return 0

        if not self._crawler.fetch(url) or self._crawler.get_contents() == '':
            logging.error("Aborting. Could not fetch base url: %s", url)
            sys.exit(1)

        # Fetching a url is processing it.
        self._urls_processed += 1
        self._parser.set_data(self._crawler.get_contents())
        num_new = self._add_new_seen_urls()

        # Update the db with the contents of this url
        self._process_content(url)

        # If fails, it's not a critical error to stop processing
        if not self._db.add_crawled_url(url):
            logging.critical("Adding %s to crawled db failed: ", url)

        return num_new

    def _process_content(self, url):
        if self._db.is_content_fetched(url) is True:
            return
        heading = self._parser.find_element('h1', 'class', 'firstHeading')
        poem = self._parser.find_element('div', 'class', 'poem')
        if heading is None or poem is None:
            logging.debug("Partial content. Skipping adding %s", url)
            return
        heading = self._parser.sanitize_text(heading)
        poem = self._parser.sanitize_text(poem)
        headingHash = hashlib.md5(heading.encode()).hexdigest()
        poemHash = hashlib.md5(poem.encode()).hexdigest()
        self._db.add_fetched_content(url, heading, headingHash, poem, poemHash)
        self._content_fetched_urls += 1

    def _get_seen_urls(self, num_to_fetch=100):
        return self._db.read_from_seen(max_to_fetch=num_to_fetch)

    # Static set of rules for some urls which need not be crawled.
    @staticmethod
    def _should_crawl_url(url):
        # TODO: Make these comparisons case insensitive
        return url.count(":Random") == 0 and url.count(
            "&printable") == 0 and url.count("oldid") == 0 and url.count(
                "action=") == 0 and url.count(
                    "mobileaction") == 0 and url.count("returnto")

    def _add_new_seen_urls(self):
        a_tags = self._parser.find_all("a")
        logging.debug("All href links in the page %d", len(a_tags))
        num_new = 0
        for a_tag in a_tags:
            if a_tag.has_attr('href'):
                url = self._crawler.canonicalize_url(a_tag['href'])
                if self._should_crawl_url(url) and (
                        self._only_base_domain_urls is False
                        or self._crawler.is_from_base_domain(url)
                ) and not self._db.is_seen(url):
                    self._db.add_seen_url(url)
                    num_new += 1
        logging.debug("Number of new URLs found: %s", num_new)
        return num_new


# Sets the global logging config
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
        type=int,
        choices=[0, 1],
        default=1,
        required=False)
    parser.add_argument(
        "--reset_tables",
        help='Reset tables before starting run',
        type=int,
        default=0,
        choices=[0, 1],  # 0 = false, 1 = true
        required=False)
    args = parser.parse_args()
    flags = vars(args)
    if flags['reset_tables'] == 1:
        flags['reset_tables'] = True
    else:
        flags['reset_tables'] = False
    if flags['only_include_base_domain_urls'] == 1:
        flags['only_include_base_domain_urls'] = True
    else:
        flags['only_include_base_domain_urls'] = False
    return flags


def main():
    flags = ProcessArgs()
    print("Flags passed: ", flags)
    SetupLogger(flags)
    print("We are starting to crawl ", flags['base_domain'])
    print("We are using", flags['db_path'],
          " as the db to store our information.")
    db = UrlDb(flags['db_path'])
    if flags['reset_tables'] is True and db.reset_tables() is False:
        logging.critical("Could not reset tables for run. Quitting")
        sys.exit(1)

    driver = CrawlDriver(db, flags)
    driver.run()


if __name__ == "__main__":
    main()
