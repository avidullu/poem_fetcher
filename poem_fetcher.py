import argparse
import concurrent.futures
import hashlib
import logging
import random

from crawler.crawler import UrlCrawler
from db.url_db import UrlDb
from url_parser.url_parser import UrlParser

class CrawlDriver:
    _db = None
    _base_url = None
    _parser = None
    _crawler = None
    _max_urls_to_process = 0
    _urls_processed = 0
    _content_fetched_urls = 0
    _only_base_domain_urls = False
    _dropped_urls = 0
    _total_visited = 0
    _no_contents = 0

    def __init__(self, flags):
        self._db = UrlDb(flags['db_path'])
        self._crawler = UrlCrawler(flags['base_domain'])
        self._parser = UrlParser()
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
        num_new = 0
        while len(
                urls) > 0 and self._urls_processed < self._max_urls_to_process:
            for u in urls:
                if self._urls_processed > self._max_urls_to_process:
                    logging.debug("Max number of URLs processed. Skipping.")
                    break
                num_new += self._process_url(u)
            urls = self._get_seen_urls(100)
            random.shuffle(urls)
        logging.info("Total URLs in the DB: %d", self._db.get_total_seen())
        print("Total visited: ", self._total_visited, ", num new urls found: ",
              num_new, "  num fetched: ", self._content_fetched_urls,
              ", no contents: ", self._no_contents)

    def _process_url(self, url):
        self._total_visited += 1
        url = self._crawler.canonicalize_url(url)
        logging.info("Processing url: %s", url)

        # TODO: This should check for the time when this was crawled eg. is_recently_crawled(url)
        if not self._should_crawl_url(url) or self._db.is_crawled(
                url) or self._db.is_forbidden(url):
            logging.debug("Url crawled or invalid. Skipping.")
            return 0

        if not self._crawler.fetch(url) or self._crawler.get_contents() == '':
            logging.info("Could not fetch base url: %s", url)
            if self._db.remove_from_seen(url):
                self._dropped_urls += 1
                self._db.add_forbidden_url(url)
            return 0

        # If this was a redirect, then set to the actual url and add this to the seen table, if eligible
        if self._crawler.is_redirect():
            url = self._crawler.canonicalize_url(
                self._crawler.get_fetched_url())
            if self._only_base_domain_urls is False or self._crawler.is_from_base_domain(
                    url):
                if not self._db.is_seen(url):
                    self._db.add_seen_url(url)
            else:
                logging.debug("Skipping a non domain redirect url: ", url)
                return 0

        self._parser.set_data(self._crawler.get_contents())
        # If fails, it's not a critical error to stop processing
        if not self._db.add_crawled_url(url):
            logging.critical("Adding %s to crawled db failed: ", url)

        # Update the db with the contents of this url.
        # If the URL is empty contents then skip it and add to forbidden
        if self._process_content(url) is None:
            logging.debug("Page is empty. Skipping collecting links.")
            self._db.add_forbidden_url(url)
            return 0

        # Counting a url which wasn't empty as processing it.
        self._urls_processed += 1
        return self._add_new_seen_urls()

    def _process_content(self, url):
        assert not self._db.is_content_fetched(url), url
        no_content = self._parser.find_element('div', 'class', 'noarticletext')
        if no_content is not None:
            self._no_contents += 1
            return None
        heading = self._parser.find_element('h1', 'class', 'firstHeading')
        poem = self._parser.find_element('div', 'class', 'poem')
        if heading is None or poem is None or len(heading) == 0 or len(
                poem) == 0:
            logging.debug("Partial content. Skipping adding %s", url)
            return False
        heading = self._parser.sanitize_text(heading)
        poem = self._parser.sanitize_text(poem)
        headingHash = hashlib.md5(heading.encode()).hexdigest()
        poemHash = hashlib.md5(poem.encode()).hexdigest()
        self._db.add_fetched_content(url, heading, headingHash, poem, poemHash)
        self._content_fetched_urls += 1
        return True

    def _get_seen_urls(self, num_to_fetch=10):
        return self._db.read_from_seen(max_to_fetch=num_to_fetch,
                                       order="random")

    # Static set of rules for some urls which need not be crawled.
    @staticmethod
    def _should_crawl_url(url):
        # TODO: Make these comparisons case insensitive
        return url.count(":Random") == 0 and url.count(
            "MobileEditor"
        ) == 0 and url.count("&printable") == 0 and url.count(
            "oldid") == 0 and url.count("&search=") == 0 and url.count(
                "&limit=") == 0 and url.count("action=") == 0 and url.count(
                    "mobileaction"
                ) == 0 and url.count("returnto") == 0 and url.count(
                    "RecentChangesLinked"
                ) == 0 and url.count("otherapps") == 0 and url.count(
                    "hidelinks"
                ) == 0 and url.count("hideredirs") == 0 and not url.startswith(
                    "http://kavitakosh.org/share") and not url.startswith(
                        "http://kavitakosh.org/kk/images")

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
                ) and not self._db.is_seen(url) and not self._db.is_forbidden(
                        url):
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
    parser.add_argument('--num_threads',
                        help='Number of threads',
                        type=int,
                        default=1)
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


def MakeAndCallDriver(flags):
    driver = CrawlDriver(flags)
    driver.run()


def main():
    flags = ProcessArgs()
    print("Flags passed: ", flags)
    SetupLogger(flags)
    print("We are starting to crawl ", flags['base_domain'])
    print("We are using", flags['db_path'],
          " as the db to store our information.")
    if flags['reset_tables'] is True:
        db = UrlDb(flags['db_path'])
        assert db.reset_tables()

    flags['max_urls_to_process'] = flags['max_urls_to_process'] / flags[
        'num_threads'] + 1
    executor = concurrent.futures.ThreadPoolExecutor()
    results = []
    for t in range(flags['num_threads']):
        results.append(executor.submit(MakeAndCallDriver, flags))
        print("Started ", t, "th thread.")
    print("Waiting for completion")
    executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
