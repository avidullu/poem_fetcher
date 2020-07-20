import datetime
import logging
from urllib.parse import urlparse, ParseResult

import urllib3


class UrlCrawler:
    _pool = None
    _base = None
    _url = None
    _crawl_time = None
    _contents = None
    _is_redirect = False

    def __init__(self, base_domain):
        self._pool = urllib3.PoolManager(10)
        self._base = urlparse(base_domain, allow_fragments=False)
        logging.info("Netloc of base: %s", self._base.netloc)

    def fetch(self, url):
        self._reset(url)
        if self._url is not False:
            self._crawl_time = datetime.datetime.now().isoformat()
            resp = self._pool.request('GET', self._url)
            self._contents = resp.data
            self._is_redirect = resp.geturl() != self._url
            if self._is_redirect is True:
                logging.debug("Redirect found: %s", self._url)
        return self._contents is not False

    def get_contents(self):
        return self._contents

    def get_crawl_time(self):
        return self._crawl_time

    def canonicalize_url(self, url):
        parsed = urlparse(url, allow_fragments=False)
        if len(parsed.netloc) > 0:
            logging.debug("Nothing to do for: %s", url)
            return url
        new_parsed = ParseResult(self._base.scheme,
                                 self._base.netloc,
                                 parsed.path,
                                 parsed.params,
                                 parsed.query,
                                 fragment='')  # Empty fragment
        logging.debug("New formed url: %s", new_parsed.geturl())
        return new_parsed.geturl()

    def is_from_base_domain(self, url):
        parsed = urlparse(url, allow_fragments=False)
        return len(parsed.netloc) == 0 or parsed.netloc == self._base.netloc

    def _reset(self, url):
        self._crawl_time = False
        self._contents = False
        self._url = self.canonicalize_url(url)
        self._is_redirect = False
