import datetime
import logging
from urllib.parse import urlparse, ParseResult, unquote

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
        assert len(self._base.netloc) > 0

    def fetch(self, url):
        assert url is not None and len(url) > 0, url
        self._reset(url)
        self._crawl_time = datetime.datetime.now().isoformat()
        resp = self._pool.request('GET', self._url, timeout=5.0)
        status_code = resp.status
        if status_code != 200 and status_code != 302:
            logging.debug("Fetching %s returned %d", self._url, status_code)
            return False
        self._contents = resp.data
        if status_code == 302 and resp.geturl(
        ) is not None and resp.geturl() != self._url:
            self._is_redirect = True
            logging.debug("Redirect found for : %s  at %s", self._url,
                          resp.geturl())
            self._url = resp.geturl()
        return True

    def get_contents(self):
        return self._contents

    def get_crawl_time(self):
        return self._crawl_time

    def canonicalize_url(self, url):
        assert url != None and len(url) > 0
        parsed = urlparse(url, allow_fragments=False)
        new_parsed = ParseResult(self._base.scheme,
                                 self._base.netloc,
                                 parsed.path,
                                 parsed.params,
                                 parsed.query,
                                 fragment='')  # Empty fragment
        new_url = self.sanitize_url(unquote(new_parsed.geturl()))
        logging.debug("New formed url: %s", new_url)
        return new_url

    def is_from_base_domain(self, url):
        parsed = urlparse(url, allow_fragments=False)
        return len(parsed.netloc) == 0 or parsed.netloc == self._base.netloc

    def is_redirect(self):
        return self._is_redirect

    def get_fetched_url(self):
        return self._url

    @staticmethod
    def sanitize_url(url):
        return url.strip('/')

    def _reset(self, url):
        self._crawl_time = False
        self._contents = False
        self._url = self.canonicalize_url(url)
        self._is_redirect = False
