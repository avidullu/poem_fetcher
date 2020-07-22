import datetime
import logging
import sqlite3
import sys


class UrlDb:
    _conn = None

    def __init__(self, db_path):
        try:
            # PARSE_DECLTYPES for parsing dates as python format
            self._conn = sqlite3.connect(db_path,
                                         detect_types=sqlite3.PARSE_DECLTYPES)
        except sqlite3.OperationalError as e:
            logging.critical("Initializing DB module failed: %s", e)
            sys.exit("DB connection error. Aborting")

    def add_seen_url(self, url, seen_time=None, crawl_time=None):
        curr = self._conn.cursor()
        # seen_time can be the same as add time but the crawl time need to be provided by caller
        if seen_time is None:
            seen_time = datetime.datetime.now().isoformat()
        logging.debug("Inserting seen url: %s %s", url, seen_time)
        try:
            curr.execute("insert into seen_urls values(?, ?, ?);",
                         (url, seen_time, crawl_time))
            self._conn.commit()
        except sqlite3.OperationalError as e:
            logging.critical("Writing to DB failed %s", e)
            return False
        return True

    def add_crawled_url(self, url, seen_time=None, crawl_time=None):
        curr = self._conn.cursor()
        if crawl_time is None:
            crawl_time = datetime.datetime.now().isoformat()
        logging.debug("Inserting crawled url: %s %s", url, crawl_time)
        try:
            curr.execute("insert into crawled_urls values(?, ?, ?);",
                         (url, seen_time, crawl_time))
            self._conn.commit()
        except sqlite3.OperationalError as e:
            logging.critical("Writing to crawled DB failed with: %s", e)
            return False
        return True

    def add_fetched_content(self, url, heading, headingHash, poem, poemHash):
        curr = self._conn.cursor()
        if poem is None or heading is None:
            logging.debug("Both of heading or poem should be available: %s",
                          url)
            return False
        logging.debug("Inserting fetched content url: %s", url)
        try:
            curr.execute("insert into fetched_content values(?, ?, ?, ?, ?);",
                         (url, heading, poem, headingHash, poemHash))
            self._conn.commit()
        except sqlite3.OperationalError as e:
            logging.critical("Writing to content DB failed with: %s", e)
            return False
        return True

    def remove_from_seen(self, url):
        curr = self._conn.cursor()
        logging.debug("Removing url: %s", url)
        try:
            curr.execute("delete from seen_urls where url = (?);", (url, ))
            self._conn.commit()
        except sqlite3.OperationalError as e:
            logging.critical("Removing from seen DB failed with: %s", e)
            return False
        return True

    def remove_from_crawled(self, url):
        curr = self._conn.cursor()
        logging.debug("Removing url: %s", url)
        try:
            curr.execute("delete from crawled_urls where url = (?);", (url, ))
            self._conn.commit()
        except sqlite3.OperationalError as e:
            logging.critical("Removing from crawled DB failed with: %s", e)
            return False
        return True

    def remove_from_fetched(self, url):
        curr = self._conn.cursor()
        logging.debug("Removing url: %s", url)
        try:
            curr.execute("delete from fetched_content where url = (?);",
                         (url, ))
            self._conn.commit()
        except sqlite3.OperationalError as e:
            logging.critical("Removing from fetched DB failed with: %s", e)
            return False
        return True

    def is_seen(self, url):
        assert len(url) > 0
        curr = self._conn.cursor()
        logging.debug("Checking existence of url: %s", url)
        try:
            curr.execute("select url from seen_urls where url = (?);", (url, ))
        except sqlite3.InterfaceError as e:
            logging.critical("Checking %s in seen DB failed: %s ", url, e)
            return False
        return len(curr.fetchall()) > 0

    def is_crawled(self, url):
        assert len(url) > 0
        curr = self._conn.cursor()
        logging.debug("Checking for crawled url: %s", url)
        try:
            curr.execute("select url from crawled_urls where url = (?);",
                         (url, ))
        except sqlite3.InterfaceError as e:
            logging.critical("Checking %s in crawled DB failed: %s", url, e)
            return False
        return len(curr.fetchall()) > 0

    def is_content_fetched(self, url):
        assert len(url) > 0
        curr = self._conn.cursor()
        logging.debug("Checking existence of url: %s", url)
        try:
            curr.execute("select url from fetched_content where url = (?);",
                         (url, ))
        except sqlite3.InterfaceError as e:
            logging.critical("Checking %s in fetched content DB failed: %s ",
                             url, e)
            return False
        return len(curr.fetchall()) > 0

    def read_fetched_content(self, url, max_to_read=1):
        assert len(url) > 0
        curr = self._conn.cursor()
        logging.debug("Reading of url: %s", url)
        try:
            curr.execute(
                "select * from fetched_content where url = (?) limit (?);",
                (url, max_to_read))
        except sqlite3.InterfaceError as e:
            logging.critical("Reading %s in fetched DB failed: %s ", url, e)
            return None
        return curr.fetchall()

    def read_seen_url(self, url, max_to_read=1):
        assert len(url) > 0
        curr = self._conn.cursor()
        logging.debug("Reading of url: %s", url)
        try:
            curr.execute("select * from seen_urls where url = (?) limit (?);",
                         (url, max_to_read))
        except sqlite3.InterfaceError as e:
            logging.critical("Reading %s in seen DB failed: %s ", url, e)
            return None
        return curr.fetchall()

    def read_crawled_url(self, url, max_to_read=1):
        assert len(url) > 0
        curr = self._conn.cursor()
        logging.debug("Reading of url: %s", url)
        try:
            curr.execute(
                "select * from crawled_urls where url = (?) limit (?);",
                (url, max_to_read))
        except sqlite3.InterfaceError as e:
            logging.critical("Reading %s in crawled DB failed: %s ", url, e)
            return None
        return curr.fetchall()

    def get_matching_content(self, poemHash):
        assert poemHash is not None
        curr = self._conn.cursor()
        logging.debug("Getting matching content for: %s", poemHash)
        try:
            curr.execute(
                "select url from fetched_content where poemHash = (?);",
                (poemHash, ))
        except sqlite3.OperationalError as e:
            logging.critical("Failed getting content from the table: %s", e)
            return None
        all_fetched = curr.fetchall()
        logging.info("All items with same poemHash size: %d", len(all_fetched))
        ret_val = []
        while len(all_fetched) > 0:
            ret_val.append(all_fetched.pop()[0])
        return ret_val

    def read_from_seen(self, max_url_time=None, max_to_fetch=100):
        if max_url_time is None:
            max_url_time = datetime.datetime.now().isoformat()
        curr = self._conn.cursor()
        logging.info("Reading urls which were crawled before: %s",
                     max_url_time)
        try:
            curr.execute(
                "select url, seen_time from seen_urls where seen_time < (?) order by 2 desc limit (?);",
                (
                    max_url_time,
                    max_to_fetch,
                ))
        except sqlite3.OperationalError as e:
            logging.critical("Fetching from URL DB failed %s", e)
            return None
        all_fetched = curr.fetchall()
        logging.info("All items read_from_seen size: %d", len(all_fetched))
        ret_val = []
        while len(all_fetched) > 0:
            ret_val.append(all_fetched.pop()[0])
        return ret_val

    def read_from_crawled(self, max_url_time=None, max_to_fetch=100):
        if max_url_time is None:
            max_url_time = datetime.datetime.now().isoformat()
        curr = self._conn.cursor()
        logging.info("Reading urls which were crawled before: %s",
                     max_url_time)
        try:
            curr.execute(
                "select url, crawl_time from crawled_urls where crawl_time < (?) order by 2 desc limit (?);",
                (
                    max_url_time,
                    max_to_fetch,
                ))
        except sqlite3.OperationalError as e:
            logging.critical("Fetching from crawled URL DB failed %s", e)
            return None
        all_fetched = curr.fetchall()
        logging.info("All items read_from_crawled size: %d", len(all_fetched))
        ret_val = []
        while len(all_fetched) > 0:
            ret_val.append(all_fetched.pop()[0])
        return ret_val

    def read_from_fetched(self, max_to_fetch=100):
        curr = self._conn.cursor()
        logging.info("Reading %d urls from fetched content table",
                     max_to_fetch)
        try:
            curr.execute("select url from fetched_content limit (?);",
                         (max_to_fetch, ))
        except sqlite3.OperationalError as e:
            logging.critical("Fetching from fetched content URL DB failed %s",
                             e)
            return None
        all_fetched = curr.fetchall()
        logging.info("All items from fetched size: %d", len(all_fetched))
        ret_val = []
        while len(all_fetched) > 0:
            ret_val.append(all_fetched.pop()[0])
        return ret_val

    def get_total_seen(self):
        curr = self._conn.cursor()
        logging.debug("Checking total number of urls in the seen_urls DB")
        try:
            curr.execute("select count(*) from seen_urls;")
        except sqlite3.OperationalError as e:
            logging.critical("Checking total urls in DB failed: %s", e)
            return -1
        return curr.fetchone()[0]

    def get_total_crawled(self):
        curr = self._conn.cursor()
        logging.debug("Checking total number of urls in the crawled DB")
        try:
            curr.execute("select count(*) from crawled_urls;")
        except sqlite3.OperationalError as e:
            logging.critical("Checking total urls in DB failed: %s", e)
            return -1
        return curr.fetchone()[0]

    def get_total_fetched(self):
        curr = self._conn.cursor()
        logging.debug(
            "Checking total number of urls in the fetched contents DB")
        try:
            curr.execute("select count(*) from fetched_content;")
        except sqlite3.OperationalError as e:
            logging.critical("Checking total urls in DB failed: %s", e)
            return -1
        return curr.fetchone()[0]

    def get_tables(self):
        c = self._conn.cursor()
        try:
            c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        except sqlite3.OperationalError as e:
            print(e)
            sys.exit("Reading from DB failed. Aborting")
        tables = c.fetchall()
        [print(table) for table in tables]
        return tables

    def reset_tables(self):
        c = self._conn.cursor()
        try:
            c.execute("drop table seen_urls;")
            c.execute("drop table crawled_urls;")
            c.execute("drop table fetched_content;")
        except sqlite3.OperationalError as e:
            logging.error("Error while dropping tables. Continuing")
            pass
        try:
            c.execute(
                "create table seen_urls(url text, seen_time datetime, crawl_time datetime);"
            )
            c.execute(
                "create table crawled_urls(url text, seen_time datetime, crawl_time datetime);"
            )
            c.execute(
                "create table fetched_content(url text, heading text, poem text, headingHash text, poemHash text);"
            )
        except sqlite3.OperationalError as e:
            logging.critical("Resetting the tables failed: %s", e)
            return False
        logging.info("Successfully reset the tables")
        return True
