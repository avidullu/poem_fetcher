import sqlite3
import logging
import datetime


class UrlDb:
    _conn = None
    _recorded_urls_table = "seen_urls"

    def __init__(self, db_path):
        try:
            # PARSE_DECLTYPES for parsing dates as python format
            self._conn = sqlite3.connect(db_path,
                                         detect_types=sqlite3.PARSE_DECLTYPES)
        except sqlite3.OperationalError as e:
            print(e)
            sys.exit("DB connection error. Aborting")

    def print_tables(self):
        c = self._conn.cursor()
        try:
            c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        except sqlite3.OperationalError as e:
            print(e)
            sys.exit("Reading from DB failed. Aborting")
        tables = c.fetchall()
        [print(table) for table in tables]

    def add_url(self, url, seen_time=None, crawl_time=None):
        curr = self._conn.cursor()
        # seen_time can be the same as add time but the crawl time need to be provided by caller
        if seen_time is None:
            seen_time = datetime.datetime.now().isoformat()
        logging.debug("Inserting url: %s %s", url, seen_time)
        try:
            curr.execute("insert into seen_urls values(?, ?, ?);",
                         (url, seen_time, crawl_time))
            self._conn.commit()
        except sqlite3.OperationalError as e:
            print(e)
            print("Writing to DB failed.")
            return False
        return True

    def remove_url(self, url):
        curr = self._conn.cursor()
        logging.debug("Removing url: %s", url)
        try:
            curr.execute("delete from seen_urls where url = (?);", (url, ))
            self._conn.commit()
        except sqlite3.OperationalError as e:
            print(e)
            print("Removing from DB failed.")
            return False
        return True

    def exists(self, url):
        assert len(url) > 0
        curr = self._conn.cursor()
        logging.debug("Checking existence of url: %s", url)
        try:
            curr.execute("select url from seen_urls where url = (?);", (url, ))
        except sqlite3.InterfaceError as e:
            print(e)
            print("Checking url in DB failed: ", url)
            return False
        return len(curr.fetchall()) > 0

    def read(self, max_url_time=None, max_to_fetch=100):
        if max_url_time is None:
            max_url_time = datetime.datetime.now().isoformat()
        curr = self._conn.cursor()
        logging.info("Reading urls which were crawled before: %s",
                     max_url_time)
        ret_val = []
        try:
            curr.execute(
                "select url from seen_urls where seen_time < (?) limit (?);", (
                    max_url_time,
                    max_to_fetch,
                ))
        except sqlite3.OperationalError as e:
            print(e)
            print("Fetching from URL DB failed.")
            return ret_val
        all_fetched = curr.fetchall()
        logging.info("All items read size: %d", len(all_fetched))
        while len(all_fetched) > 0:
            ret_val.append(all_fetched.pop()[0])
        return ret_val

    def get_total(self):
        curr = self._conn.cursor()
        logging.debug("Checking total number of urls in the DB")
        try:
            curr.execute("select count(*) from seen_urls;")
        except sqlite3.OperationalError as e:
            print(e)
            print("Checking url in DB failed.")
            return -1
        return curr.fetchone()[0]
