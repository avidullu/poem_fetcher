import sqlite3
import logging
import datetime


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

    def exists(self, url):
        curr = self.conn.cursor()
        logging.debug("Checking existence of url.", url, type(url))
        try:
          curr.execute("select count(*) from seen_urls where url = (?);", (url,))
        except sqlite3.OperationalError as e:
            print(e)
            print("Checking url in DB failed.")
        return curr.fetchone()[0] > 0
