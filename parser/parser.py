from bs4 import BeautifulSoup
import logging

# Use BeautifulSoup to parse and extract information from a fetched page
class Parser:
    _soup = None
    def __init__(self, data=None):
        if data != None:
            self._soup = BeautifulSoup(data)
        else:
            logging.info("Parser not initialized with data")
    # Consider returning a generator
    def find_all(self, tag):
        if self._soup is not None:
            return self._soup.find_all(tag)
        else:
            logging.error("Soup is not initialized with data.")
    def set_data(self, data, format="html.parser"):
        self._soup = BeautifulSoup(data, format)
