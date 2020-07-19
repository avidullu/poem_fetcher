import logging

from bs4 import BeautifulSoup


# Use BeautifulSoup to parse and extract information from a fetched page
class Parser:
    _soup = None

    def __init__(self, data=None):
        if data is not None:
            self._soup = BeautifulSoup(data)
        else:
            logging.info("Parser not initialized with data")

    # Consider returning a generator
    def find_all(self, tag):
        if self._soup is not None:
            return self._soup.find_all(tag)
        else:
            logging.error("Soup is not initialized with data.")

    # For eg. in kavitakosh this is 'div, 'class', 'poem' for the poem content and 'h1', 'class', 'firstHeading'
    def find_element(self, elem, attr, prop):
        elements = self._soup.find_all(elem)
        for elm in elements:
            if elm is not None and elm.has_attr(
                    attr) and elm.get_attribute_list(attr).count(prop) > 0:
                logging.info("Found %s in div of attr %s, value %s", prop,
                             attr, elm.getText())
                return elm.getText()
        return None

    def set_data(self, data, format="html.parser"):
        self._soup = BeautifulSoup(data, format)
