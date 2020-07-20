# The database can have some issues and this tool can be used to sanitize the
# db without having to repopulate it from scratch.

import argparse
import hashlib
# This is ugly because of python packaging
import sys
sys.path.append("../")

from db.url_db import UrlDb
from parser.parser import Parser

db_path = 'kavita_kosh2.db'


# TODO: Implement this and integrate with the main pipeline.
def dedup_db():
    pass


def sanitize_and_repopulate_fetched_contents():
    db = UrlDb(db_path)
    parser = Parser()
    total_fetched_count = db.get_total_fetched()
    print("Total urls in fetched content: ", total_fetched_count)
    all_urls = db.read_from_fetched(total_fetched_count)
    num_fixed = 0
    for url in all_urls:
        contents = db.read_one_content_fetched(url)
        heading, poem = parser.sanitize_text(
            contents[0][1]), parser.sanitize_text(contents[0][2])
        prevHH, prevPH = contents[0][3], contents[0][4]
        assert len(heading) > 0 and len(poem) > 0
        # TODO: Make this a library and use in both code and here.
        headingHash = hashlib.md5(heading.encode()).hexdigest()
        poemHash = hashlib.md5(poem.encode()).hexdigest()
        if prevHH != headingHash or prevPH != poemHash:
            if db.remove_from_fetched(url):
                if db.add_fetched_content(url, heading, headingHash, poem, poemHash):
                    num_fixed += 1
    print("Number of entries fixed: ", num_fixed)


def ProcessArgs():
    parser = argparse.ArgumentParser(description='Tool to fix crawled db')
    parser.add_argument(
        '--sanitize_and_repopulate',
        help='Sanitizes the URLs in fetched_content and rehashes with md5',
        type=bool,
        default=False)
    parser.add_argument(
        '--dedup_db',
        help=
        'Removes duplicate rows from fetched_content table by matching the hashes',
        type=bool,
        default=False)
    return vars(parser.parse_args())


def main():
    flags = ProcessArgs()
    if flags['sanitize_and_repopulate']:
        sanitize_and_repopulate_fetched_contents()
    if flags['dedup_db']:
        dedup_db()


if __name__ == "__main__":
    main()
