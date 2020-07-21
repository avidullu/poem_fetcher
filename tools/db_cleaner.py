# The database can have some issues and this tool can be used to sanitize the
# db without having to repopulate it from scratch.
# NOTE: The tools here are specific to kavitakosh.org

# export PYTHONPATH=/Users/avidullu/workspaces/PyCharm/poem_fetcher/

import argparse
import hashlib
import logging
# This is ugly because of python packaging
import sys
from urllib.parse import unquote

sys.path.append("../")

from db.url_db import UrlDb
from parser.parser import Parser


# TODO: Pull this into a common library and share
def should_include_url(url):
    # TODO: Make these comparisons case insensitive
    return url.count(":Random") == 0 and url.count(
        "&printable") == 0 and url.count("oldid") == 0 and url.count(
            "action=") == 0 and url.count("mobileaction") == 0 and url.count(
                "returnto")


# TODO: Implement this and integrate with the main pipeline.
def dedup_db(flags, db):
    # For each row in the table, select all rows which have the same poemHash
    # remove all of them and insert the one with the shortest heading length
    total_fetched_count = db.get_total_fetched()
    print("Total urls in fetched content: ", total_fetched_count)
    all_urls = db.read_from_fetched(total_fetched_count)
    num_correct, num_fixed = 0, 0
    for url in all_urls:
        contents = db.read_fetched_content(url, max_to_read=1)
        # record the entire row here
        nurl, heading, poem, hH, pH = contents[0][0], contents[0][1], contents[
            0][2], contents[0][3], contents[0][4]
        all_matching_urls = db.get_matching_content(poemHash=pH)
        if len(all_matching_urls) == 1:
            num_correct += 1
            continue
        for murl in all_matching_urls:
            mcontents = db.read_fetched_content(murl, 1)
            assert poem == mcontents[0][2]
            assert murl == mcontents[0][0]
            # Pick the heading which is shortest.
            if len(mcontents[0][1]) < len(heading):
                heading, nurl, hH = mcontents[0][1], murl, mcontents[0][3]
            if not flags['dry_run']:
                db.remove_from_fetched(murl)
        if not flags['dry_run']:
            db.add_from_fetched(nurl, heading, hH, poem, pH)
        print("Adding: ", nurl, "     ", heading)
        num_fixed += 1
    print("Num correct: ", num_correct, "  fixed: ", num_fixed)


def sanitize_and_repopulate_fetched_contents(flags, db, parser):
    total_fetched_count = db.get_total_fetched()
    print("Total urls in fetched content: ", total_fetched_count)
    all_urls = db.read_from_fetched(total_fetched_count)
    num_fixed, num_removed = 0, 0
    for url in all_urls:
        newUrl = unquote(url)
        if should_include_url(newUrl) is False or should_include_url(
                url) is False:
            logging.debug("Removing url: %s", url)
            num_removed += 1
            if not flags['dry_run']:
                db.remove_from_fetched(url)
            continue
        contents = db.read_fetched_content(url)
        heading, poem = parser.sanitize_text(
            contents[0][1]), parser.sanitize_text(contents[0][2])
        prevHH, prevPH = contents[0][3], contents[0][4]
        assert len(heading) > 0 and len(poem) > 0
        # TODO: Make this a library and use in both code and here.
        headingHash = hashlib.md5(heading.encode()).hexdigest()
        poemHash = hashlib.md5(poem.encode()).hexdigest()
        if prevHH != headingHash or prevPH != poemHash or url != newUrl:
            if not flags['dry_run']:
                db.remove_from_fetched(url)
                db.remove_from_fetched(newUrl)
                db.add_fetched_content(newUrl, heading, headingHash, poem,
                                       poemHash)
            num_fixed += 1
    print("Number of entries fixed: ", num_fixed, ", and removed: ",
          num_removed)


def sanitize_and_repopulate_seen_urls(flags, db, parser):
    total_seen_count = db.get_total_seen()
    print("Total urls in seen_urls: ", total_seen_count)
    all_urls = db.read_from_seen(max_to_fetch=total_seen_count)
    num_fixed, num_removed = 0, 0
    for url in all_urls:
        newUrl = unquote(url)
        if should_include_url(newUrl) is False or should_include_url(
                url) is False:
            logging.debug("Removing url: %s", url)
            num_removed += 1
            if not flags['dry_run']:
                db.remove_from_seen(url)
        elif url != newUrl:
            if not flags['dry_run']:
                contents = db.read_seen_url(url)
                db.remove_from_seen(url)
                db.remove_from_seen(newUrl)
                db.add_seen_url(newUrl, contents[0][1], contents[0][2])
            num_fixed += 1
    print("Number of entries fixed: ", num_fixed, ", and removed: ",
          num_removed)


def sanitize_and_repopulate_crawled_urls(flags, db, parser):
    total_crawled_count = db.get_total_crawled()
    print("Total urls in crawled_urls: ", total_crawled_count)
    all_urls = db.read_from_crawled(max_to_fetch=total_crawled_count)
    num_fixed, num_removed = 0, 0
    for url in all_urls:
        newUrl = unquote(url)
        if should_include_url(newUrl) is False or should_include_url(
                url) is False:
            logging.debug("Removing url: %s", url)
            num_removed += 1
            if not flags['dry_run']:
                db.remove_from_crawled(url)
        elif url != newUrl:
            if not flags['dry_run']:
                contents = db.read_crawled_url(url)
                db.remove_from_crawled(url)
                db.remove_from_crawled(newUrl)
                db.add_crawled_url(newUrl, contents[0][1], contents[0][2])
            num_fixed += 1
    print("Number of entries fixed: ", num_fixed, ", and removed: ",
          num_removed)


# Sets the global logging config
def SetupLogger(flags):
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s '
                        '[%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=flags['log'])


def ProcessArgs():
    parser = argparse.ArgumentParser(description='Tool to fix crawled db')
    db_list = ["", "seen_urls", "fetched_content", "crawled_urls"]
    parser.add_argument(
        '--sanitize_and_repopulate',
        help='Sanitizes the URLs in fetched_content and rehashes with md5',
        type=str,
        default="",
        choices=db_list)
    parser.add_argument('--dry_run',
                        help='Does not mutate the DB',
                        type=int,
                        default=1,
                        choices=[0,
                                 1])  # 0 = --dry_run=false, 1 = --dry_run=true
    parser.add_argument(
        '--dedup_db',
        help=
        'Removes duplicate rows from fetched_content table by matching the hashes',
        type=str,
        default="",
        choices=db_list)
    parser.add_argument('--log',
                        help='logging level',
                        type=str,
                        default="WARNING")  # NOTSET
    parser.add_argument('--db_path',
                        help='Path to database',
                        type=str,
                        default="kavita_kosh2.db")
    args = parser.parse_args()
    flags = vars(args)
    if flags['dry_run'] == 0:
        flags['dry_run'] = False
    else:
        flags['dry_run'] = True
    return flags


def main():
    flags = ProcessArgs()
    print(flags)
    if flags['dry_run']:
        print("Dry run mode. No mutations.")
    SetupLogger(flags)
    db = UrlDb(flags['db_path'])
    parser = Parser()
    if flags['sanitize_and_repopulate'] == "fetched_content":
        sanitize_and_repopulate_fetched_contents(flags, db, parser)
    if flags['sanitize_and_repopulate'] == "seen_urls":
        sanitize_and_repopulate_seen_urls(flags, db, parser)
    if flags['sanitize_and_repopulate'] == "crawled_urls":
        sanitize_and_repopulate_crawled_urls(flags, db, parser)
    if flags['dedup_db'] == "fetched_content":
        dedup_db(flags, db)


if __name__ == "__main__":
    main()
