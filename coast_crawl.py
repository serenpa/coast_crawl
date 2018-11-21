"""
    Coast crawler

    this simple crawler starts with a list of seed pages and will continuously crawl pages linked to from these as long
    as the pages are within the same top level domain. It writes the output to a Mongo database.

    To use:
        import coast_crawler
        coast_crawler.crawl(domains, db_url, db_client)

    Where the domains variable is the list of top level domains you want to begin crawling (for example, when crawling a blog, you'd
    probably want to use the sites 'archive' page), the optional db_url variable is the url to your mongo instance
    (defaulted to mongodb://localhost:27017) and the optional db_client variable is the name of the database within
    mongo that you want to use (defaulted to 'crawler_results').

    Because state is persisted to a DB, any crash can be recovered from by simply just starting the server up again.

    WARNING: This crawler does not take into account the robots.txt file.

    Future work/additions:
        - currently if a 404 response is given, the crawler moves onto the next link and continues. This isn't ideal if,
        for example, the internet connection temporarily drops. Some handling of this would be nice.

"""
from urllib.parse import urlparse

import certifi
import urllib3
import urllib.robotparser
from bs4 import BeautifulSoup
from pymongo import MongoClient
import sys
from enum import Enum


class DB:
    def __init__(self, db_url, database):
        self.db_url = db_url
        self.database = database

        class TableNames(Enum):
            TO_CRAWL = "to_crawl"
            CRAWLED_LINKS = "crawled_links"
            PAGES = "pages"

        self.table_names = TableNames

        try:
            client = MongoClient(self.db_url)
            self.db = client[database]
        except Exception as e:
            print(e)
            sys.exit()

    def add_new_domains(self, domains):
        """
        adds domains to be crawled
        :param domains: list of domains to be crawled
        """
        for domain in domains:
            exists = self.db.domains.find_one({"domain": domain})
            if not exists:
                self.db.domains.insert_one({"domain": domain, "status": "PENDING"})

    def get_pending_domains(self):
        return self.db.domains.find({"status": "PENDING"})

    def add_url_to_crawl(self, url):
        return self.__add_url_to_table(self.table_names.TO_CRAWL, url)

    def add_url_to_crawled(self, url):
        return self.__add_url_to_table(self.table_names.CRAWLED_LINKS, url)

    def __add_url_to_table(self, table_name, url):
        return self.db.get_collection(table_name.value).insert_one({"url": url})

    def remove_url_to_crawl(self, url):
        return self.db.to_crawl.remove({"url": url})

    def to_crawl_count(self):
        return self.db.to_crawl.count()

    def any_left_to_crawl(self):
        return self.to_crawl_count() != 0

    def get_url_to_crawl(self):
        link = self.db.to_crawl.find()[0]
        return link['url']

    def url_exists_in_to_crawl(self, url):
        return self.__record_exists_in_table(self.table_names.TO_CRAWL, url)

    def url_exists_in_crawled(self, url):
        return self.__record_exists_in_table(self.table_names.CRAWLED_LINKS, url)

    def __record_exists_in_table(self, table_name, url):
        records = self.db.get_collection(table_name.value).find({"url": url})
        return records.count() != 0

    def add_page(self, url, html):
        page = {"url": url, "html": html}
        return self.db.get_collection(self.table_names.PAGES.value).insert_one(page)



def get_html(url):
    """
    Given a URL, will return the HTML using urllib3.
    :param url: The url to extract the HTML from
    :return: If extracted successfully, the HTML is returned. If there is a failure, a message with HTTP status. If an exception is thrown, -1 is returned witha  description of the error
    """

    try:
        # urllib3.disable_warnings()
        # Try with new where function, but sometimes it failes
        # so then try old where function
        # Read more: https://github.com/certifi/python-certifi#usage
        try:
            http = urllib3.PoolManager(
                cert_reqs='CERT_REQUIRED',
                ca_certs=certifi.where()
            )
        except:
            http = urllib3.PoolManager(
                cert_reqs='CERT_REQUIRED',
                ca_certs=certifi.old_where()
            )

        r = http.request('GET', url, timeout=5.0)

        if str(r.status).startswith("2"):
            html = r.data.decode("utf-8")
            return html
        else:
            return "Failed to get html, status: " + str(r.status)
    except Exception as e:
        sys.stdout.write(str(e))
        return "-1: " + str(e)


def get_all_links(url, seed_domain):
    """
        Get all links from the same domain and returns as a list

        :param url: The url which will be fetched and parsed
        :param seed_domain: original seed domain
    """
    html = get_html(url)
    soup = BeautifulSoup(html, "html5lib")
    all_urls = []

    try:
        for link in soup.find_all('a'):
            all_urls.append(link.get('href'))
    except Exception as e:
        print(e)

    result_urls = []

    for url in all_urls:
        domain = urlparse(url).netloc

        if domain == seed_domain:
            result_urls.append(url)

    return result_urls, html


def get_robots_for_url(url):
    """
    creates a robotfileparser object (https://docs.python.org/3/library/urllib.robotparser.html#urllib.robotparser.RobotFileParser) based on a url
    :param url: url to try get robots.txt for
    :return: urllib.robotparser.RobotFileParser
    """
    ROBOTS = "robots.txt"
    parsed_url = urlparse(url)
    scheme, netloc = parsed_url[:2]
    domain_with_robots = "{}://{}/{}".format(scheme, netloc, ROBOTS)

    rp = urllib.robotparser.RobotFileParser()

    print("Checking robots.txt for", domain_with_robots)

    rp.set_url(domain_with_robots)
    rp.read()
    return rp


def crawl_domain(domain, db):
    """
    crawls all the links on a specific domain
    :param domain: url of domain to be crawled
    :param db: instantiated db object (see DB class)
    :return:
    """
    network_location_part = urlparse(domain).netloc
    db.add_url_to_crawl(domain)

    rp = get_robots_for_url(domain)

    flag = True

    while flag:
        print()
        print("To crawl: " + str(db.db.to_crawl.count()))
        print("Crawled: " + str(db.db.crawled_links.count()))
        print()

        if db.any_left_to_crawl():
            url = db.get_url_to_crawl()
            db.remove_url_to_crawl(url)

            if rp.can_fetch("*", url):
                print("Crawling", url)

                all_links, html = get_all_links(url, network_location_part)

                for url in set(all_links):
                    if not db.url_exists_in_to_crawl(url) and not db.url_exists_in_crawled(url):
                        print("\t\tadding url to crawl", url)
                        db.add_url_to_crawl(url)

                url_exists_in_crawled = db.url_exists_in_crawled(url)
                if not url_exists_in_crawled:
                    db.add_url_to_crawled(url)
                    db.add_page(url, html)

            else:
                print("Cannot crawl", url, "blocked by robots.txt")
                db.db.cannot_crawl.insert_one({"url": url, "reason": "blocked by robots.txt"})

        else:
            flag = False

    print("Finished Crawling domain", domain)


def crawl(domains, db_url="mongodb://localhost:27017", database="crawler_results_3"):
    """
    Craws a list of domains by searching for links on each page
    :param domains: list of domains to be crawled
    :param db_url: url to connect to mongodb
    :param database: database to connect to (eg crawler results)
    :return: none
    """

    print("Staring to crawl for domains", domains)
    db = DB(db_url, database)

    db.add_new_domains(domains)

    while True:
        pending_domains = db.get_pending_domains()

        if pending_domains.count() == 0:
            print("No uncrawled domains left")
            break
        else:
            domain_to_crawl = pending_domains[0]
            print("crawling..." + domain_to_crawl["domain"])
            crawl_domain(domain_to_crawl["domain"], db)
            post = {"domain": domain_to_crawl["domain"], "status": "CRAWLED"}
            db.db.domains.update_one({"domain": domain_to_crawl["domain"]}, {"$set": post}, upsert=False)
