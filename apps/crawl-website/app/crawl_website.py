import argparse
import os
import logging
from typing import List, Optional
from datetime import datetime, timezone
from uuid import uuid4
from urllib.parse import urlparse
import email.utils
from dataclasses import dataclass
import json

import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import Crawler, CrawlerProcess
from scrapy.http import HtmlResponse
from scrapy.exceptions import IgnoreRequest
from scrapy.item import Item
from scrapy import signals

from aperturedb.CommonLibrary import create_connector, execute_query

stats = None
error_urls = None


class ContentTypeFilterMiddleware:
    """Middleware to filter out responses based on content type."""

    def __init__(self, allowed_types):
        """Initialize middleware with allowed content types."""
        self.allowed_types = set(allowed_types)

    @classmethod
    def from_crawler(class_, crawler):
        """Load allowed types from Scrapy settings."""
        allowed_types = crawler.settings.getlist("ALLOWED_CONTENT_TYPES",
                                                 default=["text/plain", "text/html", "application/pdf"])
        return class_(allowed_types)

    def process_response(self, request, response, spider):
        """Filter responses based on content type."""
        content_type = response.headers.get(
            "Content-Type", b"").decode("utf-8").split(";")[0]

        if content_type not in self.allowed_types:
            raise IgnoreRequest(f"Filtered out {content_type}")

        return response  # Pass the response through if it's allowed


@dataclass
class ApertureDBItem:
    """Data class to hold the item to be stored in ApertureDB"""
    properties: dict
    blob: bytes


class ApertureDBSpider(CrawlSpider):
    """Spider to crawl a website and convert the results to ApertureDB items"""

    name = "aperturedb_spider"
    rules = [Rule(LinkExtractor(), callback='process_response', follow=True)]
    _follow_links = True
    # Add 4XX codes to ensure they are passed to the spider
    handle_httpstatus_list = [404, 403, 400, 401, 402, 405]

    def __init__(self,
                 start_urls: List[str],
                 allowed_domains: Optional[List[str]] = [],
                 **kwargs):
        """ApertureDBSpider

        Args:
            start_urls (List[str]): The URLs to start crawling from
        """
        super().__init__(**kwargs)
        self.start_urls = start_urls
        # Extract the domains from the URLs; we only want to crawl the same domain
        self.allowed_domains = list(
            set([urlparse(url).netloc for url in self.start_urls])) + allowed_domains
        self.error_urls = []
        self.crawler.signals.connect(self.spider_closed,
                                     signal=signals.spider_closed)

    @classmethod
    def from_crawler(class_, crawler, **kwargs):
        """Factory method to create a new instance of the spider

        Gets arguments from crawler settings.

        Args:
            crawler (Crawler): The Scrapy Crawler instance

        Returns:
            A new instance of the spider
        """
        settings = crawler.settings
        args = settings.get("APERTUREDB_PIPELINE_ARGS", {})
        spider = class_(start_urls=args.start_urls,
                        allowed_domains=args.allowed_domains,
                        crawler=crawler,
                        **kwargs)
        return spider

    def process_response(self, response) -> ApertureDBItem:
        """Process the response from the request.

        The resulting item contains a properties dictionary and a blob.
        """
        status_code = response.status
        self.crawler.stats.inc_value(
            f'spider/response_status_count/{status_code}')

        if response.status in [404, 403, 400, 401, 402, 405]:
            referrer = response.request.headers.get(
                'Referer', b'None').decode('utf-8')
            self.error_urls.append([response.status, response.url, referrer])
            return None

        properties = {}

        properties["url"] = response.url
        logging.info(f"ApertureDBPipeline: Processing {response.url}")

        domain = urlparse(response.url).netloc
        properties["domain"] = domain
        self.crawler.stats.inc_value(f'spider/domain/{domain}')

        content_type = response.headers.get(
            "Content-Type", b"").decode("utf-8")
        if content_type:
            properties["content_type"] = content_type
            simple_content_type = content_type.split(";")[0]
            properties["simple_content_type"] = simple_content_type
            self.crawler.stats.inc_value(f'content_type/{simple_content_type}')

        last_modified = response.headers.get(
            "Last-Modified", b"").decode("utf-8")
        if last_modified:
            last_modified_dt = email.utils.parsedate_to_datetime(last_modified)
            properties["last_modified"] = {
                "_date": last_modified_dt.isoformat()}

        etag = response.headers.get("ETag", b"").decode("utf-8")
        if etag:
            properties["etag"] = etag

        cache_control = response.headers.get(
            "Cache-Control", b"").decode("utf-8")
        if cache_control:
            properties["cache_control"] = cache_control
            # extract max-age from cache-control
            max_age = None
            for directive in cache_control.split(","):
                if directive.strip().startswith("max-age"):
                    max_age = int(directive.split("=")[1])
                    break
            if max_age:
                properties["cache_control_max_age"] = max_age

        expires = response.headers.get("Expires", b"").decode("utf-8")
        if expires:
            expires_dt = email.utils.parsedate_to_datetime(expires)
            properties["expires"] = {
                "_date": expires_dt.isoformat()}

        # Not exactly right, but close enough for now
        properties['crawl_time'] = {
            "_date": datetime.now(timezone.utc).isoformat()}

        return ApertureDBItem(properties=properties, blob=response.body)

    def spider_closed(self, reason):
        logging.info(f"Spider closed: {reason}")

        global stats
        assert stats is None
        stats = self.crawler.stats.get_stats()
        logging.info(f"Stats: {stats}")

        global error_urls
        assert error_urls is None
        error_urls = self.error_urls
        logging.info(f"Error URLs: {error_urls}")


class ApertureDBPipeline:
    """Pipeline to store items in ApertureDB"""

    def __init__(self, db, crawl_id):
        self.db = db
        self.crawl_id = crawl_id
        logging.info(f"ApertureDBPipeline: Using Crawl ID: {crawl_id}")

    @classmethod
    def from_crawler(class_, crawler):
        db = create_connector()
        args = crawler.settings.get("APERTUREDB_PIPELINE_ARGS", {})
        crawl_id = crawler.settings.get("APERTUREDB_CRAWL_ID")

        pipeline = class_(db, crawl_id)
        pipeline.crawler = crawler
        return pipeline

    def process_item(self, item: ApertureDBItem, spider):
        """Process the item and store it in ApertureDB"""
        self.crawler.stats.inc_value('itempipeline/item_count')
        query = [
            {
                "FindEntity": {
                    "with_class": "Crawl",
                    "constraints": {
                        "id": ["==", self.crawl_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "AddBlob": {
                    "properties": item.properties,
                    "connect": {
                        "ref": 1,
                        "class": "crawlHasDocument",
                        "direction": "in",
                    },
                }
            },
        ]

        blobs = [item.blob]

        execute_query(self.db, query, blobs)


def create_crawl(db, args):
    """Create a new Crawl entity in ApertureDB
    Also create an index on ids for faster lookups.
    """
    start_time = datetime.now(timezone.utc).isoformat()
    logging.info(f"Starting Crawler at {start_time}")
    id_ = str(uuid4())
    execute_query(db, [
        {
            "AddEntity": {
                "class": "Crawl",
                "properties": {
                    "start_urls": json.dumps(args.start_urls),
                    "allowed_domains": json.dumps(args.allowed_domains),
                    "max_documents": args.max_documents,
                    "id": id_,
                    "start_time": {"_date": start_time},
                }
            }
        },
        {
            "CreateIndex": {
                "index_type": "entity",
                "class": "Crawl",
                "property_key": "id",
            }
        },
    ])

    return id_


def update_crawl(db, crawl_id, stats):
    """Update the Crawl entity with end time, duration, and number of documents"""
    logging.info(f"Ending Crawler {crawl_id}")

    # Extract proerties from stats and convert datetimes to strings
    properties = {
        k: {"_date": v.isoformat()} if isinstance(v, datetime) else v
        for k, v in stats.items()
        if v is not None
    }

    global error_urls
    if error_urls:
        properties["error_urls"] = json.dumps(error_urls)

    execute_query(db, [{
        "FindEntity": {
            "with_class": "Crawl",
            "constraints": {
                "id": ["==", crawl_id],
            },
            "_ref": 1,
        }
    }, {
        "UpdateEntity": {
            "ref": 1,
            "properties": properties,
        }
    }])


def main(args):
    # The following line produces duplicate logs in the output.
    # Presumably scrapy is also setting up logging.
    # logging.basicConfig(level=args.log_level.upper())

    db = create_connector()

    crawl_id = create_crawl(db, args)
    logging.info(f"Starting Crawler with ID: {crawl_id}")
    process = CrawlerProcess(settings={
        "ALLOWED_CONTENT_TYPES": args.content_types.split(";"),
        "APERTUREDB_PIPELINE_ARGS": args,
        "APERTUREDB_CRAWL_ID": crawl_id,
        "CLOSESPIDER_ITEMCOUNT": args.max_documents,
        "CONCURRENT_REQUESTS": args.concurrent_requests,
        "CONCURRENT_REQUESTS_PER_DOMAIN": args.concurrent_requests_per_domain,
        "DOWNLOAD_DELAY": args.download_delay,
        "DOWNLOADER_MIDDLEWARES": {
            ContentTypeFilterMiddleware: 543,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
            'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 600,
        },
        "ITEM_PIPELINES": {
            ApertureDBPipeline: 1000,
        },
        "LOG_LEVEL": args.log_level.upper(),
    })
    process.crawl(ApertureDBSpider)
    process.start()
    update_crawl(db, crawl_id, stats)
    logging.info("Crawling complete.")


def get_args():
    obj = argparse.ArgumentParser()

    obj.add_argument('--start-urls', type=str, action='append',
                     help='The URLs to start crawling from',
                     default=os.environ.get('START_URLS', 'https://docs.aperturedata.io/').split())

    obj.add_argument('--allowed-domains', type=str, action='append',
                     help='The allowed domains to crawl (in addition to those in start URLs)',
                     default=os.environ.get('ALLOWED_DOMAINS', '').split())

    obj.add_argument('--max-documents',  type=int,
                     default=os.environ.get('MAX_DOCUMENTS', 1000))

    obj.add_argument('--content-types',  type=str,
                     default=os.environ.get('CONTENT_TYPES', 'text/plain;text/html;application/pdf'))

    obj.add_argument('--log-level', type=str,
                     default=os.environ.get('LOG_LEVEL', 'WARNING'))

    obj.add_argument('--concurrent-requests', type=int,
                     default=os.environ.get('CONCURRENT_REQUESTS', 64))

    obj.add_argument('--concurrent-requests-per-domain', type=int,
                     default=os.environ.get('CONCURRENT_REQUESTS_PER_DOMAIN', 8))

    obj.add_argument('--download-delay', type=float,
                     default=os.environ.get('DOWNLOAD_DELAY', 0))

    params = obj.parse_args()

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
