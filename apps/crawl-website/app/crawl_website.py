import argparse
import os
import logging
from typing import List, Optional
from datetime import datetime, timezone
from uuid import uuid4
from urllib.parse import urlparse
import email.utils
from dataclasses import dataclass

import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import Crawler, CrawlerProcess
from scrapy.http import HtmlResponse
from scrapy.exceptions import IgnoreRequest
from scrapy.item import Item

from aperturedb import CommonLibrary


class ContentTypeFilterMiddleware:
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
    properties: dict
    blob: bytes


class ApertureDBSpider(CrawlSpider):
    name = "aperturedb_spider"
    rules = [Rule(LinkExtractor(), callback='process_response', follow=True)]
    _follow_links = True

    def __init__(self,
                 start_url: str,
                 **kwargs):
        """ApertureDBSpider

        Args:
            start_url (str): The URL to start crawling from
        """
        super().__init__(**kwargs)
        self.start_urls = [start_url]
        # Extract the domains from the URLs; we only want to crawl the same domain
        self.allowed_domains = list(
            set([urlparse(url).netloc for url in self.start_urls]))

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
        spider = class_(start_url=args.start_url,
                        crawler=crawler,
                        **kwargs)
        return spider

    def process_response(self, response) -> ApertureDBItem:
        """Process the response from the request"""
        properties = {}

        properties["url"] = response.url
        logging.info(f"ApertureDBPipeline: Processing {response.url}")

        content_type = response.headers.get(
            "Content-Type", b"").decode("utf-8")
        if content_type:
            properties["content_type"] = content_type
            simple_content_type = content_type.split(";")[0]
            properties["simple_content_type"] = simple_content_type

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


class ApertureDBPipeline:
    def __init__(self, db, crawl_id):
        self.db = db
        self.crawl_id = crawl_id
        logging.info(f"ApertureDBPipeline: Using Crawl ID: {crawl_id}")

    @classmethod
    def from_crawler(class_, crawler):
        db = CommonLibrary.create_connector()
        args = crawler.settings.get("APERTUREDB_PIPELINE_ARGS", {})
        crawl_id = crawler.settings.get("APERTUREDB_CRAWL_ID")

        return class_(db, crawl_id)

    def process_item(self, item: ApertureDBItem, spider):
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
                "AddEntity": {
                    "class": "CrawlDocument",
                    "properties": item.properties,
                    "connect": {
                        "ref": 1,
                        "class": "crawlHasDocument",
                        "direction": "in",
                    },
                    "_ref": 2,
                }
            },
            {
                "AddBlob": {
                    "connect": {
                        "ref": 2,
                        "class": "documentContent",
                        "direction": "in",
                    }
                }
            },
        ]

        blobs = [item.blob]

        self.db.query(query, blobs)
        assert self.db.last_query_ok(), self.db.last_response


def create_crawl(db, args):
    start_time = datetime.now(timezone.utc).isoformat()
    logging.info(f"Starting Crawler at {start_time}")
    id_ = str(uuid4())
    db.query([
        {
            "AddEntity": {
                "class": "Crawl",
                "properties": {
                    "start_time": {"_date": start_time},
                    "start_url": args.start_url,
                    "max_documents": args.max_documents,
                    "id": id_,
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
    assert db.last_query_ok(), db.last_response

    return id_, start_time


def update_crawl(db, crawl_id, start_time):
    end_time = datetime.now(timezone.utc).isoformat()
    duration = (datetime.fromisoformat(end_time) -
                datetime.fromisoformat(start_time)).total_seconds()
    logging.info(f"Ending Crawler at {end_time}, {duration} seconds")

    response, _ = db.query([
        {
            "FindEntity": {
                "with_class": "Crawl",
                "constraints": {
                    "id": ["==", crawl_id],
                },
                "_ref": 1,
            },
        },
        {
            "FindEntity": {
                "with_class": "CrawlDocument",
                "is_connected_to": {"ref": 1},
                "results": {"count": True},
            },
        }
    ])
    assert db.last_query_ok(), db.last_response

    n_documents = response[1]["FindEntity"]["count"] if len(
        response) == 2 else 0
    logging.info(f"Found {n_documents} documents")

    db.query([{
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
            "properties": {
                "end_time": {"_date": end_time},
                "n_documents": n_documents,
                "duration": duration,
            }
        }
    }])
    assert db.last_query_ok(), db.last_response


def main(args):
    # logging.basicConfig(level=args.log_level.upper())

    db = CommonLibrary.create_connector()
    crawl_id, start_time = create_crawl(db, args)
    logging.info(f"Starting Crawler with ID: {crawl_id}")
    process = CrawlerProcess(settings={
        "APERTUREDB_PIPELINE_ARGS": args,
        "APERTUREDB_CRAWL_ID": crawl_id,
        "DOWNLOADER_MIDDLEWARES": {
            ContentTypeFilterMiddleware: 543,
        },
        "ITEM_PIPELINES": {
            ApertureDBPipeline: 1000,
        },
        "LOG_LEVEL": args.log_level.upper(),
        "ALLOWED_CONTENT_TYPES": args.content_types.split(";"),
        "CLOSESPIDER_ITEMCOUNT": args.max_documents,
        "CONCURRENT_REQUESTS": args.concurrent_requests,
        "CONCURRENT_REQUESTS_PER_DOMAIN": args.concurrent_requests_per_domain,
    })
    process.crawl(ApertureDBSpider)
    process.start()
    update_crawl(db, crawl_id, start_time)
    logging.info("Crawling complete.")


def get_args():
    obj = argparse.ArgumentParser()

    obj.add_argument('--start-url', type=str,
                     help='The URL to start crawling from',
                     default=os.environ.get('START_URL', 'https://docs.aperturedata.io/'))

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

    params = obj.parse_args()

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
