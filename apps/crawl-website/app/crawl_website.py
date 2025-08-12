from wf_argparse import ArgumentParser
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

logger = logging.getLogger(__name__)


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
    start_urls = []
    allowed_domains = []
    error_urls = []

    def set_allowed_domains(self, allowed_domains: List[str]):
        """Set the allowed domains for the spider

        Args:
            allowed_domains (List[str]): The allowed domains to crawl
        """
        self.allowed_domains = list(
            set([urlparse(url).netloc for url in self.start_urls])) + (allowed_domains or [])

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
        spider = super().from_crawler(crawler, **kwargs)
        spider.start_urls = args.start_urls
        spider.set_allowed_domains(args.allowed_domains)
        spider.error_urls = []
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
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

        doc_count = self.crawler.stats.get_value('itempipeline/item_count', 0)
        logging.info(f"Total documents processed: {doc_count}")
        if doc_count == 0:
            logging.error(
                "No documents processed, this may indicate an issue with the crawl.")
            self.crawler.engine.close_spider(self, "no_documents_processed")
            # Bypass scrapy's error handling to exit immediately
            os._exit(1)


class ApertureDBPipeline:
    """Pipeline to store items in ApertureDB"""

    def __init__(self, db, spec_id, run_id):
        self.db = db
        self.spec_id = spec_id
        self.run_id = run_id
        logging.info(f"ApertureDBPipeline: Using Crawl: {spec_id} / {run_id}")

    @classmethod
    def from_crawler(class_, crawler):
        db = create_connector()
        args = crawler.settings.get("APERTUREDB_PIPELINE_ARGS", {})
        spec_id = crawler.settings.get("APERTUREDB_SPEC_ID")
        run_id = crawler.settings.get("APERTUREDB_RUN_ID")

        pipeline = class_(db, spec_id, run_id)
        pipeline.crawler = crawler
        return pipeline

    def process_item(self, item: ApertureDBItem, spider):
        """Process the item and store it in ApertureDB"""
        self.crawler.stats.inc_value('itempipeline/item_count')
        properties = item.properties.copy()
        properties['spec_id'] = self.spec_id
        properties['run_id'] = self.run_id
        properties['id'] = str(uuid4())
        query = [
            {
                "FindEntity": {
                    "with_class": "CrawlSpec",
                    "constraints": {
                        "id": ["==", self.spec_id],
                    },
                    "_ref": 1,
                }
            },
            {
                "AddEntity": {
                    "class": "CrawlDocument",
                    "properties": properties,
                    "connect": {
                        "ref": 1,
                        "class": "crawlSpecHasDocument",
                        "direction": "in",
                    },
                    "_ref": 2,
                },
            },
            {
                "AddBlob": {
                    "connect": {
                        "ref": 2,
                        "class": "crawlDocumentHasBlob",
                        "direction": "in",
                    },
                }
            },
        ]

        blobs = [item.blob]

        execute_query(self.db, query, blobs)


def create_spec(db, args):
    """Create a new CrawlSpec entity in ApertureDB
    Also create an index on ids for faster lookups.
    """
    start_time = datetime.now(timezone.utc).isoformat()
    spec_id = args.output
    logging.info(f"Starting Crawler {spec_id} at {start_time}")

    # If --clean is set, they will already have been deleted
    _, results, _ = execute_query(db, [
        {
            "FindEntity": {
                "with_class": "CrawlSpec",
                "constraints": {
                    "id": ["==", spec_id],
                },
                "results": {"count": True}
            }
        }
    ])
    # TODO: Support incremental crawl here
    count = results[0]['FindEntity']['count']
    if count > 0:
        logging.error(f"Spec {spec_id} already exists, skipping creation")
        raise ValueError(
            f"Spec {spec_id} already exists, skipping creation")

    execute_query(db, [
        {
            "AddEntity": {
                "class": "CrawlSpec",
                "properties": {
                    "start_urls": json.dumps(args.start_urls),
                    "allowed_domains": json.dumps(args.allowed_domains),
                    "max_documents": args.max_documents,
                    "id": spec_id,
                }
            }
        }])


def create_indexes(db):
    logger.info(
        "Creating indexes. This will generate a partial error if the index already exists.")
    execute_query(db, [
        {
            "CreateIndex": {
                "index_type": "entity",
                "class": "CrawlSpec",
                "property_key": "id",
            }
        },
        {
            "CreateIndex": {
                "index_type": "entity",
                "class": "CrawlDocument",
                "property_key": "id",
            }
        },
        {
            "CreateIndex": {
                "index_type": "entity",
                "class": "CrawlDocument",
                "property_key": "spec_id",
            }
        },
        {
            "CreateIndex": {
                "index_type": "entity",
                "class": "CrawlDocument",
                "property_key": "run_id",
            }
        },
        {
            "CreateIndex": {
                "index_type": "entity",
                "class": "CrawlRun",
                "property_key": "id",
            }
        },
    ])


def create_run(db, spec_id, run_id, stats):
    """Create a CrawlRun entity with end time, duration, and number of documents"""
    logging.info(f"Ending Crawl {spec_id} {run_id}")

    # Extract properties from stats and convert datetimes to strings
    properties = {
        k: {"_date": v.isoformat()} if isinstance(v, datetime) else v
        for k, v in stats.items()
        if v is not None
    } if stats else {}

    global error_urls
    if error_urls:
        properties["error_urls"] = json.dumps(error_urls)

    properties['id'] = run_id
    properties['spec_id'] = spec_id

    execute_query(db, [{
        "FindEntity": {
            "with_class": "CrawlSpec",
            "constraints": {
                "id": ["==", spec_id],
            },
            "_ref": 1,
        }
    }, {
        "AddEntity": {
            "class": "CrawlRun",
            "properties": properties,
            "connect": {
                "ref": 1,
                "class": "crawlSpecHasRun",
                "direction": "in",
            },
            "_ref": 2,
        }
    }, {
        "FindEntity": {
            "with_class": "CrawlDocument",
            "constraints": {
                "run_id": ["==", run_id],
            },
            "_ref": 3,
        }
    }, {
        "AddConnection": {
            "class": "crawlRunHasDocument",
            "src": 2,
            "dst": 3,
        }
    }])


def delete_crawl(db, spec_id):
    """Delete a crawl spec and all dependent artefacts"""
    logger.info(f"Deleting Crawl {spec_id}")
    execute_query(db, [
        {
            "FindEntity": {
                "with_class": "CrawlSpec",
                "constraints": {
                    "id": ["==", spec_id],
                },
                "_ref": 1,
            },
        },
        {
            "DeleteEntity": {
                "ref": 1,
            }
        },
        {
            "FindEntity": {
                "with_class": "CrawlDocument",
                "constraints": {
                    "spec_id": ["==", spec_id],
                },
                "_ref": 2,
            }
        },
        {
            "DeleteEntity": {
                "ref": 2,
            }
        },
        {
            "FindEntity": {
                "with_class": "CrawlRun",
                "constraints": {
                    "spec_id": ["==", spec_id],
                },
                "_ref": 3,
            }
        },
        {
            "FindBlob": {
                "is_connected_to": {
                    "ref": 2,
                    "connection_class": "crawlDocumentHasBlob",
                },
                "_ref": 4,
            }
        },
        {
            "DeleteEntity": {
                "ref": 3,
            }
        },
        {
            "DeleteBlob": {
                "ref": 4,
            }
        },
    ])
    logger.info(f"Deleted Crawl {spec_id}")


def delete_all(db):
    """Delete all crawl specs and all dependent artefacts"""
    logging.info(f"Deleting all Crawls")
    _, results, _ = execute_query(db, [
        {
            "FindEntity": {
                "with_class": "CrawlSpec",
                "results": {
                    "list": ["id"],
                },
            },
        },
    ])
    if 'entities' not in results[0]['FindEntity']:
        logger.warning("No Crawls to delete")
        return

    for result in results[0]['FindEntity']['entities']:
        spec_id = result["id"]
        delete_crawl(db, spec_id)

    logger.info(f"Deleted all Crawls")


def main(args):
    db = create_connector()

    spec_id = args.output
    run_id = str(uuid4())

    if args.delete_all:
        delete_all(db)
        return

    if args.delete:
        delete_crawl(db, spec_id)
        return

    if args.clean:
        delete_crawl(db, spec_id)
        # continue

    create_spec(db, args)
    create_indexes(db)
    logging.info(f"Starting Crawler with spec {spec_id}, run {run_id}")
    process = CrawlerProcess(settings={
        "ALLOWED_CONTENT_TYPES": args.content_types.split(";"),
        "APERTUREDB_PIPELINE_ARGS": args,
        "APERTUREDB_SPEC_ID": spec_id,
        "APERTUREDB_RUN_ID": run_id,
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
        "EXTENSIONS": {
            'scrapy.extensions.closespider.CloseSpider': 100,
        },
    })
    process.crawl(ApertureDBSpider)
    process.start()
    create_run(db, spec_id, run_id, stats)
    logging.info("Crawling complete.")


def get_args():
    obj = ArgumentParser(support_legacy_envars=True)

    obj.add_argument('--start-urls', type=str,
                     help='The URLs to start crawling from',
                     default='https://docs.aperturedata.io/',
                     sep=' ')

    obj.add_argument('--allowed-domains', type=str,
                     help='The allowed domains to crawl (in addition to those in start URLs)',
                     sep=' ')

    obj.add_argument('--max-documents',  type=int,
                     default=1000)

    obj.add_argument('--content-types',  type=str,
                     default='text/plain;text/html;application/pdf')

    obj.add_argument('--log-level', type=str,
                     default='WARNING')

    obj.add_argument('--concurrent-requests', type=int,
                     default=64)

    obj.add_argument('--concurrent-requests-per-domain', type=int,
                     default=8)

    obj.add_argument('--download-delay', type=float,
                     default=0)

    obj.add_argument(
        '--output', type=str,
        help="Identifier for the crawl spec document (default is generated UUID)",
        default=str(uuid4()))

    obj.add_argument('--delete', type=bool,
                     default=False,
                     help="Delete the crawl spec and dependent artefacts; don't run the crawl")

    obj.add_argument('--delete-all', type=bool,
                     default=False,
                     help="Delete all crawl specs and their dependent artefacts; don't run the crawl")

    obj.add_argument('--clean', type=bool,
                     default=False,
                     help="Delete any existing crawl spec with the same id before running the crawl")

    params = obj.parse_args()

    logging.basicConfig(level=params.log_level.upper(), force=True)

    logger.info(f"Parsed arguments: {params}")

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
