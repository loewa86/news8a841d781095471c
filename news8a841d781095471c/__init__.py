from urllib.parse import urlparse
import aiohttp
import hashlib
import logging
from datetime import datetime as datett, timezone
from dateutil import parser
from typing import AsyncGenerator, Optional, List
import tldextract as tld
import json
import asyncio

from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
)

# Глобальные переменные для хранения состояния
last_modified: Optional[str] = None
etag: Optional[str] = None

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def is_valid_url(url: str) -> bool:
    """Проверяет, является ли строка допустимым URL."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

async def fetch_data(url, headers=None):
    global last_modified, etag

    try:
        async with aiohttp.ClientSession() as session:
            # Добавляем заголовки для проверки изменений
            if last_modified:
                headers = headers or {}
                headers["If-Modified-Since"] = last_modified
            if etag:
                headers = headers or {}
                headers["If-None-Match"] = etag

            logger.info("Sending request to fetch data...")
            async with session.get(url, headers=headers) as response:
                if response.status == 304:  # Файл не изменился
                    logger.info("File not modified since last check.")
                    return None

                if response.status == 200:
                    # Сохраняем новые значения Last-Modified и ETag
                    last_modified = response.headers.get("Last-Modified")
                    etag = response.headers.get("ETag")

                    logger.info("Data fetched successfully. Parsing JSON...")
                    try:
                        json_data = await response.json(content_type=None)
                        return json_data
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON response from {url}")
                        return None
                else:
                    response_text = await response.text()
                    logger.error(f"Error fetching data: {response.status} {response_text}")
                    return None
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return None

def convert_to_standard_timezone(_date):
    dt = parser.parse(_date)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")

async def process_entry(entry: dict) -> Optional[Item]:
    """Обрабатывает одну запись и возвращает Item или None, если запись невалидна."""
    try:
        logger.debug(f"Processing entry: {entry.get('article_id')}")

        # Проверяем наличие обязательных полей
        if not all(key in entry for key in ["pubDate", "title", "link", "article_id"]):
            logger.warning(f"Skipping entry with missing required fields: {entry}")
            return None

        pub_date = convert_to_standard_timezone(entry["pubDate"])
        logger.debug(f"Parsed pubDate: {pub_date}")

        # Исправленная строка с автором
        author = entry["creator"][0] if entry.get("creator") else "anonymous"
        sha1 = hashlib.sha1()
        sha1.update(author.encode())
        author_sha1_hex = sha1.hexdigest()
        logger.debug(f"Processed author: {author_sha1_hex}")
      
        content_article_str = entry.get("content") or entry.get("description") or entry["title"]
        domain_str = entry.get("source_url", entry.get("link", "unknown"))
        domain_str = tld.extract(domain_str).registered_domain
        logger.debug(f"Processed domain: {domain_str}")
                
        content_article_str = ""
              
        # if no content (null), then if description is not null, use it as content
        # else if description is null as well, then use title as content
        if entry.get("content"):
            content_article_str = entry["content"]
        elif entry.get("description"):
            content_article_str = entry["description"]
        else:
            content_article_str = entry["title"]

        domain_str = entry["source_url"] if entry.get("source_url") else "unknown"
        # remove the domain using tldextract to have only the domain name
        # e.g. http://www.example.com -> example.com
        domain_str = tld.extract(domain_str).registered_domain

        logger.debug(f"Entry content successfully: {content_article_str}")

        logger.debug(f"Entry processed successfully: {entry['article_id']}")
        return Item(
            content=Content(str(content_article_str)),
            author=Author(str(author_sha1_hex)),
            created_at=CreatedAt(pub_date),
            title=Title(entry["title"]),
            domain=Domain(str(domain_str)),
            url=Url(entry["link"]),
            external_id=ExternalId(entry["article_id"])
        )
    except Exception as e:
        logger.error(f"Error processing entry {entry.get('article_id')}: {e}")
        return None

async def process_entries(entries: List[dict]) -> List[Item]:
    """Обрабатывает список записей параллельно."""
    logger.info(f"Processing {len(entries)} entries in parallel...")
    tasks = [process_entry(entry) for entry in entries]
    results = await asyncio.gather(*tasks)
    valid_items = [result for result in results if result is not None]
    logger.info(f"Processed {len(valid_items)} valid entries.")
    return valid_items

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    feed_url = 'https://raw.githubusercontent.com/user1exd/rss_realtime_feed/main/data/feed.json'
    global last_modified, etag

    while True:
        logger.info("Starting new iteration...")
        data = await fetch_data(feed_url)
        if data is None:  # Файл не изменился
            logger.info("No new data. Waiting for 30 seconds...")
            await asyncio.sleep(30)
            continue

        logger.info(f"Fetched {len(data)} entries from {feed_url}")

        # Разделяем данные на чанки по 100 записей
        chunk_size = 100
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        logger.info(f"Data divided into {len(chunks)} chunks.")

        for chunk in chunks:
            logger.info(f"Processing chunk with {len(chunk)} entries...")
            valid_items = await process_entries(chunk)
            for item in valid_items:
                yield item
            logger.info(f"Chunk processed. Yielding {len(valid_items)} items.")

        logger.info(f"Iteration completed. Processed {len(data)} entries in total.")
        await asyncio.sleep(30)  # Уменьшили время ожидания до 30 секунд
