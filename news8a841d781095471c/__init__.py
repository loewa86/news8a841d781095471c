from urllib.parse import urlparse
import aiohttp
import hashlib
import logging
from datetime import datetime as datett, timezone
from dateutil import parser
from typing import AsyncGenerator, Optional
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

            async with session.get(url, headers=headers) as response:
                if response.status == 304:  # Файл не изменился
                    logging.info("File not modified since last check.")
                    return None

                if response.status == 200:
                    # Сохраняем новые значения Last-Modified и ETag
                    last_modified = response.headers.get("Last-Modified")
                    etag = response.headers.get("ETag")

                    response_text = await response.text()
                    try:
                        json_data = await response.json(content_type=None)
                        return json_data
                    except json.JSONDecodeError:
                        logging.error(f"Invalid JSON response from {url}")
                        return None
                else:
                    logging.error(f"Error fetching data: {response.status} {response_text}")
                    return None
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

def convert_to_standard_timezone(_date):
    dt = parser.parse(_date)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    feed_url = 'https://raw.githubusercontent.com/user1exd/rss_realtime_feed/main/data/feed.json'
    global last_modified, etag

    while True:
        data = await fetch_data(feed_url)
        if data is None:  # Файл не изменился
            await asyncio.sleep(300)  # Проверяем каждые 5 минут
            continue

        logging.info(f"[News stream collector] Fetching data from {feed_url}")
        logging.info(f"[News stream collector] Total entries: {len(data)}")

        for entry in data:
            try:
                # Проверяем наличие обязательных полей
                if not all(key in entry for key in ["pubDate", "title", "link", "article_id"]):
                    logging.warning(f"Skipping entry with missing required fields: {entry}")
                    continue

                pub_date = convert_to_standard_timezone(entry["pubDate"])

                sha1 = hashlib.sha1()
                author = entry.get("creator", ["unknown"])[0]
                sha1.update(author.encode())
                author_sha1_hex = sha1.hexdigest()

                content_article_str = entry.get("content") or entry.get("description") or entry["title"]
                domain_str = entry.get("source_url", entry.get("link", "unknown"))
                domain_str = tld.extract(domain_str).registered_domain

                # Проверяем, является ли URL допустимым
                if not is_valid_url(entry["link"]):
                    logging.warning(f"Skipping entry with invalid URL: {entry['link']}")
                    continue

                new_item = Item(
                    content=Content(str(content_article_str)),
                    author=Author(str(author_sha1_hex)),
                    created_at=CreatedAt(pub_date),
                    title=Title(entry["title"]),
                    domain=Domain(str(domain_str)),
                    url=Url(entry["link"]),
                    external_id=ExternalId(entry["article_id"])
                )

                yield new_item
            except Exception as e:
                logging.error(f"Error processing entry: {e}")
                continue

        logging.info(f"[News stream collector] Done processing {len(data)} entries.")
        await asyncio.sleep(300)  # Проверяем каждые 5 минут
