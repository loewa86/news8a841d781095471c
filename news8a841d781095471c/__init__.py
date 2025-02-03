import aiohttp
import hashlib
import logging
from datetime import datetime as datett, timedelta, timezone
from dateutil import parser
from typing import AsyncGenerator, Optional
import tldextract as tld
import random
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

DEFAULT_OLDNESS_SECONDS = 3600 * 3  # 3 hours
DEFAULT_MAXIMUM_ITEMS = 10
DEFAULT_MIN_POST_LENGTH = 10

# Глобальные переменные для хранения состояния
last_modified: Optional[str] = None
etag: Optional[str] = None

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

def is_within_timeframe_seconds(dt_str, timeframe_sec):
    dt = datett.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.00Z")
    dt = dt.replace(tzinfo=timezone.utc)
    current_dt = datett.now(timezone.utc)
    time_diff = current_dt - dt
    return abs(time_diff) <= timedelta(seconds=timeframe_sec)

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
    else:
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
    return max_oldness_seconds, maximum_items_to_collect, min_post_length

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    feed_url = 'https://raw.githubusercontent.com/user1exd/rss_realtime_feed/main/data/feed.json'
    global last_modified, etag

    while True:
        data = await fetch_data(feed_url)
        if data is None:  # Файл не изменился
            await asyncio.sleep(300)  # Проверяем каждые 5 минут
            continue

        max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
        logging.info(f"[News stream collector] Fetching data from {feed_url} with parameters: {parameters}")
        logging.info(f"[News stream collector] Total entries: {len(data)}")

        # Фильтрация данных
        sorted_data = [
            entry for entry in data
            if is_within_timeframe_seconds(convert_to_standard_timezone(entry["pubDate"]), max_oldness_seconds)
            and len(entry.get("content", "") or entry.get("description", "") or entry["title"]) >= min_post_length
        ]
        logging.info(f"[News stream collector] Filtered entries: {len(sorted_data)}")

        # Случайный выбор 30% данных
        selected_data = random.choices(sorted_data, k=int(len(sorted_data) * 0.3))
        logging.info(f"[News stream collector] Selected entries: {len(selected_data)}")

        yielded_items = 0
        successive_old_entries = 0

        for entry in selected_data:
            if random.random() < 0.25:
                continue

            if yielded_items >= maximum_items_to_collect:
                break

            pub_date = convert_to_standard_timezone(entry["pubDate"])

            if is_within_timeframe_seconds(pub_date, max_oldness_seconds):
                sha1 = hashlib.sha1()
                author = entry.get("creator", ["unknown"])[0]
                sha1.update(author.encode())
                author_sha1_hex = sha1.hexdigest()

                content_article_str = entry.get("content") or entry.get("description") or entry["title"]
                domain_str = entry.get("source_url", entry.get("link", "unknown"))
                domain_str = tld.extract(domain_str).registered_domain

                new_item = Item(
                    content=Content(str(content_article_str)),
                    author=Author(str(author_sha1_hex)),
                    created_at=CreatedAt(pub_date),
                    title=Title(entry["title"]),
                    domain=Domain(str(domain_str)),
                    url=Url(entry["link"]),
                    external_id=ExternalId(entry["article_id"])
                )

                yielded_items += 1
                yield new_item
            else:
                dt = datett.strptime(pub_date, "%Y-%m-%dT%H:%M:%S.00Z")
                dt = dt.replace(tzinfo=timezone.utc)
                current_dt = datett.now(timezone.utc)
                time_diff = current_dt - dt
                logging.info(f"[News stream collector] Entry is {abs(time_diff)} old: skipping.")

                successive_old_entries += 1
                if successive_old_entries >= 5:
                    logging.info(f"[News stream collector] Too many old entries. Stopping.")
                    break

        logging.info(f"[News stream collector] Done.")
        await asyncio.sleep(300)  # Проверяем каждые 5 минут
