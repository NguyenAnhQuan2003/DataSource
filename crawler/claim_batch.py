import logging
import aiohttp
import asyncio
import random
import requests
from bs4 import BeautifulSoup
from common.logging_config import setup_logging
from common.dir import BATCH_SIZE
setup_logging()

# Cấu hình logging
logging.basicConfig(level=logging.INFO)

# Giới hạn số request song song (ví dụ 200/máy)
CONCURRENCY = 200
# Số lần retry tối đa nếu lỗi
MAX_RETRIES = 3

semaphore = asyncio.Semaphore(CONCURRENCY)

async def fetch(session, url):
    """Gửi request và lấy HTML"""
    for attempt in range(MAX_RETRIES):
        try:
            async with semaphore:  # giới hạn concurrency
                async with session.get(url, timeout=10) as resp:
                    if resp.status != 200:
                        return None
                    text = await resp.text()
                    return text
        except Exception as e:
            logging.warning(f"Retry {attempt+1}/{MAX_RETRIES} for {url}: {e}")
            await asyncio.sleep(1 + attempt)  # backoff
    return None

# def crawl_product_name(url):
#     try:
#         resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
#         if resp.status_code != 200:
#             return None
#         soup = BeautifulSoup(resp.text, "html.parser")
#         title = soup.find("h1")
#         if title:
#             return title.get_text(strip=True)
#     except Exception as e:
#         logging.error("Crawling error", e)
#     return None

async def crawl_product_name(session, url):
    """Parse HTML và lấy <h1>"""
    html = await fetch(session, url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    title = soup.find("h1")
    return title.get_text(strip=True) if title else None

async def crawl_many(urls):
    """Crawl nhiều URL song song"""
    results = {}
    async with aiohttp.ClientSession(
        headers={"User-Agent": "Mozilla/5.0"}
    ) as session:
        tasks = [crawl_product_name(session, url) for url in urls]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for url, res in zip(urls, responses):
            if isinstance(res, Exception):
                logging.error(f"Error with {url}: {res}")
                results[url] = None
            else:
                results[url] = res
    return results

def claim_batch(col, size=BATCH_SIZE):
    docs = list(col.find({"status": "pending"})
                   .limit(size))
    ids = [d["_id"] for d in docs]
    if ids:
        col.update_many({"_id": {"$in": ids}}, {"$set": {"status": "processing"}})
    return docs

