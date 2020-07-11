#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import aiofiles
import logging
import re
from optparse import OptionParser
from typing import List, Tuple
from pathlib import Path
from html import unescape


BASE_URL = "https://news.ycombinator.com/"
DEFAULT_OUTPUT_DIR = "./news/"
DEFAULT_REFRESH_TIME = 60
REQUEST_TIMEOUT = 10
LIMIT_PER_HOST_CONNECTIONS = 3

RE_NEWS_LINK = r"<tr class=\'athing\' id=\'(\d+)\'>\n.+?" r'<a href="(.+?)" class="storylink".*?>'
RE_COMMENT_LINK = r'<span class="commtext.+?<a href="(.+?)"'

DOWNLOAD_CHUNK_SIZE_BYTES = 1024
DOWNLOAD_MAX_SIZE_BYTES = 3000000


async def download_url_to_dir(session: aiohttp.ClientSession, url: str, output_directory: Path,
                              timeout: int = REQUEST_TIMEOUT) -> None:
    """ Download data from 'url` to `output_dir` directory.
    """
    client_timeout = aiohttp.ClientTimeout(total=timeout)

    try:
        async with session.get(url, timeout=client_timeout) as response:
            logging.info(f"Got response {response.status} for: {url}")
            ext: str = response.content_type.split("/")[-1]
            filename: str = re.sub("\W", "_", url)
            output_path = output_directory / f"{filename}.{ext}"

            content: bytes = b""
            while True:
                chunk = await response.content.read(DOWNLOAD_CHUNK_SIZE_BYTES)
                if not chunk:
                    break
                content += chunk
                if len(content) > DOWNLOAD_MAX_SIZE_BYTES:
                    logging.error(f"URL: {url} DOWNLOAD_MAX_SIZE_BYTES limit exceeded")
                    break

            async with aiofiles.open(output_path, "wb") as fd:
                await fd.write(content)

            logging.info(f"URL: {url} has been successfully downloaded to {output_path}")

    except Exception as e:
        logging.exception(f"URL: {url} download error to {output_directory} {e}")


async def download_news(session: aiohttp.ClientSession, news_id: str, news_url: str, output_dir: Path) -> None:
    """ Downloads: news from url=news_url to `output_dir` directory and
                   comments for current news from id=news_id to `output_dir/comments` directory.
    """
    if output_dir.is_dir():
        return
    output_dir.mkdir()
    await download_url_to_dir(session, unescape(news_url), output_dir)

    comment_url = f"{BASE_URL}item?id={news_id}"
    urls_from_comment = await parse_urls(session, comment_url, RE_COMMENT_LINK)
    if urls_from_comment:
        output_comments_dir = output_dir.joinpath('comments')
        output_comments_dir.mkdir()
        for url in urls_from_comment:
            await download_url_to_dir(session, unescape(url), output_comments_dir)


async def fetch_url(session: aiohttp.ClientSession, url: str, timeout: int = REQUEST_TIMEOUT) -> Tuple[int, str]:
    """ Fetch HTML page from URL.
    """
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    async with session.get(url, timeout=client_timeout) as response:
        logging.info(f"Response status: {response.status}, for: {url}")
        return response.status, await response.text()


async def parse_urls(session: aiohttp.ClientSession, url: str, pattern: str) -> List[str]:
    """ Search pattern in html text from URL.
    """
    urls: List[str] = []
    try:
        status, html = await fetch_url(session, url)
    except Exception as e:
        logging.exception(f"Could not fetch {url} {e}")
        return urls

    urls = re.findall(pattern, html)

    logging.info(f"Found {len(urls)} urls")
    return urls


async def main(output_dir: Path, refresh_time: int) -> None:
    """ Async download news from BASE_URL to output_dir.
    """
    while True:
        logging.info("Starting download")
        num_downloaded: int = len(list(output_dir.iterdir()))
        logging.info(f"Total number of news downloaded: {num_downloaded}")

        connector = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST_CONNECTIONS, force_close=True)

        async with aiohttp.ClientSession(connector=connector) as session:
            top_news: List[any] = await parse_urls(session, BASE_URL, RE_NEWS_LINK)
            tasks = (download_news(session, news_id, url, output_dir.joinpath(news_id))
                     for news_id, url in top_news[:30])
            await asyncio.gather(*tasks)

        logging.info(f"Waiting for refresh time in {refresh_time} seconds")
        await asyncio.sleep(refresh_time)


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("-o", "--output-dir", default=DEFAULT_OUTPUT_DIR)
    op.add_option("-r", "--refresh-time", default=DEFAULT_REFRESH_TIME)

    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    logging.info(f"Crawler started with options: {opts}")

    refresh_time: int = int(opts.refresh_time)
    output_dir: Path = Path(opts.output_dir).resolve()
    output_dir.mkdir(exist_ok=True, parents=True)

    try:
        asyncio.run(main(output_dir, refresh_time))
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
