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


async def save_to_file(content_data: bytes, file_path: Path) -> None:
    """ Save data [content_data] to file [path_file].
    """
    async with aiofiles.open(file_path, "wb") as fd:
        await fd.write(content_data)


async def fetch_url(session: aiohttp.ClientSession, url: str, timeout: int = REQUEST_TIMEOUT) -> Tuple[bytes, str]:
    """ Return received from url content data [byte type] and content type.
    """
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    async with session.get(url, timeout=client_timeout) as response:
        logging.info(f"Response status: {response.status}, for: {url}")
        content_type: str = response.content_type.split("/")[-1]
        content_data: bytes = b""
        while True:
            chunk = await response.content.read(DOWNLOAD_CHUNK_SIZE_BYTES)
            if not chunk:
                break
            content_data += chunk
            if len(content_data) > DOWNLOAD_MAX_SIZE_BYTES:
                logging.error(f"URL: {url} DOWNLOAD_MAX_SIZE_BYTES limit exceeded")
                break
        return content_data, content_type


async def parse_urls(session: aiohttp.ClientSession, url: str, pattern: str, timeout: int = REQUEST_TIMEOUT) -> List[str]:
    """ Returns list of found patterns in html text from URL.
    """
    urls: List[str] = []
    try:
        content, _ = await fetch_url(session, url, timeout=timeout)
    except Exception as e:
        logging.exception(f"Could not fetch {url} {e}")
        return urls

    if content:
        urls = re.findall(pattern, content.decode('utf-8'))
    logging.info(f"Found {len(urls)} urls")
    return urls


async def download_url_to_dir(session: aiohttp.ClientSession, url: str, output_dir: Path,
                              timeout: int = REQUEST_TIMEOUT) -> None:
    """ Download data from 'url` to `output_dir` directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        content_data, content_type = await fetch_url(session, url, timeout=timeout)
        filename: str = re.sub("\W", "_", url)
        output_path = output_dir / f"{filename}.{content_type}"
        await save_to_file(content_data, output_path)
        logging.info(f"URL: {url} has been successfully downloaded to {output_path}")
    except Exception as e:
        logging.exception(f"URL: {url} download error to {output_dir} {e}")


async def main(output_dir: Path, session: aiohttp.ClientSession, downloaded_news: List[str]) -> None:
    """ Download news from BASE_URL and link from comments to output_dir.
    """
    tasks_news: List[any] = []
    tasks_comments: List[any] = []
    top_news: List[any] = await parse_urls(session, BASE_URL, RE_NEWS_LINK)
    for news_id, url in top_news[:30]:
        if news_id not in downloaded_news:
            tasks_news.append(asyncio.create_task(download_url_to_dir(session, unescape(url),
                                                                      output_dir.joinpath(news_id))))

    for news_id, _ in top_news[:30]:
        if news_id not in downloaded_news:
            downloaded_news.append(news_id)
            comment_url = f"{BASE_URL}item?id={news_id}"
            urls_from_comment = await parse_urls(session, comment_url, RE_COMMENT_LINK)
            if urls_from_comment:
                for url_comment in urls_from_comment:
                    tasks_comments.append(asyncio.create_task(
                        download_url_to_dir(session, unescape(url_comment), output_dir.joinpath(news_id, 'comments'))))

    logging.info(f"Create {len(tasks_news)+len(tasks_comments)} tasks for download")


async def repeat_main(output_dir: Path, refresh_time: int) -> None:
    """ Async run main every refresh_time seconds.
    """
    logging.info("Starting download")
    downloaded_news: List[str] = []
    for path_dir in output_dir.iterdir():
        downloaded_news.append(path_dir.parts[-1])
    logging.info(f"Total number of news downloaded: {len(downloaded_news)}")

    connector = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST_CONNECTIONS, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                await asyncio.wait_for(main(output_dir, session, downloaded_news), timeout=refresh_time)

            except asyncio.TimeoutError:
                logging.info(f'Error timeout {refresh_time}')

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
        asyncio.run(repeat_main(output_dir, refresh_time))
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
