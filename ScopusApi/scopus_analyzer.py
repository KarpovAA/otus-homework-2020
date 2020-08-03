#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
import aiohttp
import asyncio
import json
import logging
import re
import pandas as pd

from datetime import datetime, timedelta
from optparse import OptionParser
from collections import defaultdict, namedtuple
from typing import List, Tuple, Optional, Dict
from pathlib import Path
from contextlib import closing

from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch

CONFIG_FILENAME = 'config.json'
LIMIT_PER_HOST_CONNECTIONS = 3
REQUEST_TIMEOUT = 120

FORMAT_DATE = '%Y%m%d'
DUMP_DIR_NAME = 'dumps'
DUMP_DIR_PATH = Path.cwd().joinpath(DUMP_DIR_NAME)
DUMP_FILENAME = f'dump-{datetime.now().strftime(format=FORMAT_DATE)}.json'
DUMP_FILE = DUMP_DIR_PATH.joinpath(DUMP_FILENAME)
DUMP_FILENAME_DATE_PATTERN = r'\d{8}'
DUMP_FILENAME_PATTERN = r'^dump-\d{8}.json$'
DUMP_RELOAD_AFTER_DAYS = 30

CSV_DIRNAME = 'csv'
CSV_DIR = Path.cwd().joinpath(CSV_DIRNAME)


def async_fetch_urls(urls, header):
    """ Async fetch URLs list
    """
    loop = asyncio.get_event_loop()
    try:
        data = loop.run_until_complete(run_async_get_requests(urls, header))
    except Exception as e:
        logging.exception(f'Unexpected error:', e)
        return None
    loop.close()
    return data


async def run_async_get_requests(urls: List, headers):
    """ Creates tasks and runs async.
    """
    connector = aiohttp.TCPConnector(limit_per_host=LIMIT_PER_HOST_CONNECTIONS, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [(async_get_request(session, url, headers)) for url in urls]
        result = await asyncio.gather(*tasks)
        return result


async def async_get_request(session, url, headers, timeout=REQUEST_TIMEOUT):
    """ Async get request to url with headers.
    """
    # todo добавить кэширование запросов в memcached
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    if not url:
        return None
    async with session.get(url, headers=headers, timeout=client_timeout) as r:
        # todo добавить обработку других ошибок.
        if r.status != 200:
            logging.error(f'HTTP {r.status}. Error from {url}: {r.text}')
            return None
        else:
            data = await r.text()
            logging.info(f'{url} loading completed')
            return json.loads(data)


def mysql_connect(host, username: str, user_passwd: str, db_name: str = None, timeout=REQUEST_TIMEOUT):
    """ Connect to MySQL database.
    """
    con = pymysql.connect(host=host,
                          user=username,
                          passwd=user_passwd,
                          db=db_name,
                          connect_timeout=timeout)
    return con


def mysql_execute(con: pymysql.connect, query: str) -> List:
    """ Returns the result for a query in mysql.
    """
    with closing(con) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            result = [x[0] for x in result]
        return result


def get_date_from_filename(filename: Path, pattern: str = DUMP_FILENAME_DATE_PATTERN,
                           format_date: str = FORMAT_DATE) -> Optional[datetime.date]:
    """ Return datetime from filename."""
    f_name = re.findall(pattern, filename.parts[-1])
    try:
        f_date = datetime.strptime(f_name[0], format_date).date()
        return f_date
    except Exception as e:
        logging.exception(f'Error converting filename to date. File {filename}', e)
    return None


def get_last_dump_file(path_dir: Path,
                       pattern_filename: str = DUMP_FILENAME_PATTERN) -> Optional[Tuple[str, Path, datetime.date]]:
    """ Returns the last dump file in path_dir using the pattern in the filename.
    """
    if not path_dir or not path_dir.is_dir():
        logging.error(f'Directory {path_dir} not found:')
        return None
    last_file = namedtuple('last_file', ['filename', 'path', 'date'])
    f_date = f_path = None
    try:
        for file in path_dir.iterdir():
            if not file.is_dir() and re.match(pattern_filename, file.parts[-1]):
                if not f_date or f_date < get_date_from_filename(file):
                    f_date = get_date_from_filename(file)
                    f_path = file
    except Exception as e:
        logging.exception(f'Error open directory {path_dir}', e)
        return None
    if not f_path:
        return None
    return last_file(f_path.parts[-1], f_path, f_date)


def load_dump_from_file(path: Path) -> json:
    """ Return JSON data from path.
    """
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.exception(f'Error load dump from {path}', e)


def save_dump_to_file(path: Path, data: json):
    """ Save JSON data to path.
    """
    # todo check dir
    try:
        with open(path, 'w') as f:
            f.write(json.dumps(data))
    except Exception as e:
        logging.exception(f'Error save dump to {path}', e)


def get_dump_from_csv(file: Path) -> pd.DataFrame:
    """ Loads and returns data from csv file.
    """
    try:
        return pd.read_csv(file)
    except Exception as e:
        logging.exception(f'Error load dump from CSV {file}', e)


def save_dump_to_csv(dump_pd: pd.DataFrame, filename: str, force_save: bool = True) -> bool:
    """ Saves dump data to csv file.
    """
    CSV_DIR.mkdir(exist_ok=True)
    file = CSV_DIR.joinpath(filename)
    if file.is_file() and not force_save:
        logging.info(f'CSV file already exist {file}')
        return False
    try:
        return not dump_pd.to_csv(file)
    except Exception as e:
        logging.exception(f'Error save dump to CSV {file}', e)
        return False


def get_pubs_org_from_api(org_id: str, api_key=None) -> Optional[json.dumps]:
    """ Loads and returns data on publications of organization from Scopus via API.
    """
    client = ElsClient(api_key)
    search = ElsSearch(f"(AF-ID({org_id}))", 'scopus')      # AND PUBYEAR > 2019
    # todo переписать в асинхронном режиме
    search.execute(client, get_all=True)                    # загружаем данные по публикациям организации
    if client.req_status['status_code'] != 200:
        return None
    pubs = search.results

    logging.info(f'{len(pubs)} publications received')

    # составляем список тасков для загрузки данных по авторам
    tasks = defaultdict(list)
    for i, res in enumerate(pubs):
        for authors_link in res['link']:
            if authors_link['@ref'] == 'author-affiliation':
                tasks[i] = authors_link['@href']
                break

    header = get_header(api_key)
    result = async_fetch_urls(tasks.values(), header)
    for i, j in zip(tasks.keys(), result):
        pubs[i]['authors'] = j

    return pubs


def get_list_journals_from_mysql(con: pymysql.connect) -> List:
    """ Returns a list of journals from mysql.
    """
    query = "SELECT title FROM db_journals WHERE language='английский'"
    local_journals_list = mysql_execute(con, query)
    return local_journals_list


def url_journal_metrics(journal_title: str, journal_issn: str = None) -> Optional[str]:
    """ Generates url for requesting journal metrics.
    """
    base_url = 'https://api.elsevier.com/content/serial/title?'
    if journal_title:
        url = base_url + f'&title={journal_title}'
    elif journal_issn:
        url = base_url + f'&issn={journal_issn}'
    else:
        return None
    url += '&field=SJR,title,issn'
    return url


def parse_journal_sjr(r, title: str = None) -> Optional[Dict]:
    """ Parses metrics from the response.
    """
    if not r:
        return None
    if 'error' in r.get('serial-metadata-response').keys():
        logging.error(f'Error getting journal metrics from Scopus. {title}')
        return None

    metric = {}
    metrics = r.get('serial-metadata-response').get('entry')
    if len(metrics) > 1:
        for m in metrics:
            if m.get('dc:title').lower() == title.lower():
                metric = m
        if not metric:
            logging.info(f'Multiple result for {title}. ISSN required ')
            return None
    elif len(metrics) == 1:
        metric = metrics[0]
    else:
        # todo
        logging.error(f'WOW2 ???{r}')
    try:
        if metric.get('SJRList'):
            year = metric['SJRList']['SJR'][0]['@year']
            sjr = float(metric['SJRList']['SJR'][0]['$'])
            issn = metric.get('prism:issn')
            return {'jsr': sjr, 'year': year, 'issn': issn}
        else:
            logging.info(f'SJR not found for {title}')
            return None
    except Exception as e:
        logging.exception(f'Error parsing journal metrics from JSON', e)
    return None


def get_journals_metrics_from_api(api_key: str, journals_list: List) -> Optional[Dict]:
    """ Gets metrics of the journal list
    """
    tasks_url = []
    for journal in journals_list:
        tasks_url.append(url_journal_metrics(journal))
    header = get_header(api_key)
    result = async_fetch_urls(tasks_url, header)

    res_list = defaultdict()
    for title, r in zip(journals_list, result):
        res_list[title] = parse_journal_sjr(r, title)
    return res_list


def get_fresh_pubs_org(org_id: str, api_key: str = None, dump_dir: Path = DUMP_DIR_PATH,
                       pattern_file: str = DUMP_FILENAME_PATTERN, force_update: bool = False, ) -> Optional[json.loads]:
    """ Return actual dump, not older than 30 days. If local dump is older, loads data from API and saves to local file.
    """
    # todo организовать хранение полученных данных в БД по id_org и даты выгрузки
    dump_file = get_last_dump_file(dump_dir, pattern_file)
    if force_update or not dump_file or datetime.now().date() - timedelta(days=DUMP_RELOAD_AFTER_DAYS) > dump_file.date:
        dump = get_pubs_org_from_api(org_id, api_key)
        save_dump_to_file(dump_dir.joinpath(DUMP_FILENAME), dump)
        return dump
    else:
        return load_dump_from_file(dump_file.path)


def preprocessing_dump(dump_pd: pd.DataFrame) -> Optional[pd.DataFrame]:
    """" Preprocessing dump_pd: convert types.
    """
    try:
        dump_pd['prism:coverDate'] = pd.to_datetime(dump_pd['prism:coverDate'])
        dump_pd['citedby-count'] = pd.to_numeric(dump_pd['citedby-count'])
        return dump_pd
    except Exception as e:
        logging.exception(f'Error converting type in dump', e)
    return None


def filter_dump_by_years(dump_pd: pd.DataFrame, start_year: str = '1900',
                         end_year: str = str(datetime.now().year)) -> Optional[pd.DataFrame]:
    """ Filtering of data in dump by years.
    """
    try:
        start_year = int(start_year)
        end_year = int(end_year)
    except Exception as e:
        logging.exception(f'Error converting [start,end] year to int', e)
        return None
    dump_pd = dump_pd[(dump_pd['prism:coverDate'].dt.year >= start_year) &
                      (dump_pd['prism:coverDate'].dt.year <= end_year)]
    return dump_pd


def report_top_authors(dump_pd: pd.DataFrame, start_year: str = '1900', end_year: str = str(datetime.now().year),
                       org_id: str = None) -> pd.DataFrame:
    """ Authors rating report by count article and by count citation.
    """
    dump_pd = filter_dump_by_years(dump_pd, start_year, end_year)
    authors = defaultdict()
    for _, pub in dump_pd.iterrows():
        for author in pub['authors']['abstracts-retrieval-response']['authors']['author']:

            if not author.get('affiliation'):
                logging.info(f'Author {author["ce:indexed-name"]} not affiliation')
                continue

            if type(author['affiliation']).__name__ == 'dict':
                list_affiliation = [author['affiliation'], ]
            else:
                list_affiliation = author['affiliation']
            if org_id:
                if not any([org_id in x.values() for x in list_affiliation]):
                    continue

            if author['@auid'] not in authors.keys():
                authors[author['@auid']] = {'indexed-name': None, 'doi': [], 'eid': [], 'citedby-count': 0}
            authors[author['@auid']]['indexed-name'] = author['ce:indexed-name']

            authors[author['@auid']]['citedby-count'] += int(pub['citedby-count'])
            if pub.get('prism:doi'):
                authors[author['@auid']]['doi'].append(pub.get('prism:doi'))
            else:
                authors[author['@auid']]['eid'].append(pub.get('eid'))

    df_authors = pd.DataFrame(authors)
    df_pub_count = pd.DataFrame(df_authors[1:3].sum(), columns={'pub-count'}).applymap(len)
    report = pd.concat([df_authors, df_pub_count.T]).T[{'indexed-name', 'citedby-count', 'pub-count'}]
    top_authors = report.sort_values('citedby-count', ascending=False)[{'indexed-name', 'pub-count', 'citedby-count'}]
    return top_authors


def report_top_journals(dump_pd: pd.DataFrame, start_year: str = '1900',
                        end_year: str = str(datetime.now().year+1)) -> pd.DataFrame:
    """ Journals rating report by count article and by count citation.
    """
    dump_pd = filter_dump_by_years(dump_pd, start_year, end_year)
    list_journals = dump_pd[['prism:publicationName', 'prism:issn', 'prism:eIssn', 'citedby-count']]
    list_journals = list_journals.groupby(['prism:publicationName']).agg({'citedby-count': 'sum',
                                                                          'prism:publicationName': 'count'})
    list_journals = list_journals.rename(columns={'prism:publicationName': 'pub-count'})
    list_journals = list_journals.sort_values(by=['citedby-count'], ascending=False)
    return list_journals


def report_get_citedby_count(dump_pd: pd.DataFrame, start_year: str = '1900',
                             end_year: str = str(datetime.now().year+1)) -> pd.DataFrame:
    """ Rating of publications by citation
    """
    dump_pd = filter_dump_by_years(dump_pd, start_year, end_year)
    report_citedby = dump_pd[{'eid', 'prism:doi', 'citedby-count'}].sort_values(by=['citedby-count'], ascending=False)
    return report_citedby


def get_list_journals_from_dump(dump_pd: pd.DataFrame, start_year: str = '1900',
                                end_year: str = str(datetime.now().year+1)) -> pd.DataFrame:
    """ Gets a list of journals.
    """
    dump_pd = filter_dump_by_years(dump_pd, start_year, end_year)
    dump_pd = dump_pd.drop_duplicates(subset=['prism:publicationName'])
    list_journals = dump_pd[['prism:publicationName', 'prism:issn', 'prism:eIssn']].sort_values(by=['prism:publicationName'])
    return list_journals


def get_cited_count_pub_from_api(doi: str, eid: str = None) -> Optional[int]:
    """ Gets the number of citations from the api.
    """
    # todo
    pass


def get_cited_count_pub(doi: str, eid: str = None, dump: pd.DataFrame = pd.DataFrame()) -> Optional[int]:
    """ Gets the number of citations from dump or the api.
    """
    if dump.empty:
        return get_cited_count_pub_from_api(doi, eid)

    try:
        res = int(dump['citedby-count'].loc[dump['prism:doi'] == doi].values[0])
        return res
    except Exception as e:
        logging.exception(f'Error getting cited-count {doi}', e)
        return None
    pass


def analyze_profile_org(org_id: str, list_pubs: List) -> None:
    """ Analyzes the profile of the organization
    - searches for publications not included in the profile
    - reasons for exclusion
    """
    # todo
    pass


def get_header(api_key):
    header = {
        "X-ELS-APIKey": api_key,
        "Accept": 'application/json'
    }
    return header


def main(conf):
    org_id = conf['org_id']
    dump_dir: Path = Path(conf['dump_dir']).resolve()
    dump = get_fresh_pubs_org(org_id, config['apikey'], dump_dir=dump_dir)
    dump = preprocessing_dump(pd.DataFrame(dump))

    top_authors = report_top_authors(dump, org_id=org_id)
    save_dump_to_csv(top_authors, 'top_authors.csv')

    top_journals = report_top_journals(dump)
    save_dump_to_csv(top_journals, 'top_journals.csv')

    list_journals = get_list_journals_from_dump(dump)
    save_dump_to_csv(list_journals, 'list_journals.csv')

    top_pubs_by_cited = report_get_citedby_count(dump)
    save_dump_to_csv(top_pubs_by_cited, 'top_pubs.csv')

    client_mysql = mysql_connect(conf['mysql_host'], conf['mysql_user_name'], conf['mysql_user_pass'],
                                 conf['mysql_db_name'])
    local_list_journals = get_list_journals_from_mysql(client_mysql)

    best_pub = top_pubs_by_cited[['prism:doi', 'citedby-count']].values[0]
    result = get_cited_count_pub(doi=best_pub[0], dump=dump)
    if result == best_pub[1]:
        print('test ok')

    journals_metrics = get_journals_metrics_from_api(config['apikey'], local_list_journals)
    save_dump_to_csv(pd.DataFrame(journals_metrics).T, 'journals_jsr.csv')


def get_config(opts: Dict) -> Optional[Dict]:
    # Load configuration
    config_file = open(CONFIG_FILENAME)
    cfg = json.load(config_file)
    config_file.close()
    try:
        cfg = {**cfg, **opts.__dict__}
        return cfg
    except Exception as e:
        logging.exception(f'Error getting config', e)
        return None


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("-d", "--dump-dir", default=DUMP_DIR_NAME)
    (opts, args) = op.parse_args()
    config = get_config(opts)

    logging.basicConfig(filename=config['log'], level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    logging.info(f"Started with options: {opts}")

    try:
        main(config)
    except Exception as e:

        logging.exception(f"Unexpected error: {e}")
