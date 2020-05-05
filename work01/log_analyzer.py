#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';
import os
import datetime
import re
import logging
import gzip
import argparse
import configparser
from statistics import median
from collections import defaultdict, namedtuple
from string import Template
from typing import Optional, Tuple, List, Dict, Callable

CONFIG_DEFAULT = {'config_filename': 'settings.ini',
                  'logging_path': '',
                  'log_dir': './logs',
                  'pattern_logs_filename': r'nginx-access-ui.log-\d{8}(.gz)*$',
                  'report_template_filename': 'report.html',
                  'report_template_dir': './',
                  'report_dir': 'reports',
                  'report_size': '1000',
                  'parsing_error': '50.0'}


def get_config_filename_from_cmd() -> str:
    """ Возвращает имя конфиг файла из коммандной строки: --config <filename>."""
    parser = argparse.ArgumentParser("Обработка лог-файлов и генерирование отчета")
    parser.add_argument("--config", dest="config_path", default=None, help="Путь к конфигурационному файлу")
    result = parser.parse_args()
    return result.config_path


def get_config(config_default: Dict, cmd_config_path: str = None) -> Optional[Dict]:
    """ Возвращает конфиг-словарь, с учетом параметров в конфиг файле и значений по умолчанию."""
    config_filename = None
    if 'config_filename' in config_default:
        config_filename = config_default.get('config_filename')
    config_filename = cmd_config_path or config_filename
    if not config_default or not config_filename:
        logging.error('Bad config filename')
        return None
    cfg = configparser.ConfigParser()
    try:
        cfg['MAIN'] = dict(config_default)
    except Exception:
        logging.exception('Bad CONFIG_DEFAULT')
        return None
    if not os.path.exists(config_filename) or not os.path.isfile(config_filename):
        logging.error('Ini file not found')
        return None

    try:
        cfg.read(config_filename)
    except Exception:
        logging.exception('Error load settings from ini file')
        return None
    result = dict(cfg['MAIN'])
    try:
        result['parsing_error'] = float(result['parsing_error'])
    except Exception:
        logging.exception('Bad config parameters: parsing_error')
        return None
    try:
        result['report_size'] = int(result['report_size'])
    except Exception:
        logging.exception('Bad config parameters: report_size')
        return None
    return result


def init_logging(logging_output: str = None) -> bool:
    """ Устанавливает формат записи в журнал логов программы.
    :param logging_output: имя файла/потока для вывода журнала логов, при None - вывод в stdout
    :return: True - если все успешно, False - в случае ошибки.
    """
    try:
        logging.basicConfig(filename=str(logging_output),
                            filemode='a',
                            format='[%(asctime)s] %(levelname).1s %(message)s',
                            datefmt='%Y.%m.%d %H:%M:%S',
                            level=logging.INFO)
    except Exception:
        logging.exception('Error initializing the logging system')
        return False
    logging.info('Start.')
    return True


def get_last_logs_filename(logs_dir: str, pattern_log_filename: str) -> Optional[Tuple]:
    """ Возвращает имя последнего файла с логами NGINX.
    :param logs_dir: каталог с логами
    :param pattern_log_filename: шаблон имени файла с логами
    :return: Tuple[<имя последнего файла с логами>, <дата файла>] или None - в случае ошибок.
    """
    if not logs_dir or not os.path.isdir(logs_dir):
        logging.error(f'LOGs directory not found. LOG_DIR: "{logs_dir}"')
        return None
    last_filename = None
    try:
        listdir = os.listdir(logs_dir)
    except Exception:
        logging.exception(f'LOGs directory don`t open. LOG_DIR: "{logs_dir}"')
        return None
    last_filename_date = None
    for file in listdir:
        if re.match(pattern_log_filename, file):
            f_date = re.findall(r'\d{8}', file)
            try:
                filename_date = datetime.datetime.strptime(f_date[0], '%Y%m%d').date()
            except Exception:
                logging.exception(f'Error converting filename to date. File skipped "{file}"')
                continue
            if not last_filename_date or filename_date > last_filename_date:
                last_filename = file
                last_filename_date = filename_date
    if not last_filename:
        logging.info('LOGS files not found in directory')
        return None
    return last_filename, last_filename_date


def get_report_filename(report_date: datetime.date) -> Optional[str]:
    """ Возвращает имя файла HTML отчёта в соответствии с именем файла с логами.
    :param report_date: имя файла с логами NGINX
    :return: имя файла с HTML отчётом или None - в случае ошибок.
    """
    if not report_date:
        return None
    filename = 'report-{}.html'.format(report_date.strftime('%Y.%m.%d'))
    return filename


def check_exist_report_file(directory: str, filename: str) -> bool:
    """ Проверяет наличие файл HTML отчета.
    :param directory: каталог с отчетами
    :param filename: имя файла c HTML отчетом
    :return: True или False - в случае ошибок.
    """
    if filename:
        report_path = os.path.join(directory, filename)
        if report_path and os.path.exists(report_path) and os.path.isfile(report_path):
            return True
    return False


def parser_log_string(log_string: str) -> Optional[Tuple]:
    """ Парсит строку лога NGINX и возвращает кортеж значений (<URL>, <time_request>).
    192.168.122.1 - - [20/Feb/2012:14:56:26 +0000] "GET /index.html HTTP/1.1" 304 0 "-" "http_user_agent" "-" 39.023
    :param log_string: строка лог файла
    :return: tuple(url, request_time).
    """
    pattern_list = list()
    pattern_list.append(r'"(([^"]+)(\s+)([^"]+)(\s+)([^"]+))"')  # get ["GET /index.html HTTP/1.1"] from log_string
    pattern_list.append(r'\d+.\d+$')                             # get ["39.023] from log_string
    generator_pattern = [re.compile(i) for i in pattern_list]
    list_result = [n.search(log_string) for n in generator_pattern]
    if list_result[0] is not None and list_result[1] is not None:
        url = list_result[0].group().split(' ')[1]
        time_request = list_result[1].group()
        result = url, time_request
        return result
    return None


def get_func_open_file_by_extension(filename: str) -> Callable:
    """" Возвращает функцию для открытия файлов в зависимости от расширения файла.
    gz - gzip.open, else open.
    """
    if re.search(r"[.]gz$", filename):
        return gzip.open
    else:
        return open


def get_statistics_logs(log_dir: str, log_filename: str) -> Optional[Tuple[List[Dict], float]]:
    """ Читает и анализирует файл логов NGINX и возвращает данные по найденным URL и процент ошибок
    :param log_dir: - каталог с логами NGINX
    :param log_filename: - имя файла с логами NGINX
    :return tuple(list(dict), error_rate) или False - в случае ошибок.
    """
    if not log_filename:
        return None
    log_path = os.path.join(log_dir, log_filename)
    func_openfile = get_func_open_file_by_extension(log_path)
    try:
        file = func_openfile(log_path, 'rt', encoding='UTF-8')
    except Exception:
        logging.exception(f'Log file open error: "{log_path}"')
        return None
    count_error_string = 0                  # count of error string in LOG file
    count_log_string = 0                    # count string in LOG file
    time_sum = 0.0                          # sum of time_request in all URL
    urls_time_request: defaultdict = defaultdict(list)      # словарь key=URL, value=list(time_request)
    # создаем генератор для построчного анализа лог файла на выходе tuple(<url>, <time_request>)
    list_res = map(parser_log_string, (line.rstrip() for line in file))
    # формируем словарь {<url>: list(<time_request>)}
    for res in list_res:                    # проходим по строкам файла с логами
        count_log_string += 1               # считаем общее кол-во строк в лог файле
        if not res:                         # битые строки в логе пропускаем
            count_error_string += 1         # считаем битые строки
            continue
        url = res[0]                        # URL - res[0]
        time_request = float(res[1])        # TIME_REQUEST - res[1]
        # новый URL - > добавляем новый ключ, в список значений добавляем time_request
        urls_time_request[url].append(time_request)
        time_sum += time_request
    file.close()
    result = []
    count_sum = count_log_string - count_error_string
    # считаем статистику по URL -> list({'url': <url>, 'count': <url_count> ...})
    for key, value in urls_time_request.items():
        url_stat = dict()
        url_stat['url'] = key
        url_stat['count'] = len(value)
        url_stat['count_perc'] = url_stat['count'] * 100 / count_sum
        url_stat['time_sum'] = sum(value)
        url_stat['time_max'] = max(value)
        url_stat['time_perc'] = round(url_stat['time_sum'] * 100 / (time_sum or 1), 2)  # div 0: if time_sum == 0
        url_stat['time_avg'] = url_stat['time_sum'] / (url_stat['count'])
        median_value = median(value)
        url_stat['time_med'] = median_value
        result.append(url_stat)
    error = count_error_string * 100 / (count_log_string or 1)
    return result, error


def get_limit_report(urls_statistics: List, report_size: int) -> Optional[List]:
    """ Возвращает данные для отчёта по URL с наибольшим временем обработки.
    :param urls_statistics: список словарейы с данными по всем URL из файла логов NGINX
    :param report_size: кол-во URL выводимых в отчет
    :return: словарь с данными по URL или None - если исходный словарь пустой.
    """
    if not urls_statistics:
        return None
    # сортируем список по ключу словаря 'time_sum'
    try:
        report_sort = sorted(urls_statistics, key=lambda x: x['time_sum'], reverse=True)
    except Exception:
        logging.exception('!WOW!')
        return None
    # возвращаем дынные по ограниченному колиеству URL
    result = report_sort[:report_size]
    return result


def save_report_to_html_file(report_path: str,
                             template_path: str,
                             report_data: List) -> bool:
    """ Записывает отчетные табличные данные в HTML файл.
    :param report_path: - путь и имя файла с отчётом
    :param template_path: - путь и имя файла шаблона HTML отчета
    :param report_data: - отчет
    :return: True - если данные успешно записаны в файл, иначе False.
    """
    try:
        template_html_file = open(template_path)
    except Exception:
        logging.exception(f'Bad template HTML report file. "{template_path}"')
        return False
    template_report = ''.join(template_html_file.readlines())
    template_report = Template(template_report).safe_substitute(table_json=report_data)
    # открываем для записи файл HTML
    try:
        report_file = open(report_path, mode='w')
    except Exception:
        logging.exception(f'Bad HTML report file. "{report_path}"')
        return False
    # записываем отчет в HTML файл
    try:
        report_file.write(template_report)
    except Exception:
        logging.exception(f'Error write to HTML report file. "{report_path}"')
        return False
    return True


def main(cfg: Dict):
    if not cfg:
        return
    report_dir = cfg['report_dir']
    logging_ok = init_logging(cfg['logging_path'])
    if not logging_ok:
        return
    result_last_logs_filename = get_last_logs_filename(cfg['log_dir'], cfg['pattern_logs_filename'])
    if not result_last_logs_filename:
        return
    last_logs_file = namedtuple('last_logs_file', ['filename', 'date'])
    last_logs_file.filename, last_logs_file.date = result_last_logs_filename
    logging.info(f'Last LOGs file found. "{last_logs_file.filename}"')

    report_filename = get_report_filename(last_logs_file.date)
    if not report_filename:
        logging.exception(f'Error getting report filename.')
        return
    # создаем каталог с HTML отчетом
    try:
        os.makedirs(report_dir, exist_ok=True)
    except Exception:
        logging.exception(f'Error HTML report directory. "{report_dir}"')
        return

    result_exist_report_file = check_exist_report_file(report_dir, report_filename)
    if result_exist_report_file:    # файл отчета уже существует, повторный анализ не производим, выход
        logging.info(f'HTML report file already exists. Reanalysis canceled. "{os.path.join(report_dir, report_filename)}"')
        return

    result_statistics_logs = get_statistics_logs(cfg['log_dir'], last_logs_file.filename)
    if not result_statistics_logs:
        return
    statistics_logs = namedtuple('statistics_logs', ['data', 'error_rate'])
    statistics_logs.data, statistics_logs.error_rate = result_statistics_logs
    if statistics_logs.error_rate > cfg['parsing_error']:
        logging.error('To many bad LOGS in file. Exit')
        return
    report_data = get_limit_report(statistics_logs.data, cfg['report_size'])
    if not report_data:
        return

    report_path = os.path.join(report_dir, report_filename)
    report_template_path = os.path.join(cfg['report_template_dir'],
                                        cfg['report_template_filename'])
    result_flag = save_report_to_html_file(report_path, report_template_path, report_data)
    if not result_flag:
        return
    logging.info(f'Report successfully created: "{report_filename}"')
    print(f'Report successfully created: "{report_filename}"')


if __name__ == "__main__":
    config_path_from_cmd = get_config_filename_from_cmd()
    config = get_config(CONFIG_DEFAULT, config_path_from_cmd)
    if not config:
        print('Error getting config')
    else:
        try:
            main(config)
        except KeyboardInterrupt:
            print('Execution is interrupted by pressing Control+C')
