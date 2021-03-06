#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import log_analyzer
import unittest
import datetime


class TestParserLogString(unittest.TestCase):
    test_dir = './test/'
    cfg = dict(log_analyzer.CONFIG_DEFAULT)

    def test_get_config(self):
        cfg = dict(self.cfg)
        cfg_result = dict(self.cfg)
        cfg_result['parsing_error'] = float(cfg_result['parsing_error'])
        cfg_result['report_size'] = int(cfg_result['report_size'])
        null_ini = self.test_dir + 'null.ini'
        self.assertEqual(log_analyzer.get_config(dict(), None), None)
        self.assertEqual(log_analyzer.get_config(cfg, null_ini), cfg_result)
        cfg['config_filename'] = null_ini
        cfg_result['config_filename'] = null_ini
        self.assertEqual(log_analyzer.get_config(cfg, None), cfg_result)

    def test_init_logging(self):
        self.assertEqual(log_analyzer.init_logging('error.log'), True)

    def test_get_last_logs_file(self):
        self.assertEqual(log_analyzer.get_last_logs_file(None, None), None)
        self.assertEqual(log_analyzer.get_last_logs_file(self.test_dir,
                                                             self.cfg['pattern_logs_filename']),
                         ('nginx-access-ui.log-20190505',
                          datetime.datetime.strptime('20190505', '%Y%m%d').date(),))

    def test_get_report_filename(self):
        list_string_result = list()

        list_string_result.append([datetime.datetime.strptime('20190505', '%Y%m%d').date(),
                                   'report-2019.05.05.html'])
        list_string_result.append(['', None])
        list_string_result.append([datetime.datetime.strptime('20200101', '%Y%m%d').date(),
                                    'report-2020.01.01.html'])
        for line in list_string_result:
            self.assertEqual(log_analyzer.get_report_filename(line[0]), line[1])

    def test_check_exist_report_file(self):
        self.assertEqual(log_analyzer.check_exist_report_file(None, None), False)
        self.assertEqual(log_analyzer.check_exist_report_file('./', 'report.html'), True)

    def test_parser_log_string(self):
        list_string_result = list()
        list_string_result.append(['192.168.122.1 - "GET /index.html HTTP/1.1" "-" 39.023',
                                   ('/index.html', '39.023')])
        list_string_result.append(['', None])
        list_string_result.append(['"GET / HTTP/1.1" 9.99', ('/', '9.99')])
        list_string_result.append(['8.8.8. "GET /index.html HTTP/1.1" "-" ', None])
        for line in list_string_result:
            self.assertEqual(log_analyzer.parser_log_string(line[0]), line[1])

    def test_get_statistics_log(self):
        list_string_result = list()
        list_string_result.append([self.test_dir, 'nginx-access-ui.log-20190505',
                                   ([{'count': 3,
                                      'count_perc': 100.0,
                                      'url': '/index.html',
                                      'time_sum': 15.0,
                                      'time_perc': 100.00,
                                      'time_avg': 5.0, 'time_max': 5.0,
                                      'time_med': 5.0}], 50.0)])
        list_string_result.append([self.test_dir, 'fail.log', ([], 100.0)])
        list_string_result.append([None, None, None])
        for line in list_string_result:
            self.assertEqual(log_analyzer.get_statistics_logs(line[0], line[1]), line[2])

    def test_get_limit_report(self):
        list_string_result = list()
        list_string_result.append([[{'url': '/index.html', 'time_sum': 15.0},
                                  {'url': '/index.html', 'time_sum': 25.0}],
                                   1,
                                   [{'url': '/index.html', 'time_sum': 25.0}]])
        list_string_result.append([None, None, None])
        list_string_result.append([[{'time_sum': 10}], 10, [{'time_sum': 10}]])
        for line in list_string_result:
            self.assertEqual(log_analyzer.get_limit_report(line[0], line[1]), line[2])


if __name__ == '__main__':
    unittest.main()
