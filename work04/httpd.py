#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import asyncore
import asynchat
import socket
import argparse
import mimetypes
import multiprocessing
import urllib.parse
from time import strftime, gmtime
from collections import namedtuple
from typing import Optional, Tuple


def get_path_from_filter_url(path):
    path = path.split('?', 1)[0]
    path = path.split('#', 1)[0]
    path = urllib.parse.unquote(path)
    path = os.path.normpath(path)
    parts = path.split('/')
    path = os.path.join(DOCUMENT_ROOT, *parts)
    return path


class HTTPRequestHandler(asynchat.async_chat):
    def __init__(self, sock):
        asynchat.async_chat.__init__(self, sock)
        self.set_terminator(b"\r\n\r\n")
        self.socket = sock
        self.f = None

    def collect_incoming_data(self, data):
        self._collect_incoming_data(data)

    def found_terminator(self):
        self.parse_request()

    def parse_request(self):
        if not self.parse_headers():
            self.send_error(400)
            self.handle_close()
            return None
        if self.method not in ['GET', 'HEAD']:
            self.send_error(405)
            self.handle_close()
            return None
        self.handle_request()

    def parse_headers(self):
        try:
            headers_list = self._get_data().decode()
            headers_list = headers_list.split("\r\n")
            method, uri, protocol = headers_list[0].split(" ")
            headers = {"method": method, "uri": uri, "protocol": protocol}
            for header in headers_list[1:]:
                header = header.split(':', 1)
                if len(header) > 1:
                    headers[header[0]] = header[1].strip()
            self.method = method
            self.request_uri = uri
            self.http_protocol = protocol
            self.headers = headers
            return True
        except Exception as e:
            return False

    def send_error(self, code):
        try:
            message = self.responses[code]
        except KeyError:
            message = None
        self.send_response(code, message)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Connection", "close")
        self.end_headers()

    def send_header(self, keyword, value):
        self.push("{}: {}\r\n".format(keyword, value).encode())

    def get_file_to_send(self):
        file = namedtuple('f', 'path')
        file.path = get_path_from_filter_url(self.request_uri)
        log.info(f'request_uri: "{file.path}"')
        if os.path.isdir(file.path):
            file.path = os.path.join(file.path, "index.html")
            if not os.path.exists(file.path):
                return None
        try:
            file.f = open(file.path, mode='rb')
        except IOError:
            return None
        return file

    def send_head(self, f):
        """ Send Response Header
        :param f: Tuple[file, path]
        :return:
        """
        if not f:
            self.send_response(404)
            self.handle_close()

        _, ext = os.path.splitext(f.path)
        ctype = mimetypes.types_map[ext.lower()]
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", os.path.getsize(f.path))
        self.end_headers()
        return

    def end_headers(self):
        self.push("\r\n".encode())

    def handle_request(self):
        method_name = 'do_' + self.method
        if not hasattr(self, method_name):
            self.send_error(405)
            self.handle_close()
            return
        handler = getattr(self, method_name)
        handler()

    def send_response(self, code, message=None):
        if message is None:
            if code in self.responses:
                message = self.responses[code]
            else:
                message = ''
        data_push = f"{self.http_protocol} {code} {message}\r\n"
        log.info(f'Response code: "{code}"')
        self.push(data_push.encode())
        self.send_header("Server", "simple-http-server")
        self.send_header("Date", strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime()))

    def do_GET(self):
        file = self.get_file_to_send()
        self.send_head(file)
        if file:
            while True:
                data_small_block = file.f.read(4096)
                if data_small_block == b'':
                    break
                self.push(data_small_block)
            file.f.close()
            self.handle_close()

    def do_HEAD(self):
        file = self.get_file_to_send()
        self.send_head(file)
        if file:
            file.f.close()
            self.handle_close()

    responses = {
        200: 'OK',
        400: 'Bad Request',
        403: 'Forbidden',
        404: 'Not Found',
        405: 'Method Not Allowed',
    }


class HTTPServer(asyncore.dispatcher_with_send):
    def __init__(self, host="127.0.0.1", port=8000):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        try:
            self.bind((host, port))
            self.listen(5)
            log.info(f"Listening on address {host}:{port}. PID: {os.getpid()}")
        except Exception as e:
            log.exception("Socket error")
            self.close()
            raise e

    def handle_accepted(self, sock, addr):
        log.info(f"Incoming connection from {addr}. PID: {os.getpid()}")
        handler = HTTPRequestHandler(sock)

    def serve_forever(self):
        try:
            asyncore.loop(timeout=1, use_poll=True)
        except KeyboardInterrupt:
            log.debug("Worker shutdown")
        finally:
            self.close()


def parse_cmd_args():
    parser = argparse.ArgumentParser("Simple HTTP server")
    parser.add_argument("--host", dest="host", default="127.0.0.1")
    parser.add_argument("--port", dest="port", type=int, default=8000)
    parser.add_argument("--logfile", dest="logfile", default=None)
    parser.add_argument("-w", dest="n_workers", type=int, default=1)
    parser.add_argument("-r", dest="document_root", default=".")
    return parser.parse_args()


def run():
    server = HTTPServer(host=args.host, port=args.port)
    server.serve_forever()


if __name__ == "__main__":
    args = parse_cmd_args()
    logging.basicConfig(filename=None, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    log = logging.getLogger(__name__)
    log.info(f"Starting server at {args.host} {args.port}")

    DOCUMENT_ROOT = args.document_root


    server = HTTPServer(host=args.host, port=args.port)
    server.serve_forever()

    # for _ in range(args.n_workers):
    #     p = multiprocessing.Process(target=run)
    #     p.start()
