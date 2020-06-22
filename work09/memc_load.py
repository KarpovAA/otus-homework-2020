#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import time
import multiprocessing as mp
import threading
import queue
from optparse import OptionParser
from functools import partial
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import libs.appsinstalled_pb2 as appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
MEMCACHE_MAX_RETRIES = 3
MEMCACHE_TIMEOUT = 3
MEMCACHE_NUM_KEYS_IN_SET_MULTI = 100
THREADS_IN_WORKER = 4
NUM_WORKERS = mp.cpu_count()
QUEUE_TIMEOUT = 0.0001
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_client, memc_addr, list_appsinstalled, dry_run=False):
    data = {}
    for appsinstalled in list_appsinstalled:
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        if dry_run:
            data[key] = ua
        else:
            data[key] = packed

    try:
        if dry_run:
            for key, ua in data.items():
                logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc_flag = False
            for n in range(MEMCACHE_MAX_RETRIES):
                memc_flag = memc_client.set_multi(data)
                if not memc_flag:
                    # set_multi return empty list [] - it's OK
                    break
                sleep_value = MEMCACHE_TIMEOUT * n
                time.sleep(sleep_value)

            if memc_flag:
                # set_multi return not null list - it's ERROR.
                # List of keys which failed to be stored
                logging.error(f"Memcached server connection error {memc_addr}")
                return False
    except Exception as e:
        logging.exception(f"Cannot write to memcached {memc_addr}: {e}")
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info(f"Not all user apps are digits: `{line}`")
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info(f"Invalid geo coords: `{line}`")
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def handler_job(job_queue, result_queue, finish_readfile,
                memc_pools, device_memc, dry_run=False):
    processed = errors = 0
    logging.info(f"Worker: [{mp.current_process().name}]. Thread: {threading.current_thread().name}. PROCESSING")
    buffer = collections.defaultdict(list)
    while True:
        flag_push_buffer = False
        try:
            job = job_queue.get(timeout=QUEUE_TIMEOUT)
        except queue.Empty:
            if not finish_readfile.empty():
                # waiting finish reading file
                continue
            flag_push_buffer = False
            for key in buffer.keys():
                if len(buffer[key]) > 0:
                    # buffer not empty
                    flag_push_buffer = True
                    buffer_key = key
                    break
            if not flag_push_buffer:
                logging.info(f"Worker: [{mp.current_process().name}]. Thread: {threading.current_thread().name}. FINISHED")
                result_queue.put((processed, errors))
                return

        if not flag_push_buffer:
            line = job

            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error(f"Unknown device type: {appsinstalled.dev_type}")
                continue

            buffer[memc_addr].append(appsinstalled)

            if len(buffer[memc_addr]) <= MEMCACHE_NUM_KEYS_IN_SET_MULTI:
                continue

            try:
                memc_client = memc_pools[memc_addr].get(timeout=QUEUE_TIMEOUT)
            except queue.Empty:
                memc_client = memcache.Client([memc_addr])
                memc_pools[memc_addr].put(memc_client)
        else:
            memc_addr = buffer_key
            memc_client = memcache.Client([memc_addr])

        ok = insert_appsinstalled(memc_client, memc_addr, buffer[memc_addr], dry_run)

        if ok:
            processed += 1
        else:
            errors += 1

        buffer[memc_addr].clear()


def handler_logfile(fn, options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    logging.info(f"[{mp.current_process().name}] Processing {fn}")

    memc_pools = collections.defaultdict(queue.Queue)
    job_queue = queue.Queue(maxsize=0)
    result_queue = queue.Queue(maxsize=0)
    finish_readfile = queue.Queue(maxsize=0)    # not empty queue - reading file, empty - finish
    finish_readfile.put("1")  # queue -> start reading file

    thread_workers = []
    for i in range(THREADS_IN_WORKER):
        thread = threading.Thread(target=handler_job,
                                  args=(job_queue, result_queue, finish_readfile,
                                        memc_pools, device_memc, options.dry))
        thread.daemon = True
        thread_workers.append(thread)

    for thread in thread_workers:
        thread.start()

    processed = errors = 0

    fd = gzip.open(fn, 'rt')

    with fd:
        for line in fd:
            line = line.strip()
            if not line:
                continue

            job_queue.put(line)

            if not all(thread.is_alive() for thread in thread_workers):
                break

    finish_readfile.get()       # queue -> finish reading file
    logging.info(f"[{mp.current_process().name}] Finished reading file {fn}")

    for thread in thread_workers:
        if thread.is_alive():
            thread.join()

    while not result_queue.empty():
        processed_worker, errors_worker = result_queue.get()
        processed += processed_worker
        errors += errors_worker

    if processed:
        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info(f"[{mp.current_process().name}] Acceptable error rate ({err_rate}). Successfully load")
        else:
            logging.error(f"[{mp.current_process().name}] High error rate ({err_rate} > {NORMAL_ERR_RATE}). Failed load")

    return fn


def main(options):
    pool = mp.Pool(processes=NUM_WORKERS)
    file_names = sorted(fn for fn in glob.iglob(options.pattern))
    handler = partial(handler_logfile, options=options)
    for fn in pool.imap(handler, file_names):
        dot_rename(fn)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="./data/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info(f"Memc loader started with options: {opts}")
    try:
        main(opts)

    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
        sys.exit(1)
