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
MEMCACHE_TIMEOUT = 1
THREADS_IN_WORKER = 4
NUM_WORKERS = mp.cpu_count()
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_pools, memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()

    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc_flag = False
            try:
                memc = memc_pools.get(timeout=0.1)
            except queue.Empty:
                memc = memcache.Client([memc_addr])

            for n in range(MEMCACHE_MAX_RETRIES):
                memc_flag = memc.set(key, packed)
                if memc_flag:
                    break
                sleep_value = MEMCACHE_TIMEOUT * n
                time.sleep(sleep_value)

            if memc_flag:
                memc_pools.put(memc)
                return True
            else:
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


def handler_job(job_queue, result_queue):
    processed = errors = 0
    logging.info(f"Worker: [{mp.current_process().name}]. Thread: {threading.current_thread().name}. PROCESSING")
    while True:
        try:
            job = job_queue.get(timeout=0.1)
        except queue.Empty:
            logging.info(f"Worker: [{mp.current_process().name}]. Thread: {threading.current_thread().name}. FINISHED")
            result_queue.put((processed, errors))
            return

        memc_pool, memc_addr, appsinstalled, dry_run = job
        ok = insert_appsinstalled(memc_pool, memc_addr, appsinstalled, dry_run)
        if ok:
            processed += 1
        else:
            errors += 1


def main(fn, options):
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

    thread_workers = []
    for i in range(THREADS_IN_WORKER):
        thread = threading.Thread(target=handler_job, args=(job_queue, result_queue))
        thread.daemon = True
        thread_workers.append(thread)

    for thread in thread_workers:
        thread.start()

    processed = errors = 0

    if os.path.splitext(fn)[1][1:].strip().lower() in ['tsv']:
        fd = open(fn)
    else:
        try:
            fd = gzip.open(fn, 'rt')
        except Exception as e:
            raise e

    with fd:
        for line in fd:
            line = line.strip()
            if not line:
                continue
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error(f"Unknown device type: {appsinstalled.dev_type}")
                continue

            job_queue.put((memc_pools[memc_addr], memc_addr, appsinstalled, options.dry))

            if not all(thread.is_alive() for thread in thread_workers):
                break

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
            logging.info(f"Acceptable error rate ({err_rate}). Successfully load")
        else:
            logging.error(f"High error rate ({err_rate} > {NORMAL_ERR_RATE}). Failed load")

    return fn


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
        file_names = sorted(fn for fn in glob.iglob(opts.pattern))
        if file_names:
            print('files:', file_names)
            pool = mp.Pool(processes=NUM_WORKERS)
            handler = partial(main, options=opts)
            for fn in pool.map(handler, file_names):
                dot_rename(fn)
            pool.close()
        else:
            logging.info(f"Logs files not found in dir: {opts.pattern}")
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
        sys.exit(1)
