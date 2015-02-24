#!/usr/bin/env python

##---------------------------------------------------------------------------
## ObjectRocket MongoDB Collector
##
## This version of the collector spawns a thread for each vz on a brick.
## The thread will poll the mongo process every interval and collect server
## and db stats.
##---------------------------------------------------------------------------

import logging
import config
import time

from datetime import datetime
from celery_stats import StatsWorker
from logging.handlers import RotatingFileHandler

LOG_LEVEL = logging.INFO

root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

# 10MB rollover
file_handler = RotatingFileHandler('redis_stats_collector.log', maxBytes=10485760, backupCount=2)
file_handler.setFormatter(formatter)

root_logger.addHandler(file_handler)


class Tree(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value


class AuthenticationException(Exception):
    pass


""" Main Redis collector class """
class RedisStatsWorker(StatsWorker):
    def __init__(self, collection_interval, container_hostname, container_ip, container_port):
        super(RedisStatsWorker, self).__init__(collection_interval, container_hostname, container_ip, container_port)

    def run(self):
        """Main loop for this thread."""
        while True:
            print "Inside RedisStatsWorker: Sleeping: {0}s...".format(collection_interval)
            time.sleep(self.collection_interval)

    def collect_stats(self):
        """Queries the server and each database for statistics, and processes the results."""

class RedisStatsCollector(object):
    """Starts a stats collection worker for each vz on this brick. Checks
       at intervals and starts new workers if vzs are added"""
    def __init__(self, collection_interval):
        self.collection_interval = collection_interval
        self.host_stat_workers = []

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)


    def refresh_stats_workers(self, worker_start_interval):
        print "testing: {0}".format(datetime.now())
        # For loop, the actual workhorse
        redis_stats_worker = RedisStatsWorker(collection_interval, "hostname", "ip", "port")
        redis_stats_worker.run()
        time.sleep(worker_start_interval)

        self.logger.info("Worker pool refreshed.")

if __name__ == "__main__":
    # How often (in seconds) each thread will collect stats for a given vz
    collection_interval = 60

    # Since we're parsing redskull output we only need a single thread every minute
    worker_start_interval = 0

    redis_stats_collector = RedisStatsCollector(collection_interval)
    while True:
        try:
            redis_stats_collector.refresh_stats_workers(collection_interval)
           
        except KeyboardInterrupt:
            exit(0)
