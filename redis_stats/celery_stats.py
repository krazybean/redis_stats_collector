from __future__ import division

import config
import logging
import pymongo
import tasks
import threading


class Tree(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value


class AuthenticationException(Exception):
    pass


class StatsWorker:
    def __init__(self):
        logger_name = "StatsWorker_{0}".format("None")
        self.logger = logging.getLogger(logger_name)
        self._metric_cache = Tree()

    def add_data_point(self, hostname, data_type, metric_name, value, timestamp, uptime, uptime_in_millis):
        """Add a data point to the timeseries collections."""
        datetime_minute = timestamp.replace(second=0, microsecond=0)
        datetime_hour = datetime_minute.replace(minute=0)
        cache_flag = True
        cached_metric = self._metric_cache[metric_name]
        datetime_day = datetime_hour.replace(hour=0)

        print "Hour updated"
        hour_entries = cached_metric['hour_entries']
        hour_total = cached_metric['hour_total']

        if hour_entries and hour_total:
            hour_average = int(hour_total / hour_entries)
        else:
            hour_average = 0

        update_predicate = {'host': hostname,
                            'hour': datetime_hour,
                            'name': metric_name}
        update_action = {'$set': {"values.{0}".format(timestamp.hour): hour_average,
                                  'type': data_type},
                         '$inc': {'entries': 1, 'total': hour_average}}
        print "sending hour stats to rabbit"
        tasks.update_hour_stat.delay(update_predicate, update_action)

        # update the available metric names for this host
        update_predicate = {'host': hostname}
        update_action = {"$addToSet": {"metrics": metric_name}}
        tasks.update_metric_names.delay(update_predicate, update_action)

        cached_metric['hour_entries'] = 0
        cached_metric['hour_total'] = 0
        cached_metric['last_checked_hour'] = datetime_hour

        # Add current values to cached metric document
        cached_metric['last_checked_minute'] = datetime_minute



    def get_authenticated_connection(self, host, port=None):
        """Returns an authenticated connection to a particular host."""
        mongo_connection = pymongo.MongoClient(host, port, connectTimeoutMS=1000)
        admin_connection = mongo_connection['admin']

        try:
            # ghettolicious
            admin_connection.authenticate(config.AUTHENTICATION_PAIRS[0][0], config.AUTHENTICATION_PAIRS[0][1], 'admin')
        except (pymongo.errors.OperationFailure, pymongo.errors.ConnectionFailure):
            try:
                admin_connection.authenticate(config.AUTHENTICATION_PAIRS[1][0], config.AUTHENTICATION_PAIRS[1][1], 'admin')
            except (pymongo.errors.OperationFailure, pymongo.errors.ConnectionFailure):
                # Some customers don't have auth enabled, but there's no way to tell which ones
                # so log an error and keep on truckin'
                self.logger.error("Cannot authenticate to admin database (customer with auth disabled?)")

        return mongo_connection
