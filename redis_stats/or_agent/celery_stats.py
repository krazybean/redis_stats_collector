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


class StatsWorker(threading.Thread):
    def __init__(self, collection_interval, container_hostname, container_ip, container_port):
        super(StatsWorker, self).__init__()

        self.collection_interval = collection_interval
        self.container_hostname = container_hostname
        self.container_ip = container_ip
        self.container_port = container_port

        logger_name = "StatsWorker_{0}".format(self.container_hostname)
        self.logger = logging.getLogger(logger_name)
        self._metric_cache = Tree()

    def add_data_point(self, hostname, data_type, metric_name, value, timestamp, uptime, uptime_in_millis):
        """Add a data point to the timeseries collections."""
        datetime_minute = timestamp.replace(second=0, microsecond=0)

        if metric_name in self._metric_cache:
            cached_metric = self._metric_cache[metric_name]

            # Gauge data is stored as raw values
            if data_type == 'gauge':
                calculated_value = value

            # Counter data is calculated as per-second values
            elif data_type == 'counter' or 'counter_to_percent':
                uptime_elapsed = uptime - cached_metric['uptime']

                # The mongos uptime precision is only to the second, so treat subsecond as one second
                if uptime_elapsed == 0:
                    uptime_elapsed = 1

                if uptime_in_millis:
                    seconds_elapsed = uptime_elapsed / 1000
                else:
                    seconds_elapsed = uptime_elapsed

                value_difference = value - cached_metric['value']

                if data_type == 'counter':
                    calculated_value = int(value_difference / seconds_elapsed)

                elif data_type == 'counter_to_percent':
                    if not value_difference:
                        calculated_value = 0
                    else:
                        calculated_value = int((seconds_elapsed / value_difference) * 100)

            metric_document = cached_metric['document']

            # If this is the first minute we've collected, set the minute
            if 'last_checked_minute' not in cached_metric:
                cached_metric['last_checked_minute'] = datetime_minute

            # If the minute has turned over, store the values in the db
            elif cached_metric['last_checked_minute'] != datetime_minute:

                # insert a new doc into the minute collection
                metric_document.pop('value', None)
                tasks.add_minute_stat.delay(metric_document)

                # Update the hour document with an entry for this minute
                datetime_hour = datetime_minute.replace(minute=0)
                entries = metric_document['entries']
                total = metric_document['total']

                if entries and total:
                    minute_average = int(total / entries)
                else:
                    minute_average = 0

                update_predicate = {'host': self.container_hostname,
                                    'hour': datetime_hour,
                                    'name': metric_name}
                update_action = {'$set': {"values.{0}".format(timestamp.minute): minute_average,
                                          'type': data_type},
                                 '$inc': {'entries': 1, 'total': minute_average}}
                tasks.update_hour_stat.delay(update_predicate, update_action)

                # zero the minute document
                metric_document['minute'] = datetime_minute
                metric_document['entries'] = 0
                metric_document['total'] = 0
                metric_document['values'] = {}

                # Keep a running total of hour values for day calculation
                if 'hour_entries' in cached_metric:
                    cached_metric['hour_entries'] += 1
                    cached_metric['hour_total'] += minute_average
                else:
                    cached_metric['hour_entries'] = 1
                    cached_metric['hour_total'] = minute_average

                # First time we've collected in this hour
                if 'last_checked_hour' not in cached_metric:
                    cached_metric['last_checked_hour'] = datetime_hour
                # If the hour's turned over, add an entry to the day collection
                elif cached_metric['last_checked_hour'] != datetime_hour:
                    datetime_day = datetime_hour.replace(hour=0)

                    hour_entries = cached_metric['hour_entries']
                    hour_total = cached_metric['hour_total']

                    if hour_entries and hour_total:
                        hour_average = int(hour_total / hour_entries)
                    else:
                        hour_average = 0

                    update_predicate = {'host': self.container_hostname,
                                        'day': datetime_day,
                                        'name': metric_name}
                    update_action = {'$set': {"values.{0}".format(timestamp.hour): hour_average,
                                              'type': data_type},
                                     '$inc': {'entries': 1, 'total': hour_average}}
                    tasks.update_day_stat.delay(update_predicate, update_action)

                    # update the available metric names for this host
                    update_predicate = {'host': self.container_hostname}
                    update_action = {"$addToSet": {"metrics": metric_name}}
                    tasks.update_metric_names.delay(update_predicate, update_action)

                    cached_metric['hour_entries'] = 0
                    cached_metric['hour_total'] = 0
                    cached_metric['last_checked_hour'] = datetime_hour

            # Add current values to cached metric document
            metric_document['entries'] += 1
            metric_document['total'] += calculated_value
            metric_document['value'] = value
            metric_document['values'][str(timestamp.second)] = calculated_value
            cached_metric['last_checked_minute'] = datetime_minute

        else:
            # The first time we've seen this metric, prep the cache for the next data point.
            self._metric_cache[metric_name]['document'] = {"host" : self.container_hostname,
                                                           "name" : '{0}'.format(metric_name),
                                                           "minute" : datetime_minute,
                                                           "entries" : 0,
                                                           "total" : 0,
                                                           "type" : data_type,
                                                           "values" : {}}
        self._metric_cache[metric_name]['uptime'] = uptime
        self._metric_cache[metric_name]['value'] = value


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
