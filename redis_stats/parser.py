import sys
import json
import requests
import tasks
import config
import logging
from celery_stats import StatsWorker
from mongo_conn import data_setup
from pprint import pprint
from bson import json_util
from datetime import datetime
from collections import OrderedDict
from logging.handlers import RotatingFileHandler

# Logging section
LOG_LEVEL = config.LOG_LEVEL
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
file_handler = RotatingFileHandler(config.LOG_FILE,
                                   maxBytes=config.LOG_MAX_SIZE,
                                   backupCount=config.LOG_RETENTION)
file_handler.setFormatter(formatter)

dryrun = None
if '--dry-run' in sys.argv:
    dryrun = True


class RedisParser:
    """ Single script to pull dataset and restructure """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(LOG_LEVEL)
        # Iterator: Remove tag after purchase.
        self.MAX = 3

    def capture_redskull(self):
        """ Connection to redskull, #TODO need to clean this up """
        try:
            r = requests.get(config.REDSKULL_HOST)
        except requests.exceptions.ConnectionError:
            raise Exception("ParserError: Unable to find redskull host.")
        if r.status_code != 200:
            raise Exception("ParserError: Recieved non-200 response.")
        return json.loads(r.content)

    def sub_category_block(self, data, cat, subcat):
        """ Quantify the subcategory to the appropriate level """
        # Problem in this function on parsing data object
        if not cat or not subcat:
            return
        try:
            if cat == 'Stats':
                datablock = data['Master'].get(subcat, 0)
            else:
                datablock = data['Master'][cat].get(subcat, 0)
            return datablock
        except (TypeError, AttributeError) as e:
            self.logger.warning("Datablock: {0} - {1} [Error]: {2}".format(
                data['Name'], subcat, e))

    def ship_data(self, instance, segment, static_time):
        """ Send the data using celery_stats to OR """
        mongoconn = data_setup()
        mongoconn.insert(segment)
        statsworker = StatsWorker()
        self.logger.info("Sending: {0} value: {1} for inst {2}".format(
            segment['name'], segment['values'], instance))
        statsworker.add_data_point(segment['host'], "redis_type",
                                   segment['name'], segment['values'],
                                   static_time, 0, 0)

    def data_processor(self, datablock, instance, cat, subcat, static_time,
                       interval_segment, outline):
        """ rejoin the data into a structured output """
        for dataitem in datablock:
            segment = OrderedDict()
            values = {}
            segment['host'] = instance
            segment['entries'] = 1
            segment[interval_segment] = static_time
            structure = ("redis", cat, subcat, dataitem)
            segment['name'] = ('.').join(structure)
            if datablock[dataitem] == "":
                values["0"] = int(0)
            else:
                values["0"] = datablock[dataitem]
            segment['total'] = datablock[dataitem]
            segment['values'] = values
            tag = "{0}.{1}".format(segment['host'],
                                   str(static_time))
            outline[tag] = segment
            if dryrun:
                self.logger.info("DryRun, sending data:{0} - {1}".format(
                    instance, segment['name']))
                continue
            self.ship_data(instance, segment, static_time)
        return outline

    def format_results(self, category=None, interval_segment=None):
        """ Formatting each stat into its own object """
        if not category or not interval_segment:
            return
        try:
            datablock = None
            static_time = datetime.utcnow()
            datetime_minute = static_time.replace(second=0, microsecond=0)
            datetime_hour = datetime_minute.replace(minute=0)
            rsdata = self.capture_redskull()
            outline = {}
            for data in rsdata['Data'][:self.MAX]:
                instance = data['Name']
                try:
                    for cat in category:
                        if cat:
                            for subcat in category[cat]:
                                if subcat:
                                    if cat != 'Stats':
                                        datablock = self.sub_category_block(
                                            data, cat, subcat)
                                    if datablock:
                                        if len(datablock) > 1:
                                            outline = self.data_processor(
                                                datablock, instance, cat,
                                                subcat, static_time,
                                                interval_segment, outline)
                except KeyError:
                    self.logger.error("Failed Parent {0}: {1}".format(cat,
                                                                      subcat))
        except TypeError as te:
            self.logger.error("Missing category results: {0} - {1}".format(
                category[cat], te))
        return outline

    def main_parse(self, interval_segment='hour'):
        """ Partytime at jcru's house """
        categories = {'Stats': ['LatencyHistory'],
                      'Info': ['Client', 'Memory', 'Stats',
                               'Persistence', 'Commandstats']}
        return self.format_results(categories, interval_segment)


if __name__ == '__main__':
    p = RedisParser()
    result = p.main_parse()
