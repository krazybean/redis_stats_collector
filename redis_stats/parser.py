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

    def format_results(self, category=None, interval_segment=None):
        """ Formatting each stat into its own object """
        if not category or not interval_segment:
            return
        try:
            static_time = datetime.utcnow()
            datetime_minute = static_time.replace(second=0, microsecond=0)
            datetime_hour = datetime_minute.replace(minute=0)
            rsdata = self.capture_redskull()
            print "Evaluating {0} records".format(len(rsdata['Data']))
            outline = {}
            for data in rsdata['Data']:
                try:
                    for cat in category:
                        if cat:
                            for subcat in category[cat]:
                                if subcat:
                                    if cat == 'stats':
                                        try:
                                            datablock = data['Master'].get(subcat,
                                                                           0)
                                        except (TypeError, AttributeError):
                                            self.logger.error("Datablock: {0} - {1}".format(
                                                  data['Name'], subcat))
                                            pass
                                    else:
                                        try:
                                            datablock = data['Master']['Info'].get(subcat,
                                                                                   0)
                                        except (TypeError, AttributeError):
                                            self.logger.error("Datablock: {0} - {1}".format(
                                                  data['Name'], subcat))
                                            pass
                                    if datablock:
                                        if len(datablock) > 1:
                                            for dataitem in datablock:
                                                segment = OrderedDict()
                                                values = {}
                                                segment['host'] = data['Name']
                                                segment['entries'] = 1
                                                segment[interval_segment] = static_time
                                                structure = ("redis",
                                                             cat,
                                                             subcat,
                                                             dataitem)
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
                                                if not dryrun and data['Name'] == '631235ee-978e-4972-a6f4-65b0e0e326ef':
                                                    self.logger.info("Sending Instances: {0}".format(data['Name']))
                                                    ds = data_setup()
                                                    ds.insert(segment)
                                                    sw = StatsWorker()
                                                    sw.add_data_point(segment['host'],
                                                                      "redis_type",
                                                                      segment['name'],
                                                                      datablock[dataitem],
                                                                      static_time,
                                                                      0,
                                                                      0)
                except KeyError:
                    self.logger.error("Failed Parent {0}: {1}".format(cat, subcat))
                    pass
        except KeyboardInterrupt as te:
            self.logger.error("Missing category results: {0}".format(category[cat]))
        return outline

    def main_parse(self, interval_segment='hour'):
        """ Partytime at jcru's house """
        categories = {'stats': ['LatencyHistory'],
                      'info': ['Client',
                               'Memory',
                               'Stats',
                               'Persistence',
                               'Commandstats']}
        return self.format_results(categories, interval_segment)


if __name__ == '__main__':
    p = RedisParser()
    result = p.main_parse()
#    for line in result:
#        pprint(json.dumps(result[line], default=json_util.default))
