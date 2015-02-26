import json
import requests
import tasks
from bson import json_util
from datetime import datetime
from pprint import pprint
from collections import OrderedDict


# Change to configs
redskull_host = "localhost"
redskull_host = "cdbp-n01.prod.iad3.clouddb.rackspace.net"

class RedisParser:
    """ Single script to pull dataset and restructure """

    def capture_redskull(self):
        """ Connection to redskull, #TODO need to clean this up """
        try:
            r = requests.get('http://{0}:8000/api/knownpods'.format(redskull_host))
        except requests.exceptions.ConnectionError:
            raise Exception("ParserError: Unable to find broadcasted redskull host")
        if r.status_code != 200:
            raise Exception("ParserError: Recieved non-200 response from redskull")
        return json.loads(r.content)

    def format_results(self,
                       datablock=None,
                       instname=None,
                       interval_segment=None,
                       loopname=None):
        """ Formatting each stat into its own object """
        if not datablock:
            return
        wrapper = {}
        if len(datablock) > 1:
            for dataitem in datablock:
                segment = OrderedDict()
                values = {}
                segment['host'] = instname
                segment['entries'] = 1
                segment[interval_segment] = datetime.utcnow()
                structure = ("redis", "info", loopname, dataitem)
                segment['name'] = ('.').join(structure)
                values[0] = datablock[dataitem]
                segment['total'] = datablock[dataitem]
                segment['values'] = values
            wrapper[segment['name']] = segment
        return json.dumps(wrapper, default=json_util.default)

    def main_parse(self):
        """ Partytime at jcru's house """
        stat_categories = ['LatencyHistory']
        info_categories = ['Client', 'Memory', 'Stats', 'Persistence', 'Commandstats']
        MAX = 1
        interval_segment = 'minute'
        static_time = datetime.utcnow()
        rsdata = self.capture_redskull()
        outline = {}
        for data in rsdata['Data'][:MAX]:
            outline = {}
            try:
                body = {}
                instname = data['Name']
                body['host'] = instname
                # Iterate through Info:
                try:
                    for info in info_categories:
                        #print data['Master']['Info']
                        infoblock = data['Master']['Info'].get(info, 0)
                        #print self.format_results(infoblock, instname, loopname=data)
                        if len(infoblock) > 1:
                            print "Testing info: {0}".format(info)
                            for infoitem in infoblock:
                                segment = OrderedDict()
                                values = {}
                                segment['host'] = body['host']
                                segment['entries'] = 1
                                segment[interval_segment] = datetime.utcnow()
                                structure = ("redis", "info", info, infoitem)
                                segment['name'] = ('.').join(structure)
                                values[0] = infoblock[infoitem]
                                segment['total'] = infoblock[infoitem]
                                segment['values'] = values
                                tag = "{0}.{1}".format(segment['name'], str(static_time))
                                outline[tag] = segment
                                tasks.add_minute_stat.delay(segment)
#                            print json.dumps(segment, default=json_util.default)
                except KeyError:
                     print "Failed: Info: {0}".format(info)
                     pass
                # Iterate through higher elements
                try:
                    for stat in stat_categories:
                        statblock = data['Master'][stat]
                        if len(statblock) > 1:
                            print "Testing stat: {0}".format(stat)
                            for statitem in statblock:
                                segment = OrderedDict()
                                values = {}
                                segment['host'] = body['host']
                                segment['entries'] = 1
                                segment[interval_segment] = datetime.utcnow()
                                structure = ("redis", stat, statitem)
                                segment['name'] = ('.').join(structure)
                                values[0] = statblock[statitem]
                                segment['total'] = statblock[statitem]
                                segment['values'] = values
                                tag = "{0}.{1}".format(segment['name'], str(static_time))
                                tasks.add_minute_stat.delay(segment)
                                outline[tag] = segment
#                            print json.dumps(segment, default=json_util.default)
                except KeyError:
                    print "Failed: stat: {0}".format(stat)
            except KeyError:
                print "No stats for Stat.{0}".format()
                pass
        return outline

        
if __name__ == '__main__':
    p = RedisParser()
    result = p.main_parse()
    for line in result:
        pprint(json.dumps(result[line], default=json_util.default))
