import json
import requests
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
        rsdata = self.capture_redskull()
        print type(rsdata)
        for data in rsdata['Data'][:MAX]:
            outline = []
            try:
                body = {}
                instname = data['Name']
                body['host'] = instname
                # Iterate through Info: 
                for info in info_categories:
                    infoblock = data['Master']['Info'][info]
                    #print self.format_results(infoblock, instname, loopname=data)
                    if len(infoblock) > 1:
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
                            print json.dumps(segment, default=json_util.default)
                # Iterate through higher elements
                for stat in stat_categories:
                    statblock = data['Master'][stat]
                    if len(statblock) > 1:
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
                            print json.dumps(segment, default=json_util.default)
            except KeyError:
                print "No stats for Stat.{0}".format(stat)
                pass
        
if __name__ == '__main__':
    p = RedisParser()
    print p.main_parse()
