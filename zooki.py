#!/usr/bin/env python3

# usage: python3 zooki.py /zookeeper /zookeeper/zookeeper-logs/metric.out

# This script is suppose to export all zookeeper metric from one node and write to file
# from where splunk like tools can read it.

from urllib import request
import shutil
from socket import gethostname
from datetime import datetime
import json
import sys

class zooki:
    def __init__(self):
        self.zAddr = gethostname()
        self.zPort = 8080
        self.zHttpAddr = "http://" + self.zAddr + ":" + str(self.zPort) + "/commands/"
        self.cTimeNow = str(datetime.now())

    def getStorageMetric(self):
        total, used, free = shutil.disk_usage(sys.argv[1])
        _sMetric = "{" + '"@timestamp": "' + self.cTimeNow + '"' +\
                    ',"command": ' + '"disk"' +\
                    ',"totalInGB": ' + str(total // (2**30)) +\
                    ',"usedInGB": ' + str(used // (2**30)) +\
                    ',"freeInGB": ' + str(free // (2**30)) + "}"
        # print(json.dumps(_sMetric))
        return json.dumps(_sMetric)

    def getZMetric(self, commandPath):
        with request.urlopen( self.zHttpAddr + commandPath ) as f:
            _szMetric = json.loads(f.read().decode('utf-8'))
            _szMetric["@timestamp"] = self.cTimeNow
        # print(json.dumps(_szMetric))
        return json.dumps(_szMetric)

def main():

    commandPaths = ['connections', 'dump', 'leader', 'monitor',
                    'observers', 'ruok', 'server_stats',
                    'voting_view', 'watch_summary', 'watches_by_path',
                    'zabstate']

    with open(sys.argv[2], "w") as zMetricFile:
        z = zooki()
        zMetricFile.write(z.getStorageMetric())
        for c in commandPaths:
            zMetricFile.write("\n")
            zMetricFile.write(z.getZMetric(c))

main()
