#!/usr/bin/env python3

# usage: python3 zooki.py /zookeeper /zookeeper/zookeeper-logs/ dev-env-zookeeper

# This script suppose to export all zookeeper metric from one node and write to file
# from where either splunk like tools can read it.

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
        _sMetric = {
                    "@timestamp": self.cTimeNow,
                    "command": "disk",
                    "environment": sys.argv[3],
                    "totalInGB": total // (2**30),
                    "usedInGB":  used // (2**30),
                    "freeInGB": free // (2**30) 
                    }
        # print(json.dumps(_sMetric))
        return json.dumps(_sMetric)

    def getZMetric(self, commandPath):
        with request.urlopen( self.zHttpAddr + commandPath ) as f:
            _zMetric = json.loads(f.read().decode('utf-8'))
            _zMetric["@timestamp"] = self.cTimeNow
            _zMetric["environment"] = sys.argv[3]
        # print(json.dumps(_zMetric))
        return json.dumps(_zMetric)

def main():

    commandPaths = ['connections', 'dump', 'leader', 'monitor',
                    'observers', 'server_stats', 'watches_by_path',
                    'voting_view', 'watch_summary', 'zabstate', 'ruok']

    z = zooki()
    with open(sys.argv[2] + "disk.out", "w") as zMetricFile:
        zMetricFile.write(z.getStorageMetric())

    for command in commandPaths:
        with open(sys.argv[2] + command + ".out", "w") as zMetricFile:
            zMetricFile.write(z.getZMetric(command))

main()
