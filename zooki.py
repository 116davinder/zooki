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
        return json.dumps(_sMetric)

    def getZMetric(self, commandPath):
        with request.urlopen( self.zHttpAddr + commandPath ) as f:
            if f.status == 200:
                _zMetric = json.loads(f.read().decode('utf-8'))
                _zMetric["@timestamp"] = self.cTimeNow
                _zMetric["environment"] = sys.argv[3]
            else:
                _zMetric = {}
        return json.dumps(_zMetric)

# json retruned by monitor is too big to be handled by splunk indexer
# so separate function to reduce the json size
    def getMonitorMetric(self):
        with request.urlopen( self.zHttpAddr + "monitor" ) as f:
            if f.status == 200:
                _MM = json.loads(f.read().decode('utf-8'))
                _zMetric = {
                    "@timestamp": self.cTimeNow,
                    "environment": sys.argv[3],
                    "command": _MM["command"],
                    "znode_count": _MM["znode_count"],
                    "watch_count": _MM["watch_count"],
                    "outstanding_requests": _MM["outstanding_requests"],
                    "open_file_descriptor_count": _MM["open_file_descriptor_count"],
                    "ephemerals_count": _MM["ephemerals_count"],
                    "max_latency": _MM["max_latency"],
                    "avg_latency": _MM["avg_latency"],
                    "synced_followers": _MM["synced_followers"] if _MM["server_state"] == "leader" else 0,
                    "pending_syncs": _MM["pending_syncs"] if _MM["server_state"] == "leader" else 0,
                    "version": _MM["version"],
                    "quorum_size": _MM["quorum_size"],
                    "uptime": _MM["uptime"]
                }
            else:
                _zMetric = {}
        return json.dumps(_zMetric)

def main():

    commandPaths = ['connections', 'leader', 'watch_summary']

    z = zooki()
    with open(sys.argv[2] + "disk.out", "w") as zMetricFile:
        zMetricFile.write(z.getStorageMetric())

    for command in commandPaths:
        with open(sys.argv[2] + command + ".out", "w") as zMetricFile:
            zMetricFile.write(z.getZMetric(command))

    with open(sys.argv[2] + "monitor.out", "w") as zMetricFile:
        zMetricFile.write(z.getMonitorMetric())
main()
