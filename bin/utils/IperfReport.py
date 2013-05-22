#!/usr/bin/env python

import sys
import re
import json

def ConvertToMB(value,unit):
    if unit == "MBytes" or unit == "MByte":
        return value
    if unit == "KBytes" or unit == "KByte":
        return value/1024.0
    if unit == "GBytes" or unit == "GByte":
        return value*1024.0
    raise Exception("Unknown unit " + unit)

def ConvertToMbitsPerSec(value,unit):
    if unit == "Mbits/sec":
        return value
    if unit == "Kbits/sec":
        return value/1024.0
    if unit == "Gbits/sec":
        return value*1024.0
    raise "Unknown unit " + unit

class IperfReport:
    report = None

    def __init__(self,iperf_output_file):
        F = file(iperf_output_file,"r")
        tcp_window_re = re.compile("^TCP window size: (?P<window>[^ ]+) *(?P<unit>[^ ]+).*")
        bandwidth_re = re.compile("^\\[ *3\\] *(?P<start_time>[0-9.]+)- *(?P<stop_time>[0-9.]+) sec *(?P<size>[0-9.]+) (?P<size_unit>[^ ]+) *(?P<bandwidth>[0-9.]+) (?P<bandwidth_unit>[^ ]+)")

        self.report = {}

        transfer_time = None
        transfer_size = None
        transfer_unit = None
        bandwidth = None
        bandwidth_unit = None

        for line in F:
            line = line.strip()
            m = tcp_window_re.match(line)
            if m:
                self.report["tcp_window_size_MB"] = ConvertToMB(float(m.group('window')),m.group('unit'))

            m = bandwidth_re.match(line)
            if m:
                transfer_time = float(m.group('stop_time')) - float(m.group('start_time'))
                transfer_size = float(m.group('size'))
                transfer_unit = m.group('size_unit')
                bandwidth = float(m.group('bandwidth'))
                bandwidth_unit = m.group('bandwidth_unit')

        if transfer_time is not None:
            self.report["transfer_time"] = transfer_time
            self.report["transfer_size_MB"] = ConvertToMB(transfer_size,transfer_unit)
            self.report["bandwidth_Mbits_per_sec"] = ConvertToMbitsPerSec(bandwidth,bandwidth_unit)

    def toPrettyJSON(self):
        return json.dumps(self.report,sort_keys=True, indent=4)

if __name__=='__main__':
    report = IperfReport(sys.argv[1])
    print report.toPrettyJSON()
