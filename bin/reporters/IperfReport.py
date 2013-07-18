#!/usr/bin/env python

import json
import os
import re
import sys

from reportutils import get_node_report

def create_reports(dag_nodes_log_path, iperf_output_file):
    # Create report directories
    test_dir = os.path.dirname( os.path.realpath(iperf_output_file) )
    report_dir = os.path.join(test_dir, 'reports')
    os.mkdir(report_dir)

    # Create DAG node report
    node_report = get_node_report(dag_nodes_log_path)

    node_report_path = os.path.join(report_dir, 'node_report')
    with open(node_report_path, 'w') as f:
        f.write(node_report)

    # Create transfer data report
    tcp_window_re = re.compile("^TCP window size: (?P<window>[^ ]+) *(?P<unit>[^ ]+).*")
    bandwidth_re = re.compile("^\\[ *3\\] *(?P<start_time>[0-9.]+)- *(?P<stop_time>[0-9.]+) sec *(?P<size>[0-9.]+) (?P<size_unit>[^ ]+) *(?P<bandwidth>[0-9.]+) (?P<bandwidth_unit>[^ ]+)")
    transfer_time = None
    transfer_size = None
    transfer_unit = None
    bandwidth = None
    bandwidth_unit = None

    report = {}

    with open(iperf_output_file) as F:
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
            report["transfer_time"] = transfer_time
            report["transfer_size_MB"] = ConvertToMB(transfer_size,transfer_unit)
            report["bandwidth_Mbits_per_sec"] = ConvertToMbitsPerSec(bandwidth,bandwidth_unit)

    transfer_report = json.dumps(report,sort_keys=True, indent=4)
    transfer_report_path = os.path.join(report_dir, 'transfer_report')
    with open(transfer_report_path, 'w') as f:
        f.write(transfer_report)

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

if __name__=='__main__':
    create_reports(sys.argv[1], sys.argv[2])
