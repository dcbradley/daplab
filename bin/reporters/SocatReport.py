#!/usr/bin/env python

import json
import os
import sys
from datetime import datetime

from reportutils import get_node_report

def create_reports(dag_nodes_log_path, socat_err_path):
    # Create report directory
    test_dir = os.path.dirname( os.path.realpath(dag_nodes_log_path) )
    report_dir = os.path.join(test_dir, 'reports')
    os.mkdir(report_dir)

    # Create the DAG node report
    node_report = get_node_report(dag_nodes_log_path)

    node_report_path = os.path.join(report_dir, 'node_report')
    with open(node_report_path, 'w') as f:
        f.write(node_report)

    # Create transfer data report in JSON format
    transfer_data = {'start':0, 'stop':0, 'bytes':0}

    transfer_report_file = os.path.join(test_dir, 'transfer_report')

    with open(socat_err_path) as f:
        for line in f:
            parse_err_line(line, transfer_data)
            if all( transfer_data.values() ): break

    duration = transfer_data['stop'] - transfer_data['start']
    json_data = {'duration':duration, 'bytes':transfer_data['bytes']}

    transfer_report_path = os.path.join(report_dir, 'transfer_report')
    with open(transfer_report_path, 'w') as f:
        f.write( json.dumps(json_data) + '\n' )

def parse_err_line(line, transfer_data):
    # split the line into tokens seperated by whitespace
    line = line.split()
    time = line[0]+' '+line[1]
    message = line[4:]

    # ensures no attempts to access indexes beyond length of list
    if len(message) < 3: return

    # if contains byte transfer
    if message[0] == 'transferred' and message[2] == 'bytes':
        transfer_data['bytes'] += int( message[1] )
        return

    # if contains start time
    if message[0] == 'starting' and message[1] == 'data':
        transfer_data['start'] = _to_unix_time(time)
        return

    # if contains stop time
    if message[0] == 'socket' and message[-1] == 'EOF':
        transfer_data['stop'] = _to_unix_time(time)
        return

def _to_unix_time(date_string):
    stamp = datetime.strptime(date_string, '%Y/%m/%d %H:%M:%S.%f').strftime('%s.%f')
    return float(stamp)

if __name__ == '__main__':
    create_reports(sys.argv[1], sys.argv[2])
