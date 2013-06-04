#!/usr/bin/env python

import json
import os
import sys
from datetime import datetime

from reportutils import create_node_report

def create_reports(testdir):
    # make sure input dir exists
    if not os.path.exists(testdir):
        print 'Test directory {dir} does not seem to exist'.format(dir=testdir)
        return

    # the dicts to hold data parsed from files
    transfer_data = {'start':0, 'stop':0, 'bytes':0}
    nodes = {}

    # Set needed paths
    dag_dir = os.path.join(testdir, 'dag')
    report_dir = os.path.join(testdir, 'report')

    transfer_report_file = os.path.join(report_dir, 'transfer_report')

    # Create the DAG node report
    create_node_report(testdir, nodes)

    # Create transfer data report in JSON format
    server_err_file = os.path.join(testdir, 'server.err')
    with open(server_err_file) as f:
        for line in f:
            parse_err_line(line, transfer_data)
            if all( transfer_data.values() ): break

    duration = transfer_data['stop'] - transfer_data['start']
    json_data = {'duration':duration, 'bytes':transfer_data['bytes']}

    with open(transfer_report_file, 'w') as transfer_report:
        transfer_report.write( json.dumps(json_data) )

def parse_err_line(line, transfer_data):
    # split the line into tokens seperated by whitespace
    line = line.split()
    time = line[0]+' '+line[1]
    message = line[4:]

    # ensures no later attempts to access indexes beyond length of list
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
    create_reports(sys.argv[1])
