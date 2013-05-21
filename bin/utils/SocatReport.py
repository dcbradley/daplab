#!/usr/bin/env python

import json
import os
import sys
from datetime import datetime

def create_report(inputFile):
    # make sure input file is readable
    if not os.access(inputFile, os.R_OK):
        print 'Cannot open {file} for reading'.format(file=inputFile)
        return

    # the dicts to hold data parsed from files
    transfer_data = {'start':0, 'stop':0, 'bytes':0}

    # iterate over the lines of the file until all sought data is found
    with open(inputFile) as f:
        for line in f:
            _parse(line, transfer_data)
            if all( transfer_data.values() ): break

    # organize data into its final json format 
    duration = transfer_data['stop'] - transfer_data['start']
    json_data = {'duration':duration, 'bytes':transfer_data['bytes']}

    return json.dumps(json_data)

def _parse(line, transfer_data):
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
    print create_report(sys.argv[1])
