#!/usr/bin/env python

# Example usage with socat:
# socat EXEC:'./dircat.py -s /tmp/socat_test/in' EXEC:'./dircat.py -r /tmp/socat_test/out'

import os
from optparse import OptionParser
import sys

def combine(dir, output_file=sys.stdout):
    file_info = ""
    file_cat = ""

    entries = os.listdir(dir)
    for entry in entries:
        path = os.path.join(dir, entry)

        if os.path.isfile(path):
            stat = os.stat(path)
            file_info += "{name}/{size} ".format(name=entry, size=stat.st_size)

            with open(path) as f:
                file_cat += f.read()

    output = file_info + "\n" + file_cat
    output_file.write(output)
    
    return output

def divide(dir, input_file=sys.stdin):
    file_info = input_file.readline()

    for info in file_info.split():
        name, size = info.split('/')
        size = int(size)

        path = os.path.join(dir, name)
        with open(path, 'w') as f:
            f.write( input_file.read(size) )

if __name__=='__main__':
    parser = OptionParser()
    parser.add_option('-s', '--send', help='send all files in given directory to stdout')
    parser.add_option('-r', '--receive', help='receive a directory of files from stdin and write them to the given directory')
    options, args = parser.parse_args()

    if options.receive:
        divide(options.receive)

    if options.send:
        combine(options.send)
