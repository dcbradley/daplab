#!/usr/bin/env python
import os
from datetime import datetime

generic_node_data = {
                     'submit_time':None, 'execute_time':None, 'terminate_time':None, 
                     'jobid':None, 'submit_host':None, 'execute_host':None,
                     'rc':None, 'stdout':None, 'stderr':None
                    }

node_string_template = """
JOBID: {jobid}
SUBMIT TIME: {submit_time}
SUBMIT HOST: {submit_host}

EXECUTE TIME: {execute_time}
EXECUTE HOST: {execute_host}

TERMINATE TIME: {terminate_time}
RETURN CODE: {rc}

STDOUT{stdout_truncate}:
{stdout}

STDERR{stderr_truncate}:
{stderr}
"""

def create_node_report(testdir, nodes):
    # Initialize needed variables
    dag_dir = os.path.join(testdir, 'dag')
    node_log_file = os.path.join(dag_dir, 'dagfile.nodes.log')

    report_dir = os.path.join(testdir, 'report')
    node_report_file = os.path.join(report_dir, 'node_report')

    # Parse the node log file
    event_parsers= {'submitted':parse_submitted, 'executing':parse_executing, 'terminated':parse_terminated}
    valid_event_types = event_parsers.keys()

    with open(node_log_file) as node_log:
        events = node_log.read().split('\n...\n')

    for event in events:
        words = event.split()
        if len(words) > 5:
            event_type = words[5].strip('.')
            if event_type in valid_event_types:
                event_parsers[event_type](event, nodes)

    # Get a list of the nodes ordered by jobid
    del nodes['report']
    comparison = lambda a,b: cmp(nodes[a]['jobid'], nodes[b]['jobid'])
    ordered_nodes = sorted( nodes.keys(), comparison )

    # Get the stdout and stderr for each node
    truncate_size = 1000
    truncate_string = ' (Truncated to {size} bytes)'.format(size=truncate_size)

    stdout_truncate = {}
    stderr_truncate = {}

    for node_name, node_data in nodes.items():
        # stdout
        stdout_file_name = node_name + '.out'
        stdout_file = os.path.join(testdir, stdout_file_name)

        stdout_size = os.stat(stdout_file).st_size
        stdout_truncate[node_name] = ''
        if stdout_size > truncate_size:
            stdout_truncate[node_name] = truncate_string

        if stdout_size > 0:
            with open(stdout_file) as stdout:
                node_data['stdout'] = stdout.read(truncate_size)
        else:
            node_data['stdout'] = 'No output'

        # stderr
        stderr_file_name = node_name + '.err'
        stderr_file = os.path.join(testdir, stderr_file_name)

        stderr_size = os.stat(stderr_file).st_size
        stderr_truncate[node_name] = ''
        if stderr_size > truncate_size:
            stderr_truncate[node_name] = truncate_string

        if stderr_size > 0:
            with open(stderr_file) as stderr:
                node_data['stderr'] = stderr.read(truncate_size)
        else:
            node_data['stderr'] = 'No output'

    # Write the node report 
    pad_size = 8
    os.mkdir(report_dir)

    with open(node_report_file, 'w') as node_report:
        for node_name in ordered_nodes:
            node_data = nodes[node_name]

            node_string_unjust = node_string_template.format(stdout_truncate=stdout_truncate[node_name], 
                                                             stderr_truncate=stderr_truncate[node_name], 
                                                             **node_data
                                                            )
            node_string = ''
            for line in node_string_unjust.split('\n'):
                node_string += line.rjust( len(line) + pad_size ) + '\n'

            node_report.write( 'DAG NODE {name}:'.format(name=node_name) )
            node_report.write(node_string)

def parse_submitted(event, nodes):
    # Split the event string by line
    lines = event.split('\n')

    # Parse the name of the node from the second line of the event and create a dict for it
    name_line = lines[1].split()
    node_name = name_line[2]

    nodes[node_name] = generic_node_data.copy()
    node_data = nodes[node_name]

    # Parse data from the first line of the event
    main_line = lines[0].split()

    node_data['jobid'] = main_line[1].strip('()')

    date_string = main_line[2] + ' ' + main_line[3]
    node_data['submit_time'] = to_unix_time(date_string)

    node_data['submit_host'] = main_line[8].strip('<>')

def parse_executing(event, nodes):
    # split the event string by line and get the first line
    lines = event.split('\n')
    main_line = lines[0].split()

    # Parse the jobid and use it to find the name of this node
    this_jobid = main_line[1].strip('()')
    node_name = None
    for possible_node_name, data in nodes.items():
        if data['jobid'] == this_jobid:
            node_name = possible_node_name 
            break

    node_data = nodes[node_name]

    # Parse data from the main line of the event
    date_string = main_line[2] + ' ' + main_line[3]
    node_data['execute_time'] = to_unix_time(date_string)

    node_data['execute_host'] = main_line[8].strip('<>')

def parse_terminated(event, nodes):
    # Split the event string by line and get the first line
    lines = event.split('\n')
    main_line = lines[0].split()

    # Parse the jobid and use it to find the name of this node
    this_jobid = main_line[1].strip('()')
    node_name = None
    for possible_node_name, data in nodes.items():
        if data['jobid'] == this_jobid:
            node_name = possible_node_name 
            break

    node_data = nodes[node_name]

    # Parse data from the main line of the event
    date_string = main_line[2] + ' ' + main_line[3]
    node_data['terminate_time'] = to_unix_time(date_string)

    # Parse return code
    rc_line = lines[1]
    words = rc_line.split()

    rc_pos = words.index('(return') + 2
    node_data['rc'] = words[rc_pos].strip('()')

def to_unix_time(date_string):
    date = datetime.strptime(date_string, '%m/%d %H:%M:%S')
    date = date.replace(year=datetime.today().year)
    return date.strftime('%s')
