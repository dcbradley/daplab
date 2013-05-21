#!/usr/bin/env python

import os
from os.path import join as pathjoin
import sys

def get_missing_opts(given_options):
    required_options = [
                        'server_machine', 'client_machine', 'server_args', 'client_args',
                        'transfer_exec_path', 'transfer_name', 'reporter_exec_path', 
                        'wait_script', 'stop_script', 'base_submit_dir', 'template_dir'
                       ]

    given_keys = given_options.keys()
    for opt in required_options:
        if opt in given_keys:
            required_options.remove(opt)

    return required_options

def create_dirs(options):
    # create a new test directory so different tests
    # do not stomp on each other
    testdir_basename = options['transfer_name']+'_test_1'
    testdir = pathjoin(options['base_submit_dir'], testdir_basename)
    i = 1
    while os.path.isdir( testdir ):
        i += 1
        testdir = testdir[:-1] + str(i)

    dagdir = pathjoin(testdir,'dag')
    print 'Using directory', testdir

    # This will fail if there is a race and something else created
    # the test directory before we do.  Better to fail than to go
    # ahead and use the same directory as a different test instance.
    os.mkdir(testdir)
    os.mkdir(dagdir)

    # Add the directories to the options holder
    options['testdir'] = testdir
    options['dagdir'] = dagdir

def fill_templates(options):
    name = options['transfer_name']
    options['dagfile'] = create_from_template('dag_template', name+'.dag', options)
    options['server_subfile'] = create_from_template('server_sub_template', name+'_server.sub', options)
    options['client_subfile'] = create_from_template('client_sub_template', name+'_client.sub', options)
    options['wait_subfile'] = create_from_template('wait_sub_template', name+'_server_wait.sub', options)
    options['report_subfile'] = create_from_template('report_sub_template', name+'_report.sub', options)

def create_from_template(template_name, output_file_name, options):
    # Read the content of the template file into the variable template
    template_file = pathjoin(options['template_dir'], template_name)
    with open(template_file) as f:
        template = f.read()

    # Fill in the template with the chosen options
    output = template.format(**options)

    # Write the filled in template to the dagdir
    output_file = pathjoin(options['dagdir'], output_file_name)
    with open(output_file, 'w') as f:
        f.write(output)

    return output_file

def submit_dag(options):
    os.chdir(options['testdir'])

    submit_command = 'condor_submit_dag {dagfile}'.format(**options) 
    rc = os.system(submit_command)
    if rc != 0:
       sys.exit(1)

