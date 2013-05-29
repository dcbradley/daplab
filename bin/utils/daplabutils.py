#!/usr/bin/env python

from optparse import OptionParser
import os
from os.path import join as pathjoin
import sys

def get_missing_opts(given_options):
    required_options = [
                        'server_machine', 'client_machine', 'server_args', 'client_args',
                        'transfer_exec_path', 'transfer_name', 'reporter_exec_path', 'reporter_args', 
                        'wait_script', 'stop_script', 'base_submit_dir', 'template_dir'
                       ]

    given_keys = given_options.keys()
    for given_key in given_keys:
        if given_key in required_options:
            required_options.remove(given_key)

    return required_options

def add_common_options(parser, defaults):
    parser.add_option("--server-machine", default=defaults['server_machine'])
    parser.add_option("--client-machine", default=defaults['client_machine'])

    parser.add_option("--base-submit-dir", default=defaults['base_submit_dir'])
    parser.add_option("--template-dir", default=defaults['template_dir'], help="Path of dir containing templates")

    parser.add_option("--transfer_name", default=defaults['transfer_name'], help="Name of transfer executable used to label tests")
    parser.add_option("--transfer-exec-path", default=defaults['transfer_exec_path'], help="Path of transfer executable")
    parser.add_option("--server-args", default=defaults['server_args'])
    parser.add_option("--client-args", default=defaults['client_args'])

    parser.add_option("--reporter-exec-path", default=defaults['reporter_exec_path'], help="Path of reporter script")
    parser.add_option("--reporter-args", default=defaults['reporter_args'], help='Arguments provided to reporter script')
    parser.add_option("--wait-script", default=defaults['wait_script'], help="Path of wait script")
    parser.add_option("--stop-script", default=defaults['stop_script'], help="Path of stop script")

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
    options['dagfile'] = create_from_template('dag_template', 'dagfile', options)
    options['server_subfile'] = create_from_template('server_sub_template', 'server.sub', options)
    options['client_subfile'] = create_from_template('client_sub_template', 'client.sub', options)
    options['wait_subfile'] = create_from_template('wait_sub_template', 'server_wait.sub', options)
    options['report_subfile'] = create_from_template('report_sub_template', 'report.sub', options)

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


