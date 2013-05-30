#!/usr/bin/env python

from optparse import OptionParser
import os
from os.path import join as pathjoin
import re
import sys

def get_missing_opts(given_options):
    required_options = ['server_machine', 'client_machine', 'submit_dir', 'dag_template',
                        'transfer_name', 'transfer_exec_path', 'server_args', 'client_args',
                        'reporter_exec_path', 'reporter_args', 'wait_script'
                       ]

    given_keys = given_options.keys()
    for given_key in given_keys:
        if given_key in required_options:
            required_options.remove(given_key)

    return required_options

def add_common_options(parser, defaults):
    parser.add_option("--server-machine",     default=defaults['server_machine'],     help='The network address of the server machine')
    parser.add_option("--client-machine",     default=defaults['client_machine'],     help='The network address of the client machine')

    parser.add_option("--submit-dir",         default=defaults['submit_dir'],         help='The directory to write test data to')
    parser.add_option("--dag-template",       default=defaults['dag_template'],       help='Template to create DAG file from')

    parser.add_option("--transfer-name",      default=defaults['transfer_name'],      help='Name of transfer executable used to label tests')
    parser.add_option("--transfer-exec-path", default=defaults['transfer_exec_path'], help='Path of transfer executable')
    parser.add_option("--server-args",        default=defaults['server_args'],        help='Arguments provided to server transfer executable')
    parser.add_option("--client-args",        default=defaults['client_args'],        help='Arguments provided to client trasnfer executable')

    parser.add_option("--reporter-exec-path", default=defaults['reporter_exec_path'], help="Path of reporter script")
    parser.add_option("--reporter-args",      default=defaults['reporter_args'],      help='Arguments provided to reporter script')

    parser.add_option("--wait-script",        default=defaults['wait_script'],        help="Path of wait script")

def create_dirs(options):
    # create a new test directory so different tests
    # do not stomp on each other
    testdir_basename = options['transfer_name']+'_test_1'
    testdir = pathjoin(options['submit_dir'], testdir_basename)
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
    sub_templates = create_dag_from_template(options['dag_template'], 'dagfile', options)

    for sub_template_name in sub_templates:
        right_dot = sub_template_name.rfind('.')
        output_name = sub_template_name[:right_dot]

        create_sub_from_template(sub_template_name, output_name, options)

def create_dag_from_template(dag_template_name, output_name, options):
    # Open the dag template and replace any {}'d strings with values from options dict
    dag_template_path = pathjoin(options['dag_template_dir'], dag_template_name)
    with open(dag_template_path) as f:
        dag = f.read()

    dag = dag.format(**options)

    # Find all the sub file templates that will need to be filled to run this dag
    subfile_pattern = '\w+\.sub\.template'
    subfile_pattern = re.compile(subfile_pattern)

    sub_templates = []
    match = subfile_pattern.search(dag)
    while match:
        # Add the sub template name to the list of sub templates
        match_text = match.group()
        sub_templates.append(match_text)

        # Remove '.template' from filename in the output dag
        normal_name = match_text.replace('.template', '') 
        dag = dag.replace(match_text, normal_name)

        # Get the next result
        match_end = match.end()
        match = subfile_pattern.search(dag, match_end)

    # Write the filled out dag to the dagdir
    dag_output_path = pathjoin(options['dagdir'], output_name)
    with open(dag_output_path, 'w') as f:
        f.write(dag)

    options['dagfile'] = dag_output_path
    return sub_templates

def create_sub_from_template(sub_template_name, output_name, options):
    # Read the content of the template file
    sub_template_file = pathjoin(options['sub_template_dir'], sub_template_name)
    with open(sub_template_file) as f:
        sub_template = f.read()

    # Fill in the template with the chosen options
    sub = sub_template.format(**options)

    # Write the filled in template to the dagdir
    sub_output_path = pathjoin(options['dagdir'], output_name)
    with open(sub_output_path, 'w') as f:
        f.write(sub)

    options[output_name] = sub_output_path

def submit_dag(options):
    os.chdir(options['testdir'])

    submit_command = 'condor_submit_dag {dagfile}'.format(**options) 
    rc = os.system(submit_command)
    if rc != 0:
       sys.exit(1)
