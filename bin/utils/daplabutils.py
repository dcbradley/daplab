#!/usr/bin/env python

import os
import re
import shlex
from string import Formatter
from subprocess import Popen, PIPE
import sys
from time import sleep

def create_dirs(options):
    submit_dir = options['submit_dir']

    # create a new test directory so different tests
    # do not stomp on each other
    test_dir_name = options['transfer_name']+'_test_1'
    test_dir = os.path.join(submit_dir, test_dir_name)
    i = 1
    while os.path.isdir( test_dir ):
        i += 1
        test_dir = test_dir[:-1] + str(i)

    print 'Using directory', test_dir

    # This will fail if there is a race and something else created
    # the test directory before we do.  Better to fail than to go
    # ahead and use the same directory as a different test instance.
    os.mkdir(test_dir)

    # Add the directories to the options holder
    options['test_dir'] = test_dir

def fill_test_dir(options):
    test_dir = options['test_dir']
    dag = options['dag']
    subfile_templates = options['subfile_templates']
    scripts = options['scripts']

    output_path = os.path.join(test_dir, os.path.basename(dag))
    copy_file(dag, output_path)

    for subfile_template in subfile_templates:
        subfile_name = os.path.basename( subfile_template.replace('.template', '') )

        output_path = os.path.join(test_dir, subfile_name)
        copy_file(subfile_template, output_path, options)

    for script in scripts:
        script_name = os.path.basename(script)

        output_path = os.path.join(test_dir, script_name)
        copy_file(script, output_path)
        os.chmod(output_path, 0555)

def submit_dag(options):
    test_dir = options['test_dir']
    dag_name = os.path.basename(options['dag'])

    output_dag = os.path.join(test_dir, dag_name)
    submit_command = 'condor_submit_dag {output_dag}'.format(output_dag=output_dag) 

    os.chdir(test_dir)
    rc = os.system(submit_command)
    if rc != 0:
       sys.exit(1)

def show_condor_q(display_time=None):
    esc = chr(27)
    clear = lambda: sys.stdout.write( esc + '[J' )
    curs_up = lambda num: sys.stdout.write( esc + '[{n}A'.format(n=num) )

    cmd = 'condor_q -dag'
    cmd = shlex.split(cmd)
    condor_q = Popen(cmd, stdout=PIPE)
    out, err = condor_q.communicate()
    
    if display_time:
        print out
        sleep(display_time)

        ln_count = out.count('\n')
        curs_up(ln_count+1)
        clear()
    else:
        print out

def dag_is_done(test_dir, dagname):
    dagman_out = '{dagname}.dagman.out'.format(dagname=dagname)
    dagman_out_name = os.path.basename(dagman_out)
    dagman_out_file = os.path.join(test_dir, dagman_out_name)

    with open(dagman_out_file) as dag_out:
        return 'All jobs Completed!' in dag_out.read()

def scripts_from_dag(dagfile):
    with open(dagfile) as f:
        dag = f.read()

    script_names = set()
    for line in dag.splitlines():
        if line.startswith('SCRIPT'):
            script_name = line.split()[3]
            script_names.add(script_name)

    return list(script_names)

def subfiles_from_dag(dagfile):
    with open(dagfile) as f:
        dag = f.read()

    sub_re = '\w+.sub'
    sub_re = re.compile(sub_re)

    subfile_templates = set()
    match = sub_re.search(dag)
    while match:
        match_text = match.group()

        template = match_text+'.template'
        subfile_templates.add(template)

        match_end_pos = match.end()
        match = sub_re.search(dag, match_end_pos)

    return list(subfile_templates)

def opts_from_subfiles(subfile_templates):
    if not isinstance(subfile_templates, list):
        subfile_templates = [subfile_templates]

    subfile_opts = set()
    for subfile_template in subfile_templates:
        with open(subfile_template) as f:
            template = f.read()

        for _, opt_name, _, _ in Formatter().parse(template):
            if opt_name:
                subfile_opts.add(opt_name)

    return list(subfile_opts)

def opts_from_command_line(opt_value_string):
    if not isinstance(opt_value_string, str):
        return [opt_value_string]

    options = set()
    for _, opt_name, _, _ in Formatter().parse(opt_value_string):
        if opt_name:
            options.add(opt_name)

    return list(options)

def copy_file(origin_path, output_path, options=None):
    with open(origin_path) as f:
        text = f.read()

    if options:
        text = text.format(**options)

    with open(output_path, 'w') as f:
        f.write(text)

def normalize_path(sought_file, search_dirs):
    if os.access(sought_file, os.F_OK):
        return os.path.realpath(sought_file)

    if isinstance(search_dirs, str):
        search_dirs = [search_dirs]

    for search_dir in search_dirs:
        for dirpath, dirnames, filenames in os.walk(search_dir):
            if sought_file in filenames:
                return os.path.join(dirpath, sought_file)

    # if nothing was found, just give back the original string
    return sought_file
