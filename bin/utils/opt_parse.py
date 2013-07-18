#!/usr/bin/env python

from daplabutils import normalize_path, scripts_from_dag, subfiles_from_dag, opts_from_subfiles, opts_from_command_line
import os
from string import Formatter
import sys

general_usage = """Usage: submit_socat [OPTIONS]
All option flags must be followed by the option's value, using quotes to enclose any whitespace in the value. The only exception is --help, which is given by itself

General program options:
  --dag           the DAG file specifying jobs to be submitted

  --submit-dir    the directory to put the test directory in, which holds the DAG file, Condor job submission files, scripts and all other files needed for execution

  --search-dirs   directories to be searched recursively when looking for any files or scripts necessary for the execution of the DAG, including the DAG file itself

  --transfer-exec the executable file for the transfer program run within the DAG

  --transfer-name the name of the executable program run within the DAG that does the transferring. This is used to name the test directory
                  (defaults to the name of the transfer executable)

  --help          display usage instructions

DAG specified: {dag}
{dag_options}
"""

class SubmitParser():
    def __init__(self, defaults={}):
        self.args = sys.argv[1:]

        self.valid_opts = set(['dag', 'dag_name', 'submit_dir', 'search_dirs', 
                              'transfer_name', 'transfer_exec'
                             ])
        self.parsed_opts = {}
        self.dag_opts = set()
        self.defaults = defaults

        self_dir = os.path.dirname( os.path.realpath(__file__) )
        bin_dir = os.path.realpath( os.path.join(self_dir, '..') )
        self.search_dirs = [bin_dir]

    def parse(self):
        # Parse the options and values into the parsed_opts dict (without any validation)
        self._parse_command_line()
        self._add_valid_defaults()
        self._normalize_opt_values()

        # If no option was given for the DAG no more work can be done here, print the usage and exit
        if 'dag' not in self.parsed_opts:
            self._print_usage_and_exit()
        else:
            self.parsed_opts['dag_name'] = os.path.basename(self.parsed_opts['dag'])

        # Besides the options for the general DAG, there are more options needing values in
        # the condor submission file templates, add all those to the list of valid options
        dag = self.parsed_opts['dag']
        subfile_templates = subfiles_from_dag(dag)
        subfile_templates = [normalize_path(subfile, self.search_dirs) for subfile in subfile_templates]
        subfile_opts = opts_from_subfiles(subfile_templates)

        self.dag_opts.update(subfile_opts)
        self.dag_opts = self.dag_opts.difference(self.valid_opts)

        self.valid_opts.update(subfile_opts)

        self._add_valid_defaults()

        # Other possible options can exist within option values themselves, add these options
        # to the list of valid options
        for opt_value in self.parsed_opts.values():
            self.valid_opts.update( opts_from_command_line(opt_value) )
        self._add_valid_defaults()

        # Check to see if the help option was given and display the usage if so
        if 'help' in self.parsed_opts:
            self._print_usage_and_exit()

        # Make sure all options given in command line are recognized, if not, print usage and exit with error
        unrecognized_opts = self._get_unrecognized_opts()
        if unrecognized_opts:
            for opt_name, opt_value in unrecognized_opts.items():
                opt_flag = '--' + opt_name.replace('_', '-')
                print 'Unrecognized option {opt_flag} given\n'.format(opt_flag=opt_flag)

            self._print_usage_and_exit(1)

        # Each condor submission file requires arguments to pass to the transfer program,
        # fill in these argument strings with values parsed from other options given to this program
        for subfile_template in subfile_templates:
            subfile_name = os.path.basename(subfile_template)
            subfile_args_key = subfile_name.replace('.sub.template', '_args')

            if subfile_args_key in self.parsed_opts:
                self.parsed_opts[subfile_args_key] = self.parsed_opts[subfile_args_key].format(**self.parsed_opts) 

        # Add to options some more data needed later
        scripts = scripts_from_dag(dag)
        scripts = [normalize_path(script, self.search_dirs) for script in scripts]

        self.parsed_opts['scripts'] = scripts
        self.parsed_opts['subfile_templates'] = subfile_templates

        # Make sure all paths given as option values are valid and absolute, just to be safe
        self._normalize_opt_values()

        return self.parsed_opts

    def _print_usage_and_exit(self, rc=0):
        # Get the name of the DAG, or call it None if it doesn't exist
        if 'dag' in self.parsed_opts:
            dag = self.parsed_opts['dag']

            dag_options = 'Options specific to this DAG and the Condor job submission files specified in the DAG:\n'
            for dag_option in sorted(self.dag_opts):
                dag_option_flag = '--' + dag_option.replace('_', '-')
                dag_options += '  {opt}\n'.format(opt=dag_option_flag)
        else:
            dag = 'None'
            dag_options = ''

        print general_usage.format(dag=dag, dag_options=dag_options)
        sys.exit(rc)

    def _parse_command_line(self):
        expect_flag = True

        for token in self.args:
            if expect_flag:
                if token == '--help':
                    self.parsed_opts['help'] = True
                elif token.startswith('--'):
                    opt_flag = token
                    opt_name = token[2:].replace('-', '_')
                    expect_flag = False
                else:
                    print 'Expected an option flag, got {thing} instead\n'.format(thing=token)
                    self._print_usage_and_exit(1)
            else:
                self.parsed_opts[opt_name] = token
                expect_flag = True

        if (not expect_flag) and (opt_name != 'help'):
            print 'Expected a value for option {flag}, got nothing instead\n'.format(flag=opt_flag)
            self._print_usage_and_exit(1)

    def _add_valid_defaults(self):
        # Set option values to their given default value, but only if no value has been set
        # already (via the command line) and the option is in the dict of valid opts
        for option, value in self.defaults.items():
            if (option not in self.parsed_opts) and (option in self.valid_opts):
                self.parsed_opts[option] = value

    def _get_unrecognized_opts(self):
        unrecognized_opts = {}
        for opt_name, opt_value in self.parsed_opts.items():
            if not opt_name in self.valid_opts:
                unrecognized_opts[opt_name] = opt_value

        return unrecognized_opts

    def _normalize_opt_values(self):
        for opt_name, opt_value in self.parsed_opts.items():
            if isinstance(opt_value, str):
                self.parsed_opts[opt_name] = normalize_path(opt_value, self.search_dirs)
