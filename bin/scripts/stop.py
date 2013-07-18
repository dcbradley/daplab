#!/usr/bin/env python

import os
import subprocess
import sys
from time import sleep

def _die(error):
    sys.stderr.write(error + "\n")
    sys.exit(1)

def _get_job_id(server_log):
    with open(server_log) as f:
        log = f.read()

        cluster_tag = '"Cluster"><i>'
        cluster_tag_pos = log.find(cluster_tag)
        if cluster_tag_pos == -1:
            error = 'Cannot parse job id from server ulog {file}'.format(file=server_log)
            _die(error)

        job_id_start = cluster_tag_pos + len(cluster_tag)
        job_id_end = log.find('<', job_id_start)
        job_id = log[job_id_start:job_id_end]

    return job_id

def _kill_job(job_id):
    args = ['condor_ssh_to_job', job_id, 'kill $_CONDOR_JOB_PIDS']
    kill_job = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out, err = kill_job.communicate()
    rc = kill_job.returncode

    return rc, err

def stop():
    server_log = sys.argv[1]
    if not os.access(server_log, os.F_OK):
        error = 'Server ulog {file} does not exist in {dir}'.format(file=server_log, dir=os.getcwd())
        _die(error)
    if not os.access(server_log, os.R_OK):
        error = 'Cannot open server ulog {file} for reading'.format(file=server_log)
        _die(error)

    job_id = _get_job_id(server_log)
    rc, kill_job_err = _kill_job(job_id)

    if rc != 0:
        error = 'condor_ssh_to_job returned code {rc}'.format(rc=rc)
        if kill_job_err:
            error += '\ncondor_ssh_to_job stderr: {err}'.format(err=kill_job_err)

        die(error)

if __name__=='__main__':
    stop()
