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

def _get_job_status(job_id):
    args = ['condor_q', job_id, '-format', '%s\n', 'JobStatus']
    condor_q = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    status, err = condor_q.communicate()
    status = status.strip()

    return status

def wait():
    server_log = sys.argv[1]
    if not os.access(server_log, os.F_OK):
        error = 'Server ulog {file} does not exist in {dir}'.format(file=server_log, dir=os.getcwd())
        _die(error)
    if not os.access(server_log, os.R_OK):
        error = 'Cannot open server ulog {file} for reading'.format(file=server_log)
        _die(error)

    job_id = _get_job_id(server_log)

    status = _get_job_status(job_id)
    idle = '1'; running = '2'

    wait_seconds = 30
    for _ in range(wait_seconds):
        if status == running: return

        sleep(1)
        status = _get_job_status(job_id)

    if status == idle:
        error = 'After waiting {time} seconds, server job {job_id} is still idle, exiting now'.format(time=wait_seconds, job_id=job_id)
        _die(error)
    else:
        error = 'Server job {job_id} has status {status}'.format(job_id=job_id, status=status)
        _die(error)

if __name__=='__main__':
    wait()
