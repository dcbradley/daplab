#!/bin/sh

JOB_EXIT_STATUS=$1

die() {
  echo $1 1>&2
  exit 1
}

exec >& server_stop.out

SERVER_ULOG=dag/server.ulog
[ -f $SERVER_ULOG ] || die "$SERVER_ULOG does not exist in $(pwd)."

JOBID=$(awk '/^000 / {pos=match($2,/[0-9]+\.[0-9]+/); print substr($2,RSTART,RLENGTH)}' $SERVER_ULOG)

[ "$JOBID" != "" ] || die "Failed to find job id in socat_server.ulog."

condor_ssh_to_job $JOBID 'kill $_CONDOR_JOB_PIDS'

exit $JOB_EXIT_STATUS

