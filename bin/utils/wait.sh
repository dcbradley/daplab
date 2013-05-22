#!/bin/sh

die() {
  echo $1 1>&2
  exit 1
}

exec >& server_wait.out

SERVER_ULOG=$1
[ -f $SERVER_ULOG ] || die "$SERVER_ULOG does not exist in $(pwd)."

JOBID=$(awk '/^000 / {pos=match($2,/[0-9]+\.[0-9]+/); print substr($2,RSTART,RLENGTH)}' $SERVER_ULOG)

[ "$JOBID" != "" ] || die "Failed to find job id in server.ulog."

# while the server job is idle, wait
while [ 1 ]; do
 status="$(condor_q $JOBID -format '%s\n' JobStatus)"
 if [ "$status" = 1 ]; then   # idle
   sleep 2
 elif [ "$status" = 2 ]; then # running
   break
 else                         # held? removed?
   die "Unexpected server job status $status"
 fi
done

exit 0

