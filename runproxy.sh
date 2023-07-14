#!/bin/sh

DIR=/opt/tcpproxy
LOCAL=$1
REMOTE=$2
LOGDIR=$3
PIDFILE=$4

PATH=$PATH:/usr/local/bin

if [ -f $PIDFILE ]; then
    OLDPID=`cat $PIDFILE`
    if kill 2>/dev/null -0 $OLDPID; then
	echo "Process already running. Exiting."
    fi
fi

trap "rm $PIDFILE" TERM

echo $$ > $PIDFILE

tcpproxy -l $LOCAL -r $REMOTE -o $LOGDIR
