#!/bin/sh
# 
# Copyright (c) 2014 Final Level
# Author: Denys Misko <gdraal@gmail.com>
# Distributed under BSD (3-Clause) License (See
# accompanying file LICENSE)
# 
# Description: Metis manager server wrapper script

# Source function library.
. /etc/rc.d/init.d/functions

# default config
#CONFIG="/etc/metis.cnf"
PROCESS="/usr/bin/metis_manager"
PROCESS_NAME="metis"
LOGFILE="/var/log/metis/manager"
PIDFILE="/var/run/metis_manager"
DESCRIPTION="Metis manager server wrapper script"
MAIL_CMD="/bin/mail"

MAILTO="root@localhost"

if [ "$#" == "0" ]; then
	echo "Usage: ./metis_manager_wrapper.sh _num_ params"
	exit 1
fi

num=`echo $1 | tr -d '_'`
pidfile="$PIDFILE$num.pid"
shift
params=$@

ulimit -c unlimited
ulimit -n 100000


cd /tmp

WPID=$$

while true ; do
	echo "Starting $DESCRIPTION"

	if [ "$CONFIG" != "" ] 
	then
		CONFIGCMD="-c $CONFIG"
	else
		CONFIGCMD=
	fi

	"$PROCESS" $CONFIGCMD $params > /dev/null &
	DPID=$!
	echo $WPID > "$pidfile"
	echo $DPID >> "$pidfile"
	wait

	lastlog="$( echo \"*** $PROCESS_NAME DIED!!! ***\"; date; tail -2 $LOGFILE )"

	echo "$lastlog"
	if [ -f $MAIL_CMD ]
	then
		echo "$lastlog" | "MAIL_CMD" -s "$PROCESS_NAME died `hostname -s`" "$MAILTO"
	fi

	sleep 5
done