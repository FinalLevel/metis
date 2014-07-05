#!/bin/sh
# 
# Copyright (c) 2014 Final Level
# Author: Denys Misko <gdraal@gmail.com>
# Distributed under BSD (3-Clause) License (See
# accompanying file LICENSE)
# 
# chkconfig: 2345 61 39
# description: Metis Storage is a CDN and high available http server
# processname: metis_manager
# pidfile: /var/run/metis_manager.pid

# Source function library.
. /etc/rc.d/init.d/functions
. /etc/sysconfig/metis_manager

NAME=metis
DESC="metis"

PATH=/sbin:/bin:/usr/sbin:/usr/bin

# config
BIN_PATH="/usr/bin/"
NAME="metis_manager"
WRAPPER_NAME="metis_manager_wrapper.sh"
WRAPPER="$BIN_PATH$WRAPPER_NAME"
PIDFILE="/var/run/metis_manager"

function check_pid_names {
	pid="$1"
	num="$2"
	name="$3"
	PN=$( ps -o args -p $pid 2>/dev/null | grep $num | grep $name )
	if [ -z $PN ]; then
		return 0
	else
		return 1
	fi
}

function start_manager() {
	num="$1"
	params="$2"
	pidfile="$PIDFILE$num.pid"
	wrapper="$WRAPPER _${num}_ $params"
	echo "... $NAME ($num - $params)"
	if [ -f $pidfile ]; then
		dpid=$( tail -1 "$pidfile" )
		wpid=$( head -1 "$pidfile" )
		if check_pid_names $wpid "_${num}_" $WRAPPER_NAME; then
			echo "Wrapper $wrapper is already running"
			return 0
    		fi
		if check_pid_names $dpid "-s $num" $NAME; then
			echo "$NAME $dpid from $wrapper is already running"
			return 0
    		fi
		$wrapper &
	else
		$wrapper &
	fi
	return 1
}

function stop_manager() {
	$num = "$1"
	$pidfile = "$PIDFILE$num.pid"
	if [ -f $pidfile ]; then
		dpid=$( tail -1 "$pidfile" )
		wpid=$( head -1 "$pidfile" )
		if check_pid_names $wpid "_${num}_" $WRAPPER_NAME; then
			kill -9 "{$wpid}"
		else
			echo "Wrapper $WRAPPER_NAME _${num}_ hasn't runned"
			return 0
    fi
		if check_pid_names $dpid "-s $num" $NAME; then
			kill -15 "$dpid" && sleep 5 && kill -9 "{$dpid}"
		else
			echo "$NAME $dpid from $WRAPPER_NAME _${num}_ hasn't runned"
			return 0
    fi
		echo -n > "$pidfile"
	else
		echo "$NAME $num isn't running"
	fi
}

start() {
	echo "."
	for (( i = 1; i <= 36; i++ ))
	do
		param=SERVER$i
		if [ ! -z "${!param}" ]; then
			start_manager $i "${!param}"
		fi
	done
}
	

stop() {
	echo -n "Stopping $DESC: $NAME"
	for (( i = 1; i <= 36; i++ ))
	do
		param=SERVER$i
		if [ ! -z "${!param}" ]; then
			stop_manager $i
		fi
	done
}

case "$1" in
	start)
		echo -n "Starting $DESC: $NAME"

		start
		echo "." ; sleep 2
	;;

	stop)
		echo -n "Stopping $DESC: $NAME "

		stop
		exit $?
	;;

	restart|force-reload)
		echo -n "Restarting $DESC: $NAME"

		stop
		sleep 1
		start

		exit $?
	;;

	status)
		echo "Status $DESC: $NAME"

		true
		exit $?
	;;

	*)
		N=/etc/init.d/${NAME}.sh
		echo "Usage: $N {start|stop|restart|force-reload|status}" >&2
		exit 1
        ;;
esac

exit 0
