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

NAME=metis_manager
DESC="metis_manager"

PATH=/sbin:/bin:/usr/sbin:/usr/bin

# config
#BIN_PATH="/usr/bin/"
BIN_PATH="/home/draal/projects/fl/metis/"
WRAPPER_NAME="metis_manager_wrapper.sh"
WRAPPER="$BIN_PATH$WRAPPER_NAME"
PIDFILE="/var/run/metis_manager"

function check_pid_names {
	local pid="$1"
	local params="${2/-/\\-}"
	local name="$3"
	local PN=$( ps -o args -p $pid 2>/dev/null | grep "$params" | grep "$name" )
	if [ -z "$PN" ]; then
		return 1
	else
		return 0
	fi
}

function start_server() {
	local num="$1"
	local params="$2"
	local pidfile="$PIDFILE$num.pid"
	local wrapper="$WRAPPER _${num}_ $params"
	echo "... $NAME ($num - $params)"
	if [ -f $pidfile ]; then
		local dpid=$( tail -1 "$pidfile" )
		local wpid=$( head -1 "$pidfile" )
		if check_pid_names $wpid "_${num}_" $WRAPPER_NAME; then
			echo " ...  wrapper $wrapper is already running"
			return 0
    		fi
		if check_pid_names $dpid "$params" $NAME; then
			echo " ... $NAME $dpid from $wrapper is already running"
			return 0
    		fi
		$wrapper &
	else
		$wrapper &
	fi
	return 1
}

function stop_server() {
	local num="$1"
	local params="$2"
	local pidfile="$PIDFILE$num.pid"
	if [ -f $pidfile ]; then
		local dpid=$( tail -1 "$pidfile" )
		local wpid=$( head -1 "$pidfile" )
		if check_pid_names $wpid "_${num}_" $WRAPPER_NAME; then
			kill -9 "$wpid"
		else
			echo " ... wrapper $WRAPPER_NAME _${num}_ hasn't run"
			return 0
    fi
		if check_pid_names $dpid "$params" $NAME; then
			kill -15 "$dpid" && sleep 5 && kill -9 "$dpid" 2>/dev/null
		else
			echo " ... $NAME $dpid from $WRAPPER_NAME _${num}_ hasn't run"
			return 0
    fi
		echo -n > "$pidfile"
	else
		echo "... $NAME $num hasn't run"
	fi
}

start() {
	echo "."
	for (( i = 1; i <= 36; i++ ))
	do
		local param=SERVER$i
		if [ ! -z "${!param}" ]; then
			start_server $i "${!param}"
		fi
	done
}
	

stop() {
	for (( i = 1; i <= 36; i++ ))
	do
		local param=SERVER$i
		if [ ! -z "${!param}" ]; then
			echo -n "Stopping $NAME $i (${!param})"
			stop_server $i "${!param}"
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
