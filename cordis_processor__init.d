#!/bin/bash
### BEGIN INIT INFO
# Provides:          cordis_processor
# Required-Start:    $remote_fs $network
# Required-Stop:     $remote_fs $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: starts cordis processor
# Description:       Starts The Cordis Project Processing Deamon
### END INIT INFO

DAEMON_PATH="/opt/cpd"

NAME=cordisProcessorDaemon.py
DAEMON="$DAEMON_PATH/$NAME"

DAEMONOPTS=""

DESC="My daemon description"
PIDFILE=/var/run/$NAME.pid
SCRIPTNAME=/etc/init.d/$NAME
#PYTHON_ENVIROMENT="/home/slo/cordisEnviroment/bin/python"
PYTHON_ENVIROMENT="/usr/bin/python3"


case "$1" in
start)
        if [ -f $PIDFILE ]; then
            printf "%s\n" "Process already running pidfile exists"
            exit 1
        fi
  printf "%-50s" "Starting $NAME..."
  cd $DAEMON_PATH

# PID=`$DAEMON $DAEMONOPTS > /dev/null 2>&1 & echo $!`
  PID=`$PYTHON_ENVIROMENT $DAEMON > /dev/null 2>&1 & echo $!`
  #echo "Saving PID" $PID " to " $PIDFILE
        if [ -z $PID ]; then
            printf "%s\n" "Fail"
        else
            echo $PID > $PIDFILE
            printf "%s\n" "Ok"
        fi
;;
status)
        printf "%-50s" "Checking $NAME..."
        if [ -f $PIDFILE ]; then
            PID=`cat $PIDFILE`
            if [ -z "`ps axf | grep ${PID} | grep -v grep`" ]; then
                printf "%s\n" "Process dead but pidfile exists"
            else
                echo "Running"
            fi
        else
            printf "%s\n" "Service not running"
        fi
;;
stop)
        printf "%-50s" "Stopping $NAME"
            PID=`cat $PIDFILE`
            cd $DAEMON_PATH
        if [ -f $PIDFILE ]; then
            kill -HUP $PID
            printf "%s\n" "Ok"
            rm -f $PIDFILE
        else
            printf "%s\n" "pidfile not found"
        fi
;;

restart)
    $0 stop
    $0 start
;;

*)
        echo "Usage: $0 {status|start|stop|restart}"
        exit 1
esac
