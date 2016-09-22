#!/usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# 
# Runs a Hadoop NUTCH COMMAND as a daemon.
#
# Environment Variables
#
#   NUTCH_CONF_DIR   	Alternate NUTCH conf dir. Default is ${NUTCH_HOME}/conf.
#   NUTCH_LOG_DIR    	Where log files are stored.  PWD by default.
#   NUTCH_PID_DIR    	The pid files are stored. /tmp by default.
#   NUTCH_IDENT_STRING  A string representing this instance of hadoop. $USER by default
#   NUTCH_NICENESS 	The scheduling priority for daemons. Defaults to 0.
#   NUTCH_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server if it has not stopped.
#                        	Default 1200 seconds.
#
# Modelled after $HADOOP_HOME/bin/hadoop-daemon.sh

usage="Usage: nutch-daemon.sh [--config <conf-dir>]\
 (start|stop|restart|autorestart) <NUTCH-COMMAND> \
 <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

 . "$bin"/nutch-config.sh
 . "$bin"/nutch-common.sh

# get arguments
startStop=$1
shift

COMMAND=$1
shift

echo "nutch home : $NUTCH_HOME"
if [ -n $DEV_NUTCH_HOME ]; then
    echo "we are in development mode..."
    echo "development home : ${DEV_NUTCH_HOME}"
fi
echo "working direcotry : `pwd`"
echo "nutch version : `cat $NUTCH_HOME/VERSION`"

# Some variables
# Work out java location so can print version into log.
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi
JAVA=$JAVA_HOME/bin/java

export NUTCH_LOG_PREFIX=nutch-$NUTCH_IDENT_STRING-$COMMAND-$HOSTNAME
export NUTCH_LOGFILE=$NUTCH_LOG_PREFIX.log
export NUTCH_LOGOUT=$NUTCH_LOG_PREFIX.out

loglog="${NUTCH_LOG_DIR}/${NUTCH_LOGFILE}"
logout="${NUTCH_LOG_DIR}/${NUTCH_LOGOUT}"
pid=$NUTCH_PID_DIR/nutch-$NUTCH_IDENT_STRING-$COMMAND.pid

echo "NUTCH_HOME: $NUTCH_HOME"
echo "Log file : $loglog"
if [ ! -f $loglog ]; then
  echo "Log file created by $script, `date`" > $loglog
fi

check_before_start() {
    #ckeck if the process is not running
    mkdir -p "$NUTCH_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $COMMAND running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
}

nutch_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

wait_until_done ()
{
    p=$1
    cnt=${NUTCH_SLAVE_TIMEOUT:-300}
    origcnt=$cnt
    while kill -0 $p > /dev/null 2>&1; do
      if [ $cnt -gt 1 ]; then
        cnt=`expr $cnt - 1`
        sleep 1
      else
        echo "Process did not complete after $origcnt seconds, killing."
        kill -9 $p
        exit 1
      fi
    done
    return 0
}

thiscmd="$bin/$(basename ${BASH_SOURCE-$0})"
args=$@

case $startStop in

(start)
    check_before_start
    nutch_rotate_log $logout
    echo starting $COMMAND, logging to $logout
    nohup $thiscmd --config "${NUTCH_CONF_DIR}" internal_start $COMMAND $args < /dev/null > ${logout} 2>&1  &
    sleep 1; head "${logout}"
  ;;

(internal_start)
    # Add to the COMMAND log file vital stats on our environment.
    echo "`date` Starting $COMMAND on `hostname`" >> $loglog
    echo "`ulimit -a`" >> $loglog 2>&1
    nice -n $NUTCH_NICENESS "$NUTCH_HOME"/bin/nutch \
        --config "${NUTCH_CONF_DIR}" \
        $COMMAND "$@" start >> "$logout" 2>&1 &
    echo $! > $pid
    wait
  ;;

(autorestart)
    # be the same with start
    check_before_start
    nutch_rotate_log $logout
    echo starting $COMMAND, logging to $logout
    nohup $thiscmd --config "${NUTCH_CONF_DIR}" internal_start $COMMAND $args < /dev/null > ${logout} 2>&1  &
    sleep 1; head "${logout}"
  ;;

(stop)
    if [ -f $pid ]; then
      pidToKill=`cat $pid`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo -n stopping $COMMAND
        echo "`date` Terminating $COMMAND" >> $loglog
        kill $pidToKill > /dev/null 2>&1
        waitForProcessEnd $pidToKill $COMMAND
      else
        retval=$?
        echo no $COMMAND to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
    else
        echo no $COMMAND to stop because no pid file $pid
    fi
    rm -f $pid
  ;;

(restart)
    # stop the COMMAND
    $thiscmd --config "${NUTCH_CONF_DIR}" stop $COMMAND $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${NUTCH_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi
    # start the COMMAND
    $thiscmd --config "${NUTCH_CONF_DIR}" start $COMMAND $args &
    wait_until_done $!
  ;;

(*)
  echo $usage
  exit 1
  ;;
esac
