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

# Modelled after $HADOOP_HOME/bin/stop-nutch.sh.

# Stop hadoop nutch daemons.  Run this on master node.

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/nutch-config.sh
. "$bin"/nutch-common.sh

# variables needed for stop command
if [ "$NUTCH_LOG_DIR" = "" ]; then
  export NUTCH_LOG_DIR="$NUTCH_HOME/logs"
fi
mkdir -p "$NUTCH_LOG_DIR"

if [ "$NUTCH_IDENT_STRING" = "" ]; then
  export NUTCH_IDENT_STRING="$USER"
fi

export NUTCH_LOG_PREFIX=nutch-$NUTCH_IDENT_STRING-master-$HOSTNAME
export NUTCH_LOGFILE=$NUTCH_LOG_PREFIX.log
logout=$NUTCH_LOG_DIR/$NUTCH_LOG_PREFIX.out  
loglog="${NUTCH_LOG_DIR}/${NUTCH_LOGFILE}"
pid=${NUTCH_PID_DIR:-/tmp}/nutch-$NUTCH_IDENT_STRING-master.pid

echo -n stopping nutch
echo "`date` Stopping nutch (via master)" >> $loglog

nohup nice -n ${NUTCH_NICENESS:-0} "$NUTCH_HOME"/bin/nutch \
   --config "${NUTCH_CONF_DIR}" \
   master stop "$@" > "$logout" 2>&1 < /dev/null &

waitForProcessEnd `cat $pid` 'stop-master-command'

rm -f $pid

