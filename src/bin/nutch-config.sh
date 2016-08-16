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

# included in all the hbase scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*
# Modelled after $HADOOP_HOME/bin/hadoop-env.sh.

# resolve links - "${BASH_SOURCE-$0}" may be a softlink

this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin">/dev/null; pwd`
this="$bin/$script"

# the root of the hbase installation
if [ -z "$NUTCH_HOME" ]; then
  NUTCH_HOME=`dirname "$this"`/..
  NUTCH_HOME=`cd "$NUTCH_HOME">/dev/null; pwd`
  export NUTCH_HOME=$NUTCH_HOME
fi

#check to see if the conf dir or hbase home are given as an optional arguments
while [ $# -gt 1 ]
do
  if [ "--config" = "$1" ]
  then
    shift
    confdir=$1
    shift
    NUTCH_CONF_DIR=$confdir
  else
    # Presume we are at end of options and break
    break
  fi
done

# NUTCH_JOB
if [ -f "${NUTCH_HOME}"/*nutch*-job.jar ]; then
  local=false
  for f in "$NUTCH_HOME"/*nutch*-job.jar; do
    NUTCH_JOB="$f";
  done
else
  local=true
fi

# refresh static files if in develop mode
if [[ -f ${NUTCH_HOME}/../../build.xml ]]; then
  DEV_NUTCH_HOME=`cd "${NUTCH_HOME}/../.." > /dev/null; pwd`
  if [ -d ${DEV_NUTCH_HOME} ]; then
    cp -r ${DEV_NUTCH_HOME}/src/bin/* ${NUTCH_HOME}/bin/
    cp -r ${DEV_NUTCH_HOME}/conf/* ${NUTCH_HOME}/conf/
  fi
fi

if $local; then
  export NUTCH_RUNTIME_MODE="LOCAL"
else
  export NUTCH_RUNTIME_MODE="DISTRIBUTE"
  export NUTCH_JOB=$NUTCH_JOB
fi

# get log directory
if [ "$NUTCH_LOG_DIR" = "" ]; then
  export NUTCH_LOG_DIR="$NUTCH_HOME/logs"
fi
mkdir -p "$NUTCH_LOG_DIR"

if [ "$NUTCH_NICENESS" = "" ]; then
    export NUTCH_NICENESS=0
fi

if [ "$NUTCH_IDENT_STRING" = "" ]; then
  export NUTCH_IDENT_STRING="$USER"
fi

if [ "$NUTCH_PID_DIR" = "" ]; then
  export NUTCH_PID_DIR="/tmp"
fi

if [ "$NUTCH_CONF_DIR" = "" ]; then
  export NUTCH_CONF_DIR="$NUTCH_HOME/conf"
fi

if [ -f $NUTCH_CONF_DIR/slaves ]; then
  export NUMBER_SLAVES=`cat $NUTCH_CONF_DIR/slaves | wc -l`
fi

# Allow alternate nutch conf dir location.
NUTCH_CONF_DIR="${NUTCH_CONF_DIR:-$NUTCH_HOME/conf}"

# REST JMX opts
if [[ -n "$NUTCH_JMX_OPTS" && -z "$NUTCH_REST_JMX_OPTS" ]]; then
  NUTCH_REST_JMX_OPTS="$NUTCH_JMX_OPTS -Dcom.sun.management.jmxremote.port=10105"
fi

# REST opts
if [ -z "$NUTCH_REST_OPTS" ]; then
  export NUTCH_REST_OPTS="$NUTCH_REST_JMX_OPTS"
fi

# Source the hbase-env.sh.  Will have JAVA_HOME defined.
if [ -z "$NUTCH_ENV_INIT" ] && [ -f "${NUTCH_CONF_DIR}/nutch-env.sh" ]; then
  . "${NUTCH_CONF_DIR}/nutch-env.sh"
  export NUTCH_ENV_INIT="true"
fi

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

# Now having JAVA_HOME defined is required 
if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+======================================================================+
EOF
    exit 1
fi

