#!/usr/bin/env bash

usage="Usage: build-nutch.sh [--add-config <dir>] [--target <runtime|local|deploy>] [--start]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

ADD_CONFIG_DIR=
ANT_TARGET=runtime
START_NUTCH=false
#check to see if the conf dir or hbase home are given as an optional arguments
while [ $# -gt 1 ]
do
  if [ "--add-config" = "$1" ]
  then
    shift
    ADD_CONFIG_DIR=$1
  elif [ "--target" = "$1" ]
  then
    shift
    ANT_TARGET=$1
  elif [ "--start" = "$1" ]
  then
    shift
    START_NUTCH=true
  else
    # Presume we are at end of options and break
    break
  fi

  shift
done

# get arguments
start=$1
shift

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

# the root of the nutch installation
NUTCH_SRC_HOME=`cd "$bin/..">/dev/null; pwd`
cd $NUTCH_SRC_HOME

ANT=/usr/bin/ant
BUILD_DIR=$NUTCH_SRC_HOME/build
BUILD_FILE=$NUTCH_SRC_HOME/build.xml
BUILD_LOGOUT=ant-`date +%Y%m%d`.out
BUILD_LOGOUT_PATH=$BUILD_DIR/$BUILD_LOGOUT

NUTCH_RUNTIME=$NUTCH_SRC_HOME/runtime
NUTCH_DEPLOGY=$NUTCH_RUNTIME/deploy
NUTCH_LOCAL=$NUTCH_RUNTIME/local

if [[ ! -e $BUILD_DIR ]]; then
  mkdir $BUILD_DIR
fi

echo "Working direcotry : `pwd`"
echo "Nutch version : `cat $NUTCH_SRC_HOME/VERSION`"
echo "Log file : $BUILD_LOGOUT_PATH"

[[ -d $ADD_CONFIG_DIR ]] && cp -r $NUTCH_SRC_HOME/$ADD_CONFIG_DIR/* $NUTCH_SRC_HOME/conf/

# TODO : select local, deploy, both mode
# TODO : deploy to test environment
$ANT $ANT_TARGET -logfile $BUILD_LOGOUT_PATH -buildfile $BUILD_FILE
echo "Last build message : "
echo "..........................................................."
tail -n 15 $BUILD_LOGOUT_PATH
echo "..........................................................."

if $START_NUTCH; then
  $NUTCH_LOCAL/bin/start-nutch.sh
fi

