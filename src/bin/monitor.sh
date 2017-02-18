#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
# The Crawl command script : crawl <seedDir> <crawlId> <solrURL> <numberOfRounds>
#
# 
# UNLIKE THE NUTCH ALL-IN-ONE-CRAWL COMMAND THIS SCRIPT DOES THE LINK INVERSION AND 
# INDEXING FOR EACH BATCH

echo "Starting the crawl ..."
echo "$BASH_SOURCE $@"
echo

bin="`dirname "$0"`"
bin="`cd "$bin"; pwd`"

 . "$bin"/nutch-config.sh

SEEDDIR="$1"
CRAWL_ID="$2"
if [ "$#" -eq 3 ]; then
    LIMIT="$3"
elif [ "$#" -eq 4 ]; then
     SOLR_COLLECTION="$3"
     LIMIT="$4"
else
    echo "Unknown # of arguments $#"
    echo "Usage: crawl <seedDir> <crawlID> [<solrCollection>] <numberOfRounds>"
    exit -1;
fi

if [ "$SEEDDIR" = "" ]; then
    echo "Missing seedDir : crawl <seedDir> <crawlID> [<solrCollection>] <numberOfRounds>"
    exit -1;
fi

if [ "$CRAWL_ID" = "" ]; then
    echo "Missing crawlID : crawl <seedDir> <crawlID> [<solrCollection>] <numberOfRounds>"
    exit -1;
fi

if [ "$SOLR_COLLECTION" = "" ]; then
    # echo "No SOLRURL specified. Skipping indexing."
    echo "Missing solrCollection : crawl <seedDir> <crawlID> [<solrCollection>] <numberOfRounds>"
    exit -1;
fi

if [ "$LIMIT" = "" ]; then
    echo "Missing numberOfRounds : crawl <seedDir> <crawlID> [<solrCollection>] <numberOfRounds>"
    exit -1;
fi

#############################################
# MODIFY THE PARAMETERS BELOW TO YOUR NEEDS #
#############################################

# set max depth, no more links are collected in deeper pages
maxDepth=1

# max out links per page collected each time
maxOutLinks=100

# set the number of slaves nodes
numSlaves=1
if [ -n "$NUMBER_SLAVES" ]; then
 numSlaves=$NUMBER_SLAVES
fi

# and the total number of available tasks
# sets Hadoop parameter "mapreduce.job.reduces"
numTasks=`expr $numSlaves \* 2`

# number of urls to fetch in one iteration
# It's depend on how fast do you want to finish the fetch loop
sizeFetchlist=`expr $numSlaves \* 1000`

# time limit for feching
fetchJobTimeout=1h

# Adds <days> to the current time to facilitate
# crawling urls already fetched sooner then
# db.default.fetch.interval.
addDays=0
#############################################

# note that some of the options listed here could be set in the
# corresponding hadoop site xml param file
commonOptions=(
    "-D mapreduce.job.reduces=$numTasks"
    "-D mapred.child.java.opts=-Xmx1000m"
    "-D mapreduce.reduce.speculative=false"
    "-D mapreduce.map.speculative=false"
    "-D mapreduce.map.output.compress=true"
    "-D generate.max.distance=$maxDepth"
    "-D db.max.outlinks.per.page=$maxOutLinks"
)

# determines whether mode based on presence of job file
if [ $NUTCH_RUNTIME_MODE=="DISTRIBUTE" ]; then
  # check that hadoop can be found on the path
  if [ $(which hadoop | wc -l ) -eq 0 ]; then
    echo "Can't find Hadoop executable. Add HADOOP_HOME/bin to the path or run in local mode."
    exit -1;
  fi

# It's done in InjectJob, can be removed later
#  if [ -e $SEEDDIR ]; then
#    HDFS_SEED_DIR=/tmp/nutch-$USER/seeds
#    hadoop fs -test -e $HDFS_SEED_DIR
#    (( $?==0 )) && hadoop fs -mkdir -p $HDFS_SEED_DIR
#    hadoop fs -copyFromLocal -f "$SEEDDIR" "$HDFS_BASE_URI$HDFS_SEED_DIR"
#
#    SEEDDIR=$HDFS_SEED_DIR
#  fi
fi

if [ ! -e "$NUTCH_HOME/logs" ]; then
  mkdir "$NUTCH_HOME/logs"
fi

function __bin_nutch {
  # run $bin/nutch, exit if exit value indicates error

  echo "$bin/nutch $@" ; # echo command and arguments
  "$bin/nutch" "$@"

  RETCODE=$?
  if [ $RETCODE -ne 0 ]
  then
      echo "Error running:"
      echo "  $bin/nutch $@"
      echo "Failed with exit value $RETCODE."
      exit $RETCODE
  fi
}

function __is_crawl_loop_stopped {
  STOP=0
  if [ -e ".STOP" ] || [ -e ".KEEP_STOP" ]; then
   if [ -e ".STOP" ]; then
     mv .STOP ".STOP_EXECUTED_`date +%Y%m%d.%H%M%S`"
   fi

    STOP=1
  fi

  (( $STOP==1 )) && return 0 || return 1
}

function __check_index_server_available {
  for i in {1..200}
  do
    wget -q --spider $SOLR_TEST_URL

    if (( $?==0 )); then
      return 0;
    fi

    if ( __is_crawl_loop_stopped ); then
      echo "STOP file found - escaping loop"
      exit 0
    fi

    echo "Index server not available, check 15s later ..."
    sleep 15s
  done

  echo "Index server is gone."
  return 1
}

# initial injection
# echo "Injecting seed URLs"
if [ $SEEDDIR != "false" ]; then
  __bin_nutch inject "$SEEDDIR" -crawlId "$CRAWL_ID"
fi

# main loop : rounds of generate - fetch - parse - update
for ((a=1; a <= LIMIT ; a++))
do
  if ( __is_crawl_loop_stopped ); then
     echo "STOP file found - escaping loop"
     exit 0
  fi

  if ( ! __check_index_server_available ); then
    exit 1
  fi

  echo -e "\n\n--------------------------------------------------------------------"
  echo `date` ": Iteration $a of $LIMIT"

  batchId=`date +%s`-$RANDOM

  echo "Generating a new fetchlist"
  generate_args=(${commonOptions[@]}  "-D crawl.round=$a" -batchId $batchId -reGen -topN $sizeFetchlist -adddays $addDays -crawlId "$CRAWL_ID")
  echo "$bin/nutch generate ${generate_args[@]}"
  $bin/nutch generate "${generate_args[@]}"
  RETCODE=$?
  if [ $RETCODE -eq 0 ]; then
      : # ok: no error
  elif [ $RETCODE -eq 1 ]; then
    echo "Generate returned 1 (no new segments created)"
    echo "Escaping loop: no more URLs to fetch now"
    break
  else
    echo "Error running:"
    echo "  $bin/nutch generate ${generate_args[@]}"
    echo "Failed with exit value $RETCODE."
    exit $RETCODE
  fi

  echo "Fetching : "
  __bin_nutch fetch "${commonOptions[@]}" -D crawl.round=$a -D fetcher.timelimit=$fetchJobTimeout $batchId -crawlId "$CRAWL_ID" -threads 50 -index -collection $SOLR_COLLECTION

  echo "Updating outgoing pages : "
  __bin_nutch updateoutgraph "${commonOptions[@]}" -D crawl.round=$a $batchId -crawlId "$CRAWL_ID"

  echo "Updating incoming pages : "
  __bin_nutch updateingraph "${commonOptions[@]}" -D crawl.round=$a $batchId -crawlId "$CRAWL_ID"

done

exit 0
