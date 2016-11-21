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
timeLimitFetch=180

# Adds <days> to the current time to facilitate
# crawling urls already fetched sooner then
# db.default.fetch.interval.
addDays=0
#############################################

# note that some of the options listed here could be set in the
# corresponding hadoop site xml param file
commonOptions="-D mapreduce.job.reduces=$numTasks -D mapred.child.java.opts=-Xmx1000m -D mapreduce.reduce.speculative=false -D mapreduce.map.speculative=false -D mapreduce.map.output.compress=true"

# determines whether mode based on presence of job file
if [ $NUTCH_RUNTIME_MODE=="DISTRIBUTE" ]; then
  # check that hadoop can be found on the path
  if [ $(which hadoop | wc -l ) -eq 0 ]; then
    echo "Can't find Hadoop executable. Add HADOOP_HOME/bin to the path or run in local mode."
    exit -1;
  fi

  if [ $SEEDDIR != "false" ]; then
    HDFS_SEED_DIR=/tmp/nutch-$USER/seeds
    mkdir -p $HDFS_SEED_DIR
    cp -r "$SEEDDIR" "$HDFS_BASE_URI$HDFS_SEED_DIR"
    hadoop fs -mkdir -p $HDFS_SEED_DIR
    hadoop fs -copyFromLocal "$SEEDDIR" "$HDFS_BASE_URI$HDFS_SEED_DIR"

    SEEDDIR=$HDFS_SEED_DIR
  fi
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

# initial injection
# echo "Injecting seed URLs"
if [ $SEEDDIR != "false" ]; then
  __bin_nutch inject "$SEEDDIR" -crawlId "$CRAWL_ID"
fi

# main loop : rounds of generate - fetch - parse - update
for ((a=1; a <= LIMIT ; a++))
do
  DATE=`date +%s`
  if [ -e ".STOP" ]
  then
   echo "STOP file found - escaping loop"
   mv .STOP ".STOP_EXECUTED_$DATE"
   break
  fi

  echo "\n\n\n"
  echo `date` ": Iteration $a of $LIMIT"

  echo "Generating batchId"
  batchId=$DATE-$RANDOM

  echo "Generating a new fetchlist"
  generate_args=($commonOptions -topN $sizeFetchlist -adddays $addDays -crawlId "$CRAWL_ID" -batchId $batchId)
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
  # __bin_nutch fetch $commonOptions -D fetcher.timelimit.mins=$timeLimitFetch $batchId -crawlId "$CRAWL_ID" -threads 50
  __bin_nutch fetch $commonOptions -D fetcher.timelimit.mins=$timeLimitFetch $batchId -crawlId "$CRAWL_ID" -threads 50 -index -collection $SOLR_COLLECTION

  DATE=`date +%s`
  HOUR=`date +%k`
  # HOUR=1
  if (($HOUR<=6)) || (($HOUR>=22)); then
    echo "Good night, I will sleep for 30 minutes, to awake me, use command : >>>"
    echo "touch .AWAKE"
    echo "<<<"

    for i in {1..180}
    do
      if [ -e ".AWAKE" ]; then
        echo "AWAKE file found - escaping loop"
        mv .AWAKE ".AWAKE_EXECUTED_$DATE"
        break
      else
        sleep 10s
      fi
    done
  fi

done

exit 0
