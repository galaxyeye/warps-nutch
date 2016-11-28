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

CONFIG_DIR=conf/configsets/information/local
SEEDDIR=conf/configsets/information/local/seeds/all.txt
CRAWL_ID=information_tmp
SOLR_URL=http://master:8983/solr/information_1101_integration_test
LIMIT=20

__bin_nutch inject -c="$CONFIG_DIR" "$SEEDDIR" -crawlId "$CRAWL_ID"

# main loop : rounds of generate - fetch - parse - update
for ((a=1; a <= LIMIT ; a++))
do
  DATE=`date +%s`
  if [ -e ".STOP" ] || [ -e ".KEEP_STOP" ]; then
   echo "STOP file found - escaping loop"

   if [ -e ".STOP" ]; then
     mv .STOP ".STOP_EXECUTED_$DATE"
   fi
   break
  fi

  echo "\n\n\n"
  echo `date` ": Iteration $a of $LIMIT"

  echo "Generating batchId"
  batchId=$DATE-$RANDOM

  echo "Generating a new fetchlist"
  generate_args=(-c="$CONFIG_DIR" -topN 1000 -crawlId "$CRAWL_ID")
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

  batchId=`cat /tmp/nutch-$USER/last-batch-id`
  __bin_nutch fetch -c="$CONFIG_DIR" $batchId -crawlId "$CRAWL_ID" -threads 10 -crawlId $CRAWL_ID -fetchMode native -index -solrUrl $SOLR_URL

done

exit 0
