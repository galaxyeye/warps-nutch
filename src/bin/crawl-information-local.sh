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

SEEDDIR="$1"
CRAWL_ID="$2"
if [ "$#" -eq 3 ]; then
    LIMIT="$3"
elif [ "$#" -eq 4 ]; then
     SOLR_URL="$3"
     LIMIT="$4"
else
    echo "Unknown # of arguments $#"
    echo "Usage: crawl <seedDir> <crawlID> [<solrUrl>] <numberOfRounds>"
    exit -1;
fi

if [ "$SEEDDIR" = "" ]; then
    echo "Missing seedDir : crawl <seedDir> <crawlID> [<solrUrl>] <numberOfRounds>"
    exit -1;
fi

if [ "$CRAWL_ID" = "" ]; then
    echo "Missing crawlID : crawl <seedDir> <crawlID> [<solrUrl>] <numberOfRounds>"
    exit -1;
fi

if [ "$SOLR_URL" = "" ]; then
    # echo "No SOLRURL specified. Skipping indexing."
    echo "Missing solrUrl : crawl <seedDir> <crawlID> [<solrUrl>] <numberOfRounds>"
    exit -1;
fi

if [ "$LIMIT" = "" ]; then
    echo "Missing numberOfRounds : crawl <seedDir> <crawlID> [<solrUrl>] <numberOfRounds>"
    exit -1;
fi

# initial injection
# echo "Injecting seed URLs"
if [ $SEEDDIR != "false" ]; then
  __bin_nutch inject -c="$CONFIG_DIR" "$SEEDDIR" -crawlId "$CRAWL_ID"
fi

# main loop : rounds of generate - fetch - parse - update
for ((a=1; a <= LIMIT ; a++))
do
  echo -e "\n\n\n"
  echo `date` ": Iteration $a of $LIMIT"

  batchId=`date +%s`-$RANDOM

  echo "Generating a new fetchlist"
  generate_args=(-c="$CONFIG_DIR" "-D crawl.round=$a" -topN 1000 -crawlId "$CRAWL_ID")
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
  __bin_nutch fetch -c="$CONFIG_DIR" -D crawl.round=$a $batchId -crawlId "$CRAWL_ID" -threads 10 -crawlId $CRAWL_ID -fetchMode native -update -index -solrUrl $SOLR_URL

done

exit 0
