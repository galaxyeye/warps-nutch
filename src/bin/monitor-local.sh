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

CONF_DIR="$1"
SEED_DIR="$2"
CRAWL_ID="$3"
if [ "$#" -eq 4 ]; then
    LIMIT="$4"
elif [ "$#" -eq 5 ]; then
     SOLR_URL="$4"
     LIMIT="$5"
else
    echo "Unknown # of arguments $#"
    echo "Usage: crawl-information-local <confDir> <seedDir> <crawlID> [<solrUrl>] <numberOfRounds>"
    exit -1;
fi

if [ "$SEED_DIR" = "" ] || [ "$CRAWL_ID" = "" ] || [ "$SOLR_URL" = "" ] || [ "$LIMIT" = "" ]; then
    echo "Usage: crawl <seedDir> <crawlID> [<solrUrl>] <numberOfRounds>"
    exit -1;
fi

# initial injection
# echo "Injecting seed URLs"
if [ $SEED_DIR != "false" ]; then
  __bin_nutch inject -c="$CONF_DIR" "$SEED_DIR" -crawlId "$CRAWL_ID"
fi

# main loop : rounds of generate - fetch - parse - update
for ((a=1; a <= LIMIT ; a++))
do
  echo -e "\n\n\n"
  echo `date` ": Iteration $a of $LIMIT"

  batchId=`date +%s`-$RANDOM

  echo "Generating : "
  __bin_nutch generate -c="$CONF_DIR" -D crawl.round=$a -batchId $batchId -topN 1000 -crawlId "$CRAWL_ID"

  echo "Fetching : "
  __bin_nutch fetch -c="$CONF_DIR" -D crawl.round=$a $batchId -crawlId "$CRAWL_ID" -threads 10 -crawlId "$CRAWL_ID" -fetchMode native -index -solrUrl $SOLR_URL

  echo "Updating outgoing pages : "
  __bin_nutch updateoutgraph -c="$CONF_DIR" -D crawl.round=$a $batchId -crawlId "$CRAWL_ID"

  echo "Updating incoming pages : "
  __bin_nutch updateingraph -c="$CONF_DIR" -D crawl.round=$a $batchId -crawlId "$CRAWL_ID"

done

exit 0
