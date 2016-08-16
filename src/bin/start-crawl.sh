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
     SOLRURL="$3"
     LIMIT="$4"
else
    echo "Unknown # of arguments $#"
    echo "Usage: crawl <seedDir> <crawlID> [<solrUrl>] <numberOfRounds>"
    exit -1;
fi

if [ "$SEEDDIR" = "" ]; then
    echo "Missing seedDir : crawl <seedDir> <crawlID> [<solrURL>] <numberOfRounds>"
    exit -1;
fi

if [ "$CRAWL_ID" = "" ]; then
    echo "Missing crawlID : crawl <seedDir> <crawlID> [<solrURL>] <numberOfRounds>"
    exit -1;
fi

if [ "$SOLRURL" = "" ]; then
    echo "No SOLRURL specified. Skipping indexing."
fi

if [ "$LIMIT" = "" ]; then
    echo "Missing numberOfRounds : crawl <seedDir> <crawlID> [<solrURL>] <numberOfRounds>"
    exit -1;
fi

$logdir=logs
$logout=$logdir/crawl_information_native_0724.out

$bin/crawl $SEEDDIR $CRAWL_ID $SOLRURL $LIMIT > logs/crawl_information_native_0724.out 2>&1 &
