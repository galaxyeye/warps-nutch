/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.metadata;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.Text;

/**
 * A collection of Nutch internal metadata constants.
 */
public interface Nutch {

  String ORIGINAL_CHAR_ENCODING = "OriginalCharEncoding";

  String CHAR_ENCODING_FOR_CONVERSION = "CharEncodingForConversion";

  String SIGNATURE_KEY = "nutch.content.digest";

  String BATCH_NAME_KEY = "nutch.batch.name";

  String SCORE_KEY = "nutch.crawl.score";

  String GENERATE_TIME_KEY = "_ngt_";

  Text WRITABLE_GENERATE_TIME_KEY = new Text(GENERATE_TIME_KEY);

  String PROTO_STATUS_KEY = "_pst_";

  Text WRITABLE_PROTO_STATUS_KEY = new Text(PROTO_STATUS_KEY);

  String FETCH_TIME_KEY = "_ftk_";

  String FETCH_STATUS_KEY = "_fst_";

  /**
   * Sites may request that search engines don't provide access to cached
   * documents.
   */
  String CACHING_FORBIDDEN_KEY = "caching.forbidden";

  Utf8 CACHING_FORBIDDEN_KEY_UTF8 = new Utf8(CACHING_FORBIDDEN_KEY);

  /** Show both original forbidden content and summaries (default). */
  String CACHING_FORBIDDEN_NONE = "none";

  /** Don't show either original forbidden content or summaries. */
  String CACHING_FORBIDDEN_ALL = "all";

  /** Don't show original forbidden content, but show summaries. */
  String CACHING_FORBIDDEN_CONTENT = "content";

  String REPR_URL_KEY = "_repr_";

  Text WRITABLE_REPR_URL_KEY = new Text(REPR_URL_KEY);

  String ALL_BATCH_ID_STR = "-all";

  Utf8 ALL_CRAWL_ID = new Utf8(ALL_BATCH_ID_STR);

  String CRAWL_ID_KEY = "storage.crawl.id";

  String FETCH_MODE_KEY = "fetcher.fetch.mode";

  Utf8 DISTANCE = new Utf8("dist");

  // short constants for cmd-line args
  /** Crawl id to use. */
  String ARG_CRAWL = "crawl";
  /** Batch id to select. */
  String ARG_BATCH = "batch";
  /** Resume previously aborted op. */
  String ARG_RESUME = "resume";
  /** Force processing even if there are locks or inconsistencies. */
  String ARG_FORCE = "force";
  /** Sort statistics. */
  String ARG_SORT = "sort";
  /** Number of fetcher threads (per map task). */
  String ARG_THREADS = "threads";
  /** Number of fetcher tasks. */
  String ARG_NUMTASKS = "numTasks";
  /** The notion of current time. */
  String ARG_CURTIME = "curTime";
  /** Apply URLFilters. */
  String ARG_FILTER = "filter";
  /** Apply URLNormalizers. */
  String ARG_NORMALIZE = "normalize";
  /** Class to run as a NutchTool. */
  String ARG_CLASS = "class";
  /** Depth (number of cycles) of a crawl. */
  String ARG_DEPTH = "depth";

  String ARG_START_KEY = "startKey";

  String ARG_END_KEY = "endKey";

  String ARG_LIMIT = "limit";

  /**
   * Injector Relative Argments
   * */
  /** Whitespace-separated list of seed URLs. */
  String ARG_SEEDLIST = "seed";
  /** a path to a directory containing a list of seed URLs. */
  String ARG_SEEDDIR = "seedDir";

  /**
   * Fetcher Relative Argments
   * */
  /** Fetch mode. */
  String ARG_FETCH_MODE = "fetchMode";

  /**
   * Generator Relative Argments
   * */
  /** Generate topN scoring URLs. */
  String ARG_TOPN = "topN";

  /**
   * Indexer Relative Arguments
   * */
  /** Index immediately once the content is fetched. */
  String ARG_INDEX = "index";
  /** Solr URL. */
  String ARG_SOLR = "solr";
  /** ZooKeeper. */
  String ARG_ZK = "zk";
  /** Solr Collection. */
  String ARG_COLLECTION = "collection";
  /** Reindex. */
  String ARG_REINDEX = "reindex";

  String INDEX_JUST_IN_TIME = "fetch.index.just.in.time";
  String SOLR_PREFIX = "solr.";
  String SOLR_SERVER_URL = SOLR_PREFIX + "server.url";
  String SOLR_ZOOKEEPER_HOSTS = SOLR_PREFIX + "zookeeper.hosts";
  String SOLR_COLLECTION = SOLR_PREFIX + "collection";

  // short constants for status / results fields
  /** Status / result message. */
  String STAT_MESSAGE = "msg";
  /** Phase of processing. */
  String STAT_PHASE = "phase";
  /** Progress (float). */
  String STAT_PROGRESS = "progress";
  /** Jobs. */
  String STAT_JOBS = "jobs";
  /** Counters. */
  String STAT_COUNTERS = "counters";

  String COUNTER_GROUP_STATUS = "Runtime Status";

  String INFECTED_ROWS = "injectedRows";

  /** UI */
  String UI_CRAWL_ID = "qiwu.ui.crawl.id";

  /** Generator **/
  String GENERATE_UPDATE_CRAWLDB = "generate.update.crawldb";
  String GENERATOR_MIN_SCORE = "generate.min.score";
  String GENERATOR_FILTER = "generate.filter";
  String GENERATOR_NORMALISE = "generate.normalise";
  // The maximum number of urls in a single fetchlist
  String GENERATOR_MAX_COUNT = "generate.max.count";
  String GENERATOR_MAX_DISTANCE = "generate.max.distance";
  String GENERATOR_COUNT_MODE = "generate.count.mode";
  String GENERATOR_COUNT_VALUE_DOMAIN = "domain";
  String GENERATOR_COUNT_VALUE_HOST = "host";
  String GENERATOR_COUNT_VALUE_IP = "ip";
  String GENERATOR_TOP_N = "generate.topN";
  String GENERATOR_CUR_TIME = "generate.curr.time";
  String GENERATOR_DELAY = "crawl.gen.delay";
  String GENERATOR_RANDOM_SEED = "generate.partition.seed";
  String GENERATOR_BATCH_ID = "generate.batch.id";
  String GENERATOR_IGNORE_GENERATED = "generator.ignore.generated";

  /**
   * Master service
   * */
  String DEFAULT_MASTER_HOSTNAME = "master";
  int DEFAULT_MASTER_PORT = 8182;

  String NUTCH_LOCAL_COMMAND_FILE = "/tmp/.NUTCH_LOCAL_FILE_COMMAND";
}
