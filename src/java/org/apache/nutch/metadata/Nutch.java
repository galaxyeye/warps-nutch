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

import java.nio.ByteBuffer;

/**
 * A collection of Nutch internal metadata constants.
 */
public interface Nutch {

  ByteBuffer YES_VAL = ByteBuffer.wrap(new byte[] { 'y' });

  String YES_STRING = "y";

  Utf8 YES_UTF8 = new Utf8(YES_STRING);

  String ORIGINAL_CHAR_ENCODING = "OriginalCharEncoding";

  String CHAR_ENCODING_FOR_CONVERSION = "CharEncodingForConversion";

  String SIGNATURE_KEY = "nutch.content.digest";

  String SCORE_KEY = "nutch.crawl.score";

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

  int MAX_DISTANCE = Integer.MAX_VALUE;

  // The shortest url
  String SHORTEST_VALID_URL = "ftp://t.tt";
  int SHORTEST_VALID_URL_LENGTH = SHORTEST_VALID_URL.length();

  int FETCH_PRIORITY_MIN = -10 * 1000;
  int FETCH_PRIORITY_ANY = -1;
  int FETCH_PRIORITY_DEFAULT = 0;
  int FETCH_PRIORITY_DEPTH_BASE = 1000;
  int FETCH_PRIORITY_DEPTH_0 = FETCH_PRIORITY_DEPTH_BASE;
  int FETCH_PRIORITY_DEPTH_1 = FETCH_PRIORITY_DEPTH_BASE - 1;
  int FETCH_PRIORITY_DEPTH_2 = FETCH_PRIORITY_DEPTH_BASE - 2;
  int FETCH_PRIORITY_DEPTH_3 = FETCH_PRIORITY_DEPTH_BASE - 3;
  int FETCH_PRIORITY_DETAIL_PAGE = FETCH_PRIORITY_DEPTH_1;
  int FETCH_PRIORITY_MAX = 10 * 1000;

  float SCORE_DEFAULT = 1.0f;
  float SCORE_INDEX_PAGE = 1.0f;
  float SCORE_INJECTED = Float.MAX_VALUE / 1000;
  float SCORE_SEED = SCORE_INJECTED / 1000;
  float SCORE_DETAIL_PAGE = SCORE_SEED / 1000;
  float SCORE_PAGES_FROM_SEED = SCORE_SEED / 1000;

  /**
   * All arguments from command line
   * */
  // short constants for cmd-line args
  /** Crawl id to use. */
  String ARG_CRAWL = "crawl";
  /** Batch id to select. */
  String ARG_BATCH = "batch";
  /** Re-generate. */
  String ARG_REGENERATE = "reGenerate";
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
   * Injector Relative
   * */
  /** Whitespace-separated list of seed URLs. */
  String ARG_SEEDLIST = "seed";
  /** a path to a directory containing a list of seed URLs. */
  String ARG_SEEDDIR = "seedDir";

  /**
   * Fetcher Relative
   * */
  /** Fetch mode. */
  String ARG_FETCH_MODE = "fetchMode";

  /**
   * Generator Relative
   * */
  /** Generate topN scoring URLs. */
  String ARG_TOPN = "topN";

  /**
   * Reparse
   * */
  String ARG_REPARSE = "reparse";

  /**
   * Indexer Relative Arguments
   * */
  /** Index immediately once the content is fetched. */
  String ARG_INDEX = "index";
  /** Solr URL. */
  String ARG_SOLR_URL = "solrUrl";
  /** ZooKeeper. */
  String ARG_ZK = "zk";
  /** Solr Collection. */
  String ARG_COLLECTION = "collection";
  /** Reindex. */

  /**
   * Status / result message.
   * */
  String STAT_MESSAGE = "msg";
  /** Phase of processing. */
  String STAT_PHASE = "phase";
  /** Progress (float). */
  String STAT_PROGRESS = "progress";
  /** Jobs. */
  String STAT_JOBS = "jobs";
  /** Counters. */
  String STAT_COUNTERS = "counters";

  String STAT_RUNTIME_STATUS = "Runtime Status";

  String STAT_INFECTED_ROWS = "injectedRows";

  /**
   * UI
   * */
  String UI_CRAWL_ID = "qiwu.ui.crawl.id";

  /**
   * Parameters
   * */
  String PARAM_NUTCH_TMP_DIR = "nutch.tmp.dir";
  String PARAM_NUTCH_OUTPUT_DIR = "nutch.output.dir";
  String PARAM_NUTCH_REPORT_DIR = "nutch.report.dir";
  String PARAM_CRAWL_ID = "storage.crawl.id";
  String PARAM_FETCH_MODE = "fetcher.fetch.mode";
  String PARAM_FETCH_QUEUE_MODE = "fetcher.queue.mode";
  String PARAM_FETCH_MAX_THREADS_PER_QUEUE = "fetcher.threads.per.queue";
  String PARAM_MAPREDUCE_JOB_REDUCES = "mapreduce.job.reduces";
  String PARAM_NUTCH_JOB_NAME = "nutch.job.name";

  String PARAM_BATCH_ID = "nutch.batch.name";
  String PARAM_PARSE = "parser.parse";
  String PARAM_REPARSE = "parser.reparse";
  String PARAM_REINDEX = "reindex";
  String PARAM_FORCE = "force";
  String PARAM_RESUME = "nutch.job.resume";
  String PARAM_LIMIT = "limit";
  String PARAM_MAPPER_LIMIT = "nutch.mapper.limit";
  String PARAM_REDUCER_LIMIT = "nutch.reducer.limit";

  // String PARAM_SEED_FILE_LOCK_NAME = "seed.file.lock.name";

  /**
   * Fetch parameters
   * */
  String PARAM_THREADS = "fetcher.threads.fetch";

  /**
   * Dbupdate parameters
   * */
  String PARAM_DBUPDATE_JUST_IN_TIME = "fetcher.dbupdate.just.in.time";

  /**
   * Indexing parameters
   * */
  String PARAM_INDEX_JUST_IN_TIME = "fetch.index.just.in.time";

  String PARAM_SOLR_SERVER_URL = "solr.server.url";
  String PARAM_SOLR_ZK = "solr.zookeeper.hosts";
  String PARAM_SOLR_COLLECTION = "solr.collection";

  /**
   * Generator parameters
   * */
  String PARAM_GENERATE_TIME = "generate.generate.time";
  String PARAM_GENERATE_UPDATE_CRAWLDB = "generate.update.crawldb";
  String PARAM_GENERATOR_MIN_SCORE = "generate.min.score";
  String PARAM_GENERATE_REGENERATE = "generate.re.generate";
  String PARAM_GENERATE_FILTER = "generate.filter";
  String PARAM_GENERATE_NORMALISE = "generate.normalise";
  // The maximum number of urls in a single fetchlist
  String PARAM_GENERATOR_MAX_TASKS_PER_HOST = "generate.max.tasks.per.host";
  String PARAM_GENERATOR_MAX_DISTANCE = "generate.max.distance";
  String PARAM_GENERATOR_COUNT_MODE = "generate.count.mode";
  String PARAM_GENERATOR_TOP_N = "generate.topN";
  String PARAM_GENERATOR_CUR_TIME = "generate.curr.time";
  String PARAM_GENERATOR_DELAY = "crawl.gen.delay";
  String PARAM_GENERATOR_RANDOM_SEED = "generate.partition.seed";
  String PARAM_IGNORE_GENERATED = "generate.ignore.generated";

  String PARAM_NUTCH_MASTER_HOST = "nutch.master.host";

  /**
   * Document fields
   * */
  String DOC_FIELD_PAGE_TITLE = "page_title";
  String DOC_FIELD_ARTICLE_TITLE = "article_title";
  String DOC_FIELD_TEXT_CONTENT_LENGTH = "text_content_length";
  String DOC_FIELD_TEXT_CONTENT = "text_content";
  String DOC_FIELD_HTML_CONTENT = "html_content";
  String DOC_FIELD_PAGE_CATEGORY = "page_category";
  String DOC_FIELD_PUBLISH_TIME = "publish_time";

  /**
   * Variable holders
   * */
  String VAR_ORDERED_OUTLINKS = "ordered_outlinks";

  /**
   * Program keys
   * */
  String GENERATE_TIME_KEY = "_ngt_";
  Text WRITABLE_GENERATE_TIME_KEY = new Text(GENERATE_TIME_KEY);

  /**
   * Variable values
   * */
  String GENERATE_COUNT_VALUE_DOMAIN = "domain";
  String GENERATE_COUNT_VALUE_HOST = "host";
  String GENERATE_COUNT_VALUE_IP = "ip";

  /**
   * Master service
   * */
  String DEFAULT_MASTER_HOSTNAME = "master";
  int DEFAULT_MASTER_PORT = 8182;

  String PATH_NUTCH_TMP_DIR = "/tmp/nutch-" + System.getenv("USER");
  String PATH_NUTCH_OUTPUT_DIR = PATH_NUTCH_TMP_DIR;
  String PATH_NUTCH_REPORT_DIR = PATH_NUTCH_OUTPUT_DIR + "/report";
  String PATH_LOCAL_COMMAND = PATH_NUTCH_TMP_DIR + "/NUTCH_LOCAL_FILE_COMMAND";
  String PATH_LAST_BATCH_ID = PATH_NUTCH_TMP_DIR + "/last-batch-id";

  String FILE_UNREACHABLE_HOSTS = "unreachable-hosts.txt";

  // TODO : mark it as a hdfs path
  String PATH_ALL_SEED_FILE = "/tmp/nutch-seeds/all.txt";
}
