/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.indexer;

import org.apache.avro.util.Utf8;
import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.mapreduce.StringComparator;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.mapreduce.NutchJob;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * Generic indexer which relies on the plugins implementing IndexWriter
 **/
public class IndexingJob extends NutchJob implements Tool {

  public static Logger LOG = LoggerFactory.getLogger(IndexingJob.class);

  public static final String RESUME_KEY = "fetcher.job.resume";
  public static final String PARSE_KEY = "fetcher.parse";
  public static final String THREADS_KEY = "fetcher.threads.fetch";
  public static final String INDEXER_PARAMS = "indexer.additional.params";
  public static final String INDEXER_DELETE = "indexer.delete";
  public static final String INDEXER_DELETE_ROBOTS_NOINDEX = "indexer.delete.robots.noindex";
  public static final String INDEXER_DELETE_SKIPPED = "indexer.delete.skipped.by.indexingfilter";
  public static final String INDEXER_SKIP_NOTMODIFIED = "indexer.skip.notmodified";
  public static final String URL_FILTERING = "indexer.url.filters";
  public static final String URL_NORMALIZING = "indexer.url.normalizers";
  public static final String INDEXER_BINARY_AS_BASE64 = "indexer.binary.base64";

  private static final Collection<WebPage.Field> FIELDS = new HashSet<>();

  private static final Utf8 REINDEX = new Utf8("-reindex");

  private String batchId = Nutch.ALL_BATCH_ID_STR;
  private int numTasks = 2;

  static {
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.PARSE_STATUS);
    FIELDS.add(WebPage.Field.SCORE);
    FIELDS.add(WebPage.Field.MARKERS);
  }

  private static Collection<WebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();

    Collection<WebPage.Field> columns = new HashSet<>(FIELDS);
    IndexingFilters filters = new IndexingFilters(conf);
    columns.addAll(filters.getFields());
    ScoringFilters scoringFilters = new ScoringFilters(conf);
    columns.addAll(scoringFilters.getFields());

    return columns;
  }

  @Override
  public void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Configuration conf = getConf();

    String crawlId = NutchUtil.get(args, Nutch.ARG_CRAWL, conf.get(Nutch.CRAWL_ID_KEY));
    batchId = NutchUtil.get(args, Nutch.ARG_BATCH, Nutch.ALL_BATCH_ID_STR);
    int threads = NutchUtil.getInt(args, Nutch.ARG_THREADS, 5);
    boolean resume = NutchUtil.getBoolean(args, Nutch.ARG_RESUME, false);
    boolean reindex = NutchUtil.getBoolean(args, Nutch.ARG_REINDEX, false);
    // since mapred.reduce.tasks is deprecated
    // numTasks = getConf().getInt("mapreduce.job.reduces", 2);
    numTasks = NutchUtil.getInt(args, Nutch.ARG_NUMTASKS, conf.getInt("mapred.reduce.tasks", 2));
    int limit = NutchUtil.getInt(args, Nutch.ARG_LIMIT, -1);
    // indexer parameters
    String solrUrl = NutchUtil.get(args, Nutch.SOLR_SERVER_URL, conf.get(Nutch.SOLR_SERVER_URL));
    String zkHostString = NutchUtil.get(args, Nutch.SOLR_ZOOKEEPER_HOSTS, conf.get(Nutch.SOLR_ZOOKEEPER_HOSTS));
    String solrCollection = NutchUtil.get(args, Nutch.SOLR_COLLECTION, conf.get(Nutch.SOLR_COLLECTION));

    /**
     * Re-set computed properties
     * */
    conf.set(Nutch.CRAWL_ID_KEY, crawlId);
    conf.setInt(THREADS_KEY, threads);
    conf.set(Nutch.GENERATOR_BATCH_ID, batchId);
    conf.setBoolean(RESUME_KEY, resume);
    conf.setBoolean(Nutch.ARG_REINDEX, reindex);
    conf.setInt(Nutch.ARG_LIMIT, limit);

    NutchUtil.setIfNotNull(conf, Nutch.SOLR_SERVER_URL, solrUrl);
    NutchUtil.setIfNotNull(conf, Nutch.SOLR_ZOOKEEPER_HOSTS, zkHostString);
    NutchUtil.setIfNotNull(conf, Nutch.SOLR_COLLECTION, solrCollection);

    /**
     * Report paramemters
     * */
    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "numTasks", numTasks,
        "threads", threads,
        "resume", resume,
        "reindex", reindex,
        "limit", limit,
        "solrUrl", solrUrl,
        "zkHostString", zkHostString,
        "solrCollection", solrCollection
    ));
  }

  @Override
  public void doRun(Map<String, Object> args) throws Exception {
    // TODO: Figure out why this needs to be here
    currentJob.getConfiguration().setClass("mapred.output.key.comparator.class",
        StringComparator.class, RawComparator.class);

    Collection<WebPage.Field> fields = getFields(currentJob);
    MapFieldValueFilter<String, WebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(currentJob, fields, String.class, IndexDocument.class, IndexingMapper.class, batchIdFilter);

    // The data is write to a network sink using IndexerOutputFormat.write
    currentJob.setOutputFormatClass(IndexerOutputFormat.class);

    // there is no reduce phase, everything has been done in mapper, so set reduce tasks to be 0
    currentJob.setNumReduceTasks(0);

    // used to get schema name
    DataStore<String, WebPage> storage = StorageUtils.createWebStore(getConf(), String.class, WebPage.class);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", storage.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  protected MapFieldValueFilter<String, WebPage> getBatchIdFilter(String batchId) {
    if (batchId.equals(REINDEX.toString()) || batchId.equals(Nutch.ALL_CRAWL_ID.toString())) {
      return null;
    }

    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<>();
    filter.setFieldName(WebPage.Field.MARKERS.toString());
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.setMapKey(Mark.UPDATEDB_MARK.getName());
    filter.getOperands().add(new Utf8(batchId));

    return filter;
  }

  /**
   * Run fetcher.
   *
   * @param batchId
   *          batchId (obtained from Generator) or null to fetch all generated
   *          fetchlists
   * @param threads
   *          number of threads per map task
   * @param resume
   * @param numTasks
   *          number of fetching tasks (reducers). If set to < 1 then use the
   *          default, which is mapred.map.tasks.
   * @return 0 on success
   * @throws Exception
   * */
  public int index(String crawlId, String batchId,
                   int threads, boolean resume, boolean reindex, int limit, int numTasks,
                   String solrUrl, String zkHostString, String collection) throws Exception {
    run(StringUtil.toArgMap(
        Nutch.ARG_CRAWL, crawlId,
        Nutch.ARG_BATCH, batchId,
        Nutch.ARG_THREADS, threads,
        Nutch.ARG_RESUME, resume,
        Nutch.ARG_REINDEX, reindex,
        Nutch.ARG_LIMIT, limit > 0 ? limit : null,
        Nutch.ARG_NUMTASKS, numTasks > 0 ? numTasks : null,
        Nutch.SOLR_SERVER_URL, solrUrl,
        Nutch.SOLR_ZOOKEEPER_HOSTS, zkHostString,
        Nutch.SOLR_COLLECTION, collection
    ));

    return 0;
  }

  private void printUsage() {
    String usage = "Usage: IndexingJob (<batchId> | -all | -reindex) [-crawlId <id>] "
        + "\n \t \t   [-resume] [-threads N] [-limit limit] [-numTasks N]\n"
        + "\n \t \t   [-solrUrl url] [-zkHostString zk] [-collection collection]\n"
        + "    <batchId>     - crawl identifier returned by Generator, or -all for all \n \t \t    generated batchId-s\n"
        + "    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\n"
        + "    -fetchMode <mode> - the fetch mode, can be one of [native|proxy|crowdsourcing], \n \t \t    (default: fetcher.fetch.mode)\");"
        + "    -threads N    - number of fetching threads per task\n"
        + "    -resume       - resume interrupted job\n"
        + "    -numTasks N   - if N > 0 then use this many reduce tasks for fetching \n \t \t    (default: mapred.map.tasks)"
        + "    -solrUrl - solr server url, for example, http://localhost:8983/solr/gettingstarted\n"
        + "    -zkHostString  - zk host string, zoomkeeper higher priority then solrUrl\n"
        + "    -collection    - collection name if zkHostString is specified\n";

    System.err.println(usage);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      return -1;
    }

    Configuration conf = getConf();
    String crawlId = conf.get(Nutch.CRAWL_ID_KEY, "");
    String batchId = Nutch.ALL_BATCH_ID_STR;
    String solrUrl = conf.get(Nutch.SOLR_SERVER_URL);
    String zkHostString = conf.get(Nutch.SOLR_ZOOKEEPER_HOSTS);
    String collection = conf.get(Nutch.SOLR_COLLECTION);

    int numTasks = -1;
    int threads = 10;
    boolean resume = false;
    boolean reindex = false;
    int limit = -1;

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else if ("-batchId".equals(args[i])) {
        batchId = args[++i];
      } else if ("-threads".equals(args[i])) {
        threads = Integer.parseInt(args[++i]);
      } else if ("-resume".equals(args[i])) {
        resume = true;
      } else if ("-reindex".equals(args[i])) {
        reindex = true;
      } else if ("-numTasks".equals(args[i])) {
        numTasks = Integer.parseInt(args[++i]);
      } else if ("-limit".equals(args[i])) {
        limit = Integer.parseInt(args[++i]);
      } else if ("-solrUrl".equals(args[i])) {
        solrUrl = args[++i];
      } else if ("-zkHostString".equals(args[i])) {
        zkHostString = args[++i];
      } else if ("-collection".equals(args[i])) {
        collection = args[++i];
      } else {
        throw new IllegalArgumentException("arg " + args[i] + " not recognized");
      }
    }

    return index(crawlId, batchId, threads, resume, reindex, limit, numTasks, solrUrl, zkHostString, collection);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("---------------------------------------------------\n\n");

    final int res = ToolRunner.run(NutchConfiguration.create(), new IndexingJob(), args);
    System.exit(res);
  }
}
