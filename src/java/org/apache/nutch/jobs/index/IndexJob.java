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
package org.apache.nutch.jobs.index;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.mapreduce.StringComparator;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.common.Params;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.indexer.IndexerOutputFormat;
import org.apache.nutch.indexer.IndexingFilters;
import org.apache.nutch.jobs.NutchJob;
import org.apache.nutch.persist.Mark;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.*;

/**
 * Generic indexer which relies on the plugins implementing IndexWriter
 **/
public class IndexJob extends NutchJob implements Tool {

  public static Logger LOG = LoggerFactory.getLogger(IndexJob.class);

  public static final String THREADS_KEY = "fetcher.threads.fetch";
  public static final String INDEXER_PARAMS = "indexer.additional.params";
  public static final String INDEXER_DELETE = "indexer.delete";

  private static final Collection<GoraWebPage.Field> FIELDS = new HashSet<>();

  private static final Utf8 REINDEX = new Utf8("-reindex");

  private String batchId = ALL_BATCH_ID_STR;
  private int numTasks = 2;

  static {
    FIELDS.add(GoraWebPage.Field.SIGNATURE);
    FIELDS.add(GoraWebPage.Field.PARSE_STATUS);
    FIELDS.add(GoraWebPage.Field.SCORE);
    FIELDS.add(GoraWebPage.Field.MARKERS);
  }

  public static Collection<GoraWebPage.Field> getFields(Configuration conf) {
    Collection<GoraWebPage.Field> columns = new HashSet<>(FIELDS);
    IndexingFilters filters = new IndexingFilters(conf);
    columns.addAll(filters.getFields());
    ScoringFilters scoringFilters = new ScoringFilters(conf);
    columns.addAll(scoringFilters.getFields());

    return columns;
  }

  @Override
  public void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Params params = new Params(args);
    Configuration conf = getConf();

    String crawlId = params.get(ARG_CRAWL, conf.get(PARAM_CRAWL_ID));
    batchId = params.get(ARG_BATCH, ALL_BATCH_ID_STR);
    boolean reindex = batchId.equalsIgnoreCase("-reindex");
    batchId = reindex ? "-all" : batchId;
    int threads = params.getInt(ARG_THREADS, 5);
    boolean resume = params.getBoolean(ARG_RESUME, false);
    // TODO : It seems not used yet
    numTasks = params.getInt(ARG_NUMTASKS, conf.getInt(PARAM_MAPREDUCE_JOB_REDUCES, 2));
    int limit = params.getInt(ARG_LIMIT, -1);
    // limit /= numTasks;

    // solr parameters
    String solrUrl = params.get(PARAM_SOLR_SERVER_URL, conf.get(PARAM_SOLR_SERVER_URL));
    String zkHostString = params.get(PARAM_SOLR_ZK, conf.get(PARAM_SOLR_ZK));
    String solrCollection = params.get(PARAM_SOLR_COLLECTION, conf.get(PARAM_SOLR_COLLECTION));

    int round = conf.getInt(PARAM_CRAWL_ROUND, 0);

    /**
     * Re-set computed properties
     * */
    conf.set(PARAM_CRAWL_ID, crawlId);
    conf.setInt(THREADS_KEY, threads);
    conf.set(PARAM_BATCH_ID, batchId);
    conf.setInt(PARAM_LIMIT, limit);
    conf.setBoolean(PARAM_REINDEX, reindex);

    ConfigUtils.setIfNotNull(conf, PARAM_SOLR_SERVER_URL, solrUrl);
    ConfigUtils.setIfNotNull(conf, PARAM_SOLR_ZK, zkHostString);
    ConfigUtils.setIfNotNull(conf, PARAM_SOLR_COLLECTION, solrCollection);

    /**
     * Report parameters
     * */
    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "round", round,
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
    currentJob.getConfiguration().setClass("mapred.output.key.comparator.class", StringComparator.class, RawComparator.class);

    Collection<GoraWebPage.Field> fields = getFields(getConf());
    MapFieldValueFilter<String, GoraWebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(currentJob, fields, String.class, IndexDocument.class, IndexMapper.class, batchIdFilter);

    // The data is write to a network sink using IndexerOutputFormat.write
    currentJob.setOutputFormatClass(IndexerOutputFormat.class);

    // there is no reduce phase, everything has been done in mapper, so set reduce tasks to be 0
    currentJob.setNumReduceTasks(0);

    // used to get schema name
    DataStore<String, GoraWebPage> storage = StorageUtils.createWebStore(getConf(), String.class, GoraWebPage.class);

    LOG.info("Loaded Query Fields : " + StringUtils.join(StorageUtils.toStringArray(fields), ", "));

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", storage.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  protected MapFieldValueFilter<String, GoraWebPage> getBatchIdFilter(String batchId) {
    if (batchId.equals(REINDEX.toString()) || batchId.equals(ALL_BATCH_ID_STR)) {
      return null;
    }

    MapFieldValueFilter<String, GoraWebPage> filter = new MapFieldValueFilter<>();
    filter.setFieldName(GoraWebPage.Field.MARKERS.toString());
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.setMapKey(WebPage.wrapKey(Mark.UPDATEING));
    filter.getOperands().add(WebPage.wrapValue(batchId));

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
   *          resume index job
   * @param numTasks
   *          number of fetching tasks (reducers). If set to < 1 then use the
   *          default, which is jobs.job.reduces.
   * @return 0 on success
   * @throws Exception
   * */
  public int index(String crawlId, String batchId,
                   int threads, boolean resume, int limit, int numTasks,
                   String solrUrl, String zkHostString, String collection) throws Exception {
    run(Params.toArgMap(
        ARG_CRAWL, crawlId,
        ARG_BATCH, batchId,
        ARG_THREADS, threads,
        ARG_RESUME, resume,
        ARG_LIMIT, limit > 0 ? limit : null,
        ARG_NUMTASKS, numTasks > 0 ? numTasks : null,
        PARAM_SOLR_SERVER_URL, solrUrl,
        PARAM_SOLR_ZK, zkHostString,
        PARAM_SOLR_COLLECTION, collection
    ));

    return 0;
  }

  private void printUsage() {
    String usage = "Usage: IndexJob (<batchId> | -all | -reindex) [-crawlId <id>] "
        + "\n \t \t   [-resume] [-threads N] [-limit limit] [-numTasks N]\n"
        + "\n \t \t   [-solrUrl url] [-zkHostString zk] [-collection collection]\n"
        + "    <batchId>     - crawl identifier returned by Generator, or -all for all \n \t \t    generated batchId-s\n"
        + "    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\n"
        + "    -fetchMode <mode> - the fetch mode, can be one of [native|proxy|crowdsourcing], \n \t \t    (default: fetcher.fetch.mode)\");"
        + "    -threads N    - number of fetching threads per task\n"
        + "    -limit        - limit\n"
        + "    -resume       - resume interrupted job\n"
        + "    -numTasks N   - if N > 0 then use this many reduce tasks for fetching \n \t \t    (default: jobs.job.reduces)"
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

    String batchId = args[0];
    if (!batchId.equals("-all") && !batchId.equals("-reindex") && batchId.startsWith("-")) {
      printUsage();
      return -1;
    }

    String crawlId = conf.get(PARAM_CRAWL_ID, "");
    String solrUrl = conf.get(PARAM_SOLR_SERVER_URL);
    String zkHostString = conf.get(PARAM_SOLR_ZK);
    String collection = conf.get(PARAM_SOLR_COLLECTION);

    int numTasks = -1;
    int threads = 10;
    boolean resume = false;
    int limit = -1;

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else if ("-threads".equals(args[i])) {
        threads = Integer.parseInt(args[++i]);
      } else if ("-resume".equals(args[i])) {
        resume = true;
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

    return index(crawlId, batchId, threads, resume, limit, numTasks, solrUrl, zkHostString, collection);
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(ConfigUtils.create(), new IndexJob(), args);
    System.exit(res);
  }
}
