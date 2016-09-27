/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.mapreduce;

import org.apache.avro.util.Utf8;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.URLPartitioner.FetchEntryPartitioner;
import org.apache.nutch.fetch.FetchMode;
import org.apache.nutch.fetch.data.FetchEntry;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.*;

/**
 * Fetch job
 * */
public class FetchJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(FetchJob.class);

  public static final String PROTOCOL_REDIR = "protocol";
  public static final int PERM_REFRESH_TIME = 5;
  public static final Utf8 REDIRECT_DISCOVERED = new Utf8("___rdrdsc__");

  private static final Collection<WebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.REPR_URL);
    FIELDS.add(WebPage.Field.FETCH_TIME);
  }

  private int numTasks = 2;
  private String batchId = Nutch.ALL_BATCH_ID_STR;

  public FetchJob() {
  }

  public FetchJob(Configuration conf) {
    setConf(conf);
  }

  public Collection<WebPage.Field> getFields(Job job) {
    Collection<WebPage.Field> fields = new HashSet<>(FIELDS);
    if (job.getConfiguration().getBoolean(PARAM_PARSE, false)) {
      ParserJob parserJob = new ParserJob();
      fields.addAll(parserJob.getFields(job));
    }

    ProtocolFactory protocolFactory = new ProtocolFactory(job.getConfiguration());
    fields.addAll(protocolFactory.getFields());

    return fields;
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Params params = new Params(args);
    Configuration conf = getConf();

    checkConfiguration(conf);

    String crawlId = params.get(ARG_CRAWL, conf.get(Nutch.PARAM_CRAWL_ID));
    FetchMode fetchMode = params.getEnum(ARG_FETCH_MODE, conf.getEnum(PARAM_FETCH_MODE, FetchMode.NATIVE));
    batchId = params.get(ARG_BATCH, ALL_BATCH_ID_STR);
    int threads = params.getInt(ARG_THREADS, 5);
    boolean resume = params.getBoolean(ARG_RESUME, false);
    int limit = params.getInt(ARG_LIMIT, -1);
    numTasks = params.getInt(ARG_NUMTASKS, conf.getInt(PARAM_MAPREDUCE_JOB_REDUCES, 2));
    boolean index = params.getBoolean(ARG_INDEX, false);

    /** Solr */
    String solrUrl = params.get(ARG_SOLR_URL, conf.get(PARAM_SOLR_SERVER_URL));
    String zkHostString = params.get(ARG_ZK, conf.get(PARAM_SOLR_ZK));
    String solrCollection = params.get(ARG_COLLECTION, conf.get(PARAM_SOLR_COLLECTION));

    /** Set re-computed config variables */
    NutchConfiguration.setIfNotNull(conf, PARAM_CRAWL_ID, crawlId);
    conf.setEnum(PARAM_FETCH_MODE, fetchMode);
    NutchConfiguration.setIfNotNull(conf, PARAM_GENERATOR_BATCH_ID, batchId);

    conf.setInt(PARAM_THREADS, threads);
    conf.setBoolean(PARAM_RESUME, resume);
    conf.setInt(PARAM_FETCH_MAPPER_LIMIT, limit);
    conf.setInt(PARAM_MAPREDUCE_JOB_REDUCES, numTasks);

    conf.setBoolean(PARAM_INDEX_JUST_IN_TIME, index);
    NutchConfiguration.setIfNotNull(conf, PARAM_SOLR_SERVER_URL, solrUrl);
    NutchConfiguration.setIfNotNull(conf, PARAM_SOLR_ZK, zkHostString);
    NutchConfiguration.setIfNotNull(conf, PARAM_SOLR_COLLECTION, solrCollection);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "fetchMode", fetchMode,
        "numTasks", numTasks,
        "threads", threads,
        "resume", resume,
        "limit", limit,
        "index", index,
        "solrUrl", solrUrl,
        "zkHostString", zkHostString,
        "solrCollection", solrCollection
    ));
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    // For politeness, don't permit parallel execution of a single task
    currentJob.setReduceSpeculativeExecution(false);

    Collection<WebPage.Field> fields = getFields(currentJob);
    MapFieldValueFilter<String, WebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(currentJob, fields, IntWritable.class,
        FetchEntry.class, FetchMapper.class, FetchEntryPartitioner.class,
        batchIdFilter, false);
    StorageUtils.initReducerJob(currentJob, FetchReducer.class);

    currentJob.setNumReduceTasks(numTasks);

    // used to get schema name
    DataStore<String, WebPage> store = StorageUtils.createWebStore(getConf(), String.class, WebPage.class);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  public int fetch(String crawlId, String fetchMode, String batchId, int threads, boolean resume, int limit) throws Exception {
    return fetch(crawlId, fetchMode, batchId, threads, resume, limit, 2, false, null, null, null);
  }

  public int fetch(String crawlId, String fetchMode, String batchId, int threads, boolean resume, int limit, int numTasks) throws Exception {
    return fetch(crawlId, fetchMode, batchId, threads, resume, limit, numTasks, false, null, null, null);
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
  public int fetch(String crawlId, String fetchMode, String batchId, int threads, boolean resume, int limit, int numTasks,
                   boolean index, String solrUrl, String zkHostString, String collection) throws Exception {
    run(Params.toArgMap(
        ARG_CRAWL, crawlId,
        ARG_FETCH_MODE, fetchMode,
        ARG_BATCH, batchId,
        ARG_THREADS, threads,
        ARG_RESUME, resume,
        ARG_NUMTASKS, numTasks > 0 ? numTasks : null,
        ARG_LIMIT, limit > 0 ? limit : null,
        ARG_NUMTASKS, numTasks > 0 ? numTasks : null,
        ARG_INDEX, index,
        ARG_SOLR_URL, solrUrl,
        ARG_ZK, zkHostString,
        ARG_COLLECTION, collection
    ));

    return 0;
  }

  void checkConfiguration(Configuration conf) {
    // ensure that a value has been set for the agent name
    String agentName = conf.get("http.agent.name");
    if (agentName == null || agentName.trim().length() == 0) {
      String message = "No agents listed in 'http.agent.name'" + " property.";

      LOG.error(message);

      throw new IllegalArgumentException(message);
    }
  }

  private void printUsage() {
    String usage = "Usage: FetchJob (<batchId> | -all) [-crawlId <id>] [-threads N] "
        + "\n \t \t  [-resume] [-numTasks N]\n"
        + "\n \t \t   [-solrUrl url] [-zkHostString zk] [-collection collection]\n"
        + "    <batchId>     - crawl identifier returned by Generator, or -all for all \n \t \t    generated batchId-s\n"
        + "    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\n"
        + "    -fetchMode <mode> - the fetch mode, can be one of [native|proxy|crowdsourcing], \n \t \t    (default: fetcher.fetch.mode));"
        + "    -threads N    - number of fetching threads per task\n"
        + "    -resume       - resume interrupted job\n"
        + "    -index        - index in time\n"
        + "    -numTasks N   - if N > 0 then use this many reduce tasks for fetching \n \t \t    (default: mapred.map.tasks)"
        + "    -solrUrl      - solr server url, for example, http://localhost:8983/solr/gettingstarted\n"
        + "    -zkHostString - zk host string, zoomkeeper higher priority then solrUrl\n"
        + "    -collection   - collection name if zkHostString is specified\n";

    System.err.println(usage);
  }

  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      printUsage();
      return -1;
    }

    Configuration conf = getConf();

    String batchId = args[0];
    if (!batchId.equals("-all") && batchId.startsWith("-")) {
      printUsage();
      return -1;
    }

    String crawlId = null;
    String fetchMode = null;
    String solrUrl = null;
    String zkHostString = null;
    String collection = null;

    int numTasks = -1;
    int threads = 10;
    boolean resume = false;
    boolean index = false;
    int limit = -1;

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else if ("-fetchMode".equals(args[i])) {
        fetchMode = args[++i].toUpperCase();
      } else if ("-threads".equals(args[i])) {
        // found -threads option
        threads = Integer.parseInt(args[++i]);
      } else if ("-resume".equals(args[i])) {
        resume = true;
      } else if ("-numTasks".equals(args[i])) {
        numTasks = Integer.parseInt(args[++i]);
      } else if ("-limit".equals(args[i])) {
        limit = Integer.parseInt(args[++i]);
      } else if ("-index".equals(args[i])) {
        index = true;
      } else if ("-solrUrl".equals(args[i])) {
        solrUrl = args[++i];
      } else if ("-zk".equals(args[i])) {
        zkHostString = args[++i];
      } else if ("-collection".equals(args[i])) {
        collection = args[++i];
      } else {
        throw new IllegalArgumentException("arg " + args[i] + " not recognized");
      }
    }

    return fetch(crawlId, fetchMode, batchId, threads, resume, limit, numTasks, index, solrUrl, zkHostString, collection);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("---------------------------------------------------\n\n");

    Configuration conf = NutchConfiguration.create();
    FetchMode fetchMode = conf.getEnum(PARAM_FETCH_MODE, FetchMode.NATIVE);
    if (fetchMode == FetchMode.CROWDSOURCING) {
      NutchServer.startInDaemonThread(conf);
    }

    int res = ToolRunner.run(conf, new FetchJob(), args);
    System.exit(res);
  }
}
