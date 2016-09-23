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
package org.apache.nutch.fetcher;

import org.apache.avro.util.Utf8;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.URLPartitioner.FetchEntryPartitioner;
import org.apache.nutch.fetcher.data.FetchEntry;
import org.apache.nutch.mapreduce.NutchJob;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.service.NutchServer;
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
 * Fetch job
 */
public class FetchJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(FetchJob.class);

  public static final String PROTOCOL_REDIR = "protocol";
  public static final int PERM_REFRESH_TIME = 5;
  public static final Utf8 REDIRECT_DISCOVERED = new Utf8("___rdrdsc__");
  public static final String RESUME_KEY = "fetcher.job.resume";
  public static final String PARSE_KEY = "fetcher.parse";
  public static final String THREADS_KEY = "fetcher.threads.fetch";

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
    if (job.getConfiguration().getBoolean(PARSE_KEY, false)) {
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

    Configuration conf = getConf();

    checkConfiguration(conf);

    String crawlId = NutchUtil.get(args, Nutch.ARG_CRAWL, conf.get(Nutch.CRAWL_ID_KEY));
    String fetchMode = NutchUtil.get(args, Nutch.ARG_FETCH_MODE, "native");
    batchId = NutchUtil.get(args, Nutch.ARG_BATCH, Nutch.ALL_BATCH_ID_STR);
    int threads = NutchUtil.getInt(args, Nutch.ARG_THREADS, 5);
    boolean resume = NutchUtil.getBoolean(args, Nutch.ARG_RESUME, false);

    numTasks = getConf().getInt("mapred.reduce.tasks", 2); // default value
    // since mapred.reduce.tasks is deprecated
    // numTasks = getConf().getInt("mapreduce.job.reduces", 2);
    numTasks = NutchUtil.getInt(args, Nutch.ARG_NUMTASKS, numTasks);

    boolean index = NutchUtil.getBoolean(args, Nutch.ARG_INDEX, false);
    int limit = NutchUtil.getInt(args, Nutch.ARG_LIMIT, -1);
    // indexer parameters
    String solrUrl = NutchUtil.get(args, Nutch.SOLR_SERVER_URL, conf.get(Nutch.SOLR_SERVER_URL));
    String zkHostString = NutchUtil.get(args, Nutch.SOLR_ZOOKEEPER_HOSTS, conf.get(Nutch.SOLR_ZOOKEEPER_HOSTS));
    String solrCollection = NutchUtil.get(args, Nutch.SOLR_COLLECTION, conf.get(Nutch.SOLR_COLLECTION));

    conf.set(Nutch.CRAWL_ID_KEY, crawlId);
    conf.set(Nutch.FETCH_MODE_KEY, fetchMode);
    conf.setInt(THREADS_KEY, threads);
    conf.set(Nutch.GENERATOR_BATCH_ID, batchId);
    conf.setBoolean(RESUME_KEY, resume);
    conf.setInt(Nutch.ARG_LIMIT, limit);

    conf.setBoolean(Nutch.INDEX_JUST_IN_TIME, index);
    NutchUtil.setIfNotNull(conf, Nutch.SOLR_SERVER_URL, solrUrl);
    NutchUtil.setIfNotNull(conf, Nutch.SOLR_ZOOKEEPER_HOSTS, zkHostString);
    NutchUtil.setIfNotNull(conf, Nutch.SOLR_COLLECTION, solrCollection);

    LOG.info(StringUtil.formatParams(
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

    // or we can use "mapreduce.job.reduces" directly?
    currentJob.setNumReduceTasks(numTasks);

    // used to get schema name
    DataStore<String, WebPage> store = StorageUtils.createWebStore(getConf(), String.class, WebPage.class);

    LOG.info(StringUtil.formatParams(
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
    run(StringUtil.toArgMap(
        Nutch.ARG_CRAWL, crawlId,
        Nutch.ARG_FETCH_MODE, fetchMode,
        Nutch.ARG_BATCH, batchId,
        Nutch.ARG_THREADS, threads,
        Nutch.ARG_RESUME, resume,
        Nutch.ARG_NUMTASKS, numTasks > 0 ? numTasks : null,
        Nutch.ARG_LIMIT, limit > 0 ? limit : null,
        Nutch.ARG_NUMTASKS, numTasks > 0 ? numTasks : null,
        Nutch.ARG_INDEX, index,
        Nutch.SOLR_SERVER_URL, solrUrl,
        Nutch.SOLR_ZOOKEEPER_HOSTS, zkHostString,
        Nutch.SOLR_COLLECTION, collection
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

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY, "");
    String fetchMode = conf.get(Nutch.FETCH_MODE_KEY, FetchMode.NATIVE.value());
    if (!FetchMode.validate(fetchMode)) {
      System.out.println("-fetchMode accepts one of [native|proxy|crowdsourcing]");
      return -1;
    }

    // TODO : 1. use Nutch.ARG_ series, 2. solrUrl should not contain collection
    String solrUrl = conf.get(Nutch.SOLR_SERVER_URL);
    String zkHostString = conf.get(Nutch.SOLR_ZOOKEEPER_HOSTS);
    String collection = conf.get(Nutch.SOLR_COLLECTION);

    int numTasks = -1;
    int threads = 10;
    boolean resume = false;
    boolean index = false;
    int limit = -1;

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else if ("-fetchMode".equals(args[i])) {
        fetchMode = args[++i];
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
      } else if ("-zkHostString".equals(args[i])) {
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
    String fetchMode = conf.get("fetcher.fetch.mode", FetchMode.NATIVE.value());
    if (fetchMode.equalsIgnoreCase(FetchMode.CROWDSOURCING.value())) {
      NutchServer.startInDaemonThread(conf);
    }

    int res = ToolRunner.run(conf, new FetchJob(), args);
    System.exit(res);
  }
}
