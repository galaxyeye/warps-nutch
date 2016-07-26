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
package org.apache.nutch.parse;

import org.apache.avro.util.Utf8;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.mapreduce.NutchJob;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.IdentityPageReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class ParserJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(ParserJob.class);

  public static final String RESUME_KEY = "parse.job.resume";
  public static final String FORCE_KEY = "parse.job.force";
  public static final String SKIP_TRUNCATED = "parser.skip.truncated";
  public static final Utf8 REPARSE = new Utf8("-reparse");

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private String batchId;

  static {
    FIELDS.add(WebPage.Field.STATUS);
    FIELDS.add(WebPage.Field.CONTENT);
    FIELDS.add(WebPage.Field.CONTENT_TYPE);
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.PARSE_STATUS);
    FIELDS.add(WebPage.Field.OUTLINKS);
    FIELDS.add(WebPage.Field.METADATA);
    FIELDS.add(WebPage.Field.HEADERS);
  }

  public ParserJob() {

  }

  public ParserJob(Configuration conf) {
    setConf(conf);
  }

  public Collection<WebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
    ParserFactory parserFactory = new ParserFactory(conf);
    ParseFilters parseFilters = new ParseFilters(conf);

    Collection<WebPage.Field> parsePluginFields = parserFactory.getFields();
    Collection<WebPage.Field> signaturePluginFields = SignatureFactory.getFields(conf);
    Collection<WebPage.Field> htmlParsePluginFields = parseFilters.getFields();

    if (parsePluginFields != null) {
      fields.addAll(parsePluginFields);
    }

    if (signaturePluginFields != null) {
      fields.addAll(signaturePluginFields);
    }

    if (htmlParsePluginFields != null) {
      fields.addAll(htmlParsePluginFields);
    }

    return fields;
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Configuration conf = getConf();

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);
    String fetchMode = conf.get(Nutch.FETCH_MODE_KEY);

    batchId = NutchUtil.get(args, Nutch.ARG_BATCH, Nutch.ALL_BATCH_ID_STR);
    Boolean resume = NutchUtil.getBoolean(args, Nutch.ARG_RESUME, false);
    Boolean force = NutchUtil.getBoolean(args, Nutch.ARG_FORCE, false);

    getConf().set(Nutch.GENERATOR_BATCH_ID, batchId);
    getConf().setBoolean(RESUME_KEY, resume);
    getConf().setBoolean(FORCE_KEY, force);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "batchId", batchId,
        "fetchMode", fetchMode,
        "force", force,
        "resume", resume
    ));
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    Collection<WebPage.Field> fields = getFields(currentJob);
    MapFieldValueFilter<String, WebPage> batchIdFilter = getBatchIdFilter(batchId);

    StorageUtils.initMapperJob(currentJob, fields, String.class, WebPage.class, ParserMapper.class, batchIdFilter);
    StorageUtils.initReducerJob(currentJob, IdentityPageReducer.class);

    // there is no reduce phase, so set reduce tasks to be 0
    currentJob.setNumReduceTasks(0);

    // used to get schema name
    DataStore<String, WebPage> store = StorageUtils.createWebStore(getConf(), String.class, WebPage.class);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName(),
        "batchId", batchId
    ));

    currentJob.waitForCompletion(true);
  }

  private void printUsage() {
    System.err
        .println("Usage: ParserJob (<batchId> | -all) [-crawlId <id>] [-resume] [-force]");
    System.err
        .println("    <batchId>     - symbolic batch ID created by Generator");
    System.err
        .println("    -all          - consider pages from all crawl jobs");
    System.err
        .println("    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)");
    System.err
        .println("    -resume       - resume a previous incomplete job");
    System.err
        .println("    -force        - force re-parsing even if a page is already parsed");
  }

  public int run(String[] args) throws Exception {

    if (args.length < 1) {
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

    boolean resume = false;
    boolean force = false;

    for (int i = 0; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      }
      else if ("-resume".equals(args[i])) {
        resume = true;
      } else if ("-force".equals(args[i])) {
        force = true;
      }
    }

    parse(batchId, resume, force);

    return 0;
  }

  public void parse(String batchId, boolean resume, boolean force) {
    run(StringUtil.toArgMap(Nutch.ARG_BATCH, batchId, Nutch.ARG_RESUME, resume, Nutch.ARG_FORCE, force));
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(), new ParserJob(), args);
    System.exit(res);
  }
}
