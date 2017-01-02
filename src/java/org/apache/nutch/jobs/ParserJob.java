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
package org.apache.nutch.jobs;

import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.parse.ParseFilters;
import org.apache.nutch.parse.ParserFactory;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.IdentityPageReducer;
import org.apache.nutch.util.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.*;

public class ParserJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(ParserJob.class);

  public static final String SKIP_TRUNCATED = "parser.skip.truncated";

  private static final Collection<GoraWebPage.Field> FIELDS = new HashSet<GoraWebPage.Field>();

  private String batchId;

  static {
    FIELDS.add(GoraWebPage.Field.STATUS);
    FIELDS.add(GoraWebPage.Field.CONTENT);
    FIELDS.add(GoraWebPage.Field.CONTENT_TYPE);
    FIELDS.add(GoraWebPage.Field.SIGNATURE);
    FIELDS.add(GoraWebPage.Field.MARKERS);
    FIELDS.add(GoraWebPage.Field.PARSE_STATUS);
    FIELDS.add(GoraWebPage.Field.OUTLINKS);
    FIELDS.add(GoraWebPage.Field.METADATA);
    FIELDS.add(GoraWebPage.Field.HEADERS);
  }

  public ParserJob() {

  }

  public ParserJob(Configuration conf) {
    setConf(conf);
  }

  public static Collection<GoraWebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<GoraWebPage.Field> fields = new HashSet<>(FIELDS);
    ParserFactory parserFactory = new ParserFactory(conf);
    ParseFilters parseFilters = new ParseFilters(conf);

    Collection<GoraWebPage.Field> parsePluginFields = parserFactory.getFields();
    Collection<GoraWebPage.Field> signaturePluginFields = SignatureFactory.getFields(conf);
    Collection<GoraWebPage.Field> htmlParsePluginFields = parseFilters.getFields();

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

    Params params = new Params(args);
    Configuration conf = getConf();

    String crawlId = params.get(ARG_CRAWL, conf.get(PARAM_CRAWL_ID));
    String fetchMode = conf.get(PARAM_FETCH_MODE);

    batchId = params.get(ARG_BATCH, ALL_BATCH_ID_STR);
    Boolean reparse = batchId.equalsIgnoreCase("-reparse");
    batchId = reparse ? "-all" : batchId;
    int limit = params.getInt(ARG_LIMIT, -1);
    Boolean resume = params.getBoolean(ARG_RESUME, false);
    Boolean force = params.getBoolean(ARG_FORCE, false);

    getConf().set(PARAM_CRAWL_ID, crawlId);
    getConf().set(PARAM_BATCH_ID, batchId);
    conf.setInt(PARAM_LIMIT, limit);
    getConf().setBoolean(PARAM_RESUME, resume);
    getConf().setBoolean(PARAM_FORCE, force);
    getConf().setBoolean(PARAM_REPARSE, reparse);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "fetchMode", fetchMode,
        "resume", resume,
        "force", force,
        "reparse", reparse,
        "limit", limit
    ));
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    Collection<GoraWebPage.Field> fields = getFields(currentJob);
    MapFieldValueFilter<String, GoraWebPage> batchIdFilter = getBatchIdFilter(batchId);

    StorageUtils.initMapperJob(currentJob, fields, String.class, GoraWebPage.class, ParserMapper.class, batchIdFilter);
    StorageUtils.initReducerJob(currentJob, IdentityPageReducer.class);

    // there is no reduce phase, so set reduce tasks to be 0
    currentJob.setNumReduceTasks(0);

    // used to get schema name
    DataStore<String, GoraWebPage> store = StorageUtils.createWebStore(getConf(), String.class, GoraWebPage.class);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName(),
        "batchId", batchId
    ));

    currentJob.waitForCompletion(true);
  }

  private void printUsage() {
    System.err.println("Usage: ParserJob (<batchId> | -all | -reparse) [-crawlId <id>] [-resume] [-force]");
    System.err.println("    <batchId>     - symbolic batch ID created by Generator");
    System.err.println("    -all          - consider pages from all crawl jobs");
    System.err.println("    -reparse      - reparse pages from all crawl jobs");
    System.err.println("    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: persist.crawl.id)");
    System.err.println("    -limit        - limit");
    System.err.println("    -resume       - resume a previous incomplete job");
    System.err.println("    -force        - force re-parsing even if a page is already parsed");
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      return -1;
    }

    Configuration conf = getConf();

    String batchId = args[0];
    if (!batchId.equals("-all") && !batchId.equals("-reparse") && batchId.startsWith("-")) {
      printUsage();
      return -1;
    }

    String crawlId = conf.get(PARAM_CRAWL_ID, "");

    int limit = -1;
    boolean resume = false;
    boolean force = false;

    for (int i = 0; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
        // getConf().set(PARAM_CRAWL_ID, args[++i]);
      }
      else if ("-limit".equals(args[i])) {
        limit = Integer.parseInt(args[++i]);
      }
      else if ("-resume".equals(args[i])) {
        resume = true;
      }
      else if ("-force".equals(args[i])) {
        force = true;
      }    }

    parse(crawlId, batchId, limit, resume, force);

    return 0;
  }

  public void parse(String crawlId, String batchId, int limit, boolean resume, boolean force) {
    run(Params.toArgMap(
        ARG_CRAWL, crawlId,
        ARG_BATCH, batchId,
        ARG_RESUME, resume,
        ARG_FORCE, force,
        ARG_LIMIT, limit > 0 ? limit : null
    ));
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(ConfigUtils.create(), new ParserJob(), args);
    System.exit(res);
  }
}
