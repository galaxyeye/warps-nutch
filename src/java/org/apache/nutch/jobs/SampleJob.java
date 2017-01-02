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

import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.*;

public class SampleJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(SampleJob.class);

  private static final Collection<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.METADATA);
    FIELDS.add(GoraWebPage.Field.HEADERS);
    FIELDS.add(GoraWebPage.Field.OUTLINKS);
    FIELDS.add(GoraWebPage.Field.INLINKS);
    FIELDS.add(GoraWebPage.Field.STATUS);
    FIELDS.add(GoraWebPage.Field.FETCH_TIME);
    FIELDS.add(GoraWebPage.Field.MODIFIED_TIME);
    FIELDS.add(GoraWebPage.Field.FETCH_INTERVAL);
    FIELDS.add(GoraWebPage.Field.CONTENT_TYPE);
    FIELDS.add(GoraWebPage.Field.CONTENT);
  }

  public SampleJob() {
  }

  public SampleJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Params params = new Params(args);
    Configuration conf = getConf();

    String crawlId = params.get(ARG_CRAWL, conf.get(PARAM_CRAWL_ID));
    String batchId = params.get(ARG_BATCH, ALL_BATCH_ID_STR);
    int limit = params.getInt(ARG_LIMIT, -1);

    conf.set(PARAM_CRAWL_ID, crawlId);
    conf.set(PARAM_BATCH_ID, batchId);
    conf.setInt(PARAM_MAPPER_LIMIT, limit);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "limit", limit
    ));
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    HashSet<GoraWebPage.Field> fields = new HashSet<>(FIELDS);

    StorageUtils.initMapperJob(currentJob, fields, Text.class, GoraWebPage.class, SampleMapper.class);
    StorageUtils.initReducerJob(currentJob, SampleReducer.class);

    DataStore<String, GoraWebPage> store = StorageUtils.createWebStore(getConf(), String.class, GoraWebPage.class);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  private int runSample(String crawlId, String batchId, int limit) throws Exception {
    run(Params.toArgMap(Nutch.ARG_CRAWL, crawlId, ARG_BATCH, batchId, ARG_LIMIT, limit));
    return 0;
  }

  private void printUsage() {
    String usage = "Usage: SampleJob [-crawlId <id>] [-batchId <batchId>] [-limit <limit>] \n"
        + "    -crawlId <id>      - the id to prefix the schemas to operate on (default: persist.crawl.id)\n"
        + "    -batchId <batchId> - the batch id (default: -all)\n"
        + "    -limit   <limit>   - the limit \n ";

    System.err.println(usage);
  }

  public int run(String[] args) throws Exception {
    printUsage();

    String crawlId = null;
    String batchId = null;
    int limit = -1;

    for (int i = 0; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else if ("-batchId".equals(args[i])) {
        batchId = args[++i].toUpperCase();
      } else if ("-limit".equals(args[i])) {
        limit = Integer.parseInt(args[++i]);
      } else {
        throw new IllegalArgumentException("arg " + args[i] + " not recognized");
      }
    }

    return runSample(crawlId, batchId, limit);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("---------------------------------------------------\n\n");

    int res = ToolRunner.run(ConfigUtils.create(), new SampleJob(), args);
    System.exit(res);
  }
}
