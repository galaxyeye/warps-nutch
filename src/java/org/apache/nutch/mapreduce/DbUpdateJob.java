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
package org.apache.nutch.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.crawl.UrlWithScore.UrlOnlyPartitioner;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator.UrlOnlyComparator;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.*;

public class DbUpdateJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateJob.class);

  private static final Collection<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.OUTLINKS);
    FIELDS.add(GoraWebPage.Field.INLINKS);
    FIELDS.add(GoraWebPage.Field.STATUS);
    FIELDS.add(GoraWebPage.Field.PREV_SIGNATURE);
    FIELDS.add(GoraWebPage.Field.SIGNATURE);
    FIELDS.add(GoraWebPage.Field.MARKERS);
    FIELDS.add(GoraWebPage.Field.METADATA);
    FIELDS.add(GoraWebPage.Field.RETRIES_SINCE_FETCH);
    FIELDS.add(GoraWebPage.Field.FETCH_TIME);
    FIELDS.add(GoraWebPage.Field.MODIFIED_TIME);
    FIELDS.add(GoraWebPage.Field.FETCH_INTERVAL);
    FIELDS.add(GoraWebPage.Field.PREV_FETCH_TIME);
    FIELDS.add(GoraWebPage.Field.PREV_MODIFIED_TIME);
    FIELDS.add(GoraWebPage.Field.HEADERS);
  }

  private String batchId = ALL_BATCH_ID_STR;

  public DbUpdateJob() {
  }

  public DbUpdateJob(Configuration conf) {
    setConf(conf);
  }

  public static Collection<GoraWebPage.Field> getFields(Job job) {
    return FIELDS;
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Params params = new Params(args);
    Configuration conf = getConf();

    String crawlId = params.get(ARG_CRAWL, conf.get(PARAM_CRAWL_ID));

    batchId = params.get(ARG_BATCH, ALL_BATCH_ID_STR);

    int round = conf.getInt(PARAM_CRAWL_ROUND, 0);

    conf.set(PARAM_BATCH_ID, batchId);
    conf.set(PARAM_CRAWL_ID, crawlId);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "round", round,
        "crawlId", crawlId,
        "batchId", batchId
    ));
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    ScoringFilters scoringFilters = new ScoringFilters(currentJob.getConfiguration());
    HashSet<GoraWebPage.Field> fields = new HashSet<>(FIELDS);
    fields.addAll(scoringFilters.getFields());

    // Partition by {url}, sort by {url,score} and group by {url}.
    // This ensures that the inlinks are sorted by score when they enter
    // the reducer.

    currentJob.setPartitionerClass(UrlOnlyPartitioner.class);
    currentJob.setSortComparatorClass(UrlScoreComparator.class);
    currentJob.setGroupingComparatorClass(UrlOnlyComparator.class);

    MapFieldValueFilter<String, GoraWebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(currentJob, fields, UrlWithScore.class,
        NutchWritable.class, DbUpdateMapper.class, batchIdFilter);
    StorageUtils.initReducerJob(currentJob, DbUpdateReducer.class);

    DataStore<String, GoraWebPage> store = StorageUtils.createWebStore(getConf(), String.class, GoraWebPage.class);

    LOG.info("Loaded Fields : " + StringUtils.join(StorageUtils.toStringArray(fields), ", "));

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  private int updateTable(String crawlId, String batchId) throws Exception {
    run(Params.toArgMap(Nutch.ARG_CRAWL, crawlId, ARG_BATCH, batchId));
    return 0;
  }

  private void printUsage() {
    String usage = "Usage: DbUpdateJob (<batchId> | -all) [-crawlId <id>] "
        + "    <batchId>     - crawl identifier returned by Generator, or -all for all \n \t \t    generated batchId-s\n"
        + "    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\n";

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

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID, "");

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else if ("-batchId".equals(args[i])) {
        batchId = args[++i];
      } else {
        throw new IllegalArgumentException("arg " + args[i] + " not recognized");
      }
    }

    return updateTable(crawlId, batchId);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("---------------------------------------------------\n\n");

    int res = ToolRunner.run(ConfigUtils.create(), new DbUpdateJob(), args);
    System.exit(res);
  }
}
