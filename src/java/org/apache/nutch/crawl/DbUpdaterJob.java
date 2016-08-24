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
package org.apache.nutch.crawl;

import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.UrlWithScore.UrlOnlyPartitioner;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator.UrlOnlyComparator;
import org.apache.nutch.fetcher.FetchMode;
import org.apache.nutch.mapreduce.NutchJob;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class DbUpdaterJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdaterJob.class);

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.OUTLINKS);
    FIELDS.add(WebPage.Field.INLINKS);
    FIELDS.add(WebPage.Field.STATUS);
    FIELDS.add(WebPage.Field.PREV_SIGNATURE);
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.METADATA);
    FIELDS.add(WebPage.Field.RETRIES_SINCE_FETCH);
    FIELDS.add(WebPage.Field.FETCH_TIME);
    FIELDS.add(WebPage.Field.MODIFIED_TIME);
    FIELDS.add(WebPage.Field.FETCH_INTERVAL);
    FIELDS.add(WebPage.Field.PREV_FETCH_TIME);
    FIELDS.add(WebPage.Field.PREV_MODIFIED_TIME);
    FIELDS.add(WebPage.Field.HEADERS);
  }

  private String batchId = Nutch.ALL_BATCH_ID_STR;

  public DbUpdaterJob() {

  }

  public DbUpdaterJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Configuration conf = getConf();

    String crawlId = NutchUtil.get(args, Nutch.ARG_CRAWL, conf.get(Nutch.CRAWL_ID_KEY));
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);
    String fetchMode = conf.get(Nutch.FETCH_MODE_KEY);
    batchId = NutchUtil.get(args, Nutch.ARG_BATCH, Nutch.ALL_BATCH_ID_STR);

    getConf().set(Nutch.BATCH_NAME_KEY, batchId);
    getConf().set(Nutch.CRAWL_ID_KEY, crawlId);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "fetchMode", fetchMode,
        "batchId", batchId
    ));
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    ScoringFilters scoringFilters = new ScoringFilters(currentJob.getConfiguration());
    HashSet<WebPage.Field> fields = new HashSet<>(FIELDS);
    fields.addAll(scoringFilters.getFields());

    // Partition by {url}, sort by {url,score} and group by {url}.
    // This ensures that the inlinks are sorted by score when they enter
    // the reducer.

    currentJob.setPartitionerClass(UrlOnlyPartitioner.class);
    currentJob.setSortComparatorClass(UrlScoreComparator.class);
    currentJob.setGroupingComparatorClass(UrlOnlyComparator.class);

    MapFieldValueFilter<String, WebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(currentJob, fields, UrlWithScore.class,
        NutchWritable.class, DbUpdateMapper.class, batchIdFilter);
    StorageUtils.initReducerJob(currentJob, DbUpdateReducer.class);

    DataStore<String, WebPage> store = StorageUtils.createWebStore(getConf(), String.class, WebPage.class);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  private int updateTable(String crawlId, String batchId) throws Exception {
    run(StringUtil.toArgMap(Nutch.ARG_CRAWL, crawlId, Nutch.ARG_BATCH, batchId));
    return 0;
  }

  private void printUsage() {
    String usage = "Usage: DbUpdaterJob (<batchId> | -all) [-crawlId <id>] "
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

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY, "");
    String fetchMode = conf.get(Nutch.FETCH_MODE_KEY, FetchMode.NATIVE.value());
    if (!FetchMode.validate(fetchMode)) {
      System.out.println("-fetchMode accepts one of [native|proxy|crowdsourcing]");
      return -1;
    }

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else if ("-batchId".equals(args[i])) {
        batchId = args[++i];
      } else if ("-fetchMode".equals(args[i])) {
        fetchMode = args[++i];
      } else {
        throw new IllegalArgumentException("arg " + args[i] + " not recognized");
      }
    }

    conf.set(Nutch.CRAWL_ID_KEY, crawlId);
    conf.set(Nutch.FETCH_MODE_KEY, fetchMode);

    return updateTable(crawlId, batchId);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("---------------------------------------------------\n\n");

    int res = ToolRunner.run(NutchConfiguration.create(), new DbUpdaterJob(), args);
    System.exit(res);
  }

}
