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

import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.URLPartitioner.SelectorEntryPartitioner;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.nutch.crawl.URLPartitioner.PARTITION_MODE_KEY;
import static org.apache.nutch.metadata.Nutch.*;

public class GenerateJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(GenerateJob.class);

  private static final Set<WebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(WebPage.Field.FETCH_TIME);
    FIELDS.add(WebPage.Field.SCORE);
    FIELDS.add(WebPage.Field.STATUS);
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.METADATA);
  }

  public GenerateJob() {
  }

  public GenerateJob(Configuration conf) {
    setConf(conf);
  }

  public Collection<WebPage.Field> getFields(Job job) {
    Collection<WebPage.Field> fields = new HashSet<>(FIELDS);
    fields.addAll(FetchScheduleFactory.getFetchSchedule(job.getConfiguration()).getFields());
    return fields;
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Params params = new Params(args);
    Configuration conf = getConf();

    String crawlId = params.get(ARG_CRAWL, conf.get(PARAM_CRAWL_ID));
    String batchId = params.get(ARG_BATCH, NutchUtil.generateBatchId());
    boolean reGenerate = params.getBoolean(ARG_REGENERATE, false);
    long topN = params.getLong(ARG_TOPN, Long.MAX_VALUE);
    boolean filter = params.getBoolean(ARG_FILTER, true);
    boolean norm = params.getBoolean(ARG_NORMALIZE, true);
    long pseudoCurrTime = params.getLong(ARG_CURTIME, startTime);
    String nutchTmpDir = conf.get(PARAM_NUTCH_TMP_DIR, PATH_NUTCH_TMP_DIR);

    conf.set(PARAM_CRAWL_ID, crawlId);
    conf.set(PARAM_BATCH_ID, batchId);
    conf.setLong(GENERATE_TIME_KEY, startTime); // seems not used, (or pseudoCurrTime used?)
    conf.setLong(PARAM_GENERATOR_CUR_TIME, pseudoCurrTime);
    conf.setLong(PARAM_GENERATOR_TOP_N, topN);
    conf.setBoolean(PARAM_GENERATE_REGENERATE, reGenerate);
    conf.setBoolean(PARAM_GENERATE_FILTER, filter);
    conf.setBoolean(PARAM_GENERATE_NORMALISE, norm);

    URLUtil.HostGroupMode hostGroupMode = conf.getEnum(PARAM_GENERATOR_COUNT_MODE, URLUtil.HostGroupMode.BY_HOST);
    conf.setEnum(PARTITION_MODE_KEY, hostGroupMode);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "batchId", batchId,
        "filter", filter,
        "norm", norm,
        "pseudoCurrTime", TimingUtil.format(pseudoCurrTime),
        "topN", topN,
        "reGenerate", reGenerate,
        PARAM_GENERATOR_COUNT_MODE, hostGroupMode,
        PARTITION_MODE_KEY, hostGroupMode,
        "nutchTmpDir", nutchTmpDir
    ));

    Files.write(Paths.get(PATH_LAST_BATCH_ID), (batchId + "\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    DataStore<String, WebPage> store = StorageUtils.createWebStore(getConf(), String.class, WebPage.class);

    Query<String, WebPage> query = initQuery(store);

    GoraMapper.initMapperJob(currentJob, query, store, SelectorEntry.class,
        WebPage.class, GenerateMapper.class, SelectorEntryPartitioner.class, true);
    StorageUtils.initReducerJob(currentJob, GenerateReducer.class);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  private Query<String, WebPage> initQuery(DataStore<String, WebPage> store) {
    Query<String, WebPage> query = store.newQuery();

    Collection<WebPage.Field> fields = getFields(currentJob);
    query.setFields(StorageUtils.toStringArray(fields));

    return query;
  }

  public static class SelectorEntry implements WritableComparable<SelectorEntry> {
    String url;
    float score;

    public SelectorEntry() {
    }

    public SelectorEntry(String url, float score) {
      this.url = url;
      this.score = score;
    }

    public String getUrl() {
      return url;
    }

    public float getScore() {
      return score;
    }

    public void readFields(DataInput in) throws IOException {
      url = Text.readString(in);
      score = in.readFloat();
    }

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, url);
      out.writeFloat(score);
    }

    public int compareTo(SelectorEntry se) {
      if (se.score > score)
        return 1;
      else if (se.score == score)
        return url.compareTo(se.url);
      return -1;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + url.hashCode();
      result = prime * result + Float.floatToIntBits(score);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      SelectorEntry other = (SelectorEntry) obj;
      if (!url.equals(other.url))
        return false;
      if (Float.floatToIntBits(score) != Float.floatToIntBits(other.score))
        return false;
      return true;
    }

    /**
     * Sets url with score on this writable. Allows for writable reusing.
     * 
     * @param url
     * @param score
     */
    public void set(String url, float score) {
      this.url = url;
      this.score = score;
    }
  }

  public static class SelectorEntryComparator extends WritableComparator {
    public SelectorEntryComparator() {
      super(SelectorEntry.class, true);
    }
  }

  static {
    WritableComparator.define(SelectorEntry.class, new SelectorEntryComparator());
  }

  private void printUsage() {
    System.out.println("Usage: GenerateJob [-crawlId <id>] [-batchId <id>] [-fetchMod <native|proxy|crowdsourcing>] " +
        "[-reGen] [-topN N] [-noFilter] [-noNorm] [-adddays numDays]");
    System.out.println("    -crawlId <id>     - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\");");
    System.out.println("    -fetchMode <mode> - the fetch mode, can be one of [native|proxy|crowdsourcing], \n \t \t    (default: fetcher.fetch.mode)\");");
    System.out.println("    -batchId <id>     - the batch id ");
    System.out.println("    -topN <N>         - number of top URLs to be selected, default is Long.MAX_VALUE ");
    System.out.println("    -noFilter         - do not activate the filter plugin to filter the url, default is true ");
    System.out.println("    -noNorm           - do not activate the normalizer plugin to normalize the url, default is true ");
    System.out.println("    -adddays          - Adds numDays to the current time to facilitate crawling urls already");
    System.out.println("                        fetched sooner then db.fetch.interval.default. Default value is 0.");
    System.out.println("----------------------------------------------------------------------------------------------------");
    System.out.println("Please set the params.");
  }

  /**
   * Mark URLs ready for fetching
   *
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * */
  public String generate(long topN, String crawlId, String batchId, boolean reGenerate, long pseudoCurrTime, boolean filter, boolean norm) throws Exception {
    run(Params.toArgMap(
        ARG_TOPN, topN,
        ARG_CRAWL, crawlId,
        ARG_BATCH, batchId,
        ARG_REGENERATE, reGenerate,
        ARG_CURTIME, pseudoCurrTime, ARG_FILTER, filter,
        ARG_NORMALIZE, norm));

    return getConf().get(PARAM_BATCH_ID);
  }

  public int run(String[] args) throws Exception {
    if (args.length <= 0) {
      printUsage();
      return -1;
    }

    Configuration conf = getConf();

    String crawlId = conf.get(PARAM_CRAWL_ID, "");

    long pseudoCurrTime = System.currentTimeMillis();
    boolean reGenerate = false;
    long topN = Long.MAX_VALUE;
    boolean filter = true;
    boolean norm = true;
    String batchId = NutchUtil.generateBatchId();

    for (int i = 0; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else if ("-batchId".equals(args[i])) {
        batchId = args[++i];
      } else if ("-reGen".equals(args[i])) {
        reGenerate = true;
      } else if ("-topN".equals(args[i])) {
        topN = Long.parseLong(args[++i]);
      } else if ("-noFilter".equals(args[i])) {
        filter = false;
      } else if ("-noNorm".equals(args[i])) {
        norm = false;
      } else if ("-adddays".equals(args[i])) {
        long numDays = Integer.parseInt(args[++i]);
        pseudoCurrTime += numDays * 1000L * 60 * 60 * 24;
      } else {
        System.err.println("Unrecognized arg " + args[i]);
        return -1;
      }
    }

    try {
      return (generate(topN, crawlId, batchId, reGenerate, pseudoCurrTime, filter, norm) != null) ? 0 : 1;
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String args[]) throws Exception {
    LOG.info("---------------------------------------------------\n\n");

    int res = ToolRunner.run(NutchConfiguration.create(), new GenerateJob(), args);

    System.exit(res);
  }
}
