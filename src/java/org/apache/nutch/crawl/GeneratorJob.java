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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
import org.apache.nutch.crawl.URLPartitioner.SelectorEntryPartitioner;
import org.apache.nutch.mapreduce.NutchJob;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratorJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(GeneratorJob.class);

  private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.FETCH_TIME);
    FIELDS.add(WebPage.Field.SCORE);
    FIELDS.add(WebPage.Field.STATUS);
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.METADATA);
  }

  public GeneratorJob() {
  }

  public GeneratorJob(Configuration conf) {
    setConf(conf);
  }

  public Collection<WebPage.Field> getFields(Job job) {
    Collection<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
    fields.addAll(FetchScheduleFactory.getFetchSchedule(job.getConfiguration()).getFields());
    return fields;
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Configuration conf = getConf();

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);
    String batchId = NutchUtil.get(args, Nutch.ARG_BATCH, NutchUtil.generateBatchId());
    long topN = NutchUtil.getLong(args, Nutch.ARG_TOPN, Long.MAX_VALUE);
    boolean filter = NutchUtil.getBoolean(args, Nutch.ARG_FILTER, true);
    boolean norm = NutchUtil.getBoolean(args, Nutch.ARG_NORMALIZE, true);
    long pseudoCurrTime = NutchUtil.getLong(args, Nutch.ARG_CURTIME, startTime);

    conf.set(Nutch.GENERATOR_BATCH_ID, batchId);
    conf.setLong(Nutch.GENERATE_TIME_KEY, startTime); // seems not used
    conf.setLong(Nutch.GENERATOR_CUR_TIME, pseudoCurrTime);
    conf.setLong(Nutch.GENERATOR_TOP_N, topN);
    conf.setBoolean(Nutch.GENERATOR_FILTER, filter);
    conf.setBoolean(Nutch.GENERATOR_NORMALISE, norm);

    String domainMode = conf.get(Nutch.GENERATOR_COUNT_MODE, Nutch.GENERATOR_COUNT_VALUE_HOST);
    if (Nutch.GENERATOR_COUNT_VALUE_HOST.equalsIgnoreCase(domainMode)) {
      conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
    } else if (Nutch.GENERATOR_COUNT_VALUE_DOMAIN.equalsIgnoreCase(domainMode)) {
      conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_DOMAIN);
    } else {
      LOG.warn("Unknown generator.max.count mode '" + domainMode + "', using mode=" + Nutch.GENERATOR_COUNT_VALUE_HOST);
      conf.set(Nutch.GENERATOR_COUNT_MODE, Nutch.GENERATOR_COUNT_VALUE_HOST);
      conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
    }

    recordAndLogParams(
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "batchId", batchId,
        "filter", filter,
        "norm", norm,
        "pseudoCurrTime", pseudoCurrTime,
        "topN", topN,
        "domainMode", domainMode);
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    DataStore<String, WebPage> store = StorageUtils.createWebStore(getConf(), String.class, WebPage.class);

    Query<String, WebPage> query = initQuery(store);

    GoraMapper.initMapperJob(currentJob, query, store, SelectorEntry.class,
        WebPage.class, GeneratorMapper.class, SelectorEntryPartitioner.class, true);
    StorageUtils.initReducerJob(currentJob, GeneratorReducer.class);

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

  /**
   * Mark URLs ready for fetching
   * 
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * */
  public String generate(long topN, long pseudoCurrTime, boolean filter, boolean norm) throws Exception {
    run(NutchUtil.toArgMap(Nutch.ARG_TOPN, topN,
        Nutch.ARG_CURTIME, pseudoCurrTime, Nutch.ARG_FILTER, filter,
        Nutch.ARG_NORMALIZE, norm));

    String batchId = getConf().get(Nutch.GENERATOR_BATCH_ID);

    return batchId;
  }

  public int run(String[] args) throws Exception {
    if (args.length <= 0) {
      System.out
          .println("Usage: GeneratorJob [-topN N] [-crawlId id] [-noFilter] [-noNorm] [-adddays numDays]");
      System.out
          .println("    -topN <N>      - number of top URLs to be selected, default is Long.MAX_VALUE ");
      System.out
          .println("    -crawlId <id>  - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\");");
      System.out
          .println("    -noFilter      - do not activate the filter plugin to filter the url, default is true ");
      System.out
          .println("    -noNorm        - do not activate the normalizer plugin to normalize the url, default is true ");
      System.out
          .println("    -adddays       - Adds numDays to the current time to facilitate crawling urls already");
      System.out
          .println("                     fetched sooner then db.fetch.interval.default. Default value is 0.");
      System.out.println("    -batchId       - the batch id ");
      System.out.println("----------------------");
      System.out.println("Please set the params.");
      return -1;
    }

    long pseudoCurrTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    boolean filter = true;
    boolean norm = true;
    String batchId = NutchUtil.generateBatchId();

    for (int i = 0; i < args.length; i++) {
      if ("-topN".equals(args[i])) {
        topN = Long.parseLong(args[++i]);
      } else if ("-noFilter".equals(args[i])) {
        filter = false;
      } else if ("-noNorm".equals(args[i])) {
        norm = false;
      } else if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      } else if ("-adddays".equals(args[i])) {
        long numDays = Integer.parseInt(args[++i]);
        pseudoCurrTime += numDays * 1000L * 60 * 60 * 24;
      } else if ("-batchId".equals(args[i])) {
        batchId = args[++i];
      }
      else {
        System.err.println("Unrecognized arg " + args[i]);
        return -1;
      }
    }

    getConf().set(Nutch.GENERATOR_BATCH_ID, batchId);

    try {
      return (generate(topN, pseudoCurrTime, filter, norm) != null) ? 0 : 1;
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String args[]) throws Exception {
    LOG.info("---------------------------------------------------\n\n");

    int res = ToolRunner.run(NutchConfiguration.create(), new GeneratorJob(), args);
    System.exit(res);
  }
}
