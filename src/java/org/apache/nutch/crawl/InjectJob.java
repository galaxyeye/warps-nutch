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

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.fetcher.FetchMode;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.mapreduce.NutchJob;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class takes a flat file of URLs and adds them to the of pages to be
 * crawled. Useful for bootstrapping the system. The URL files contain one URL
 * per line, optionally followed by custom metadata separated by tabs with the
 * metadata key separated from the corresponding value by '='. <br>
 * Note that some metadata keys are reserved : <br>
 * - <i>nutch.score</i> : allows to set a custom score for a specific URL <br>
 * - <i>nutch.fetchIntervalSec</i> : allows to set a custom fetch fetchIntervalSec for a
 * specific URL <br>
 * e.g. http://www.nutch.org/ \t nutch.score=10 \t nutch.fetchIntervalSec=2592000
 * \t userType=open_source
 **/
public class InjectJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(InjectJob.class);

  private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private static final Utf8 YES_STRING = new Utf8("y");

  static {
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.STATUS);
  }

  /** metadata key reserved for setting a custom score for a specific URL */
  public static final String NutchScoreMDName = "nutch.score";
  /**
   * metadata key reserved for setting a custom fetchIntervalSec for a specific URL
   */
  public static final String NutchFetchIntervalMDName = "nutch.fetchIntervalSec";
  // The shortest url
  public static final String ShortestValidUrl = "ftp://t.tt";

  public static class UrlMapper extends Mapper<LongWritable, Text, String, WebPage> {
    private Configuration conf;

    private String crawlId;
    /** Fetch interval in second */
    private int fetchIntervalSec;
    private float scoreInjected;
    private ScoringFilters scoreFilters;
    private long currentTime;

    /** Custom page score */
    private float customPageScore;
    /** Custom fetch interval in second */
    private int customFetchIntervalSec;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      conf = context.getConfiguration();
      scoreFilters = new ScoringFilters(conf);

      crawlId = conf.get(Nutch.CRAWL_ID_KEY);
      fetchIntervalSec = getFetchIntervalSec();
      scoreInjected = conf.getFloat("db.score.injected", Float.MAX_VALUE);
      currentTime = conf.getLong("injector.current.time", System.currentTimeMillis());

      LOG.info(StringUtil.formatParams(
          "className", this.getClass().getSimpleName(),
          "crawlId", crawlId,
          "fetchIntervalSec", fetchIntervalSec,
          "scoreInjected", scoreInjected,
          "injectTime", TimingUtil.format(currentTime)
      ));
    }

    private int getFetchIntervalSec() {
      // Fetch fetchIntervalSec, the default value is 30 days
      // int fetchIntervalSec = conf.getInt("db.fetch.fetchIntervalSec.default", TimingUtil.MONTH);

      // For seeds, we re-fetch it every time we start the loop
      int fetchInterval = TimingUtil.MINUTE;

      return fetchInterval;
    }

    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
      String urlLine = line.toString().trim();

      // LOG.debug("Inject : " + urlLine);

      /* Ignore line that start with # */
      if (urlLine.length() < ShortestValidUrl.length() || urlLine.startsWith("#")) {
        return;
      }

      String url = StringUtils.substringBefore(urlLine, "\t");

      Map<String, String> metadata = getMetadata(urlLine);

      String reversedUrl = null;
      WebPage row = WebPage.newBuilder().build();
      try {
        reversedUrl = TableUtil.reverseUrl(url);
        row.setFetchTime(currentTime);
        row.setFetchInterval(fetchIntervalSec);
      }
      catch (MalformedURLException e) {
        LOG.warn("Ignore illegal formatted url : " + url);
      }

      if (reversedUrl == null) {
        LOG.warn("Ignore illegal formatted url : " + url);
        return;
      }

      // Add metadata to page
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        String k = entry.getKey();
        String v = entry.getValue();

        // TODO : @vincent It's very strange to use ByteBuffer, it causes argly code
        // row.getMetadata().put(new Utf8(k), ByteBuffer.wrap(v.getBytes()));
        TableUtil.putMetadata(row, k, v);
      }

      if (customPageScore != -1f) {
        row.setScore(customPageScore);
      }
      else {
        row.setScore(scoreInjected);
      }

      try {
        scoreFilters.injectedScore(url, row);
      } catch (ScoringFilterException e) {
        LOG.warn("Cannot filter injected score for " + url + ", using default. (" + e.getMessage() + ")");
      }

      row.getMarkers().put(Nutch.DISTANCE, new Utf8(String.valueOf(0)));
      Mark.INJECT_MARK.putMark(row, YES_STRING);

      context.write(reversedUrl, row);

      // LOG.debug("Injected : " + url);

      // getCounter().updateAffectedRows(url);

      context.getCounter(Nutch.COUNTER_GROUP_STATUS, NutchCounter.Counter.totalPages.name()).increment(1);
    }

    private Map<String, String> getMetadata(String seedUrl) {
      Map<String, String> metadata = new TreeMap<>();

      // if tabs : metadata that could be stored
      // must be name=value and separated by \t
      if (seedUrl.contains("\t")) {
        String[] splits = seedUrl.split("\t");

        for (int s = 1; s < splits.length; s++) {
          // find separation between name and value
          int indexEquals = splits[s].indexOf("=");
          if (indexEquals == -1) {
            // skip anything without a =
            continue;
          }

          String name = splits[s].substring(0, indexEquals);
          String value = splits[s].substring(indexEquals + 1);
          if (name.equals(NutchScoreMDName)) {
            this.customPageScore = StringUtil.tryParseFloat(value, -1f);
          }
          else if (name.equals(NutchFetchIntervalMDName)) {
            this.customFetchIntervalSec = StringUtil.tryParseInt(value, fetchIntervalSec);
          }
          else {
            metadata.put(name, value);
          }
        } // for
      } // if

      return metadata;
    }
  }

  public InjectJob() {
  }

  public InjectJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Configuration conf = getConf();

    String crawlId = NutchUtil.get(args, Nutch.ARG_CRAWL, conf.get(Nutch.CRAWL_ID_KEY));
    String seedDir = NutchUtil.get(args, Nutch.ARG_SEEDDIR);

    conf.set(Nutch.CRAWL_ID_KEY, crawlId);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "seedDir", seedDir,
        "jobStartTime", TimingUtil.format(startTime)
    ));
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    Configuration conf = getConf();

    String seedDir = NutchUtil.get(args, Nutch.ARG_SEEDDIR);
    Path input = new Path(seedDir);

    if (!input.getFileSystem(conf).exists(input)) {
      LOG.warn("Seed dir does not exit!!!");
      return;
    }

    // TODO : local file system does not support directory while hdfs file system does
//    if (!input.getFileSystem(conf).isFile(input)) {
//      LOG.warn("Seed dir does not exit!!!");
//      return;
//    }

    FileInputFormat.addInputPath(currentJob, input);
    currentJob.setMapperClass(UrlMapper.class);
    currentJob.setMapOutputKeyClass(String.class);
    currentJob.setMapOutputValueClass(WebPage.class);
    currentJob.setOutputFormatClass(GoraOutputFormat.class);

    DataStore<String, WebPage> store = StorageUtils.createWebStore(currentJob.getConfiguration(), String.class, WebPage.class);
    GoraOutputFormat.setOutput(currentJob, store, true);

    currentJob.setReducerClass(Reducer.class);
    currentJob.setNumReduceTasks(0);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName(),
        "seedDir", seedDir
    ));

    currentJob.waitForCompletion(true);
  }

  public void inject(Path path, String crawlId) throws Exception {
    run(StringUtil.toArgMap(
        Nutch.ARG_CRAWL, crawlId,
        Nutch.ARG_SEEDDIR, path.toString()
    ));
  }

  public void inject(String urlDir, String crawlId) throws Exception {
    run(StringUtil.toArgMap(
        Nutch.ARG_SEEDDIR, urlDir,
        Nutch.ARG_CRAWL, crawlId
    ));
  }

  private void printUsage() {
    System.err.println("Usage: InjectJob <url_dir> [-crawlId <id>] [-fetchMode <native|crowdsourcing|proxy>]");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      return -1;
    }

    Configuration conf = getConf();

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY, "");
    String fetchMode = conf.get(Nutch.FETCH_MODE_KEY, FetchMode.NATIVE.value());
    if (!FetchMode.validate(fetchMode)) {
      System.out.println("-fetchMode accepts one of [native|proxy|crowdsourcing]");
      return -1;
    }

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else {
        System.err.println("Unrecognized arg " + args[i]);
        return -1;
      }
    }

    try {
      inject(args[0], crawlId);
      return 0;
    } catch (Exception e) {
      LOG.error("InjectJob: " + StringUtil.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    LOG.info("---------------------------------------------------\n\n");
    int res = ToolRunner.run(NutchConfiguration.create(), new InjectJob(), args);
    System.exit(res);
  }
}
