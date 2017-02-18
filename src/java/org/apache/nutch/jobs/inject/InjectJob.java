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
package org.apache.nutch.jobs.inject;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.io.Files;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.common.Params;
import org.apache.nutch.crawl.SeedBuilder;
import org.apache.nutch.jobs.NutchCounter;
import org.apache.nutch.jobs.NutchJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.*;

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

  public InjectJob() {
  }

  public InjectJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Params params = new Params(args);
    Configuration conf = getConf();

    String crawlId = params.get(PARAM_CRAWL_ID, conf.get(PARAM_CRAWL_ID));
    String seedPath = params.getString(PARAM_INJECT_SEED_PATH);
    String seedUrls = params.get(PARAM_INJECT_SEED_URLS, "");

    // Seed urls have higher priority
    if (!seedUrls.isEmpty()) {
      File seedFile = File.createTempFile("seed", ".txt");
      Files.write(seedUrls.getBytes(), seedFile);
      seedPath = seedFile.getAbsolutePath();
    }

    if (HadoopFSUtil.isDistributedFS(conf)) {
      LOG.info("Running under hadoop distributed file system");

      FileSystem fs = FileSystem.get(conf);
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(seedPath);
      fs.copyFromLocalFile(path, path);
    }

    conf.set(PARAM_CRAWL_ID, crawlId);
    conf.set(PARAM_INJECT_SEED_PATH, seedPath);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "seedPath", seedPath,
        "seedUrls", seedUrls,
        "jobStartTime", DateTimeUtil.format(startTime)
    ));
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    Configuration conf = getConf();

    String seedPath = conf.get(PARAM_INJECT_SEED_PATH);

    FileInputFormat.addInputPath(currentJob, new Path(seedPath));
    currentJob.setMapperClass(UrlMapper.class);
    currentJob.setMapOutputKeyClass(String.class);
    currentJob.setMapOutputValueClass(WebPage.class);
    currentJob.setOutputFormatClass(GoraOutputFormat.class);

    DataStore<String, GoraWebPage> store = StorageUtils.createWebStore(currentJob.getConfiguration(), String.class, GoraWebPage.class);
    GoraOutputFormat.setOutput(currentJob, store, true);

    currentJob.setReducerClass(Reducer.class);
    currentJob.setNumReduceTasks(0);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  public void inject(Path path, String crawlId) throws Exception {
    run(Params.toArgMap(
            PARAM_CRAWL_ID, crawlId,
            PARAM_INJECT_SEED_PATH, path.toString()
    ));
  }

  public void inject(String seedDir, String crawlId) throws Exception {
    run(Params.toArgMap(
            PARAM_CRAWL_ID, seedDir,
            PARAM_INJECT_SEED_PATH, crawlId
    ));
  }

  static class UrlMapper extends Mapper<LongWritable, Text, String, GoraWebPage> {
    private Configuration conf;
    private SeedBuilder seedBuiler;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      conf = context.getConfiguration();

      seedBuiler = new SeedBuilder(conf);

      String crawlId = conf.get(PARAM_CRAWL_ID);

      Params.of(
              "className", this.getClass().getSimpleName(),
              "crawlId", crawlId
      ).merge(seedBuiler.getParams()).withLogger(InjectJob.LOG).info();
    }

    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
      if (line == null || line.toString().isEmpty()) {
        return;
      }

      String urlLine = line.toString().trim();
      if (urlLine.startsWith("#")) {
        return;
      }

      WebPage row = seedBuiler.buildWebPage(urlLine);

      if (row != null) {
        context.write(row.reversedUrl(), row.get());
        context.getCounter(Nutch.STAT_RUNTIME_STATUS, NutchCounter.Counter.totalPages.name()).increment(1);
      }
    }
  }

  private class Options {
    @Parameter(required = true, description = "Seed file path or seed url")
    List<String> uris = new ArrayList<>();
    @Parameter(names = {"-update", "-u"}, description = "Update if already exists")
    boolean update = false;
    @Parameter(names = {"-watch", "-w"}, description = "Watch the health for the seed")
    boolean watch = false;
    @Parameter(names = {"-outLinkPattern", "-p"}, description = "Out link patthen")
    String outLinkPattern = ".+";
    @Parameter(names = {"-fetchInterval", "-i"}, description = "Fetch intervel")
    String fetchInterval = null;
    @Parameter(names = {"-minAnchorLen", "-l"}, description = "Shortest anchors for links to follow")
    Integer minAnchorLen = 8;
    @Parameter(names = "-crawlId", description = "Crawl id, (default : \"storage.crawl.id\")")
    String crawlId = null;
    @Parameter(names = {"-help", "-h"}, help = true)
    boolean help;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options opt = new Options();
    JCommander jc = new JCommander(opt, args);

    if (args.length < 1) {
      jc.usage();
      return -1;
    }

    String url = opt.uris.get(0);
    try {
      run(Params.toArgMap(
              PARAM_INJECT_SEED_PATH, url,
              PARAM_CRAWL_ID, opt.crawlId,
              PARAM_INJECT_UPDATE, opt.update,
              PARAM_INJECT_WATCH, opt.watch,
              PARAM_PARSE_OUTLINK_PATTERN, opt.outLinkPattern,
              PARAM_PARSE_MIN_ANCHOR_LENGTH, opt.minAnchorLen,
              PARAM_FETCH_INTERVAL, opt.fetchInterval
      ));

      return 0;
    } catch (Exception e) {
      LOG.error("InjectJob: " + StringUtil.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
     int res = ToolRunner.run(ConfigUtils.create(), new InjectJob(), args);
     System.exit(res);
  }
}
