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
import org.apache.nutch.crawl.SeedBuilder;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  public static class UrlMapper extends Mapper<LongWritable, Text, String, WebPage> {
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
      ).merge(seedBuiler.getParams()).withLogger(LOG).info();
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
      String reversedUrl = row.getTemporaryVariableAsString("reversedUrl");

      if (reversedUrl != null) {
        context.write(reversedUrl, row);
        context.getCounter(Nutch.STAT_RUNTIME_STATUS, NutchCounter.Counter.totalPages.name()).increment(1);
      }
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

    Params params = new Params(args);
    Configuration conf = getConf();

    String crawlId = params.get(ARG_CRAWL, conf.get(PARAM_CRAWL_ID));
    String seedDir = params.get(ARG_SEEDDIR);

    conf.set(PARAM_CRAWL_ID, crawlId);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "seedDir", seedDir,
        "jobStartTime", DateTimeUtil.format(startTime)
    ));
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    Params params = new Params(args);
    Configuration conf = getConf();

    String seedDir = params.get(ARG_SEEDDIR);
    Path input = new Path(seedDir);

    if (!FileSystem.get(conf).exists(input)) {
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

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName(),
        "seedDir", seedDir
    ));

    currentJob.waitForCompletion(true);
  }

  public void inject(Path path, String crawlId) throws Exception {
    run(Params.toArgMap(
        ARG_CRAWL, crawlId,
        ARG_SEEDDIR, path.toString()
    ));
  }

  public void inject(String urlDir, String crawlId) throws Exception {
    run(Params.toArgMap(
        ARG_SEEDDIR, urlDir,
        ARG_CRAWL, crawlId
    ));
  }

  private void printUsage() {
    System.err.println("Usage: InjectJob <url_dir> [-crawlId <id>]");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      return -1;
    }

    Configuration conf = getConf();

    String crawlId = conf.get(PARAM_CRAWL_ID, "");

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
