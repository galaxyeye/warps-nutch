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
package org.apache.nutch.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.jobs.db.WebTableReader;
import org.apache.nutch.jobs.fetch.FetchJob;
import org.apache.nutch.jobs.generate.GenerateJob;
import org.apache.nutch.jobs.inject.InjectJob;
import org.apache.nutch.jobs.parse.ParserJob;
import org.apache.nutch.jobs.update.OutGraphUpdateJob;
import org.apache.nutch.fetch.FetchMode;
import org.apache.nutch.util.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.GENERATE_COUNT_VALUE_HOST;
import static org.apache.nutch.metadata.Nutch.PARAM_GENERATOR_COUNT_MODE;
import static org.apache.nutch.metadata.Nutch.PARAM_GENERATOR_MAX_TASKS_PER_HOST;

public class Benchmark extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

  public static void main(String[] args) throws Exception {
    Configuration conf = ConfigUtils.create();
    int res = ToolRunner.run(conf, new Benchmark(), args);
    System.exit(res);
  }

  private void createSeeds(FileSystem fs, Path seedsDir, int count)
      throws Exception {
    OutputStream os = fs.create(new Path(seedsDir, "configsets/default/seeds"));
    for (int i = 0; i < count; i++) {
      String url = "http://www.test-" + i + ".com/\r\n";
      os.write(url.getBytes());
    }
    os.flush();
    os.close();
  }

  public static final class BenchmarkResults {
    Map<String, Map<String, Long>> timings = new HashMap<String, Map<String, Long>>();
    List<String> runs = new ArrayList<String>();
    List<String> stages = new ArrayList<String>();
    int seeds, depth, threads;
    long topN;
    long elapsed;
    String plugins;

    public void addTiming(String stage, String run, long timing) {
      if (!runs.contains(run)) {
        runs.add(run);
      }
      if (!stages.contains(stage)) {
        stages.add(stage);
      }
      Map<String, Long> t = timings.get(stage);
      if (t == null) {
        t = new HashMap<String, Long>();
        timings.put(stage, t);
      }
      t.put(run, timing);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("* Plugins:\t" + plugins + "\n");
      sb.append("* Seeds:\t" + seeds + "\n");
      sb.append("* Depth:\t" + depth + "\n");
      sb.append("* Threads:\t" + threads + "\n");
      sb.append("* TopN:\t" + topN + "\n");
      sb.append("* TOTAL ELAPSED:\t" + elapsed + "\n");
      for (String stage : stages) {
        Map<String, Long> timing = timings.get(stage);
        if (timing == null)
          continue;
        sb.append("- stage: " + stage + "\n");
        for (String r : runs) {
          Long Time = timing.get(r);
          if (Time == null) {
            continue;
          }
          sb.append("\trun " + r + "\t" + Time + "\n");
        }
      }
      return sb.toString();
    }

    public List<String> getStages() {
      return stages;
    }

    public List<String> getRuns() {
      return runs;
    }
  }

  public int run(String[] args) throws Exception {
    String plugins = "protocol-http|parse-tika|scoring-opic|urlfilter-regex|urlnormalizer-pass";
    int seeds = 1;
    int depth = 10;
    int threads = 10;
    // boolean delete = true;
    long topN = Long.MAX_VALUE;

    if (args.length == 0) {
      System.err
          .println("Usage: Benchmark [-crawlId <id>] [-seeds NN] [-depth NN] [-threads NN] [-maxPerHost NN] [-plugins <regex>]");
      System.err
          .println("\t-crawlId id\t the id to prefix the schemas to operate on, (default: storage.crawl.id)");
      System.err
          .println("\t-seeds NN\tcreate NN unique hosts in a seed list (default: 1)");
      System.err.println("\t-depth NN\tperform NN crawl cycles (default: 10)");
      System.err
          .println("\t-threads NN\tuse NN threads per SimpleFetcher task (default: 10)");
      // XXX what is the equivalent here? not an additional job...
      // System.err.println("\t-keep\tkeep batchId data (default: delete after updatedb)");
      System.err.println("\t-plugins <regex>\toverride 'plugin.includes'.");
      System.err.println("\tNOTE: if not specified, this is reset to: "
          + plugins);
      System.err
          .println("\tNOTE: if 'default' is specified then a value set in nutch-default/nutch-site is used.");
      System.err
          .println("\t-maxPerHost NN\tmax. # of URLs per host in a fetchlist");
      return -1;
    }
    int maxPerHost = Integer.MAX_VALUE;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-crawlId")) {
        getConf().set(Nutch.PARAM_CRAWL_ID, args[++i]);
      } else if (args[i].equals("-seeds")) {
        seeds = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-threads")) {
        threads = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-depth")) {
        depth = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-plugins")) {
        plugins = args[++i];
      } else if (args[i].equalsIgnoreCase("-maxPerHost")) {
        maxPerHost = Integer.parseInt(args[++i]);
      } else {
        LOG.error("Invalid argument: '" + args[i] + "'");
        return -1;
      }
    }
    BenchmarkResults res = benchmark(seeds, depth, threads, maxPerHost, topN,
        plugins);
    System.out.println(res);
    return 0;
  }

  private void initConf(int maxPerHost, String plugins) {
    Configuration conf = getConf();
    conf.set("http.proxy.host", "localhost");
    conf.setInt("http.proxy.port", 8181);
    conf.set("http.agent.name", "test");
    conf.set("http.robots.agents", "test,*");

    if (!plugins.equals("default")) {
      conf.set("plugin.includes", plugins);
    }

    conf.setInt(PARAM_GENERATOR_MAX_TASKS_PER_HOST, maxPerHost);
    conf.set(PARAM_GENERATOR_COUNT_MODE, GENERATE_COUNT_VALUE_HOST);
  }

  public BenchmarkResults benchmark(int seeds, int depth, int threads,
      int maxPerHost, long topN, String plugins) throws Exception {
    initConf(maxPerHost, plugins);

    Job job = Job.getInstance(getConf());

    Configuration conf = job.getConfiguration();
    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID, "test");

    FileSystem fs = FileSystem.get(conf);
    Path dir = new Path(getConf().get("hadoop.tmp.dir"), "bench-"
        + System.currentTimeMillis());
    fs.mkdirs(dir);
    Path rootUrlDir = new Path(dir, "seed");
    fs.mkdirs(rootUrlDir);
    createSeeds(fs, rootUrlDir, seeds);

    if (LOG.isInfoEnabled()) {
      LOG.info("crawl started in: " + dir);
      LOG.info("rootUrlDir = " + rootUrlDir);
      LOG.info("threads = " + threads);
      LOG.info("depth = " + depth);
    }

    BenchmarkResults res = new BenchmarkResults();
    res.depth = depth;
    res.plugins = plugins;
    res.seeds = seeds;
    res.threads = threads;
    res.topN = topN;

    res.elapsed = System.currentTimeMillis();
    InjectJob injector = new InjectJob(conf);
    GenerateJob generator = new GenerateJob(conf);
    FetchJob fetcher = new FetchJob(conf);
    ParserJob parseBatch = new ParserJob(conf);
    OutGraphUpdateJob crawlDbTool = new OutGraphUpdateJob(conf);
    // not needed in the new API
    // LinkDb linkDbTool = new LinkDb(getConf());

    long start = System.currentTimeMillis();
    // initialize crawlDb
    injector.inject(rootUrlDir, crawlId);
    long delta = System.currentTimeMillis() - start;
    res.addTiming("inject", "0", delta);
    int i;
    for (i = 0; i < depth; i++) { // generate new batch
      start = System.currentTimeMillis();
      String batchId = generator.generate(topN, crawlId, NutchUtil.generateBatchId(), false, System.currentTimeMillis(), false, false);
      delta = System.currentTimeMillis() - start;
      res.addTiming("generate", i + "", delta);
      if (batchId == null) {
        LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
        break;
      }
      boolean isParsing = getConf().getBoolean("fetcher.parse", false);
      start = System.currentTimeMillis();

      fetcher.fetch("", FetchMode.NATIVE.name(), batchId, threads, false, -1, 1); // fetch it
      delta = System.currentTimeMillis() - start;
      res.addTiming("fetch", i + "", delta);
      if (!isParsing) {
        start = System.currentTimeMillis();
        parseBatch.parse("", batchId, -1, false, false); // parse it, if needed
        delta = System.currentTimeMillis() - start;
        res.addTiming("parse", i + "", delta);
      }
      start = System.currentTimeMillis();
      crawlDbTool.run(new String[0]); // update crawldb
      delta = System.currentTimeMillis() - start;
      res.addTiming("update", i + "", delta);
    }

    if (i == 0) {
      LOG.warn("No URLs to fetch - check your seed list and URL filters.");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("crawl finished: " + dir);
    }
    res.elapsed = System.currentTimeMillis() - res.elapsed;
    WebTableReader dbreader = new WebTableReader();
    dbreader.setConf(conf);
    dbreader.processStatJob(false);
    return res;
  }

}
