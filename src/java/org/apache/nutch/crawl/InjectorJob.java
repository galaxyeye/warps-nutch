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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.avro.util.Utf8;
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
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes a flat file of URLs and adds them to the of pages to be
 * crawled. Useful for bootstrapping the system. The URL files contain one URL
 * per line, optionally followed by custom metadata separated by tabs with the
 * metadata key separated from the corresponding value by '='. <br>
 * Note that some metadata keys are reserved : <br>
 * - <i>nutch.score</i> : allows to set a custom score for a specific URL <br>
 * - <i>nutch.fetchInterval</i> : allows to set a custom fetch interval for a
 * specific URL <br>
 * e.g. http://www.nutch.org/ \t nutch.score=10 \t nutch.fetchInterval=2592000
 * \t userType=open_source
 **/
public class InjectorJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(InjectorJob.class);

  private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private static final Utf8 YES_STRING = new Utf8("y");

  static {
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.STATUS);
  }

  /** metadata key reserved for setting a custom score for a specific URL */
  public static String nutchScoreMDName = "nutch.score";
  /**
   * metadata key reserved for setting a custom fetchInterval for a specific URL
   */
  public static String nutchFetchIntervalMDName = "nutch.fetchInterval";

  public static class UrlMapper extends Mapper<LongWritable, Text, String, WebPage> {
    private int interval;
    private float scoreInjected;
    private ScoringFilters scfilters;
    private long curTime;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      interval = context.getConfiguration().getInt("db.fetch.interval.default", 2592000);
      scfilters = new ScoringFilters(context.getConfiguration());
      scoreInjected = context.getConfiguration().getFloat("db.score.injected", Float.MAX_VALUE);
      curTime = context.getConfiguration().getLong("injector.current.time", System.currentTimeMillis());
    }

    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
      String url = line.toString().trim();

//      LOG.debug("Inject : " + url);

      /* Ignore line that start with # */
      if (url.length() == 0 || url.startsWith("#")) {
        return;
      }

      // if tabs : metadata that could be stored
      // must be name=value and separated by \t
      float customScore = -1f;
      int customInterval = interval;
      Map<String, String> metadata = new TreeMap<String, String>();
      if (url.indexOf("\t") != -1) {
        String[] splits = url.split("\t");
        url = splits[0];
        for (int s = 1; s < splits.length; s++) {
          // find separation between name and value
          int indexEquals = splits[s].indexOf("=");
          if (indexEquals == -1) {
            // skip anything without a =
            continue;
          }

          String metaname = splits[s].substring(0, indexEquals);
          String metavalue = splits[s].substring(indexEquals + 1);
          if (metaname.equals(nutchScoreMDName)) {
            try {
              customScore = Float.parseFloat(metavalue);
            } catch (NumberFormatException nfe) {
            }
          } else if (metaname.equals(nutchFetchIntervalMDName)) {
            try {
              customInterval = Integer.parseInt(metavalue);
            } catch (NumberFormatException nfe) {
            }
          } else {
            metadata.put(metaname, metavalue);
          }
        }
      } // if

      String reversedUrl = TableUtil.reverseUrl(url); // collect it
      WebPage row = WebPage.newBuilder().build();
      row.setFetchTime(curTime);
      row.setFetchInterval(customInterval);

      // now add the metadata
      Iterator<String> keysIter = metadata.keySet().iterator();
      while (keysIter.hasNext()) {
        String keymd = keysIter.next();
        String valuemd = metadata.get(keymd);
        row.getMetadata().put(new Utf8(keymd), ByteBuffer.wrap(valuemd.getBytes()));
      }

      if (customScore != -1) {
        row.setScore(customScore);
      }
      else {
        row.setScore(scoreInjected);
      }

      try {
        scfilters.injectedScore(url, row);
      } catch (ScoringFilterException e) {
        LOG.warn("Cannot filter injected score for url " + url + ", using default (" + e.getMessage() + ")");
      }

      row.getMarkers().put(Nutch.DISTANCE, new Utf8(String.valueOf(0)));
      Mark.INJECT_MARK.putMark(row, YES_STRING);

      context.write(reversedUrl, row);

      context.getCounter(Nutch.COUNTER_GROUP_STATUS, NutchCounter.Counter.totalPages.name()).increment(1);

      LOG.debug("Injected : " + url);
    }
  }

  public InjectorJob() {
  }

  public InjectorJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    getConf().setLong("injector.current.time", startTime);    
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    Path input;
    Object path = args.get(Nutch.ARG_SEEDDIR);
    if (path instanceof Path) {
      input = (Path) path;
    } else {
      input = new Path(path.toString());
    }

    LOG.info("Seed dir : " + input.toString());

    FileInputFormat.addInputPath(currentJob, input);
    currentJob.setMapperClass(UrlMapper.class);
    currentJob.setMapOutputKeyClass(String.class);
    currentJob.setMapOutputValueClass(WebPage.class);
    currentJob.setOutputFormatClass(GoraOutputFormat.class);

    DataStore<String, WebPage> store = StorageUtils.createWebStore(
        currentJob.getConfiguration(), String.class, WebPage.class);
    GoraOutputFormat.setOutput(currentJob, store, true);

    currentJob.setReducerClass(Reducer.class);
    currentJob.setNumReduceTasks(0);

    // NUTCH-1471 Make explicit which datastore class we use
//    Object dataStoreClass = StorageUtils.getDataStoreClass(getConf());
//    Object dataStoreClass2 = StorageUtils.getDataStoreClass(currentJob.getConfiguration());
//    LOG.debug("dataStoreClass : " + dataStoreClass + ", " + dataStoreClass2);
    LOG.info("schemaName : " + store.getSchemaName());

    currentJob.waitForCompletion(true);
  }

  public void inject(Path urlDir) throws Exception {
    LOG.info("InjectorJob: Injecting urlDir: " + urlDir);
    run(NutchUtil.toArgMap(Nutch.ARG_SEEDDIR, urlDir));
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: InjectorJob <url_dir> [-crawlId <id>]");
      return -1;
    }

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[i + 1]);
        i++;
      } else {
        System.err.println("Unrecognized arg " + args[i]);
        return -1;
      }
    }

    try {
      inject(new Path(args[0]));
      return 0;
    } catch (Exception e) {
      LOG.error("InjectorJob: " + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    LOG.info("---------------------------------------------------\n\n");
    int res = ToolRunner.run(NutchConfiguration.create(), new InjectorJob(), args);
    System.exit(res);
  }
}
