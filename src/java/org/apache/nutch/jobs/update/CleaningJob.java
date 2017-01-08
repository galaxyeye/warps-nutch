package org.apache.nutch.jobs.update;

/**
 * Created by vincent on 16-10-21.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */

import org.apache.gora.mapreduce.StringComparator;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.indexer.IndexCleaningFilters;
import org.apache.nutch.indexer.IndexWriters;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.jobs.NutchJob;
import org.apache.nutch.jobs.NutchMapper;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.common.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class CleaningJob extends NutchJob implements Tool {

  public static final String ARG_COMMIT = "commit";
  public static final Logger LOG = LoggerFactory.getLogger(CleaningJob.class);
  private Configuration conf;

  private static final Collection<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.STATUS);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Collection<GoraWebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<GoraWebPage.Field> columns = new HashSet<>(FIELDS);
    IndexCleaningFilters filters = new IndexCleaningFilters(conf);
    columns.addAll(filters.getFields());
    return columns;
  }

  public static class CleanMapper extends NutchMapper<String, GoraWebPage, String, GoraWebPage> {

    private IndexCleaningFilters filters;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      Configuration conf = context.getConfiguration();
      filters = new IndexCleaningFilters(conf);
    }

    @Override
    public void map(String key, GoraWebPage page, Context context) throws IOException, InterruptedException {
      try {
        if (page.getStatus() == CrawlStatus.STATUS_GONE || filters.remove(key, WebPage.wrap(page))) {
          context.write(key, page);
        }
      } catch (IndexingException e) {
        LOG.warn("Error indexing " + key + ": " + e);
      }
    }
  }

  public static class CleanReducer extends Reducer<String, GoraWebPage, NullWritable, NullWritable> {
    private int numDeletes = 0;
    private boolean commit;
    IndexWriters writers = null;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      writers = new IndexWriters(conf);
      writers.open(conf);
      commit = conf.getBoolean(ARG_COMMIT, false);
    }

    public void reduce(String key, Iterable<GoraWebPage> values, Context context) throws IOException {
      writers.delete(key);
      numDeletes++;
      context.getCounter("SolrClean", "DELETED").increment(1);
    }

    @Override
    public void cleanup(Context context) throws IOException {
      writers.close();
      if (numDeletes > 0 && commit) {
        writers.commit();
      }
      LOG.info("CleaningJob: deleted a total of " + numDeletes + " documents");
    }
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    getConf().setBoolean(ARG_COMMIT, (Boolean) args.get(ARG_COMMIT));
    currentJob.getConfiguration().setClass(
        "mapred.output.key.comparator.class", StringComparator.class,
        RawComparator.class);

    Collection<GoraWebPage.Field> fields = getFields(currentJob);
    StorageUtils.initMapperJob(currentJob, fields, String.class, GoraWebPage.class, CleanMapper.class);
    currentJob.setReducerClass(CleanReducer.class);
    currentJob.setOutputFormatClass(NullOutputFormat.class);

    DataStore<String, GoraWebPage> store = StorageUtils.createWebStore(getConf(), String.class, GoraWebPage.class);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  public int delete(boolean commit) throws Exception {
    run(Params.toArgMap(ARG_COMMIT, commit));
    return 0;
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: CleaningJob [-crawlId <id>] [-noCommit]");
      return 1;
    }

    boolean commit = true;
    if (args.length == 3 && args[2].equals("-noCommit")) {
      commit = false;
    }
    if (args.length == 3 && "-crawlId".equals(args[0])) {
      getConf().set(Nutch.PARAM_CRAWL_ID, args[1]);
    }

    return delete(commit);
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(ConfigUtils.create(), new CleaningJob(), args);
    System.exit(result);
  }
}
