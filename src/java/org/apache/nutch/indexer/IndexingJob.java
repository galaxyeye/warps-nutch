/*
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
 */
package org.apache.nutch.indexer;

import org.apache.avro.util.Utf8;
import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.mapreduce.StringComparator;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.nutch.mapreduce.NutchJob;
import org.apache.nutch.mapreduce.NutchMapper;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.parse.ParserMapper;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class IndexingJob extends NutchJob implements Tool {

  public static Logger LOG = LoggerFactory.getLogger(IndexingJob.class);

  private static final Collection<WebPage.Field> FIELDS = new HashSet<>();

  private static final Utf8 REINDEX = new Utf8("-reindex");

  private String batchId = Nutch.ALL_BATCH_ID_STR;

  static {
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.PARSE_STATUS);
    FIELDS.add(WebPage.Field.SCORE);
    FIELDS.add(WebPage.Field.MARKERS);
  }

  public static class IndexerMapper extends NutchMapper<String, WebPage, String, NutchDocument> {

    public enum Counter { notUpdated, indexFailed };

    public IndexUtil indexUtil;
    public DataStore<String, WebPage> store;
    protected Utf8 batchId;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      Configuration conf = context.getConfiguration();
      getCounter().register(ParserMapper.Counter.class);

      batchId = new Utf8(conf.get(Nutch.GENERATOR_BATCH_ID, Nutch.ALL_BATCH_ID_STR));
      indexUtil = new IndexUtil(conf);
      try {
        store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      String crawlId = conf.get(Nutch.CRAWL_ID_KEY);

      LOG.info(StringUtil.formatParams(
          "className", this.getClass().getSimpleName(),
          "crawlId", crawlId,
          "batchId", batchId
      ));
    }

    @Override
    protected void cleanup(Context context) {
      super.cleanup(context);

      store.close();
    };

    @Override
    public void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
      ParseStatus pstatus = page.getParseStatus();
      if (pstatus == null || !ParseStatusUtils.isSuccess(pstatus)
          || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
        return; // filter urls not parsed
      }

      Utf8 mark = Mark.UPDATEDB_MARK.checkMark(page);
      if (mark == null) {
        getCounter().increase(Counter.notUpdated);
        return;
      }

      NutchDocument doc = indexUtil.index(key, page);
      if (doc == null) {
        getCounter().increase(Counter.indexFailed);
        return;
      }

      Mark.INDEX_MARK.putMark(page, Mark.UPDATEDB_MARK.checkMark(page));
      store.put(key, page);

      context.write(key, doc);

      context.getCounter("IndexerJob", "DocumentCount").increment(1);

      getCounter().updateAffectedRows(doc.getUrl());
    }
  }

  private static Collection<WebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<WebPage.Field> columns = new HashSet<WebPage.Field>(FIELDS);
    IndexingFilters filters = new IndexingFilters(conf);
    columns.addAll(filters.getFields());
    ScoringFilters scoringFilters = new ScoringFilters(conf);
    columns.addAll(scoringFilters.getFields());
    return columns;
  }

  @Override
  public void setup(Map<String, Object> args) {
    batchId = NutchUtil.get(args, Nutch.ARG_BATCH, Nutch.ALL_BATCH_ID_STR);
    getConf().set(Nutch.GENERATOR_BATCH_ID, batchId);
  }

  @Override
  public void doRun(Map<String, Object> args) throws Exception {
    // TODO: Figure out why this needs to be here
    currentJob.getConfiguration().setClass("mapred.output.key.comparator.class",
        StringComparator.class, RawComparator.class);

    Collection<WebPage.Field> fields = getFields(currentJob);
    MapFieldValueFilter<String, WebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(currentJob, fields, String.class, NutchDocument.class, IndexerMapper.class, batchIdFilter);

    // The data is write to a network sink using IndexerOutputFormat.write
    currentJob.setOutputFormatClass(IndexerOutputFormat.class);

    // there is no reduce phase, everythine has been done in mapper, so set reduce tasks to be 0
    currentJob.setNumReduceTasks(0);

    currentJob.waitForCompletion(true);
  }

  @Override
  protected MapFieldValueFilter<String, WebPage> getBatchIdFilter(String batchId) {
    if (batchId.equals(REINDEX.toString()) || batchId.equals(Nutch.ALL_CRAWL_ID.toString())) {
      return null;
    }

    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<String, WebPage>();
    filter.setFieldName(WebPage.Field.MARKERS.toString());
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.setMapKey(Mark.UPDATEDB_MARK.getName());
    filter.getOperands().add(new Utf8(batchId));

    return filter;
  }

  public void index(String batchId) throws Exception {
    LOG.info("IndexingJob : starting");

    run(StringUtil.toArgMap(Nutch.ARG_BATCH, batchId));
    // NOW PASSED ON THE COMMAND LINE AS A HADOOP PARAM
    // do the commits once and for all the reducers in one go
    // getConf().set(SolrConstants.SERVER_URL,solrUrl);

    Configuration conf = getConf();

    IndexWriters writers = new IndexWriters(conf);
    LOG.info(writers.describe());

    writers.open(conf);
    if (conf.getBoolean(SolrConstants.COMMIT_INDEX, true)) {
      writers.commit();
    }

    LOG.info("IndexingJob: done.");
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err
          .println("Usage: IndexingJob (<batchId> | -all | -reindex) [-crawlId <id>]");
      return -1;
    }

    if (args.length == 3 && "-crawlId".equals(args[1])) {
      getConf().set(Nutch.CRAWL_ID_KEY, args[2]);
    }

    try {
      index(args[0]);
      return 0;
    } catch (final Exception e) {
      LOG.error("SolrIndexerJob: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(), new IndexingJob(), args);
    System.exit(res);
  }
}
