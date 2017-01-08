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
package org.apache.nutch.jobs.update;

import org.apache.commons.lang3.StringUtils;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.graph.GraphGroupKey;
import org.apache.nutch.graph.GraphGroupKey.GraphKeyComparator;
import org.apache.nutch.graph.GraphGroupKey.UrlOnlyComparator;
import org.apache.nutch.graph.GraphGroupKey.UrlOnlyPartitioner;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.common.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.ALL_BATCH_ID_STR;

public class InGraphUpdateJob extends WebGraphUpdateJob {

  public static final Logger LOG = LoggerFactory.getLogger(InGraphUpdateJob.class);

  private static final Collection<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.INLINKS);
    FIELDS.add(GoraWebPage.Field.MARKERS);
    FIELDS.add(GoraWebPage.Field.METADATA);
  }

  private String batchId = ALL_BATCH_ID_STR;

  public InGraphUpdateJob() {
  }

  @Override
  public Collection<GoraWebPage.Field> getFields(Job job) {
    ScoringFilters scoringFilters = new ScoringFilters(job.getConfiguration());
    HashSet<GoraWebPage.Field> fields = new HashSet<>(FIELDS);
    fields.addAll(scoringFilters.getFields());

    return fields;
  }

  @Override
  protected void doRun(Map<String, Object> args) throws Exception {
    // Partition by {url}, sort by {url,score} and group by {url}.
    // This ensures that the inlinks are sorted by score when they enter the reducer.
    currentJob.setPartitionerClass(UrlOnlyPartitioner.class);
    currentJob.setGroupingComparatorClass(UrlOnlyComparator.class);
    currentJob.setSortComparatorClass(GraphKeyComparator.class);

    Collection<GoraWebPage.Field> fields = getFields(currentJob);
    MapFieldValueFilter<String, GoraWebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(currentJob, fields, GraphGroupKey.class, WebGraphWritable.class, InGraphUpdateMapper.class, batchIdFilter);
    StorageUtils.initReducerJob(currentJob, InGraphUpdateReducer.class);

    DataStore<String, GoraWebPage> store = StorageUtils.createWebStore(getConf(), String.class, GoraWebPage.class);

    LOG.info("Loaded Fields : " + StringUtils.join(StorageUtils.toStringArray(fields), ", "));
    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "workingDir", currentJob.getWorkingDirectory(),
        "jobName", currentJob.getJobName(),
        "realSchema", store.getSchemaName()
    ));

    currentJob.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(ConfigUtils.create(), new InGraphUpdateJob(), args);
    System.exit(res);
  }
}
