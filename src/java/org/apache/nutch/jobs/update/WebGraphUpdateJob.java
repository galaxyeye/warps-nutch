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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.jobs.NutchJob;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import static org.apache.nutch.metadata.Nutch.*;

abstract class WebGraphUpdateJob extends NutchJob implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(WebGraphUpdateJob.class);

  private String batchId = ALL_BATCH_ID_STR;

  public abstract Collection<GoraWebPage.Field> getFields(Job job);

  @Override
  protected void setup(Map<String, Object> args) throws Exception {
    super.setup(args);

    Params params = new Params(args);
    Configuration conf = getConf();

    String crawlId = params.get(ARG_CRAWL, conf.get(PARAM_CRAWL_ID));

    batchId = params.get(ARG_BATCH, ALL_BATCH_ID_STR);

    int round = conf.getInt(PARAM_CRAWL_ROUND, 0);

    conf.set(PARAM_BATCH_ID, batchId);
    conf.set(PARAM_CRAWL_ID, crawlId);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "round", round,
        "crawlId", crawlId,
        "batchId", batchId
    ));
  }

  private int updateTable(String crawlId, String batchId) throws Exception {
    run(Params.toArgMap(ARG_CRAWL, crawlId, ARG_BATCH, batchId));
    return 0;
  }

  private void printUsage() {
    String usage = "Usage: OutGraphUpdateJob (<batchId> | -all) [-crawlId <id>] "
        + "    <batchId>     - crawl identifier returned by Generator, or -all for all \n \t \t    generated batchId-s\n"
        + "    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: persist.crawl.id)\n";

    System.err.println(usage);
  }

  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      printUsage();
      return -1;
    }

    Configuration conf = getConf();

    String batchId = args[0];
    if (!batchId.equals("-all") && batchId.startsWith("-")) {
      printUsage();
      return -1;
    }

    String crawlId = conf.get(PARAM_CRAWL_ID, "");

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
      } else if ("-batchId".equals(args[i])) {
        batchId = args[++i];
      } else {
        throw new IllegalArgumentException("arg " + args[i] + " not recognized");
      }
    }

    return updateTable(crawlId, batchId);
  }
}
