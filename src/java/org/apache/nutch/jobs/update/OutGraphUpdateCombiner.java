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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.graph.GraphGroupKey;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.common.Params;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.nutch.graph.io.WebGraphWritable.OptimizeMode.IGNORE_TARGET;

class OutGraphUpdateCombiner extends Reducer<GraphGroupKey, WebGraphWritable, GraphGroupKey, WebGraphWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(OutGraphUpdateCombiner.class);

  private Configuration conf;
  private WebGraphWritable webGraphWritable;
  private int edgeCountBeforeCombine = 0;
  private int edgeCountAfterCombine = 0;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    conf = context.getConfiguration();
    webGraphWritable = new WebGraphWritable(null, IGNORE_TARGET, conf);

    Params.of(
        "className", this.getClass().getSimpleName()
    ).withLogger(LOG).info();
  }

  @Override
  protected void reduce(GraphGroupKey key, Iterable<WebGraphWritable> subGraphs, Context context) {
    WebGraph graph = new WebGraph();

    try {

      for (WebGraphWritable graphWritable : subGraphs) {
        edgeCountBeforeCombine += graphWritable.get().edgeSet().size();
        graph.combine(graphWritable.get());
      }

      edgeCountAfterCombine += graph.edgeSet().size();
      context.write(key, webGraphWritable.reset(graph));
    } catch (Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
  }

  @Override
  protected void cleanup(Context context) {
    LOG.info("Edge count in combiner : " + edgeCountBeforeCombine + " -> " + edgeCountAfterCombine);
  }
}
