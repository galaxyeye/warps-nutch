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
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class OutGraphUpdateCombiner extends Reducer<GraphGroupKey, WebGraphWritable, GraphGroupKey, WebGraphWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(OutGraphUpdateCombiner.class);

  private Configuration conf;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    conf = context.getConfiguration();

    Params.of(
        "className", this.getClass().getSimpleName()
    ).withLogger(LOG).info();
  }

  @Override
  protected void reduce(GraphGroupKey key, Iterable<WebGraphWritable> subGraphs, Context context) {
    try {
      WebGraph graph = new WebGraph();

      for (WebGraphWritable graphWritable : subGraphs) {
        WebGraph subGraph = graphWritable.get();
        WebEdge edge = subGraph.firstEdge();
        graph.addEdgeLenient(subGraph.getEdgeSource(edge), subGraph.getEdgeTarget(edge), subGraph.getEdgeWeight(edge));
      }

      graph.edgeSet().forEach(edge -> writeAsSubGraph(edge, graph, context));
    } catch (Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
  }

  /**
   * The following graph shows the in-link graph. Every reduce group contains a center vertex and a batch of edges.
   * The center vertex has a web page inside, and the edges has in-link information.
   *
   *        v1
   *        |
   *        v
   * v2 -> vc <- v3
   *       ^ ^
   *      /  \
   *     v4  v5
   * */
  private void writeAsSubGraph(WebEdge edge, WebGraph graph, Context context) {
    try {
      WebGraph subgraph = WebGraph.of(edge, graph);

      String reverseUrl = TableUtil.reverseUrl(edge.getTargetUrl());
      // noinspection unchecked
      context.write(new GraphGroupKey(reverseUrl, graph.getEdgeWeight(edge)), new WebGraphWritable(subgraph, conf));
    } catch (IOException|InterruptedException e) {
      LOG.error("Failed to write to hdfs. " + StringUtil.stringifyException(e));
    }
  }
}
