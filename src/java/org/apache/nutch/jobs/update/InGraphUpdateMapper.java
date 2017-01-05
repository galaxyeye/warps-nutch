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
import org.apache.nutch.graph.GraphGroupKey;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.WebVertex;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.jobs.NutchCounter;
import org.apache.nutch.jobs.NutchMapper;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.nutch.jobs.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Nutch.PARAM_CRAWL_ID;
import static org.apache.nutch.persist.Mark.UPDATEOUTG;

class InGraphUpdateMapper extends NutchMapper<String, GoraWebPage, GraphGroupKey, WebGraphWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(InGraphUpdateMapper.class);

  public enum Counter { rowsMapped, newRowsMapped, noInLinks, notUpdated, urlFiltered, tooDeep }

  private Configuration conf;
  private Context context;
  private NutchCounter counter;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    conf = context.getConfiguration();
    this.context = context;

    counter = getCounter();
    counter.register(Counter.class);

    String crawlId = conf.get(PARAM_CRAWL_ID);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId
    ));
  }

  @Override
  public void map(String reversedUrl, GoraWebPage row, Context context) throws IOException, InterruptedException {
    counter.increase(rows);

    WebPage page = WebPage.wrap(row);

    if (!page.hasMark(UPDATEOUTG)) {
      counter.increase(Counter.notUpdated);
      return;
    }

    if (page.getInlinks().isEmpty()) {
      counter.increase(Counter.noInLinks);
      return;
    }

    String url = TableUtil.unreverseUrl(reversedUrl);
    WebGraph graph = new WebGraph();
    WebVertex v2 = new WebVertex(url, page);

    /* A loop in the graph */
    graph.addEdgeLenient(v2, v2, Float.MAX_VALUE / 2);

    page.getInlinks().entrySet().forEach(l -> graph.addEdgeLenient(new WebVertex(l.getKey()), v2));
    graph.incomingEdgesOf(v2).forEach(edge -> writeAsSubGraph(edge, graph));

    counter.increase(Counter.rowsMapped);
    counter.increase(Counter.newRowsMapped, graph.inDegreeOf(v2));
  }

  /**
   * The following graph shows the in-link graph. Every reduce group contains a center vertex and a batch of edges.
   * The center vertex has a web page inside, and the edges has in-link information.
   *
   * <pre>
   *       v1
   *       ^
   *       |
   * v2 <- vc -> v3
   *      / \
   *     v  v
   *    v4  v5
   *</pre>
   * */
  private void writeAsSubGraph(WebEdge edge, WebGraph graph) {
    try {
      WebGraph subGraph = WebGraph.of(edge, graph);
      String reverseUrl = TableUtil.reverseUrl(edge.getSourceUrl());
      // noinspection unchecked
      context.write(new GraphGroupKey(reverseUrl, graph.getEdgeWeight(edge)), new WebGraphWritable(subGraph, conf));
    } catch (IOException|InterruptedException e) {
      LOG.error("Failed to write to hdfs. " + StringUtil.stringifyException(e));
    }
  }
}
