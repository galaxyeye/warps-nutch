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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.persist.graph.*;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Metadata.Name.PUBLISH_TIME;
import static org.apache.nutch.metadata.Metadata.Name.TMP_CHARS;
import static org.apache.nutch.metadata.Nutch.PARAM_GENERATOR_MAX_DISTANCE;
import static org.apache.nutch.persist.Mark.FETCH;

public class DbUpdateMapper extends NutchMapper<String, GoraWebPage, GraphGroupKey, Writable> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateMapper.class);

  public enum Counter { rowsMapped, newRowsMapped, notFetched, urlFiltered, tooDeep }

  private Configuration conf;
  private Mapper.Context context;
  private NutchCounter counter;

  private int maxDistance;
  private int maxOutlinks = 1000;

  // reuse writables
//  private Graph graph;
//  private NutchWritable nutchWritable = new NutchWritable();
//  private WebPageWritable pageWritable;
  private ScoringFilters scoringFilters;
//  private final List<Edge> edgeData = new ArrayList<>();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    conf = context.getConfiguration();
    this.context = context;

    counter = getCounter();
    counter.register(Counter.class);

//    pageWritable = new WebPageWritable(context.getConfiguration(), null);

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);

    maxDistance = conf.getInt(PARAM_GENERATOR_MAX_DISTANCE, -1);
    scoringFilters = new ScoringFilters(conf);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "maxDistance", maxDistance,
        "maxOutlinks", maxOutlinks
    ));
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
   *
   * And this is a out-link graph:
   *        v1
   *        ^
   *        |
   * v2 <- vc -> v3
   *       / \
   *      v  v
   *     v4  v5
   * */
  @Override
  public void map(String reversedUrl, GoraWebPage row, Context context) throws IOException, InterruptedException {
    counter.increase(rows);

    WebPage page = WebPage.wrap(row);

    if (!page.hasMark(FETCH)) {
      counter.increase(Counter.notFetched);
      return;
    }

    String url = TableUtil.unreverseUrl(reversedUrl);

    final int depth = page.getDepth();
    if (depth >= maxDistance) {
      counter.increase(Counter.tooDeep);
      return;
    }

    Graph graph = new Graph(conf);
//    graph.reset();

    Vertex v1 = new Vertex(url, "", page, depth, conf);
    v1.addMetadata(PUBLISH_TIME, page.getPublishTime());
    v1.addMetadata(TMP_CHARS, page.sniffTextLength());

    /* Self-connected edge  */
    graph.addEdge(new Edge(v1, v1, 0.0f));

    page.getOutlinks().entrySet().stream()
        .map(e -> new Vertex(e.getKey().toString(), e.getValue().toString(), null, depth + 1, conf))
        .forEach(v2 -> graph.addEdge(new Edge(v1, v2, 0.0f)));

    try {
      scoringFilters.distributeScoreToOutlinks(url, page, graph.getEdges(), graph.edgeCount());
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL: " + reversedUrl + StringUtils.stringifyException(e));
    }

    GraphGroupKey key = new GraphGroupKey(reversedUrl, GraphMode.IN_LINK_GRAPH, Float.MAX_VALUE);
    context.write(key, v1);
    graph.getEdges().forEach(edge -> writeEdge(edge, GraphMode.IN_LINK_GRAPH));

    key = new GraphGroupKey(reversedUrl, GraphMode.OUT_LINK_GRAPH, Float.MAX_VALUE);
    context.write(key, v1);
    graph.getEdges().forEach(edge -> writeEdge(edge, GraphMode.OUT_LINK_GRAPH));

    counter.increase(Counter.rowsMapped);
    counter.increase(Counter.newRowsMapped, graph.edgeCount());
  }

  private void writeEdge(Edge edge, GraphMode graphMode) {
    // partition by source url, so the reducer get a group of inlinks
    try {
      String centerVertexUrl;
      if (graphMode.isInLinkGraph()) {
        centerVertexUrl = TableUtil.reverseUrl(edge.getV2().getUrl());
      }
      else {
        centerVertexUrl = TableUtil.reverseUrl(edge.getV1().getUrl());
      }
      context.write(new GraphGroupKey(centerVertexUrl, graphMode, edge.getScore()), edge);
    } catch (IOException|InterruptedException e) {
      LOG.error("Failed to write to hdfs. " + StringUtil.stringifyException(e));
    }
  }
}
