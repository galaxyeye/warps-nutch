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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.graph.*;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Nutch.PARAM_GENERATOR_MAX_DISTANCE;
import static org.apache.nutch.persist.Mark.FETCH;

public class DbUpdateMapper extends NutchMapper<String, GoraWebPage, GraphGroupKey, WebGraphWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateMapper.class);

  public enum Counter { rowsMapped, newRowsMapped, notFetched, urlFiltered, tooDeep }

  private Configuration conf;
  private Mapper.Context context;
  private NutchCounter counter;

  private int maxDistance;
  private int maxOutlinks = 1000;

  // reuse writables
//  private WebGraph graph;
//  private NutchWritable nutchWritable = new NutchWritable();
//  private WebPageWritable pageWritable;
  private ScoringFilters scoringFilters;
//  private final List<WebEdge> edgeData = new ArrayList<>();

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

    WebGraph graph = new WebGraph();
//    graph.reset();

    WebVertex v1 = new WebVertex(url, page, depth);
//    v1.setPublishTime(page.getPublishTime());
//    v1.addMetadata(PUBLISH_TIME, page.getPublishTime());
//    v1.addMetadata(TMP_CHARS, page.sniffTextLength());

    /* A loop in the graph */
    graph.addVerticesAndEdge(v1, v1);

    page.getOutlinks().entrySet().stream()
        .map(l -> new WebVertex(l.getKey().toString(), l.getValue().toString(), null, depth + 1))
        .forEach(v2 -> graph.addVerticesAndEdge(v1, v2));

    try {
      scoringFilters.distributeScoreToOutlinks(url, page, graph.edgesOf(v1), graph.outDegreeOf(v1));
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL: " + reversedUrl + StringUtils.stringifyException(e));
    }

//    GraphGroupKey key = new GraphGroupKey(reversedUrl, GraphMode.IN_LINK_GRAPH, page.getScore());
//    context.write(key, new WebGraphWritable(graph, conf));

//    GraphGroupKey key = new GraphGroupKey(reversedUrl, GraphMode.IN_LINK_GRAPH, Float.MAX_VALUE);
    // context.write(key, new WebVertexWritable(v1, conf));
    graph.edgesOf(v1).forEach(e -> writeSubGraph(e.getSource(), e.getTarget(), e.getScore(), GraphMode.IN_LINK_GRAPH));
//    graph.edgesOf(v1).forEach(edge -> writeEdge(edge, GraphMode.OUT_LINK_GRAPH));

//    key = new GraphGroupKey(reversedUrl, GraphMode.OUT_LINK_GRAPH, Float.MAX_VALUE);
//    context.write(key, new WebVertexWritable(v1, conf));
//    graph.edgesOf(v1).forEach(edge -> writeEdge(edge, GraphMode.OUT_LINK_GRAPH));

    counter.increase(Counter.rowsMapped);
    counter.increase(Counter.newRowsMapped, graph.outDegreeOf(v1));
  }

  private void writeSubGraph(WebVertex source, WebVertex target, double score, GraphMode graphMode) {
    WebGraph graph = new WebGraph();
    graph.addVerticesAndEdge(source, target);

    // partition by source url, so the reducer get a group of inlinks
    try {
      String reversedUrl;
      if (graphMode.isInLinkGraph()) {
        reversedUrl = TableUtil.reverseUrl(target.getUrl());
      }
      else {
        reversedUrl = TableUtil.reverseUrl(source.getUrl());
      }
      context.write(new GraphGroupKey(reversedUrl, graphMode, score), new WebGraphWritable(graph, conf));
    } catch (IOException|InterruptedException e) {
      LOG.error("Failed to write to hdfs. " + StringUtil.stringifyException(e));
    }
  }
}
