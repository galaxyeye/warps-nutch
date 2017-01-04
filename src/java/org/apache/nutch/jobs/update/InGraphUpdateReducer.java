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

import org.apache.gora.store.DataStore;
import org.apache.nutch.graph.GraphGroupKey;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.WebVertex;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.jobs.NutchReducer;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.nutch.jobs.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Metadata.Name.GENERATE_TIME;
import static org.apache.nutch.metadata.Metadata.Name.REDIRECT_DISCOVERED;
import static org.apache.nutch.metadata.Nutch.VAR_PAGE_EXISTENCE;
import static org.apache.nutch.persist.Mark.*;

class InGraphUpdateReducer extends NutchReducer<GraphGroupKey, WebGraphWritable, String, GoraWebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(InGraphUpdateReducer.class);

  public enum Counter { existInPages, pagesPeresist }
  private DataStore<String, GoraWebPage> datastore;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);

    try {
      datastore = StorageUtils.createWebStore(conf, String.class, GoraWebPage.class);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    Params.of(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId
    ).withLogger(LOG).info();
  }

  @Override
  protected void reduce(GraphGroupKey key, Iterable<WebGraphWritable> values, Context context) {
    try {
      doReduce(key, values, context);
    } catch (Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
  }

  private void doReduce(GraphGroupKey key, Iterable<WebGraphWritable> subGraphs, Context context)
      throws IOException, InterruptedException {
    getCounter().increase(rows);

    String reversedUrl = key.getReversedUrl();

    WebGraph graph = buildGraph(reversedUrl, subGraphs);
    WebPage page = graph.getFocus().getWebPage();

    if (page == null) {
      return;
    }

    updateGraph(graph);
    updateMetadata(page);

    context.write(reversedUrl, page.get());

    getCounter().increase(Counter.pagesPeresist);
    getCounter().updateAffectedRows(TableUtil.unreverseUrl(reversedUrl));
  }

  /**
   * The graph should be like this:
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
  private WebGraph buildGraph(String reversedUrl, Iterable<WebGraphWritable> subGraphs) {
    WebGraph graph = new WebGraph();

    WebPage page = null;
    for (WebGraphWritable graphWritable : subGraphs) {
      WebGraph subGraph = graphWritable.get();
      WebEdge edge = subGraph.firstEdge();
      if (edge.isLoop()) {
        page = edge.getTargetWebPage();
      }
      graph.addEdgeLenient(edge.getSource(), edge.getTarget(), subGraph.getEdgeWeight(edge));
    }

    if (page == null) {
      WebPage loadedPage = WebPage.wrap(datastore.get(reversedUrl));

      // Page is already in the db
      if (!loadedPage.isEmpty()) {
        page = loadedPage;
        getCounter().increase(Counter.existInPages);
      }
    }

    WebVertex focus = graph.firstEdge().getSource();
    focus.setWebPage(page);
    graph.setFocus(focus);

    return graph;
  }

  private void updateGraph(WebGraph graph) {
    WebVertex focus = graph.getFocus();
    WebPage page = focus.getWebPage();

    for (WebEdge outgoingEdge : graph.outgoingEdgesOf(focus)) {
      if (outgoingEdge.isLoop()) {
        continue;
      }

      WebPage outgoingPage = graph.getEdgeTarget(outgoingEdge).getWebPage();

      Params.of(
          "reversedUrl", TableUtil.reverseUrlOrEmpty(outgoingEdge.getSourceUrl()),
          "edge", outgoingEdge.getSourceUrl() + " -> " + outgoingEdge.getTargetUrl(),
          "baseUrl", outgoingPage.getBaseUrl() + " -> " + page.getBaseUrl(),
          "score", graph.getEdgeWeight(outgoingEdge),
          "publishTime", outgoingPage.getRefPublishTime() + " -> " + page.getPublishTime(),
          "pageCategory", outgoingPage.getPageCategory(),
          "likelihood", outgoingPage.getPageCategoryLikelihood()
      ).withLogger(LOG).info(true);

      /* Update out-link page */
      if (outgoingPage.isDetailPage(0.80f)) {
        page.updateRefPublishTime(outgoingPage.getPublishTime());
        page.increaseRefChars(outgoingPage.getTextContentLength());
        page.increaseRefArticles(1);
      }
    }
  }

  private void updateMetadata(WebPage page) {
    // Clear temporary metadata
    page.clearMetadata(REDIRECT_DISCOVERED);
    page.clearMetadata(GENERATE_TIME);

    page.putMarkIfNonNull(UPDATEING, page.getMark(UPDATEOUTG));

    page.removeMark(INJECT);
    page.removeMark(GENERATE);
    page.removeMark(FETCH);
    page.removeMark(PARSE);
  }
}
