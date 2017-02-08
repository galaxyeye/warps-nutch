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
import org.apache.nutch.common.Params;
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
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.nutch.graph.io.WebGraphWritable.OptimizeMode.IGNORE_SOURCE;
import static org.apache.nutch.jobs.NutchCounter.Counter.rows;
import static org.apache.nutch.jobs.update.InGraphUpdateReducer.Counter.pagesNotExist;
import static org.apache.nutch.metadata.Mark.*;
import static org.apache.nutch.metadata.Metadata.Name.GENERATE_TIME;
import static org.apache.nutch.metadata.Metadata.Name.REDIRECT_DISCOVERED;

class InGraphUpdateReducer extends NutchReducer<GraphGroupKey, WebGraphWritable, String, GoraWebPage> {

    public static final Logger LOG = LoggerFactory.getLogger(InGraphUpdateReducer.class);

    public enum Counter {pagesPassed, pagesLoaded, pagesNotExist, pagesUpdated, totalUpdates}

    private NutchMetrics nutchMetrics;

    private DataStore<String, GoraWebPage> datastore;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        getCounter().register(Counter.class);
//    getReporter().setLog(LOG_ADDITIVITY);

        String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);
        nutchMetrics = NutchMetrics.getInstance(conf);

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
        String url = TableUtil.unreverseUrl(reversedUrl);

        WebGraph graph = buildGraph(url, reversedUrl, subGraphs);
        WebPage page = graph.getFocus().getWebPage();

        if (page == null) {
            getCounter().increase(pagesNotExist);
            return;
        }

        updateGraph(graph);
        updateMetadata(page);
        updateMarks(page);

        context.write(reversedUrl, page.get());
        getCounter().updateAffectedRows(url);
        nutchMetrics.reportWebPage(url, page);
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
     * </pre>
     */
    private WebGraph buildGraph(String url, String reversedUrl, Iterable<WebGraphWritable> subGraphs) {
        WebGraph graph = new WebGraph();

        WebVertex focus = new WebVertex(url);
        for (WebGraphWritable graphWritable : subGraphs) {
            assert (graphWritable.getOptimizeMode().equals(IGNORE_SOURCE));

            WebGraph subGraph = graphWritable.get();
            subGraph.edgeSet().forEach(edge -> {
                if (edge.isLoop()) {
                    focus.setWebPage(edge.getTargetWebPage());
                }
                graph.addEdgeLenient(focus, edge.getTarget(), subGraph.getEdgeWeight(edge));
            });
        }

        if (!focus.hasWebPage()) {
            WebPage page = WebPage.wrap(datastore.get(reversedUrl));

            // Page is already in the db
            if (!page.isEmpty()) {
                focus.setWebPage(page);
                getCounter().increase(Counter.pagesLoaded);
            }
        } else {
            getCounter().increase(Counter.pagesPassed);
        }

        graph.setFocus(focus);

        return graph;
    }

    private boolean updateGraph(WebGraph graph) {
        WebVertex focus = graph.getFocus();
        WebPage page = focus.getWebPage();

        int totalUpdates = 0;
        for (WebEdge outgoingEdge : graph.outgoingEdgesOf(focus)) {
            if (outgoingEdge.isLoop()) {
                continue;
            }

            /* Update out-link page */
            String targetUrl = outgoingEdge.getTargetUrl();
            WebPage targetPage = outgoingEdge.getTargetWebPage();
            if (targetPage.veryLikeDetailPage(targetUrl)) {
                page.updateRefPublishTime(targetPage.getPublishTime());
                page.increaseRefChars(targetPage.sniffTextLength());
                page.increaseRefArticles(1);
                ++totalUpdates;
            }
        }

        if (totalUpdates > 0) {
            getCounter().increase(Counter.pagesUpdated);
            getCounter().increase(Counter.totalUpdates, totalUpdates);

            return true;
        }

        return false;
    }

    private void updateMetadata(WebPage page) {
        // Clear temporary metadata
        page.clearMetadata(REDIRECT_DISCOVERED);
        page.clearMetadata(GENERATE_TIME);
    }

    private void updateMarks(WebPage page) {
        page.putMarkIfNonNull(UPDATEING, page.getMark(UPDATEOUTG));
        page.removeMark(INJECT);
        page.removeMark(GENERATE);
        page.removeMark(FETCH);
        page.removeMark(PARSE);
        page.removeMark(INDEX);
    }
}
