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
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.graph.GraphGroupKey;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.WebVertex;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.jobs.NutchCounter;
import org.apache.nutch.jobs.NutchReducer;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static org.apache.nutch.graph.io.WebGraphWritable.OptimizeMode.IGNORE_TARGET;
import static org.apache.nutch.jobs.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Mark.PARSE;
import static org.apache.nutch.metadata.Mark.UPDATEOUTG;
import static org.apache.nutch.metadata.Nutch.*;

class OutGraphUpdateReducer extends NutchReducer<GraphGroupKey, WebGraphWritable, String, GoraWebPage> {

    public static final Logger LOG = LoggerFactory.getLogger(OutGraphUpdateReducer.class);

    private enum PageExistence {PASSED, LOADED, CREATED}

    public enum Counter {
        pagesDepthUp, pagesDepth0, pagesDepth1, pagesDepth2, pagesDepth3, pagesDepthN,
        pagesPassed, pagesLoaded, pagesCreated, newDetailPages
    }

    private FetchSchedule fetchSchedule;
    private ScoringFilters scoringFilters;
    private int maxInLinks;
    private int round;
    private DataStore<String, GoraWebPage> datastore;
    private NutchMetrics nutchMetrics;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        getCounter().register(Counter.class);
        // getReporter().setLog(LOG_ADDITIVITY);

        String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);
        fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
        scoringFilters = new ScoringFilters(conf);
        maxInLinks = conf.getInt(PARAM_UPDATE_MAX_IN_LINKS, 100);
        round = conf.getInt(PARAM_CRAWL_ROUND, -1);

        nutchMetrics = NutchMetrics.getInstance(conf);

        try {
            datastore = StorageUtils.createWebStore(conf, String.class, GoraWebPage.class);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }

        Params.of(
                "className", this.getClass().getSimpleName(),
                "crawlId", crawlId,
                "maxLinks", maxInLinks,
                "fetchSchedule", fetchSchedule.getClass().getSimpleName(),
                "scoringFilters", scoringFilters.toString()
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

    /**
     * We get a list of score datum who are inlinks to a webpage after partition,
     * so the dbupdate phrase calculates the scores of all pages
     * <p>
     * The following graph shows the in-link graph. Every reduce group contains a center vertex and a batch of edges.
     * The center vertex has a web page inside, and the edges has in-link information.
     * <pre>
     *        v1
     *        |
     *        v
     * v2 -> vc <- v3
     *       ^ ^
     *      /  \
     *     v4  v5
     * </pre>
     * And this is a out-link graph:
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
    private void doReduce(GraphGroupKey key, Iterable<WebGraphWritable> subGraphs, Context context)
            throws IOException, InterruptedException {
        // Instant reduceStart = Instant.now();

        getCounter().increase(rows);

        String reversedUrl = key.getReversedUrl();
        String url = TableUtil.unreverseUrl(reversedUrl);

        WebGraph graph = buildGraph(url, reversedUrl, subGraphs);
        WebPage page = graph.getFocus().getWebPage();

        if (page == null) {
            return;
        }

        // 1. update depth, 2. update in-links, 3. update score from in-coming pages
        updateGraph(graph);
        updateMarks(page);

        context.write(reversedUrl, page.get());
        getCounter().updateAffectedRows(url);
    }

    /**
     * The graph should be like this:
     * <pre>
     *            v1
     *            |
     *            v
     * v2 -> TargetVertex <- v3
     *       ^          ^
     *      /           \
     *     v4           v5
     * </pre>
     */
    private WebGraph buildGraph(String url, String reversedUrl, Iterable<WebGraphWritable> subGraphs) {
        WebGraph graph = new WebGraph();

        WebVertex focus = new WebVertex(url);
        for (WebGraphWritable graphWritable : subGraphs) {
            assert (graphWritable.getOptimizeMode().equals(IGNORE_TARGET));

            WebGraph subGraph = graphWritable.get();
            subGraph.edgeSet().forEach(edge -> {
                if (edge.isLoop()) {
                    focus.setWebPage(edge.getSourceWebPage());
                }
                graph.addEdgeLenient(edge.getSource(), focus, subGraph.getEdgeWeight(edge));
            });
        }

        if (focus.hasWebPage()) {
            focus.getWebPage().setTempVar(VAR_PAGE_EXISTENCE, PageExistence.PASSED);
            getCounter().increase(Counter.pagesPassed);
        } else {
            focus.setWebPage(loadOrCreateWebPage(url, reversedUrl));
        }
        graph.setFocus(focus);

        return graph;
    }

    /**
     * There are three cases of a page's state of existence
     * 1. the page have been fetched in this batch and so it's passed from the mapper
     * 2. the page have been fetched in other batch, so it's in the data store
     * 3. the page have not be fetched yet
     */
    private WebPage loadOrCreateWebPage(String url, String reversedUrl) {
        // TODO : Can we sure datastore.get is a local operation? Or it's a distributed operation? And is there a cache?
        WebPage loadedPage = round <= 1 ? WebPage.newWebPage(url) : WebPage.wrap(url, datastore.get(reversedUrl), false);
        WebPage page;

        // Page is already in the db
        if (!loadedPage.isEmpty()) {
            page = loadedPage;
            page.setTempVar(VAR_PAGE_EXISTENCE, PageExistence.LOADED);
            getCounter().increase(Counter.pagesLoaded);
        } else {
            // Here we got a new webpage from outlink
            page = createNewRow(url);
            page.setTempVar(VAR_PAGE_EXISTENCE, PageExistence.CREATED);

            getCounter().increase(Counter.pagesCreated);
            if (page.getPageCategory().isDetail()) {
                getCounter().increase(Counter.newDetailPages);
            }
        }

        return page;
    }

    private WebPage createNewRow(String url) {
        WebPage page = WebPage.newWebPage(url);

        fetchSchedule.initializeSchedule(url, page);
        page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);
        scoringFilters.initialScore(url, page);

        return page;
    }

    /**
     * <pre>
     *            v1
     *            |
     *            v
     * v2 -> TargetVertex <- v3
     *       ^          ^
     *      /           \
     *     v4           v5
     * </pre>
     */
    private void updateGraph(WebGraph graph) {
        WebVertex focus = graph.getFocus();
        WebPage page = focus.getWebPage();
        Set<WebEdge> incomingEdges = graph.incomingEdgesOf(focus);

        page.getInlinks().clear();
        int newDepth = page.getDepth();
        for (WebEdge incomingEdge : incomingEdges) {
            if (incomingEdge.isLoop()) {
                continue;
            }

            /* Update in-links */
            if (page.getInlinks().size() <= maxInLinks) {
                page.getInlinks().put(incomingEdge.getSourceUrl(), incomingEdge.getAnchor());
            }

            WebPage incomingPage = incomingEdge.getSourceWebPage();
            if (newDepth > 1 && incomingPage.getDepth() + 1 < newDepth) {
                newDepth = incomingPage.getDepth() + 1;
            }
        }

        /* Update depth */
        if (newDepth < page.getDepth()) {
            if (page.getDepth() < MAX_DISTANCE) {
                nutchMetrics.debugDepthUpdated(page.getDepth() + " -> " + newDepth + ", " + focus.getUrl());
            }
            page.setDepth(newDepth);
            updateDepthCounter(newDepth, page);
        }

        /* Update score */
        scoringFilters.updateScore(focus.getUrl(), page, graph, incomingEdges);
    }

    private void updateMarks(WebPage page) {
        // Clear temporary metadata
        page.putMarkIfNonNull(UPDATEOUTG, page.getMark(PARSE));
    }

    private void updateDepthCounter(int depth, WebPage page) {
        NutchCounter counter = getCounter();

        PageExistence pageExistence = page.getTempVar(VAR_PAGE_EXISTENCE, PageExistence.PASSED);
        if (pageExistence != PageExistence.CREATED) {
            counter.increase(Counter.pagesDepthUp);
        }

        if (depth == 0) {
            counter.increase(Counter.pagesDepth0);
        } else if (depth == 1) {
            counter.increase(Counter.pagesDepth1);
        } else if (depth == 2) {
            counter.increase(Counter.pagesDepth2);
        } else if (depth == 3) {
            counter.increase(Counter.pagesDepth3);
        } else {
            counter.increase(Counter.pagesDepthN);
        }
    }
}
