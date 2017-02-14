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
import org.apache.nutch.crawl.SignatureComparator;
import org.apache.nutch.graph.GraphGroupKey;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.WebVertex;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.jobs.NutchCounter;
import org.apache.nutch.jobs.NutchReducer;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.nutch.graph.io.WebGraphWritable.OptimizeMode.IGNORE_SOURCE;
import static org.apache.nutch.jobs.NutchCounter.Counter.rows;
import static org.apache.nutch.jobs.update.InGraphUpdateReducer.Counter.pagesNotExist;
import static org.apache.nutch.metadata.Mark.*;
import static org.apache.nutch.metadata.Metadata.Name.GENERATE_TIME;
import static org.apache.nutch.metadata.Metadata.Name.REDIRECT_DISCOVERED;
import static org.apache.nutch.metadata.Nutch.*;

class InGraphUpdateReducer extends NutchReducer<GraphGroupKey, WebGraphWritable, String, GoraWebPage> {

    public static final Logger LOG = LoggerFactory.getLogger(InGraphUpdateReducer.class);

    public enum Counter {pagesPassed, pagesLoaded, pagesNotExist, pagesUpdated, totalUpdates, badModTime}

    private DataStore<String, GoraWebPage> datastore;
    private NutchMetrics nutchMetrics;
    private FetchSchedule fetchSchedule;
    private int retryMax;
    private Duration maxFetchInterval;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        getCounter().register(Counter.class);
        // getReporter().setLog(LOG_ADDITIVITY);

        String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);
        nutchMetrics = NutchMetrics.getInstance(conf);
        fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
        retryMax = conf.getInt(PARAM_FETCH_MAX_RETRY, 3);
        maxFetchInterval = ConfigUtils.getDuration(conf, PARAM_FETCH_MAX_INTERVAL, Duration.ofDays(90));

        try {
            datastore = StorageUtils.createWebStore(conf, String.class, GoraWebPage.class);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }

        Params.of(
                "className", this.getClass().getSimpleName(),
                "crawlId", crawlId,
                "retryMax", retryMax,
                "maxFetchInterval", maxFetchInterval,
                "fetchSchedule", fetchSchedule.getClass().getSimpleName()
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
        if (page.hasMark(FETCH)) {
            updateFetchSchedule(url, page);
            updateStatusCounter(page);
        }
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
            WebPage page = WebPage.wrap(reversedUrl, datastore.get(reversedUrl), true);

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
            if (targetPage.getPageCategory().isDetail()) {
                page.updateRefPublishTime(targetPage.getPublishTime());
                page.increaseRefChars(targetPage.sniffTextContentLength());
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

    private void updateFetchSchedule(String url, WebPage page) {
        byte status = page.getStatus().byteValue();

        switch (status) {
            case CrawlStatus.STATUS_FETCHED: // successful fetch
            case CrawlStatus.STATUS_REDIR_TEMP: // successful fetch, redirected
            case CrawlStatus.STATUS_REDIR_PERM:
            case CrawlStatus.STATUS_NOTMODIFIED: // successful fetch, not modified
                int modified = FetchSchedule.STATUS_UNKNOWN;
                if (status == CrawlStatus.STATUS_NOTMODIFIED) {
                    modified = FetchSchedule.STATUS_NOTMODIFIED;
                }

                ByteBuffer prevSig = page.getPrevSignature();
                ByteBuffer signature = page.getSignature();
                if (prevSig != null && signature != null) {
                    if (SignatureComparator.compare(prevSig, signature) != 0) {
                        modified = FetchSchedule.STATUS_MODIFIED;
                    } else {
                        modified = FetchSchedule.STATUS_NOTMODIFIED;
                    }
                }

                Instant prevFetchTime = page.getPrevFetchTime();
                Instant fetchTime = page.getFetchTime();

                Instant prevModifiedTime = page.getPrevModifiedTime();
                Instant modifiedTime = page.getModifiedTime();
                Instant newModifiedTime = getModifiedTime(page);
                if (newModifiedTime.isAfter(modifiedTime)) {
                    prevModifiedTime = modifiedTime;
                    modifiedTime = newModifiedTime;
                }

                fetchSchedule.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, modified);

                Duration fetchInterval = page.getFetchInterval();
                if (!page.hasMark(Mark.INACTIVE) && fetchInterval.compareTo(maxFetchInterval) > 0) {
                    LOG.info("Force refetch page " + url + ", fetch interval : " + fetchInterval);
                    fetchSchedule.forceRefetch(url, page, false);
                }

                if(modifiedTime.isBefore(TCP_IP_STANDARDIZED_TIME)) {
                    getCounter().increase(Counter.badModTime);
                    nutchMetrics.reportBadModifiedTime(Params.of(
                            "PFT", prevFetchTime, "FT", fetchTime,
                            "PMT", prevModifiedTime, "MT", modifiedTime,
                            "HMT", page.getHeader(HttpHeaders.LAST_MODIFIED, Instant.EPOCH.toString()),
                            "U", url
                    ).formatAsLine());
                }

                break;
            case CrawlStatus.STATUS_RETRY:
                fetchSchedule.setPageRetrySchedule(url, page, Instant.EPOCH, page.getPrevModifiedTime(), page.getFetchTime());
                if (page.getRetriesSinceFetch() < retryMax) {
                    page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);
                } else {
                    page.setStatus((int) CrawlStatus.STATUS_GONE);
                }
                break;
            case CrawlStatus.STATUS_GONE:
                fetchSchedule.setPageGoneSchedule(url, page, Instant.EPOCH, page.getPrevModifiedTime(), page.getFetchTime());
                break;
        }
    }

    private Instant getModifiedTime(WebPage page) {
        Instant modifiedTime = page.getModifiedTime();
        Instant headerModifiedTime = page.getHeaderLastModifiedTime(modifiedTime);
        Instant contentModifiedTime = page.getContentModifiedTime();

        // A fix
        if (modifiedTime.isAfter(Instant.now().plus(1, ChronoUnit.DAYS))) {
            LOG.warn("Invalid modified time " + DateTimeUtil.isoInstantFormat(modifiedTime) + ", url : " + page.url());
            modifiedTime = Instant.EPOCH;
        }

        if (headerModifiedTime.isAfter(modifiedTime)) {
            modifiedTime = headerModifiedTime;
        }

        if (contentModifiedTime.isAfter(modifiedTime)) {
            modifiedTime = contentModifiedTime;
        }

        return modifiedTime;
    }

    private void updateStatusCounter(WebPage page) {
        byte status = page.getStatus().byteValue();

        switch (status) {
            case CrawlStatus.STATUS_FETCHED:
                getCounter().increase(NutchCounter.Counter.stFetched);
                break;
            case CrawlStatus.STATUS_REDIR_TEMP:
                getCounter().increase(NutchCounter.Counter.stRedirTemp);
                break;
            case CrawlStatus.STATUS_REDIR_PERM:
                getCounter().increase(NutchCounter.Counter.stRedirPerm);
                break;
            case CrawlStatus.STATUS_NOTMODIFIED:
                getCounter().increase(NutchCounter.Counter.stNotModified);
                break;
            case CrawlStatus.STATUS_RETRY:
                getCounter().increase(NutchCounter.Counter.stRetry);
                break;
            case CrawlStatus.STATUS_UNFETCHED:
                getCounter().increase(NutchCounter.Counter.stUnfetched);
                break;
            case CrawlStatus.STATUS_GONE:
                getCounter().increase(NutchCounter.Counter.stGone);
                break;
            default:
                break;
        }
    }
}
