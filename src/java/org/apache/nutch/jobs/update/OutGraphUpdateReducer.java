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

import org.apache.commons.lang3.StringUtils;
import org.apache.gora.store.DataStore;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.SignatureComparator;
import org.apache.nutch.filter.CrawlFilter;
import org.apache.nutch.graph.GraphGroupKey;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.WebVertex;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.jobs.NutchCounter;
import org.apache.nutch.jobs.NutchReducer;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.nutch.jobs.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Metadata.Name.GENERATE_TIME;
import static org.apache.nutch.metadata.Metadata.Name.REDIRECT_DISCOVERED;
import static org.apache.nutch.metadata.Nutch.*;
import static org.apache.nutch.persist.Mark.*;

class OutGraphUpdateReducer extends NutchReducer<GraphGroupKey, WebGraphWritable, String, GoraWebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(OutGraphUpdateReducer.class);

  private enum PageExistence { IN_BATCH, OUT_OF_BATCH, NOT_FETCHED }

  public enum Counter { pagesDepthUp, pagesInBatch, pagesOutBatch, newPages, newDetailPages }

  private int retryMax;
  private Duration maxFetchInterval;
  private FetchSchedule fetchSchedule;
  private ScoringFilters scoringFilters;
  private boolean additionsAllowed;
  private int maxLinks;
  private DataStore<String, GoraWebPage> datastore;
  private String reportSuffix;
  private NutchMetrics nutchMetrics;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);
    retryMax = conf.getInt("db.fetch.retry.max", 3);
    additionsAllowed = conf.getBoolean(PARAM_CRAWLDB_ADDITIONS_ALLOWED, true);
    maxFetchInterval = Duration.ofSeconds(conf.getLong("db.fetch.interval.max", Duration.ofDays(90).getSeconds()));
    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);
    nutchMetrics = NutchMetrics.getInstance(conf);
    reportSuffix = conf.get(PARAM_NUTCH_JOB_NAME, "job-unknown-" + DateTimeUtil.now("MMdd.HHmm"));

    try {
      datastore = StorageUtils.createWebStore(conf, String.class, GoraWebPage.class);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    Params.of(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "additionsAllowed", additionsAllowed,
        "maxLinks", maxLinks,
        "retryMax", retryMax,
        "maxFetchInterval", maxFetchInterval,
        "maxLinks", maxLinks,
        "fetchSchedule", fetchSchedule.getClass().getSimpleName(),
        "scoringFilters", StringUtils.join(scoringFilters.getScoringFilterNames(), ",")
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
   *</pre>
   */
  private void doReduce(GraphGroupKey key, Iterable<WebGraphWritable> subGraphs, Context context)
      throws IOException, InterruptedException {
    getCounter().increase(rows);

    String reversedUrl = key.getReversedUrl();
    String url = TableUtil.unreverseUrl(reversedUrl);

    WebGraph graph = buildGraph(url, reversedUrl, subGraphs);
    WebPage page = graph.getFocus().getWebPage();

    if (page == null) {
      return;
    }

    updateGraph(graph);

    PageExistence pageExistence = page.getTempVar(VAR_PAGE_EXISTENCE, PageExistence.NOT_FETCHED);
    if (pageExistence == PageExistence.IN_BATCH) {
      updateFetchSchedule(url, page);
      updateMetadata(page);
      updateStatusCounter(page);
      getCounter().increase(Counter.pagesInBatch);
    }

    context.write(reversedUrl, page.get());
    getCounter().updateAffectedRows(TableUtil.unreverseUrl(reversedUrl));
    getCounter().increase(NutchCounter.Counter.inlinks, page.getInlinks().size());
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
   * */
  private WebGraph buildGraph(String url, String reversedUrl, Iterable<WebGraphWritable> subGraphs) {
    WebGraph graph = new WebGraph();

    WebPage page = null;
    for (WebGraphWritable graphWritable : subGraphs) {
      WebGraph subGraph = graphWritable.get();
      WebEdge edge = subGraph.firstEdge();
      if (edge.isLoop()) {
        page = edge.getSourceWebPage();
        page.setTempVar(VAR_PAGE_EXISTENCE, PageExistence.IN_BATCH);
      }
      graph.addEdgeLenient(edge.getSource(), edge.getTarget(), subGraph.getEdgeWeight(edge));
    }

    if (page == null) {
      page = loadOrCreateWebPage(url, reversedUrl);
    }

    WebVertex focus = graph.firstEdge().getTarget();
    focus.setWebPage(page);
    graph.setFocus(focus);

    return graph;
  }

  /**
   * There are three cases of a page's state of existence
   * 1. the page have been fetched in this batch and so it's passed from the mapper
   * 2. the page have been fetched in other batch, so it's in the data store
   * 3. the page have not be fetched yet
   * */
  private WebPage loadOrCreateWebPage(String url, String reversedUrl) {
    WebPage page = null;
    WebPage loadedPage = WebPage.wrap(datastore.get(reversedUrl));

    // Page is already in the db
    if (!loadedPage.isEmpty()) {
      page = loadedPage;
      page.setTempVar(VAR_PAGE_EXISTENCE, PageExistence.OUT_OF_BATCH);
      getCounter().increase(Counter.pagesOutBatch);
    } else if (additionsAllowed) {
      // Here we got a new webpage from outlink
      page = createNewRow(url);
      page.setTempVar(VAR_PAGE_EXISTENCE, PageExistence.NOT_FETCHED);

      getCounter().increase(Counter.newPages);
      if (CrawlFilter.sniffPageCategory(url).isDetail()) {
        getCounter().increase(Counter.newDetailPages);
      }
    }

    return page;
  }

  private WebPage createNewRow(String url) {
    WebPage page = WebPage.newWebPage();

    fetchSchedule.initializeSchedule(url, page);
    page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);

    try {
      scoringFilters.initialScore(url, page);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      getCounter().increase(NutchCounter.Counter.errors);
    }

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
   * */
  private void updateGraph(WebGraph graph) {
    WebVertex focus = graph.getFocus();
    Set<WebEdge> incomingEdges = graph.incomingEdgesOf(focus);
    WebPage page = focus.getWebPage();

    Map<CharSequence, CharSequence> inLinks = new HashMap<>();
    int newDepth = page.getDepth();
    for (WebEdge incomingEdge : incomingEdges) {
      if (incomingEdge.isLoop()) {
        continue;
      }

      inLinks.put(incomingEdge.getSourceUrl(), incomingEdge.getAnchor());
      /* Find out the smallest depth of this page */
      WebPage incomingWebPage = graph.getEdgeSource(incomingEdge).getWebPage();

//      Params.of(
//          "reversedUrl", TableUtil.reverseUrlOrEmpty(incomingEdge.getSourceUrl()),
//          "edge", incomingEdge.getSourceUrl() + " -> " + incomingEdge.getTargetUrl(),
////          "baseUrl", incomingWebPage.getBaseUrl() + " -> " + page.getBaseUrl(),
//          "score", graph.getEdgeWeight(incomingEdge),
//          "depth", newDepth + " -> " + (incomingWebPage.getDepth() + 1) // should be 0
//      ).withLogger(LOG).info(true);

      if (incomingWebPage.getDepth() + 1 < newDepth) {
        newDepth = incomingWebPage.getDepth() + 1;
      }
    }

    if (!inLinks.isEmpty()) {
      page.setInlinks(inLinks);
    }

    if (newDepth < page.getDepth()) {
      getCounter().increase(Counter.pagesDepthUp);
      nutchMetrics.debugDepthUpdated(page.getDepth() + " -> " + newDepth + ", " + page.getBaseUrl(), reportSuffix);
      page.setDepth(newDepth);
    }

    /* Update score */
    try {
      scoringFilters.updateScore(focus.getUrl(), page, graph, incomingEdges);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      getCounter().increase(NutchCounter.Counter.errors);
    }

//    Params.of(
//        "reversedUrl", TableUtil.reverseUrlOrEmpty(focus.getUrl()),
//        "inlinks", page.getInlinks().size(),
//        "score", page.getScore(),
//        "depth", page.getDepth()
//    ).withLogger(LOG).info(true);
  }

  private void updateMetadata(WebPage page) {
    // Clear temporary metadata
    page.clearMetadata(REDIRECT_DISCOVERED);
    page.clearMetadata(GENERATE_TIME);
    page.putMarkIfNonNull(UPDATEOUTG, page.getMark(PARSE));
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

        Instant latestModifiedTime = page.getHeaderLastModifiedTime(modifiedTime);
        if (latestModifiedTime.isAfter(modifiedTime)) {
          modifiedTime = latestModifiedTime;
          prevModifiedTime = modifiedTime;
        }
        else {
          LOG.trace("Bad last modified time : {} -> {}", modifiedTime, page.getHeader(HttpHeaders.LAST_MODIFIED, "unknown time"));
        }

        fetchSchedule.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, modified);

        Duration fetchInterval = page.getFetchInterval();
        if (fetchInterval.toDays() < NEVER_FETCH_INTERVAL_DAYS && fetchInterval.compareTo(maxFetchInterval) > 0) {
          LOG.info("Force refetch page " + url + ", fetch interval : " + fetchInterval);
          fetchSchedule.forceRefetch(url, page, false);
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
