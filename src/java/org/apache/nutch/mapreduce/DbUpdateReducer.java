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

import org.apache.commons.lang3.StringUtils;
import org.apache.gora.store.DataStore;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.SignatureComparator;
import org.apache.nutch.graph.GraphGroupKey;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.WebVertex;
import org.apache.nutch.graph.io.WebGraphWritable;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.StorageUtils;
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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Metadata.Name.GENERATE_TIME;
import static org.apache.nutch.metadata.Metadata.Name.REDIRECT_DISCOVERED;
import static org.apache.nutch.metadata.Nutch.MAX_DISTANCE;
import static org.apache.nutch.metadata.Nutch.NEVER_FETCH_INTERVAL_DAYS;
import static org.apache.nutch.metadata.Nutch.YES_STRING;
import static org.apache.nutch.persist.Mark.*;

public class DbUpdateReducer extends NutchReducer<GraphGroupKey, WebGraphWritable, String, GoraWebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateReducer.class);

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

  public enum Counter {newRows}

  private int retryMax;
  private Duration maxFetchInterval;
  private FetchSchedule fetchSchedule;
  private ScoringFilters scoringFilters;
  private boolean additionsAllowed;
  private int maxLinks;
  public DataStore<String, GoraWebPage> datastore;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    retryMax = conf.getInt("db.fetch.retry.max", 3);
    additionsAllowed = conf.getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    maxFetchInterval = Duration.ofSeconds(conf.getLong("db.fetch.interval.max", Duration.ofDays(90).getSeconds()));
    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);
    additionsAllowed = conf.getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);
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
  private void doReduce(GraphGroupKey key, Iterable<WebGraphWritable> values, Context context)
      throws IOException, InterruptedException {
    getCounter().increase(rows);

    String reversedUrl = key.getReversedUrl();
    String url = TableUtil.unreverseUrl(reversedUrl);

//    WebVertex targetVertex = null;
//
//    for (WebGraphWritable graphWritable : values) {
//      targetVertex = graphWritable.get().vertexSet().stream().filter(WebVertex::hasWebPage).findAny().orElse(null);
//    }

    // Calculate in-linked score data, and return the main web page
    WebGraph graph = buildGraph(url, reversedUrl, values);
    WebVertex targetVertex = graph.getFocus();
    WebPage page = targetVertex.getWebPage();

    // Passed in
    if (targetVertex.getWebPage().hasMetadata("TMP_PASSED_FROM_MAPPER")) {
      updateFetchSchedule(url, page);
    }

    page = updateInLinkedWebPage(page, graph);

    updateScore(url, targetVertex, graph);

    updateMetadata(page);

    updateStatusCounter(url, page);

    context.write(reversedUrl, page.get());
  }

  private WebPage loadOrCreateWebPage(String url, String reversedUrl) {
    WebPage page;
    WebPage loadedPage = WebPage.wrap(datastore.get(reversedUrl));

    // Page is already in the db
    if (loadedPage.isEmpty()) {
      // Here we got a new webpage from outlink
      if (!additionsAllowed) {
        return null;
      }
      page = createNewRow(url, MAX_DISTANCE);
    } else {
      page = loadedPage;
    }

    return page;
  }

  private WebPage createNewRow(String url, int depth) {
    WebPage page = WebPage.newWebPage();

    page.setDistance(depth);

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
   * Updated the old row if necessary
   * */
  private WebPage updateInLinkedWebPage(WebPage page, WebGraph graph) {
//    if (!graph.getGraphMode().isInLinkGraph()) {
//      return page;
//    }

    int smallestDistance = Integer.MAX_VALUE;
    Instant latestRefPublishTime = page.getRefPublishTime();
    int referredArticles = 0;
    int referredChars = 0;

    for (WebVertex referVertex : graph.vertexSet()) {
      if (referVertex.getDepth() < smallestDistance) {
        smallestDistance = referVertex.getDepth();
      }

      Instant refPublishTime = referVertex.getReferredPublishTime();
      if (refPublishTime.isAfter(latestRefPublishTime)) {
        latestRefPublishTime = refPublishTime;
      }

//      boolean isDetail = referVertex.getMetadata(TMP_IS_DETAIL, false);
//      if (isDetail) {
//        ++referredArticles;
//      }

      referredChars += referVertex.getReferredChars();
    }

    if (smallestDistance < page.getDistance()) {
      page.setDistance(smallestDistance);
    }
    page.updateRefPublishTime(latestRefPublishTime);
    page.setRefArticles(page.getRefArticles() + referredArticles);
    page.setRefChars(page.getRefChars() + referredChars);

    return page;
  }

  /**
   * In the mapper phrase, NutchWritable is sets to be a WebPage or a WebEdge,
   * the WrappedWebPageWritable is the webpage to be updated, and the score datum is calculated from the outlinks
   * */
  public WebGraph buildGraph(String url, String reversedUrl, Iterable<WebGraphWritable> values) {
    WebGraph graph = new WebGraph();
    WebVertex targetVertex = null;
    WebPage page;

    for (WebGraphWritable graphWritable : values) {
      WebEdge edge = graphWritable.get().edgeSet().iterator().next();

      if (edge.getTarget().hasWebPage()) {
        targetVertex = edge.getTarget();
        page = targetVertex.getWebPage();
        page.putMetadata("TMP_PASSED_FROM_MAPPER", YES_STRING);
      }

      graph.addVerticesAndEdge(edge.getSource(), targetVertex, edge.getWeight());
    }

    if (targetVertex == null) {
      page = loadOrCreateWebPage(url, reversedUrl);
      targetVertex = graph.edgeSet().iterator().next().getTarget();
      targetVertex.setWebPage(page);
    }

    graph.setFocus(targetVertex);

    return graph;
  }

  private void updateScore(String url, WebVertex vertex, WebGraph graph) {
    try {
      scoringFilters.updateScore(url, vertex.getPage(), graph.edgesOf(vertex));
    } catch (ScoringFilterException e) {
      vertex.getPage().setScore(0.0f);
      getCounter().increase(NutchCounter.Counter.errors);
    }

    // if page contains date string of today, and it's an index page
    // it should have a very high score
  }

  private void updateMetadata(WebPage page) {
    // Clear temporary metadata
    page.clearMetadata(REDIRECT_DISCOVERED);
    page.clearMetadata(GENERATE_TIME);

    page.putMarkIfNonNull(UPDATEDB, page.getMark(PARSE));

    page.removeMark(INJECT);
    page.removeMark(GENERATE);
    page.removeMark(FETCH);
    page.removeMark(PARSE);
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

  private void updateStatusCounter(String url, WebPage page) throws IOException {
    getCounter().updateAffectedRows(url);

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
