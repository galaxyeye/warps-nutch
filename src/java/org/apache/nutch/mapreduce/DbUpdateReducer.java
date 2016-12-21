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

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.SignatureComparator;
import org.apache.nutch.filter.CrawlFilter;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.StorageUtils;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.persist.graph.Edge;
import org.apache.nutch.persist.graph.Graph;
import org.apache.nutch.persist.graph.GraphGroupKey;
import org.apache.nutch.persist.graph.Vertex;
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
import java.util.ArrayList;
import java.util.List;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Metadata.Name.PUBLISH_TIME;
import static org.apache.nutch.metadata.Metadata.Name.TMP_CHARS;
import static org.apache.nutch.metadata.Metadata.Name.TMP_IS_DETAIL;
import static org.apache.nutch.metadata.Nutch.*;
import static org.apache.nutch.persist.Mark.*;

public class DbUpdateReducer extends NutchReducer<GraphGroupKey, Writable, String, GoraWebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateReducer.class);

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

  public enum Counter {newRows}

  ;

  private int retryMax;
  private Duration maxFetchInterval;
  private FetchSchedule fetchSchedule;
  private ScoringFilters scoringFilters;
  private List<Edge> inlinkedScoreData = new ArrayList<>();
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
  protected void reduce(GraphGroupKey key, Iterable<Writable> values, Context context) {
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
  private void doReduce(GraphGroupKey key, Iterable<Writable> values, Context context)
      throws IOException, InterruptedException {
    getCounter().increase(rows);

    String reversedUrl = key.getReversedUrl();
    String url = TableUtil.unreverseUrl(reversedUrl);

    // Calculate in-linked score data, and return the main web page
    Pair<Vertex, Graph> g = buildGraph(key, values);
    Vertex focusedVertex = g.getKey();
    Graph graph = g.getValue();

    WebPage page = focusedVertex.getWebPage();
    if (!page.isEmpty()) {
      updateFetchSchedule(url, page);
    } else {
      page = getWebPage(url, reversedUrl, focusedVertex);
    }

    if (graph.getGraphMode().isInLinkGraph()) {
      page = updateInLinkedWebPage(page, graph);
    } else {
      page = updateOutLinkedWebPage(page, graph);
    }

    updateDistance(page);

    updateScore(url, page);

    updateMetadata(page);

    updateStatusCounter(url, page);

    context.write(reversedUrl, page.get());
  }

  private WebPage getWebPage(String url, String reversedUrl, Vertex focusedVertex) {
    WebPage page = focusedVertex.getWebPage();
    if (page.isEmpty()) {
      WebPage loadedPage = WebPage.wrap(datastore.get(reversedUrl));

      // Page is already in the db
      if (loadedPage.isEmpty()) {
        // Here we got a new webpage from outlink
        if (!additionsAllowed) {
          return page;
        }
        page = createNewRow(url, MAX_DISTANCE);
      } else {
        page = loadedPage;
      }
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

  private WebPage updateOutLinkedWebPage(WebPage page, Graph graph) {
    if (!graph.getGraphMode().isOutLinkGraph()) {
      return page;
    }

    for (Vertex referVertex : graph.getEndVertices()) {

    }

    return page;
  }

  /**
   * Updated the old row if necessary
   * */
  private WebPage updateInLinkedWebPage(WebPage page, Graph graph) {
    if (!graph.getGraphMode().isInLinkGraph()) {
      return page;
    }

    int smallestDistance = Integer.MAX_VALUE;
    Instant latestRefPublishTime = page.getRefPublishTime();
    int referredArticles = 0;
    int referredChars = 0;

    for (Vertex referVertex : graph.getEndVertices()) {
      if (referVertex.getDepth() < smallestDistance) {
        smallestDistance = referVertex.getDepth();
      }

      Instant refPublishTime = referVertex.getMetadata(PUBLISH_TIME, Instant.EPOCH);
      if (refPublishTime.isAfter(latestRefPublishTime)) {
        latestRefPublishTime = refPublishTime;
      }

      boolean isDetail = referVertex.getMetadata(TMP_IS_DETAIL, false);
      if (isDetail) {
        ++referredArticles;
      }

      referredChars += referVertex.getMetadata(TMP_CHARS, 0);
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
   * Distance calculation.
   * Retrieve smallest distance from all inlinks distances
   * Calculate new distance for current page: smallest inlink distance plus 1.
   * If the new distance is smaller than old one (or if old did not exist yet),
   * write it to the page.
   * */
  private void updateDistance(WebPage page) {
    page.getInlinks().clear();

    int smallestDist = MAX_DISTANCE;
//    Instant latestRefPublishTime = page.getRefPublishTime();
//    long refArticles = 0;
//    long refChars = page.getRefChars();

    for (Edge inlink : inlinkedScoreData) {
      int inlinkDist = inlink.getV2().getDepth();
      if (inlinkDist < smallestDist) {
        smallestDist = inlinkDist;
      }

//      Instant refPublishTime = inlink.getMetadata(REFERRED_PUBLISH_TIME, Instant.EPOCH);
//      if (refPublishTime.isAfter(latestRefPublishTime)) {
//        latestRefPublishTime = refPublishTime;
//      }
//      refChars += inlink.getMetadata(TMP_CHARS, 0L);
//      if (refChars > 1000) {
//        refArticles += 1;
//      }

      page.getInlinks().put(new Utf8(inlink.getV2().getUrl()), new Utf8(inlink.getV2().getAnchor()));
    }

    if (smallestDist != MAX_DISTANCE) {
      int oldDistance = page.getDepth();
      int newDistance = smallestDist + 1;

      if (newDistance < oldDistance) {
        page.setDistance(newDistance);
      }
    }

//    page.updateRefPublishTime(latestRefPublishTime);
//    page.setRefArticles(refArticles);
//    page.setRefChars(refChars);
  }

  /**
   * In the mapper phrase, NutchWritable is sets to be a WebPage or a Edge,
   * the WrappedWebPageWritable is the webpage to be updated, and the score datum is calculated from the outlinks
   * */
  public Pair<Vertex, Graph> buildGraph(GraphGroupKey key, Iterable<Writable> values) {
    Vertex focusedVertex = new Vertex(conf);
    Graph graph = new Graph(conf);

//    WebPage page = null;
    inlinkedScoreData.clear();

    for (Writable val : values) {
      // Writable val = nutchWritable.get();
      if (val instanceof Vertex) {
        focusedVertex = (Vertex)val;
//        page = ((Vertex) val).getWebPage();
      }
      else if (val instanceof Edge) {
        Edge edge = (Edge) val;

        if (key.getGraphMode().isInLinkGraph()) {
          if (edge.getV2().equals(focusedVertex)) {
            edge.setV2(focusedVertex);
          }
        }

        graph.addEdge(edge);
      }
//      else {
//        inlinkedScoreData.add((Edge) val);
//
//        if (inlinkedScoreData.size() >= maxLinks) {
//          LOG.info("Limit reached, skipping further inlinks for " + key);
//          break;
//        }
//      }
    } // for

    return Pair.of(focusedVertex, graph);
  }

  private void updateScore(String url, WebPage page) {
    try {
      scoringFilters.updateScore(url, page, inlinkedScoreData);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      getCounter().increase(NutchCounter.Counter.errors);
    }

    // if page contains date string of today, and it's an index page
    // it should have a very high score
  }

  private void updateMetadata(WebPage page) {
    // Clear temporary metadata
    page.clearMetadata(FetchJob.REDIRECT_DISCOVERED);
    page.clearMetadata(Metadata.Name.GENERATE_TIME);

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
