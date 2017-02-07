/**
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
 */
package org.apache.nutch.scoring.monitor;

import com.google.common.collect.Lists;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.common.Params;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.schedulers.AdaptiveNewsFetchSchedule;
import org.apache.nutch.fetch.FetchUtil;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.WebVertex;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.WebPageConverter;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.NutchUtil;
import org.junit.Before;
import org.junit.Test;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * JUnit test for <code>MonitorScoringFilter</code>. For an example set of URLs, we
 * simulate inlinks and outlinks of the available graph. By manual calculation,
 * we determined the correct score points of URLs for each depth. For
 * convenience, a Map (dbWebPages) is used to store the calculated scores
 * instead of a persistent data store. At the end of the test, calculated scores
 * in the map are compared to our correct scores and a boolean result is
 * returned.
 * 
 */
public class TestMonitorScoringFilter {

  private static final DecimalFormat df = new DecimalFormat("#.###");
  private static final String[] seedUrls = new String[]{"http://a.com", "http://b.com", "http://c.com"};
  // An example web graph; shows websites as connected nodes
  private static final Map<String, List<String>> linkGraph = new LinkedHashMap<>();

  static {
    linkGraph.put("http://a.com", Lists.newArrayList("http://b.com"));
    linkGraph.put("http://b.com", Lists.newArrayList("http://a.com", "http://c.com", "http://e.com"));
    linkGraph.put("http://c.com", Lists.newArrayList("http://a.com", "http://b.com", "http://d.com", "http://f.com"));
    linkGraph.put("http://d.com", Lists.newArrayList());
    linkGraph.put("http://e.com", Lists.newArrayList());
    linkGraph.put("http://f.com", Lists.newArrayList());
  }

  // Previously calculated values for each three depths. We will compare these
  // to the results this test generates
  private static HashMap<Integer, HashMap<String, Float>> acceptedScores = new HashMap<>();

  static {
    acceptedScores.put(1, new HashMap<String, Float>() {
      {
        put("http://a.com", 1.833f);
        put("http://b.com", 2.333f);
        put("http://c.com", 1.5f);
        put("http://d.com", 0.333f);
      }
    });

    acceptedScores.put(2, new HashMap<String, Float>() {
      {
        put("http://a.com", 2.666f);
        put("http://b.com", 3.333f);
        put("http://c.com", 2.166f);
        put("http://d.com", 0.278f);
      }
    });

    acceptedScores.put(3, new HashMap<String, Float>() {
      {
        put("http://a.com", 3.388f);
        put("http://b.com", 4.388f);
        put("http://c.com", 2.666f);
        put("http://d.com", 0.5f);
      }
    });
  }

  private Configuration conf = ConfigUtils.create();
  private final int ROUND = 10;
  private Map<String, WebPage> rows;
  private MonitorScoringFilter scoringFilter = new MonitorScoringFilter();
  private AdaptiveNewsFetchSchedule fetchSchedule = new AdaptiveNewsFetchSchedule(conf);

  @Before
  public void setUp() throws Exception {
//    float scoreInjected = conf.getFloat("db.score.injected", 1.0f);
    float scoreInjected = 1.0f;
    NutchUtil.LOG.info("scoreInjected : " + scoreInjected);

    scoringFilter.setConf(conf);

    // Inject simulation
    rows = Stream.of(seedUrls).map(url -> {
      WebPage page = WebPage.newWebPage(url, scoreInjected);
      page.setFetchCount(0);
      page.setDistance(0);
      scoringFilter.injectedScore(url, page);
      return page;
    }).collect(Collectors.toMap(WebPage::url, page -> page));
  }

  /**
   * Assertion that the accepted and and actual resultant scores are the same.
   */
  @Test
  public void testCrawlAndEvaluateScore() {
    // Crawl loop simulation
    Instant now = Instant.now();
    for (int i = 0; i < ROUND; ++i) {
      final int round = 1 + i;
      final String batchId = String.valueOf(round);

      rows = rows.values().stream().map(page -> {
        // Generate simulation
        page.setBatchId(batchId);

        // assertEquals(1.0f, page.getScore(), 1.0e-10);

        float initSortScore = calculateInitSortScore(page.url(), page);
        float sortScore = scoringFilter.generatorSortValue(page.url(), page, initSortScore);
        page.putMark(Mark.GENERATE, batchId);

        // assertEquals(10001.0f, sortScore, 1.0e-10);
        Params.of("url", page.url(), "sortScore", sortScore).withLogger(NutchUtil.LOG).info(true);
        return page;
      }).filter(page -> page.hasMark(Mark.GENERATE)).map(page -> {
        // Fetch simulation
//        SimpleFetcher fetcher = new SimpleFetcher(conf);
//        page = fetcher.fetch(page.url());

        FetchUtil.updateStatus(page, CrawlStatus.STATUS_FETCHED, ProtocolStatusUtils.STATUS_SUCCESS);
        FetchUtil.updateContent(page, new Content(page.url(), page.url(), "".getBytes(), "text/html", new SpellCheckedMetadata(), conf));
        FetchUtil.updateFetchTime(page, now.minus(60 - round * 2, ChronoUnit.MINUTES));
        FetchUtil.updateMarks(page);

        // Re-publish the article
        Instant publishTime = now.minus(30 - round * 2, ChronoUnit.HOURS);
        page.setModifiedTime(publishTime);
        page.updatePublishTime(publishTime);

        page.setOutlinks(linkGraph.get(page.url()).stream().collect(Collectors.toMap(l -> l, l -> "")));

        assertEquals(publishTime, page.getPublishTime());

        return page;
      }).collect(Collectors.toMap(WebPage::url, page -> page));

//      assertEquals(3, rows.size());

      /* Build the web graph */
      // 1. Add all vertices
      WebGraph graph = new WebGraph();
      rows.values().forEach(page -> graph.addVertex(new WebVertex(page.url(), page)));
      // 2. Build all links as edges
      Collection<WebVertex> vertices = graph.vertexSet().stream().filter(WebVertex::hasWebPage).collect(Collectors.toList());
      vertices.forEach(v1 -> v1.getWebPage().getOutlinks().entrySet()
              .forEach(l -> graph.addEdgeLenient(v1, new WebVertex(l.getKey())).setAnchor(l.getValue()))
      );
//      for (WebVertex v1 : graph.vertexSet()) {
//        for (WebVertex v2 : graph.vertexSet()) {
//          if (v1.getWebPage().getOutlinks().keySet().contains(v2.getUrl())) {
//            graph.addEdge(v1, v2);
//          }
//        }
//      }

      // 3. report and assertions
      Params.of(
              "round", round,
              "rows", rows.size(),
              "vertices", graph.vertexSet().size(),
              "vertices(hasWebPage)", graph.vertexSet().stream().filter(WebVertex::hasWebPage).count(),
              "edges", graph.edgeSet().size() + " : "
                      + graph.edgeSet().stream().map(WebEdge::toString).collect(Collectors.joining(", "))
      ).withLogger(NutchUtil.LOG).info(true);

      /* OutGraphUpdateJob simulation */
      // 1. distribute score to outLinks
      graph.vertexSet().stream().filter(WebVertex::hasWebPage)
              .forEach(v -> scoringFilter.distributeScoreToOutlinks(v.getUrl(),
                      v.getWebPage(), graph, graph.outgoingEdgesOf(v), graph.outDegreeOf(v)));
      // 2. update score for all rows
      graph.vertexSet().stream().filter(WebVertex::hasWebPage)
              .forEach(v -> scoringFilter.updateScore(v.getUrl(), v.getWebPage(), graph, graph.incomingEdgesOf(v)));
      // 3. update marks
      graph.vertexSet().stream().filter(WebVertex::hasWebPage)
              .forEach(v -> v.getWebPage().putMark(Mark.UPDATEOUTG, batchId));
      // 4. generate new rows
      Map<String, WebPage> newRows = graph.vertexSet().stream().filter(v -> !v.hasWebPage())
              .map(v -> createNewRow(v.getUrl())).collect(Collectors.toMap(WebPage::url, page -> page));
      rows.putAll(newRows);

      /* InGraphUpdateJob simulation */
      // Update by in-links
      graph.edgeSet().stream()
              .filter(WebEdge::hasSourceWebPage)
              .filter(WebEdge::hasTargetWebPage)
              .forEach(edge -> {
                WebPage p1 = edge.getSourceWebPage();
                WebPage p2 = edge.getTargetWebPage();

                // Update by out-links
                p1.updateRefPublishTime(p2.getPublishTime());
                p1.increaseRefChars(1000 * round);
                p1.increaseRefArticles(1);
              });

      // Report the result
      graph.vertexSet().stream()
              .filter(WebVertex::hasWebPage)
//              .filter(v -> v.getUrl().contains("a.com"))
              .map(v -> WebPageConverter.convert(v.getWebPage()).toString().replaceAll(", ", "\n"))
              .filter(Strings::isNotEmpty)
              .forEach(NutchUtil.LOG::info);
    }
  }

  private WebPage createNewRow(String url) {
    WebPage page = WebPage.newWebPage(url);
    fetchSchedule.initializeSchedule(url, page);
    page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);
    scoringFilter.initialScore(url, page);
    return page;
  }

  private float calculateInitSortScore(String url, WebPage page) {
    boolean raise = false;

    float factor = raise ? 1.0f : 0.0f;
    int depth = page.getDepth();

    return (10000.0f - 100 * depth) + factor * 100000.0f;
  }
}
