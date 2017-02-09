/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.samples;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.common.Params;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.schedulers.AdaptiveNewsFetchSchedule;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.graph.WebVertex;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.ConfigUtils;
import org.apache.nutch.util.NutchUtil;
import org.apache.nutch.util.URLUtil;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimpleCrawler {

    private Configuration conf;
    private ScoringFilters scoringFilters;
    private AdaptiveNewsFetchSchedule fetchSchedule;
    private Map<String, WebPage> rows;

    public SimpleCrawler(Configuration conf) {
        this.conf = conf;
        this.scoringFilters = new ScoringFilters(conf);
        this.fetchSchedule = new AdaptiveNewsFetchSchedule(conf);
    }

    public void inject(String... seedUrls) throws Exception {
        float scoreInjected = 1.0f;
        NutchUtil.LOG.info("scoreInjected : " + scoreInjected);

        // Inject simulation
        rows = Stream.of(seedUrls).map(url -> {
            WebPage page = WebPage.newWebPage(url, scoreInjected);
            page.setFetchCount(0);
            page.setDistance(0);
            scoringFilters.injectedScore(url, page);
            return page;
        }).collect(Collectors.toMap(WebPage::url, page -> page));
    }

    /**
     * Assertion that the accepted and and actual resultant scores are the same.
     */
    public void crawl(final int depth) {
        // Crawl loop simulation
        for (int i = 0; i < depth; ++i) {
            crawlOneLoop(i + 1);
        }
    }

    private void crawlOneLoop(int round) {
        // Crawl loop simulation
        final String batchId = String.valueOf(round);

        rows = rows.values().stream().map(page -> {
            // Generate simulation
            page.setBatchId(batchId);

            float initSortScore = calculateInitSortScore(page.url(), page);
            float sortScore = 0;
            sortScore = scoringFilters.generatorSortValue(page.url(), page, initSortScore);
            page.putMark(Mark.GENERATE, batchId);

            Params.of("url", page.url(), "sortScore", sortScore).withLogger(NutchUtil.LOG).info(true);
            return page;
        }).filter(page -> page.hasMark(Mark.GENERATE)).map(page -> {
            // Fetch simulation
            SimpleParser parser = new SimpleParser(conf);
            parser.parse(page.url());
            page = parser.getWebPage();

            return page;
        }).collect(Collectors.toMap(WebPage::url, page -> page));

            /* Build the web graph */
        // 1. Add all vertices
        WebGraph graph = new WebGraph();
        rows.values().forEach(page -> graph.addVertex(new WebVertex(page.url(), page)));
        // 2. Build all links as edges
        Collection<WebVertex> vertices = graph.vertexSet().stream().filter(WebVertex::hasWebPage).collect(Collectors.toList());
        vertices.forEach(v1 -> v1.getWebPage().getOutlinks().entrySet()
                .forEach(l -> graph.addEdgeLenient(v1, new WebVertex(l.getKey())).setAnchor(l.getValue()))
        );

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
                .forEach(v -> scoringFilters.distributeScoreToOutlinks(v.getUrl(),
                        v.getWebPage(), graph, graph.outgoingEdgesOf(v), graph.outDegreeOf(v)));
        // 2. update score for all rows
        graph.vertexSet().stream().filter(WebVertex::hasWebPage)
                .forEach(v -> scoringFilters.updateScore(v.getUrl(), v.getWebPage(), graph, graph.incomingEdgesOf(v)));
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
//        graph.vertexSet().stream()
//                .filter(WebVertex::hasWebPage)
//                .map(v -> WebPageConverter.convert(v.getWebPage()).toString().replaceAll(", ", "\n"))
//                .filter(Strings::isNotEmpty)
//                .forEach(NutchUtil.LOG::info);
    }

    private WebPage createNewRow(String url) {
        WebPage page = WebPage.newWebPage(url);
        fetchSchedule.initializeSchedule(url, page);
        page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);
        scoringFilters.initialScore(url, page);
        return page;
    }

    private float calculateInitSortScore(String url, WebPage page) {
        boolean raise = false;

        float factor = raise ? 1.0f : 0.0f;
        int depth = page.getDepth();

        return (10000.0f - 100 * depth) + factor * 100000.0f;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage : SimpleCrawler [-depth <depth>] url");
            System.exit(0);
        }

        int depth = 1;
        String url = null;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-depth")) {
                depth = Integer.parseInt(args[++i]);
            } else {
                url = URLUtil.toASCII(args[i]);
            }
        }

        if (url == null) {
            System.out.println("Usage : SimpleCrawler [-depth <depth>] url");
            System.exit(0);
        }

        SimpleCrawler crawler = new SimpleCrawler(ConfigUtils.create());
        crawler.inject(url);
        crawler.crawl(depth);
    }
}
