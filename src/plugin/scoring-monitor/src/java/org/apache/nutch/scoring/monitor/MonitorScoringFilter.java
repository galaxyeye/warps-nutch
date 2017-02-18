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

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.WebGraph;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.scoring.ScoringFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * This plugin implements a variant of an Online Page Importance Computation
 * (OPIC) score, described in this paper: <a
 * href="http://www2003.org/cdrom/papers/refereed/p007/p7-abiteboul.html"/>
 * Abiteboul, Serge and Preda, Mihai and Cobena, Gregory (2003), Adaptive
 * On-Line Page Importance Computation </a>.
 * 
 * @author Andrzej Bialecki
 */
public class MonitorScoringFilter implements ScoringFilter {

  private final static Logger LOG = LoggerFactory.getLogger(MonitorScoringFilter.class);

  private final static Set<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.METADATA);
    FIELDS.add(GoraWebPage.Field.SCORE);
  }

  private Configuration conf;
  private float scorePower;
  private float internalScoreFactor;
  private float externalScoreFactor;

  public MonitorScoringFilter() { super(); }

  public MonitorScoringFilter(Configuration conf) {
    super();
    setConf(conf);
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    scorePower = conf.getFloat("indexer.score.power", 0.5f);
    internalScoreFactor = conf.getFloat("db.score.link.internal", 1.0f);
    externalScoreFactor = conf.getFloat("db.score.link.external", 1.0f);
  }

  @Override
  public void injectedScore(String url, WebPage page) {
    float score = page.getScore();
    page.setCash(score);
  }

  /**
   * Set to 0.0f (unknown value) - inlink contributions will bring it to a
   * correct level. Newly discovered pages have at least one inlink.
   */
  @Override
  public void initialScore(String url, WebPage row) {
    row.setScore(0.0f);
    row.setCash(0.0f);
  }

  /** Use {@link WebPage#getScore()}. */
  @Override
  public float generatorSortValue(String url, WebPage row, float initSort) {
    return initSort + row.getScore();
  }

  /** Increase the score by a sum of inlinked scores. */
  @Override
  public void updateScore(String url, WebPage page, WebGraph graph, Collection<WebEdge> inLinkEdges) {
    float factor1 = 1.0f;
    float inLinkScore = 0.0f;
    for (WebEdge edge : inLinkEdges) {
      if (!edge.isLoop()) {
        inLinkScore += graph.getEdgeWeight(edge);
      }
    }

    float factor2 = 1.2f;
    float oldArticleScore = page.getContentScore();
    float newArticleScore = calculateArticleScore(page);
    float articleScoreDelta = newArticleScore - oldArticleScore;
//    if (articleScoreDelta < 0) {
//      LOG.debug("Nagtive article score, the article might be too old : " + newArticleScore + ", " + url);
//    }

    page.setContentScore(newArticleScore);
    page.setScore(page.getScore() + factor1 * inLinkScore + factor2 * articleScoreDelta);
    page.setCash(page.getCash() + factor1 * inLinkScore);
  }

  /** Get cash on hand, divide it by the number of outlinks and apply. */
  @Override
  public void distributeScoreToOutlinks(String fromUrl, WebPage page, WebGraph graph, Collection<WebEdge> outgoingEdges, int allCount) {
    float cash = page.getCash();
    if (cash == 0) {
      return;
    }

    // TODO: count filtered vs. all count for outlinks
    float scoreUnit = cash / allCount;
    // internal and external score factor
    float internalScore = scoreUnit * internalScoreFactor;
    float externalScore = scoreUnit * externalScoreFactor;
    for (WebEdge edge : outgoingEdges) {
      if (edge.isLoop()) {
        continue;
      }

      double score = graph.getEdgeWeight(edge);

      try {
        String toHost = new URL(edge.getTarget().getUrl()).getHost();
        String fromHost = new URL(fromUrl).getHost();

        if (toHost.equalsIgnoreCase(fromHost)) {
          graph.setEdgeWeight(edge, score + internalScore);
        } else {
          graph.setEdgeWeight(edge, score + externalScore);
        }
      } catch (MalformedURLException e) {
        LOG.error("Failed with the following MalformedURLException: ", e);
        graph.setEdgeWeight(edge, score + externalScore);
      }
    }

    page.setCash(0.0f);
  }

  private float calculateArticleScore(WebPage row) {
    float f1 = 1.2f;
    float f2 = 1.0f;
    float f3 = 1.2f;

    // Each article contributes 2 base score
    long ra = row.getRefArticles();
    // Each one thousand chars contributes another 1 base score
    long rc = row.getRefChars();
    // Each day descreases 1 base score
    long rptd = ChronoUnit.DAYS.between(row.getRefContentPublishTime(), Instant.now());
    if (rptd > 30) {
      // Invalid ref publish time, descrease 1 base score
      rptd = 1;
    }

    return f1 * ra + f2 * (rc / 1000) - f3 * rptd;
  }

  /** Dampen the boost value by scorePower. */
  public float indexerScore(String url, IndexDocument doc, WebPage row, float initScore) {
    return (float) Math.pow(row.getScore(), scorePower) * initScore;
  }

  @Override
  public Collection<GoraWebPage.Field> getFields() {
    return FIELDS;
  }
}
