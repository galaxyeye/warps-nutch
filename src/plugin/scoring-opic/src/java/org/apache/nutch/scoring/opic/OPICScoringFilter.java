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

package org.apache.nutch.scoring.opic;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
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
public class OPICScoringFilter implements ScoringFilter {

  private final static Logger LOG = LoggerFactory.getLogger(OPICScoringFilter.class);

  private final static Set<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.METADATA);
    FIELDS.add(GoraWebPage.Field.SCORE);
  }

  private Configuration conf;
  private float scorePower;
  private float internalScoreFactor;
  private float externalScoreFactor;

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
  public void injectedScore(String url, WebPage row) throws ScoringFilterException {
    float score = row.getScore();
    row.setCash(score);
  }

  /**
   * Set to 0.0f (unknown value) - inlink contributions will bring it to a
   * correct level. Newly discovered pages have at least one inlink.
   */
  @Override
  public void initialScore(String url, WebPage row) throws ScoringFilterException {
    row.setScore(0.0f);
    row.setCash(0.0f);
  }

  /** Use {@link WebPage#getScore()}. */
  @Override
  public float generatorSortValue(String url, WebPage row, float initSort) throws ScoringFilterException {
    return initSort + row.getScore();
  }

  /** Increase the score by a sum of inlinked scores. */
  @Override
  public void updateScore(String url, WebPage row, Collection<WebEdge> inlinkedScoreData) {
    float addInlinkedScore = 0.0f;
    float factor1 = 1.0f;
    float articleScoreDelta = updateArticleScore(row);
    float factor2 = 2.0f;

    // There is no inlinked score data in updateJIT mode
    for (WebEdge edge : inlinkedScoreData) {
      addInlinkedScore += edge.getScore();
    }

    row.setScore(row.getScore() + addInlinkedScore + articleScoreDelta);
    row.setCash(row.getCash() + factor1 * addInlinkedScore);
  }

  /** Get cash on hand, divide it by the number of outlinks and apply. */
  @Override
  public void distributeScoreToOutlinks(String fromUrl, WebPage row, Collection<WebEdge> scoreData, int allCount) {
    float cash = row.getCash();
    if (cash == 0) {
      return;
    }

    // TODO: count filtered vs. all count for outlinks
    float scoreUnit = cash / allCount;
    // internal and external score factor
    float internalScore = scoreUnit * internalScoreFactor;
    float externalScore = scoreUnit * externalScoreFactor;
    for (WebEdge edge : scoreData) {
      double score = edge.getScore();

      try {
        String toHost = new URL(edge.getTarget().getUrl()).getHost();
        String fromHost = new URL(fromUrl).getHost();

        if (toHost.equalsIgnoreCase(fromHost)) {
          edge.setScore(score + internalScore);
        } else {
          edge.setScore(score + externalScore);
        }
      } catch (MalformedURLException e) {
        LOG.error("Failed with the following MalformedURLException: ", e);
        edge.setScore(score + externalScore);
      }
    }

    row.setCash(0.0f);
  }

  private float updateArticleScore(WebPage row) {
    float oldScore = row.getArticleScore();

    float f1 = 2.0f;
    float f2 = 1.0f;
    float f3 = 2.0f;

    // Each article contributes 2 base score
    long ra = row.getRefArticles();
    // Each one thousand chars contributes another 1 base score
    long rc = row.getRefChars();
    // Each day descreases 1 base score
    long rptd = ChronoUnit.DAYS.between(row.getRefPublishTime(), Instant.now());
    if (rptd > 30) {
      // Invalid ref publish time, descrease 1 base score
      rptd = 1;
    }

    float newScore = f1 * ra + f2 * (rc / 1000) - f3 * rptd;

    row.setArticleScore(newScore);

    return newScore - oldScore;
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
