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
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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

  private final static Set<WebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(WebPage.Field.METADATA);
    FIELDS.add(WebPage.Field.SCORE);
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
    TableUtil.setCash(row, score);
  }

  /**
   * Set to 0.0f (unknown value) - inlink contributions will bring it to a
   * correct level. Newly discovered pages have at least one inlink.
   */
  @Override
  public void initialScore(String url, WebPage row) throws ScoringFilterException {
    row.setScore(0.0f);
    TableUtil.setCash(row, 0.0f);
  }

  /** Use {@link WebPage#getScore()}. */
  @Override
  public float generatorSortValue(String url, WebPage row, float initSort) throws ScoringFilterException {
    return initSort + row.getScore();
  }

  /** Increase the score by a sum of inlinked scores. */
  @Override
  public void updateScore(String url, WebPage row, List<ScoreDatum> inlinkedScoreData) {
    float adjust = 0.0f;
    float factor = 1.0f;

    // There is no inlinked score data in updateJIT mode
    for (ScoreDatum scoreDatum : inlinkedScoreData) {
      adjust += scoreDatum.getScore();
    }
    adjust += adjustArticlePageScore(row);

    float oldScore = row.getScore();
    row.setScore(oldScore + adjust);

    float cash = TableUtil.getCash(row);
    TableUtil.setCash(row, cash + factor * adjust);
  }

  /** Get cash on hand, divide it by the number of outlinks and apply. */
  @Override
  public void distributeScoreToOutlinks(String fromUrl, WebPage row, Collection<ScoreDatum> scoreData, int allCount) {
    float cash = TableUtil.getCash(row);
    if (cash == 0) {
      return;
    }

    // TODO: count filtered vs. all count for outlinks
    float scoreUnit = cash / allCount;
    // internal and external score factor
    float internalScore = scoreUnit * internalScoreFactor;
    float externalScore = scoreUnit * externalScoreFactor;
    for (ScoreDatum scoreDatum : scoreData) {
      float score = scoreDatum.getScore();

      try {
        String toHost = new URL(scoreDatum.getUrl()).getHost();
        String fromHost = new URL(fromUrl).getHost();

        if (toHost.equalsIgnoreCase(fromHost)) {
          scoreDatum.setScore(score + internalScore);
        } else {
          scoreDatum.setScore(score + externalScore);
        }
      } catch (MalformedURLException e) {
        LOG.error("Failed with the following MalformedURLException: ", e);
        scoreDatum.setScore(score + externalScore);
      }
    }

    TableUtil.setCash(row, 0.0f);
  }

  private float adjustArticlePageScore(WebPage row) {
    float f1 = 1.0f;
    float f2 = 2.0f;

    long ra = TableUtil.getReferredArticles(row);
    long rc = TableUtil.getReferredChars(row);

    return f1 * ra + f2 * ((rc - 1000) / 1000);
  }

  /** Dampen the boost value by scorePower. */
  public float indexerScore(String url, IndexDocument doc, WebPage row, float initScore) {
    return (float) Math.pow(row.getScore(), scorePower) * initScore;
  }

  @Override
  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }
}
