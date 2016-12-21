/*
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
package org.apache.nutch.scoring.link;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.persist.graph.Edge;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LinkAnalysisScoringFilter implements ScoringFilter {

  private Configuration conf;
  private float normalizedScore = 1.00f;

  private final static Set<GoraWebPage.Field> FIELDS = new HashSet<>();

  static {
    FIELDS.add(GoraWebPage.Field.METADATA);
    FIELDS.add(GoraWebPage.Field.SCORE);
  }

  public LinkAnalysisScoringFilter() {
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    normalizedScore = conf.getFloat("link.analyze.normalize.score", 1.00f);
  }

  @Override
  public Collection<GoraWebPage.Field> getFields() {
    return FIELDS;
  }

  @Override
  public void injectedScore(String url, WebPage page)
      throws ScoringFilterException {
  }

  @Override
  public void initialScore(String url, WebPage page)
      throws ScoringFilterException {
    page.setScore(0.0f);
  }

  @Override
  public float generatorSortValue(String url, WebPage page, float initSort)
      throws ScoringFilterException {
    return page.getScore() * initSort;
  }

  @Override
  public void distributeScoreToOutlinks(String fromUrl, WebPage page,
                                        Collection<Edge> scoreData, int allCount)
      throws ScoringFilterException {
  }

  @Override
  public void updateScore(String url, WebPage page,
      List<Edge> inlinkedScoreData) throws ScoringFilterException {
  }

  @Override
  public float indexerScore(String url, IndexDocument doc, WebPage page,
      float initScore) throws ScoringFilterException {
    return (normalizedScore * page.getScore());
  }

}
