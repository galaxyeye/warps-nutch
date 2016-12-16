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

package org.apache.nutch.scoring.tld;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexDocument;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WrappedWebPage;
import org.apache.nutch.util.domain.DomainSuffix;
import org.apache.nutch.util.domain.DomainSuffixes;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Scoring filter to boost tlds.
 * 
 * @author Enis Soztutar &lt;enis.soz.nutch@gmail.com&gt;
 */
public class TLDScoringFilter implements ScoringFilter {

  private Configuration conf;
  private DomainSuffixes tldEntries;

  private final static Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  public TLDScoringFilter() {
    tldEntries = DomainSuffixes.getInstance();
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }

  @Override
  public void injectedScore(String url, WrappedWebPage page)
      throws ScoringFilterException {
  }

  @Override
  public void initialScore(String url, WrappedWebPage page)
      throws ScoringFilterException {

  }

  @Override
  public float generatorSortValue(String url, WrappedWebPage page, float initSort)
      throws ScoringFilterException {
    return initSort;
  }

  @Override
  public void distributeScoreToOutlinks(String fromUrl, WrappedWebPage page,
      Collection<ScoreDatum> scoreData, int allCount)
      throws ScoringFilterException {
  }

  @Override
  public void updateScore(String url, WrappedWebPage page,
      List<ScoreDatum> inlinkedScoreData) throws ScoringFilterException {
  }

  @Override
  public float indexerScore(String url, IndexDocument doc, WrappedWebPage page,
      float initScore) throws ScoringFilterException {
    List<Object> tlds = doc.getFieldValues("tld");
    float boost = 1.0f;

    if (tlds != null) {
      for (Object tld : tlds) {
        DomainSuffix entry = tldEntries.get(tld.toString());
        if (entry != null)
          boost *= entry.getBoost();
      }
    }
    return initScore * boost;
  }
}
