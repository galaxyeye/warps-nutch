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
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.nutch.metadata.Nutch.PARAM_BATCH_ID;

public class DbUpdateMapper2 extends GoraMapper<String, GoraWebPage, UrlWithScore, NutchWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateMapper2.class);

  private ScoringFilters scoringFilters;

  private final List<ScoreDatum> scoreData = new ArrayList<>();

  @SuppressWarnings("unused")
  private Utf8 batchId;

  // reuse writables
  private UrlWithScore urlWithScore = new UrlWithScore();
  private NutchWritable nutchWritable = new NutchWritable();
  private WebPageWritable pageWritable;

  @Override
  public void map(String key, GoraWebPage row, Context context) throws IOException, InterruptedException {
    WebPage page = WebPage.wrap(row);
    if (page.hasMark(Mark.GENERATE)) {
      LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; not generated yet");
      return;
    }

    String url = TableUtil.unreverseUrl(key);

    scoreData.clear();
    Map<CharSequence, CharSequence> outlinks = page.getOutlinks();
    if (outlinks != null) {
      for (Entry<CharSequence, CharSequence> e : outlinks.entrySet()) {
//        int depth = TableUtil.getDepth(page);
        int depth = 0; // wrong, temp
        scoreData.add(new ScoreDatum(0.0f, e.getKey().toString(), e.getValue().toString(), depth));
      }
    }

    // TODO: Outlink filtering (i.e. "only keep the first n outlinks")
    try {
      scoringFilters.distributeScoreToOutlinks(url, page, scoreData, (outlinks == null ? 0 : outlinks.size()));
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL: " + key + " exception:" + StringUtils.stringifyException(e));
    }

    urlWithScore.setReversedUrl(key);
    urlWithScore.setScore(Float.MAX_VALUE);
    pageWritable.setWebPage(page.get());
    nutchWritable.set(pageWritable);
    context.write(urlWithScore, nutchWritable);

    for (ScoreDatum scoreDatum : scoreData) {
      String reversedOut = TableUtil.reverseUrl(scoreDatum.getUrl());
      scoreDatum.setUrl(url); // the referrer page's url
      urlWithScore.setReversedUrl(reversedOut); // out page's url
      urlWithScore.setScore(scoreDatum.getScore()); // out page's init score
      nutchWritable.set(scoreDatum);
      context.write(urlWithScore, nutchWritable);
    }
  }

  @Override
  public void setup(Context context) {
    scoringFilters = new ScoringFilters(context.getConfiguration());
    pageWritable = new WebPageWritable(context.getConfiguration(), null);
    batchId = new Utf8(context.getConfiguration().get(PARAM_BATCH_ID, Nutch.ALL_BATCH_ID_STR));
  }
}
