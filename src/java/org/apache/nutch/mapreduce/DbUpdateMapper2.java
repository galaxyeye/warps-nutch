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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.graph.io.WebEdgeWritable;
import org.apache.nutch.mapreduce.io.WebPageWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.persist.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.graph.GraphGroupKey;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.nutch.metadata.Nutch.PARAM_BATCH_ID;

public class DbUpdateMapper2 extends GoraMapper<String, GoraWebPage, GraphGroupKey, NutchWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateMapper2.class);

  private ScoringFilters scoringFilters;

  private final List<WebEdge> scoreData = new ArrayList<>();

  @SuppressWarnings("unused")
  private Utf8 batchId;

  private Configuration conf;
  // reuse writables
  private GraphGroupKey subGraphGroupKey = new GraphGroupKey();
  private NutchWritable nutchWritable = new NutchWritable();
  private WebPageWritable pageWritable;

  @Override
  public void map(String key, GoraWebPage row, Context context) throws IOException, InterruptedException {
    WebPage page = WebPage.wrap(row);
    if (!page.hasMark(Mark.GENERATE)) {
      LOG.debug("Skipping " + TableUtil.unreverseUrl(key) + "; not generated yet");
      return;
    }

    String url = TableUtil.unreverseUrl(key);

    scoreData.clear();
    Map<CharSequence, CharSequence> outlinks = page.getOutlinks();
    if (outlinks != null) {
      for (Entry<CharSequence, CharSequence> e : outlinks.entrySet()) {
//        int depth = TableUtil.getDepth(page);
//        int depth = 0; // wrong, temp
//        scoreData.add(new WebEdge(url, e.getKey(), e.getValue(), 0.0f, depth));
      }
    }

    // TODO: Outlink filtering (i.e. "only keep the first n outlinks")
    try {
      scoringFilters.distributeScoreToOutlinks(url, page, scoreData, (outlinks == null ? 0 : outlinks.size()));
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL: " + key + " exception:" + StringUtils.stringifyException(e));
    }

    subGraphGroupKey.setReversedUrl(key);
    subGraphGroupKey.setScore(Float.MAX_VALUE);
    pageWritable.setWebPage(page);
    nutchWritable.set(pageWritable);
    context.write(subGraphGroupKey, nutchWritable);

    for (WebEdge edgeDatum : scoreData) {
      String reversedOut = TableUtil.reverseUrl(edgeDatum.getTarget().getUrl());
//      edgeDatum.setSourceUrl(url); // the referrer page's url
      subGraphGroupKey.setReversedUrl(reversedOut); // out page's url
      subGraphGroupKey.setScore(edgeDatum.getWeight()); // out page's init score
      nutchWritable.set(new WebEdgeWritable(edgeDatum, conf));
      context.write(subGraphGroupKey, nutchWritable);
    }
  }

  @Override
  public void setup(Context context) {
    scoringFilters = new ScoringFilters(context.getConfiguration());
    pageWritable = new WebPageWritable(context.getConfiguration(), null);
    batchId = new Utf8(context.getConfiguration().get(PARAM_BATCH_ID, Nutch.ALL_BATCH_ID_STR));
  }
}
