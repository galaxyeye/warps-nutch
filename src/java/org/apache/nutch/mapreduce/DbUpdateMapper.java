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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.dbupdate.MapDatumBuilder;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.gora.GoraWebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Metadata.Name.*;
import static org.apache.nutch.metadata.Nutch.PARAM_BATCH_ID;
import static org.apache.nutch.metadata.Nutch.PARAM_GENERATOR_MAX_DISTANCE;
import static org.apache.nutch.storage.Mark.FETCH;

public class DbUpdateMapper extends NutchMapper<String, GoraWebPage, UrlWithScore, NutchWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateMapper.class);

  public enum Counter { rowsMapped, newRowsMapped, notFetched, urlFiltered, tooDeep }

  private Configuration conf;

  private int maxDistance;
  private int maxOutlinks = 1000;

  // reuse writables
  private UrlWithScore urlWithScore = new UrlWithScore();
  private NutchWritable nutchWritable = new NutchWritable();
  private WebPageWritable pageWritable;
  private ScoringFilters scoringFilters;
  private final List<ScoreDatum> scoreData = new ArrayList<>();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    conf = context.getConfiguration();
    pageWritable = new WebPageWritable(context.getConfiguration(), null);

    String crawlId = conf.get(Nutch.PARAM_CRAWL_ID);

    maxDistance = conf.getInt(PARAM_GENERATOR_MAX_DISTANCE, -1);
    scoringFilters = new ScoringFilters(conf);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "maxDistance", maxDistance,
        "maxOutlinks", maxOutlinks
    ));
  }

  /**
   * One row map to several rows
   * */
  @Override
  public void map(String reversedUrl, GoraWebPage row, Context context) throws IOException, InterruptedException {
    getCounter().increase(rows);

    WebPage page = WebPage.wrap(row);

    if (!page.hasMark(FETCH)) {
      getCounter().increase(Counter.notFetched);
      return;
    }

    String url = TableUtil.unreverseUrl(reversedUrl);

    final int depth = page.getDepth();
    if (depth >= maxDistance) {
      getCounter().increase(Counter.tooDeep);
      return;
    }

    scoreData.clear();
    Map<CharSequence, CharSequence> outlinks = page.getOutlinks();
    outlinks.entrySet().forEach(e -> scoreData.add(createScoreDatum(page, e.getKey(), e.getValue(), depth)));
    getCounter().increase(NutchCounter.Counter.outlinks, outlinks.size());

    try {
      scoringFilters.distributeScoreToOutlinks(url, page, scoreData, outlinks.size());
    } catch (ScoringFilterException e) {
      LOG.warn("Distributing score failed for URL: " + reversedUrl + StringUtils.stringifyException(e));
    }

    urlWithScore.setReversedUrl(reversedUrl);
    urlWithScore.setScore(Float.MAX_VALUE);
    pageWritable.setWebPage(page.get());
    nutchWritable.set(pageWritable);
    context.write(urlWithScore, nutchWritable);
    getCounter().increase(Counter.rowsMapped);

    for (ScoreDatum scoreDatum : scoreData) {
      String reversedOut = TableUtil.reverseUrl(scoreDatum.getUrl());
      scoreDatum.setUrl(url); // the referrer page's url
      urlWithScore.setReversedUrl(reversedOut); // out page's url
      urlWithScore.setScore(scoreDatum.getScore()); // out page's init score
      nutchWritable.set(scoreDatum);
      context.write(urlWithScore, nutchWritable);
    }
    getCounter().increase(Counter.newRowsMapped, scoreData.size());
  }

  private ScoreDatum createScoreDatum(WebPage page, CharSequence url, CharSequence anchor, int depth) {
    ScoreDatum scoreDatum = new ScoreDatum(0.0f, url, anchor, depth);
    scoreDatum.addMetadata(PUBLISH_TIME, page.getPublishTime());
    scoreDatum.addMetadata(TMP_CHARS, page.sniffTextLength());
    // scoreDatum.addMetadata("IS_ARTICLE", 1);
    return scoreDatum;
  }
}
