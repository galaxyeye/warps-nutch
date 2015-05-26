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
package org.apache.nutch.crawl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.util.Utf8;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.mapreduce.NutchMapper;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.mapreduce.WebPageWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbUpdateMapper extends NutchMapper<String, WebPage, UrlWithScore, NutchWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateMapper.class);

  public static final String UPDATE_FILTER = "update.filter";

  public static enum Counter {
    rows, errors, outlinkCount, notFetched
  };

  private ScoringFilters scoringFilters;

  private final List<ScoreDatum> scoreData = new ArrayList<ScoreDatum>();

  private Utf8 batchId;

  // reuse writables
  private UrlWithScore urlWithScore = new UrlWithScore();
  private NutchWritable nutchWritable = new NutchWritable();
  private WebPageWritable pageWritable;
  private boolean filter;
  private URLFilters filters;
  private CrawlFilters crawlFilters;
  private boolean pageRankEnabled = false;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    scoringFilters = new ScoringFilters(conf);
    pageWritable = new WebPageWritable(conf, null);
    batchId = new Utf8(conf.get(Nutch.BATCH_NAME_KEY, Nutch.ALL_BATCH_ID_STR));

    pageRankEnabled = conf.getBoolean("db.score.pagerank.enabled", false);
    if (!pageRankEnabled) {
      // Only when we disable page rank algorithm,
      // we need and can filter out interrelated urls

      filter = conf.getBoolean(UPDATE_FILTER, true);
      if (filter) {
        filters = new URLFilters(conf);
      }
    }
    crawlFilters = CrawlFilters.create(conf);

    LOG.info(NutchUtil.printArgMap(
        "batchId", batchId, 
        "filter", filter, 
        "pageRankEnabled", pageRankEnabled
    ));
  }

  @Override
  public void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
    String url = TableUtil.unreverseUrl(key);

//    LOG.debug(url);

    if (!shouldProcess(url, key, page)) {
      return;
    }

    float score = calculatePageScore(url, page);
    urlWithScore.setUrl(key);
    urlWithScore.setScore(score);
    pageWritable.setWebPage(page);
    nutchWritable.set(pageWritable);
    context.write(urlWithScore, nutchWritable);

    getCounter().increase(Counter.rows);

    createNewPageFromOutlinks(url, page, context);
  }

  private boolean shouldProcess(String url, String reversedUrl, WebPage page) {
    if (!Mark.FETCH_MARK.hasMark(page)) {
      if (LOG.isDebugEnabled()) {
        getCounter().increase(Counter.notFetched);
        // LOG.debug("Skipping " + url + "; not fetched yet");
      }

      return false;
    }

    return true;
  }

  /**
   * Generate new Pages from outlinks
   * */
  private void createNewPageFromOutlinks(String url, WebPage page, Context context) throws IOException, InterruptedException {
    scoreData.clear();
    Map<CharSequence, CharSequence> outlinks = page.getOutlinks();
    if (outlinks != null) {
      for (Entry<CharSequence, CharSequence> e : outlinks.entrySet()) {
        int depth = Integer.MAX_VALUE;
        CharSequence depthUtf8 = page.getMarkers().get(Nutch.DISTANCE);

        if (depthUtf8 != null) {
          depth = Integer.parseInt(depthUtf8.toString());
        }

        scoreData.add(new ScoreDatum(0.0f, e.getKey().toString(), e.getValue().toString(), depth));
      }
    }

    // TODO : Outlink filtering (i.e. "only keep the first n outlinks")
    try {
      scoringFilters.distributeScoreToOutlinks(url, page, scoreData, (outlinks == null ? 0 : outlinks.size()));
    } catch (ScoringFilterException e) {
      getCounter().increase(Counter.errors);
      LOG.warn("Distributing score failed for URL: " + url + " exception:" + 
          org.apache.hadoop.util.StringUtils.stringifyException(e));
    }

    /**
     * vincent annotation : the code is used to calculate page scores for each outlink of this page
     * in our case, we are not interested in the weight of each page,
     * we need the crawl goes faster, so we disable page rank by default
     * */
    int outlinkCount = 0;
    for (ScoreDatum scoreDatum : scoreData) {
      ++outlinkCount;
//      LOG.debug("Create new page, url : " + url + " outUrl : " + scoreDatum.getUrl());

      String reversedOut = TableUtil.reverseUrl(scoreDatum.getUrl());
      scoreDatum.setUrl(url);
      urlWithScore.setUrl(reversedOut);
      urlWithScore.setScore(scoreDatum.getScore());
      nutchWritable.set(scoreDatum);

      context.write(urlWithScore, nutchWritable);
    }

    getCounter().increase(Counter.outlinkCount, outlinkCount);
  }

  /**
   * TODO : This is a temporary solution, we should use score filters
   * basically, if a page looks more like a detail page,
   * the page should be fetched with higher priority
   * @author vincent
   * */
  private float calculatePageScore(String url, WebPage page) {
    float factor = 1.0f;

    if (crawlFilters.isDetailUrl(url)) {
      factor = 10000.0f;
    }

    return pageRankEnabled ? Float.MAX_VALUE : 1.0f * factor;
  }
}
