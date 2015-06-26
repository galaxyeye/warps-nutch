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
import java.net.MalformedURLException;

import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.crawl.filters.CrawlFilter;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.mapreduce.NutchMapper;
import org.apache.nutch.mapreduce.NutchUtil;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratorMapper extends NutchMapper<String, WebPage, SelectorEntry, WebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(GeneratorMapper.class);

  private static enum Counter {
    rows, rowsAfterFinished, rowsInjected, rowsBeforeStart, rowsNotInRange, pagesAlreadyGenerated, 
    pagesTooFarAway, rowsNormalisedToNull, rowsFiltered, pagesFetchLater
  };

  private URLFilters filters;
  private URLNormalizers normalizers;
  private CrawlFilters crawlFilters;
  private boolean filter;
  private boolean normalise;
  private FetchSchedule schedule;
  private ScoringFilters scoringFilters;
  private long pseudoCurrTime;
  private SelectorEntry entry = new SelectorEntry();
  private long topN;
  private int maxDistance;
  private String[] keyRange;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    filter = conf.getBoolean(Nutch.GENERATOR_FILTER, true);
    if (filter) {
      filters = new URLFilters(conf);
    }

    normalise = conf.getBoolean(Nutch.GENERATOR_NORMALISE, true);
    if (normalise) {
      normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
    }

    maxDistance = conf.getInt("generate.max.distance", -1);
    pseudoCurrTime = conf.getLong(Nutch.GENERATOR_CUR_TIME, startTime);
    topN = conf.getLong(Nutch.GENERATOR_TOP_N, Long.MAX_VALUE);
    // topN /= job.numReducerTasks();
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    crawlFilters = CrawlFilters.create(conf);
    keyRange = crawlFilters.getMaxReversedKeyRange();

    LOG.info(NutchUtil.printArgMap(
        "filter", filter,
        "topN", topN,
        "normalise", normalise,
        "maxDistance", maxDistance,
        "pseudoCurrTime", TimingUtil.format(pseudoCurrTime),
        "schedule", schedule.getClass().getName(),
        "scoringFilters", scoringFilters.getClass().getName(),
        "crawlFilters", crawlFilters,
        "keyRange", keyRange[0] + " - " + keyRange[1]
    ));
  }

  @Override
  public void map(String reversedUrl, WebPage page, Context context) throws IOException, InterruptedException {
    String url = TableUtil.unreverseUrl(reversedUrl);

    // Page is injected, this is a seed url and has the highest fetch priority
    if (Mark.INJECT_MARK.hasMark(page)) {
      entry.set(url, Float.MAX_VALUE);
      context.write(entry, page);

      // LOG.debug("Generate injected url : " + url);
      getCounter().increase(Counter.rowsInjected);

      return;
    }

    if (Mark.GENERATE_MARK.hasMark(page)) {
      // LOG.debug("Skipping {}; already generated", url);
      getCounter().increase(Counter.pagesAlreadyGenerated);
      return;
    }

    // filter on distance
    if (maxDistance > -1) {
      CharSequence distanceUtf8 = page.getMarkers().get(Nutch.DISTANCE);
      if (distanceUtf8 != null) {
        int distance = Integer.parseInt(distanceUtf8.toString());
        if (distance > maxDistance) {
          getCounter().increase(Counter.pagesTooFarAway);
          return;
        }
      }
    }

    // url filter
    if (!shouldProcess(reversedUrl)) {
      return;
    }

    // check fetch schedule
    if (!schedule.shouldFetch(url, page, pseudoCurrTime)) {
      if (LOG.isDebugEnabled()) {
        // LOG.debug("Fetch later '" + url + "', fetchTime=" + page.getFetchTime() + ", curTime=" + pseudoCurrTime);
      }

      // getCounter().increase("pagesFetchLater");
      getCounter().increase(Counter.pagesFetchLater);

      return;
    }

    float score = page.getScore();
    try {
      score = scoringFilters.generatorSortValue(url, page, score);
    } catch (ScoringFilterException e) {
      // ignore
    }

    entry.set(url, score);
    context.write(entry, page);

    getCounter().increase(Counter.rows);

    // TODO : use topN
    if (getCounter().get(Counter.rows) > topN) {
      stop("Mapped enough recoreds, hit topN : " + topN);
    }
  }

  private boolean shouldProcess(String reversedUrl) {
    // TODO : CrawlFilter may be move to be a plugin
    // key before start key
    if (!CrawlFilter.keyGreaterEqual(reversedUrl, keyRange[0])) {
      getCounter().increase(Counter.rowsBeforeStart);
      return false;
    }

    // key after end key, finish the mapper
    if (!CrawlFilter.keyLessEqual(reversedUrl, keyRange[1])) {
      stop("Complete mapper, reason : hit end key " + reversedUrl + ", upper bound : " + keyRange[1] + 
          ", diff : " + reversedUrl.compareTo(keyRange[1]));
      return false;
    }

    // key not fall in key ranges
    if (!crawlFilters.testKeyRangeSatisfied(reversedUrl)) {
      getCounter().increase(Counter.rowsNotInRange);
      return false;
    }

    String url = TableUtil.unreverseUrl(reversedUrl);

    // If filtering is on don't generate URLs that don't pass URLFilters
    try {
      if (normalise) {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
      }

      if (url == null) {
        getCounter().increase(Counter.rowsNormalisedToNull);
        return false;
      }

      if (filter && filters.filter(url) == null) {
        return false;
      }
    } catch (URLFilterException | MalformedURLException e) {
      LOG.warn("Filter failed, url: {} \n {}", url, e.getMessage());
      return false;
    }

    return true;
  }
}
