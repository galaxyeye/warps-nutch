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

import org.apache.nutch.crawl.GenerateJob.SelectorEntry;
import org.apache.nutch.crawl.filters.CrawlFilter;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.mapreduce.NutchMapper;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;

public class GenerateMapper extends NutchMapper<String, WebPage, SelectorEntry, WebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(GenerateMapper.class);

  private enum Counter {
    rows, rowsAfterFinished, rowsInjected, rowsBeforeStart, rowsNotInRange, pagesAlreadyGenerated,
    pagesTooFarAway, rowsNormalisedToNull, rowsFiltered, pagesFetchLater
  };

  private URLFilters filters;
  private URLNormalizers normalizers;
  private CrawlFilters crawlFilters;
  private boolean filter;
  private boolean normalise;
  private FetchSchedule fetchSchedule;
  private ScoringFilters scoringFilters;
  private long pseudoCurrTime;
  private SelectorEntry entry = new SelectorEntry();
  private long topN;
  private int maxDistance;
  private String[] keyRange;
  private boolean ignoreAlreadyGenerated = true;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);
    String fetchMode = conf.get(Nutch.FETCH_MODE_KEY);

    filter = conf.getBoolean(Nutch.GENERATOR_FILTER, true);
    if (filter) {
      filters = new URLFilters(conf);
    }

    normalise = conf.getBoolean(Nutch.GENERATOR_NORMALISE, true);
    if (normalise) {
      normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
    }

    maxDistance = conf.getInt(Nutch.GENERATOR_MAX_DISTANCE, -1);
    pseudoCurrTime = conf.getLong(Nutch.GENERATOR_CUR_TIME, startTime);
    topN = conf.getLong(Nutch.GENERATOR_TOP_N, Long.MAX_VALUE);
    // topN /= job.numReducerTasks();
    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    crawlFilters = CrawlFilters.create(conf);
    keyRange = crawlFilters.getMaxReversedKeyRange();
    ignoreAlreadyGenerated = conf.getBoolean(Nutch.GENERATOR_IGNORE_GENERATED, false);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "fetchMode", fetchMode,
        "filter", filter,
        "topN", topN,
        "normalise", normalise,
        "maxDistance", maxDistance,
        "pseudoCurrTime", TimingUtil.format(pseudoCurrTime),
        "fetchSchedule", fetchSchedule.getClass().getName(),
        "scoringFilters", scoringFilters.getClass().getName(),
        "crawlFilters", crawlFilters,
        "keyRange", keyRange[0] + " - " + keyRange[1],
        "ignoreAlreadyGenerated", ignoreAlreadyGenerated
    ));
  }

  @Override
  public void map(String reversedUrl, WebPage page, Context context) throws IOException, InterruptedException {
    String url = TableUtil.unreverseUrl(reversedUrl);

    // Page is injected, this is a seed url and has the highest fetch priority
    // INJECT_MARK will be removed in DbUpdate stage
    if (Mark.INJECT_MARK.hasMark(page)) {
      entry.set(url, Float.MAX_VALUE);
      context.write(entry, page);

      // LOG.debug("Generate injected url : " + url);
      getCounter().increase(Counter.rowsInjected);

      return;
    }

    if (Mark.GENERATE_MARK.hasMark(page)) {
      getCounter().increase(Counter.pagesAlreadyGenerated);

      /**
       * Fetch entries are generated, empty webpage entries are created in the database(HBase)
       * case 1. another fetcher job is fetching the generated batch. In this case, we should not generate it.
       * case 2. another fetcher job handled the generated batch, but failed, which means the pages are not fetched.
       *
       * There are three ways to fetch pages that are generated but not fetched nor fetching.
       * 1. Restart a crawl with ignoreAlreadyGenerated set to be false
       * 2. Resume a FetchJob with resume set to be true
       * */
      if (ignoreAlreadyGenerated) {
        // LOG.debug("Skipping {}; already generated", url);

        // TODO : check generate time, if the generate time is too old, we generate it again
        // TableUtil.putMetadata(page, "Nutch-GenerateTime", String.valueOf(startTime));
        String generateTimeStr = TableUtil.getMetadata(page, Nutch.GENERATOR_GENERATE_TIME);
        long generateTime = StringUtil.tryParseLong(generateTimeStr, -1);

        if (generateTime > 0 && generateTime < TimingUtil.DAY) {
          return;
        }
      }
    }

    // Filter on distance
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

    // Url filter
    if (!shouldProcess(reversedUrl)) {
      return;
    }

    // Fetch schedule, timing filter
    if (!fetchSchedule.shouldFetch(url, page, pseudoCurrTime)) {
      if (LOG.isDebugEnabled()) {
        // LOG.debug("Fetch later '" + url + "', fetchTime=" + page.getFetchTime() + ", curTime=" + pseudoCurrTime);
      }

      // getCounter().increase("pagesFetchLater");
      getCounter().increase(Counter.pagesFetchLater);

      return;
    }

    float score = page.getScore();
    try {
      // Typically, we use OPIC scoring filter,
      score = scoringFilters.generatorSortValue(url, page, score);
    } catch (ScoringFilterException e) {
      // ignore
    }

    // Generate time, we will use this mark to decide if we re-generate this page
    TableUtil.putMetadata(page, Nutch.GENERATOR_GENERATE_TIME, String.valueOf(startTime));

    // Sort by score
    entry.set(url, score);
    context.write(entry, page);

    getCounter().increase(Counter.rows);
  }

  // Url filter
  private boolean shouldProcess(String reversedUrl) {
    // TODO : CrawlFilter may be move to be a plugin
    // key before start key
    if (!CrawlFilter.keyGreaterEqual(reversedUrl, keyRange[0])) {
      getCounter().increase(Counter.rowsBeforeStart);
      return false;
    }

    // key after end key, finish the mapper
    if (!CrawlFilter.keyLessEqual(reversedUrl, keyRange[1])) {
      stop("Complete mapper, reason : hit end key " + reversedUrl
          + ", upper bound : " + keyRange[1]
          + ", diff : " + reversedUrl.compareTo(keyRange[1]));
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
