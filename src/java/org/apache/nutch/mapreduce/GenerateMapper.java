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

import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.mapreduce.GenerateJob.SelectorEntry;
import org.apache.nutch.crawl.filters.CrawlFilter;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;

import static org.apache.nutch.metadata.Metadata.META_IS_SEED;
import static org.apache.nutch.metadata.Nutch.*;

public class GenerateMapper extends NutchMapper<String, WebPage, SelectorEntry, WebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(GenerateMapper.class);

  private enum Counter {
    rows, rowsAfterFinished, rowsInjected, rowsIsSeed, rowsBeforeStart, rowsNotInRange, pagesAlreadyGenerated,
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
  private boolean ignoreGenerated = true;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    String crawlId = conf.get(PARAM_CRAWL_ID);
    String fetchMode = conf.get(PARAM_FETCH_MODE);
    ignoreGenerated = conf.getBoolean(PARAM_IGNORE_GENERATED, false);

    filter = conf.getBoolean(PARAM_GENERATE_FILTER, true);
    if (filter) {
      filters = new URLFilters(conf);
    }

    normalise = conf.getBoolean(PARAM_GENERATE_NORMALISE, true);
    if (normalise) {
      normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
    }

    maxDistance = conf.getInt(PARAM_GENERATOR_MAX_DISTANCE, -1);
    pseudoCurrTime = conf.getLong(PARAM_GENERATOR_CUR_TIME, startTime);
    topN = conf.getLong(PARAM_GENERATOR_TOP_N, Long.MAX_VALUE);

    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    crawlFilters = CrawlFilters.create(conf);
    keyRange = crawlFilters.getMaxReversedKeyRange();

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
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
        "ignoreGenerated", ignoreGenerated
    ));
  }

  @Override
  public void map(String reversedUrl, WebPage page, Context context) throws IOException, InterruptedException {
    String url = TableUtil.unreverseUrl(reversedUrl);

    /**
     * [Temporary Strategy] Now we crawl seed every time we start, AdaptiveScheduler is just works
     * */
    String isSeed = TableUtil.getMetadata(page, META_IS_SEED);
    if (isSeed != null) {
      entry.set(url, Float.MAX_VALUE);
      context.write(entry, page);

      // LOG.debug("Generate injected url : " + url);
      getCounter().increase(Counter.rowsIsSeed);

      return;
    }

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
       * 1. Restart a crawl with ignoreGenerated set to be false
       * 2. Resume a FetchJob with resume set to be true
       * */
      if (ignoreGenerated) {
        // LOG.debug("Skipping {}; already generated", url);
        String generateTimeStr = TableUtil.getMetadata(page, Nutch.PARAM_GENERATE_TIME);
        long generateTime = StringUtil.tryParseLong(generateTimeStr, -1);

        // Do not re-generate pages in one day if it's marked as "GENERATED"
        if (generateTime > 0 && generateTime < Duration.ofDays(1).toMillis()) {
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
    // Sort by score
    entry.set(url, score);

//    if () {
//
//    }

//    String pageCategory = StringUtil.sniffPageCategory(url, 1, 1);
//    if (pageCategory.equalsIgnoreCase("index")) {
//      score *= 100;
//    }

    // Generate time, we will use this mark to decide if we re-generate this page
    TableUtil.putMetadata(page, Nutch.PARAM_GENERATE_TIME, String.valueOf(startTime));

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
