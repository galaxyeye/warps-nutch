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
import org.apache.nutch.crawl.filters.CrawlFilter;
import org.apache.nutch.crawl.filters.CrawlFilters;
import org.apache.nutch.mapreduce.GenerateJob.SelectorEntry;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;
import java.time.Year;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Metadata.META_IS_SEED;
import static org.apache.nutch.metadata.Nutch.*;

public class GenerateMapper extends NutchMapper<String, WebPage, SelectorEntry, WebPage> {

  public static final Logger LOG = GenerateJob.LOG;

  private static final int year = Year.now().getValue();
  private static final int yearLowerBound = 1990;
  private static final int yearUpperBound = Year.now().getValue();

  private enum Counter {
    malformedUrl, rowsInjected, rowsIsSeed, rowsBeforeStart, rowsNotInRange, rowsHostUnreachable, pagesAlreadyGenerated,
    pagesTooFarAway, rowsNormalisedToNull, rowsFiltered, pagesFetchLater
  }

  private NutchMetrics nutchMetrics;
  private final Set<String> unreachableHosts = new HashSet<>();
  private final SelectorEntry entry = new SelectorEntry();

  private URLUtil.HostGroupMode hostGroupMode;
  private boolean filter;
  private boolean normalise;
  private URLFilters urlFilters;
  private URLNormalizers urlNormalizers;
  private ScoringFilters scoringFilters;
  private CrawlFilters crawlFilters;
  private FetchSchedule fetchSchedule;
  private long pseudoCurrTime;
  private long topN;
  private long maxDetailPageCount;
  private Set<String> oldYears;
  private int maxDistance;
  private String[] keyRange;
  private boolean reGenerate = false;
  private boolean ignoreGenerated = true;

  private int detailPages = 0;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    String crawlId = conf.get(PARAM_CRAWL_ID);
    String fetchMode = conf.get(PARAM_FETCH_MODE);
    hostGroupMode = conf.getEnum(PARAM_FETCH_QUEUE_MODE, URLUtil.HostGroupMode.BY_HOST);
    reGenerate = conf.getBoolean(PARAM_GENERATE_REGENERATE, false);
    ignoreGenerated = !reGenerate || conf.getBoolean(PARAM_IGNORE_GENERATED, false);

    this.nutchMetrics = new NutchMetrics(conf);
    boolean ignoreUnreachableHosts = conf.getBoolean("generator.ignore.unreachable.hosts", true);
    if (ignoreUnreachableHosts) {
      nutchMetrics.loadUnreachableHosts(unreachableHosts);
    }

    filter = conf.getBoolean(PARAM_GENERATE_FILTER, true);
    urlFilters = filter ? new URLFilters(conf) : null;
    normalise = conf.getBoolean(PARAM_GENERATE_NORMALISE, true);
    urlNormalizers = normalise ? new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT) : null;

    maxDistance = conf.getInt(PARAM_GENERATOR_MAX_DISTANCE, -1);
    pseudoCurrTime = conf.getLong(PARAM_GENERATOR_CUR_TIME, startTime);
    topN = conf.getLong(PARAM_GENERATOR_TOP_N, Long.MAX_VALUE);
    maxDetailPageCount = Math.round(topN * 0.667);
    oldYears = IntStream.range(yearLowerBound, yearUpperBound - 1).mapToObj(String::valueOf).collect(Collectors.toSet());

    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    crawlFilters = CrawlFilters.create(conf);
    keyRange = crawlFilters.getMaxReversedKeyRange();

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "fetchMode", fetchMode,
        "hostGroupMode", hostGroupMode,
        "filter", filter,
        "topN", topN,
        "normalise", normalise,
        "maxDistance", maxDistance,
        "pseudoCurrTime", TimingUtil.format(pseudoCurrTime),
        "fetchSchedule", fetchSchedule.getClass().getName(),
        "scoringFilters", scoringFilters.getClass().getName(),
        "crawlFilters", crawlFilters,
        "keyRange", keyRange[0] + " - " + keyRange[1],
        "ignoreGenerated", ignoreGenerated,
        "ignoreUnreachableHosts", ignoreUnreachableHosts,
        "unreachableHostsPath", nutchMetrics.getUnreachableHostsPath(),
        "unreachableHosts", unreachableHosts.size()
    ));
  }

  @Override
  public void map(String reversedUrl, WebPage page, Context context) throws IOException, InterruptedException {
    getCounter().increase(rows);

//    if (LOG.isDebugEnabled()) {
//      LOG.debug(reversedUrl);
//      return;
//    }

    String url = TableUtil.unreverseUrl(reversedUrl);

    // Host
    String host = URLUtil.getHost(url, hostGroupMode);

    if (host == null) {
      getCounter().increase(Counter.malformedUrl);
      return;
    }

    if (unreachableHosts.contains(host)) {
      getCounter().increase(Counter.rowsHostUnreachable);
      return;
    }

    /**
     * We crawl seed every time we start, AdaptiveScheduler is just works
     * */
    String isSeed = TableUtil.getMetadata(page, META_IS_SEED);
    if (isSeed != null) {
      entry.set(url, SCORE_SEED);
      output(url, entry, page, context);
      getCounter().increase(Counter.rowsIsSeed);
      return;
    }

    // Page is injected, this is a seed url and has the highest fetch priority
    // INJECT_MARK will be removed in DbUpdate stage
    if (Mark.INJECT_MARK.hasMark(page)) {
      entry.set(url, SCORE_INJECTED);
      output(url, entry, page, context);
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
    } // if

    // Filter on distance
    // TODO : We have already filtered urls on dbupdate phrase, check if that is right @vincent
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
    if (!shouldProcess(url, reversedUrl)) {
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
      // Typically, we use OPIC scoring filter
      // TODO : use a better scoring filter for information monitoring @vincent
      score = scoringFilters.generatorSortValue(url, page, score);
    } catch (ScoringFilterException ignored) {
    }

    // TODO : Move to a ScoreFilter
    // Detail pages comes first, but we still need some pages with other category
    if (crawlFilters.isDetailUrl(url)) {
      ++detailPages;

      if (detailPages < maxDetailPageCount) {
        score = SCORE_DETAIL_PAGE;
      }

      // For urls who contains year information
      // http://bond.hexun.com/2011-01-07/126641872.html
      for (String lastYear : oldYears) {
        if (url.contains("/" + lastYear)) {
          score = SCORE_DEFAULT;
          break;
          // return;
        } // if
      } // for
    }

    // Sort by score
    entry.set(url, score);

    output(url, entry, page, context);

    getCounter().updateAffectedRows(url);
  }

//  private boolean shouldFetch(String url, WebPage page, long currTime) {
//    boolean should = false;
//
//
//
//    should = fetchSchedule.shouldFetch(url, page, pseudoCurrTime);
//
//    return should;
//  }

  private void output(String url, SelectorEntry entry, WebPage page, Context context) throws IOException, InterruptedException {
    // Generate time, we will use this mark to decide if we re-generate this page
    TableUtil.putMetadata(page, PARAM_GENERATE_TIME, String.valueOf(startTime));

    context.write(entry, page);
  }

  // Url filter
  private boolean shouldProcess(String url, String reversedUrl) {
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

    // If filtering is on don't generate URLs that don't pass URLFilters
    try {
      if (normalise) {
        url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
      }

      if (url == null) {
        getCounter().increase(Counter.rowsNormalisedToNull);
        return false;
      }

      if (filter && urlFilters.filter(url) == null) {
        getCounter().increase(Counter.rowsFiltered);
        return false;
      }
    } catch (URLFilterException | MalformedURLException e) {
      LOG.warn("Filter failed, url: {} \n {}", url, e.getMessage());
      return false;
    }

    return true;
  }
}
