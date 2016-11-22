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
import org.apache.nutch.crawl.SeedBuilder;
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
import java.util.HashSet;
import java.util.Set;

import static org.apache.nutch.mapreduce.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Nutch.*;

public class GenerateMapper extends NutchMapper<String, WebPage, SelectorEntry, WebPage> {
  public static final Logger LOG = GenerateJob.LOG;

  private enum Counter {
    malformedUrl, rowsAddedAsSeed, rowsInjected, rowsIsSeed, rowsDetailFromSeed, rowsBeforeStart, rowsNotInRange, rowsHostUnreachable,
    rowsNormalisedToNull, rowsFiltered, oldUrlDate,
    tieba, bbs, news, blog,
    pagesAlreadyGenerated, pagesTooFarAway, pagesFetchLater, pagesNeverFetch
  }

  private NutchMetrics nutchMetrics;
  private final Set<String> unreachableHosts = new HashSet<>();

  /**
   * Injector
   */
  private SeedBuilder seedBuiler;
  private Set<String> seedUrls = new HashSet<>();

  private String batchId;
  private URLUtil.HostGroupMode hostGroupMode;
  private boolean filter;
  private boolean normalise;
  private URLFilters urlFilters;
  private URLNormalizers urlNormalizers;
  private ScoringFilters scoringFilters;
  private CrawlFilters crawlFilters;
  private FetchSchedule fetchSchedule;
  private long pseudoCurrTime;
  private long maxDetailPageCount;
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
    batchId = conf.get(PARAM_BATCH_ID, ALL_BATCH_ID_STR);
    String fetchMode = conf.get(PARAM_FETCH_MODE);
    hostGroupMode = conf.getEnum(PARAM_FETCH_QUEUE_MODE, URLUtil.HostGroupMode.BY_HOST);
    reGenerate = conf.getBoolean(PARAM_GENERATE_REGENERATE, false);
    ignoreGenerated = !reGenerate || conf.getBoolean(PARAM_IGNORE_GENERATED, false);

    this.nutchMetrics = NutchMetrics.getInstance(conf);
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
    long topN = conf.getLong(PARAM_GENERATOR_TOP_N, 100000);
    maxDetailPageCount = Math.round(topN * 0.667);

    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    crawlFilters = CrawlFilters.create(conf);
    keyRange = crawlFilters.getMaxReversedKeyRange();

    this.seedBuiler = new SeedBuilder(conf);
    seedUrls = HadoopFSUtil.loadSeeds(PATH_ALL_SEED_FILE, conf.get(PARAM_NUTCH_JOB_NAME), conf);

    LOG.info(Params.format(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "fetchMode", fetchMode,
        "batchId", batchId,
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

    createSeedRows(context);
  }

  @Override
  public void map(String reversedUrl, WebPage page, Context context) throws IOException, InterruptedException {
    getCounter().increase(rows);

    String url = TableUtil.unreverseUrl(reversedUrl);

    if (!checkHost(url)) {
      return;
    }

    if (handleSpecialCases(url, page, context)) {
      return;
    }

    if (Mark.GENERATE_MARK.hasMark(page)) {
      getCounter().increase(Counter.pagesAlreadyGenerated);

      /*
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
      if (page.getFetchTime() - pseudoCurrTime <= Duration.ofDays(30).toMillis()) {
        getCounter().increase(Counter.pagesFetchLater);
      }
      else {
        getCounter().increase(Counter.pagesNeverFetch);
      }

      return;
    }

    float score = page.getScore();
    try {
      // Typically, we use OPIC scoring filter
      // TODO : use a better scoring filter for information monitoring @vincent
      score = scoringFilters.generatorSortValue(url, page, score);
    } catch (ScoringFilterException ignored) {}

    // TODO : Move to a ScoreFilter
    // Detail pages comes first, but we still need some pages with other category
    if (crawlFilters.isDetailUrl(url)) {
      ++detailPages;

      if (detailPages < maxDetailPageCount) {
        score = SCORE_DETAIL_PAGE;
      }
    }
    else if (crawlFilters.isIndexUrl(url)) {
      if (score > SCORE_DETAIL_PAGE) {
        LOG.debug("Too high score : " + score + ", index url : " + url);
      }
    }

    output(url, new SelectorEntry(url, score), page, context);

    getCounter().updateAffectedRows(url);
  }

  // Check Host
  private boolean checkHost(String url) {
    String host = URLUtil.getHost(url, hostGroupMode);

    if (host == null) {
      getCounter().increase(Counter.malformedUrl);
      return false;
    }

    if (unreachableHosts.contains(host)) {
      getCounter().increase(Counter.rowsHostUnreachable);
      return false;
    }

    return true;
  }

  private boolean handleSpecialCases(String url, WebPage page, Context context) throws IOException, InterruptedException {
    float score = -1.0f;
    Counter counter = null;

    /*
     * We crawl seed every time we start, AdaptiveScheduler is just works
     * */
    if (seedUrls.contains(url)) {
      // TODO : check this indicator to see if the scoring/schedule system is OK for seed urls
      counter = Counter.rowsAddedAsSeed;
    }
    else if (TableUtil.isSeed(page)) {
      score = SCORE_SEED;
      counter = Counter.rowsIsSeed;
    }
    else if (TableUtil.isFromSeed(page) && crawlFilters.isDetailUrl(url)) {
      score = SCORE_PAGES_FROM_SEED;
      counter = Counter.rowsDetailFromSeed;
    }
    else if (Mark.INJECT_MARK.hasMark(page)) {
      // Page is injected, this is a seed url and has the highest fetch priority
      // INJECT_MARK will be removed in DbUpdate stage

      score = SCORE_INJECTED;
      counter = Counter.rowsInjected;
    }

    // TODO : handle tieba, bbs and blog

    if (score > 0) {
      output(url, new SelectorEntry(url, score), page, context);
    }

    if (counter != null) {
      getCounter().increase(counter);
    }

    return score > 0;
  }

  /**
   * Seed rows creation should be done in mapper phrase because there might be much fewer reducers in the cluster
   *
   * TODO : The code can be refined and removed by enhancing scoring/schedule system
   * This is a temporary solution to ensure every seed page can be checked each time the crawl loop starts
   *
   * */
  private void createSeedRows(Context context) throws IOException, InterruptedException {
    for (String urlLine : seedUrls) {
      if (urlLine == null || urlLine.isEmpty()) {
        continue;
      }

      urlLine = urlLine.trim();
      if (urlLine.startsWith("#")) {
        continue;
      }

      getCounter().increase(rows);

      WebPage page = seedBuiler.buildWebPage(urlLine);

      // TODO : Check the difference between hbase.url and page.baseUrl
      String url = page.getVariableAsString("url");

      output(url, new SelectorEntry(url, Float.MAX_VALUE), page, context);

      getCounter().increase(Counter.rowsInjected);
      getCounter().updateAffectedRows(url);
    }
  }

  private void output(String url, SelectorEntry entry, WebPage page, Context context) throws IOException, InterruptedException {
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
