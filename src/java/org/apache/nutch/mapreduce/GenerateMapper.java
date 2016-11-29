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
    malformedUrl, rowsAddedAsSeed, rowsInjected, rowsIsSeed, rowsDetailFromSeed,
    rowsDepth0, rowsDepth1, rowsDepth2, rowsDepth3,
    rowsBeforeStart, rowsNotInRange, rowsHostUnreachable,
    rowsNormalisedToNull, rowsFiltered, oldUrlDate,
    tieba, bbs, news, blog,
    pagesAlreadyGenerated, pagesTooDeep, pagesFetchLater, seedsFetchLater, pagesNeverFetch
  }

  private NutchMetrics nutchMetrics;
  private String reportSuffix;
  private final Set<String> unreachableHosts = new HashSet<>();

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
    this.reportSuffix = conf.get(PARAM_NUTCH_JOB_NAME, "job-unknown-" + DateTimeUtil.now("MMdd.HHmm"));

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
        "pseudoCurrTime", DateTimeUtil.format(pseudoCurrTime),
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

    String url = TableUtil.unreverseUrl(reversedUrl);

    /*
     * TODO : use hbase query filter directly
     * */
    if (TableUtil.isNoMoreFetch(page)) {
      getCounter().increase(Counter.pagesNeverFetch);
      return;
    }

    int depth = TableUtil.getDepth(page);
      // Fetch schedule, timing filter
    if (!fetchSchedule.shouldFetch(url, page, pseudoCurrTime)) {
      if (page.getFetchTime() - pseudoCurrTime <= Duration.ofDays(30).toMillis()) {
        getCounter().increase(Counter.pagesFetchLater);
      }

      if (depth == 0) {
        getCounter().increase(Counter.seedsFetchLater);

        String report = "CurrTime : " + DateTimeUtil.format(pseudoCurrTime)
            + "\tLastReferredPT : " +  DateTimeUtil.format(TableUtil.getLatestReferredPublishTime(page))
            + "\tReferredPC : " + TableUtil.getReferredArticles(page)
            + "\tFetchTime : " + DateTimeUtil.format(page.getFetchTime())
            + "\tisSeed : " + TableUtil.isSeed(page)
            + "\t->\t" + url;
        nutchMetrics.debugFetchLaterSeeds(report, reportSuffix);
      }

      return;
    }

    // Url filter
    if (!shouldProcess(url, reversedUrl, page)) {
      return;
    }

    int priority = TableUtil.calculateFetchPriority(page);
    float score = page.getScore();
    try {
      // Typically, we use OPIC scoring filter
      score = scoringFilters.generatorSortValue(url, page, score);
    } catch (ScoringFilterException ignored) {}

    /*
     * Raise detail page's priority so them can be fetched sooner
     * Detail pages comes first, but we still need keep chances for pages with other category
     * */
    if (crawlFilters.isDetailUrl(url) && ++detailPages < maxDetailPageCount) {
      priority = FETCH_PRIORITY_DETAIL_PAGE;
      score = SCORE_DETAIL_PAGE - depth;
    }

    output(url, new SelectorEntry(url, priority, score), page, context);

    updateStatus(url, page);
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

  private void updateStatus(String url, WebPage page) throws IOException, InterruptedException {
    int depth = TableUtil.getDepth(page);
    Counter counter = null;

    if (TableUtil.isSeed(page)) {
      counter = Counter.rowsIsSeed;
    }

    if (Mark.INJECT_MARK.hasMark(page)) {
      counter = Counter.rowsInjected;
    }

    if (depth == 0) {
      counter = Counter.rowsDepth0;
    }
    else if (depth == 1) {
      counter = Counter.rowsDepth1;
      if (crawlFilters.isDetailUrl(url)) {
        counter = Counter.rowsDetailFromSeed;
      }
    }
    else if (depth == 2) {
      counter = Counter.rowsDepth2;
    }
    else if (depth == 3) {
      counter = Counter.rowsDepth3;
    }

    if (counter != null) {
      getCounter().increase(counter);
    }

    getCounter().updateAffectedRows(url);
  }

  private void output(String url, SelectorEntry entry, WebPage page, Context context) throws IOException, InterruptedException {
    context.write(entry, page);
  }

  private boolean shouldProcess(String url, String reversedUrl, WebPage page) {
    if (!checkHost(url)) {
      return false;
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
        long generateTime = TableUtil.getGenerateTime(page);

        // Do not re-generate pages in one day if it's marked as "GENERATED"
        if (generateTime > 0 && (startTime - generateTime) < Duration.ofDays(1).toMillis()) {
          return false;
        }
      }
    } // if

    // Filter on distance
    if (maxDistance > -1) {
      int distance = TableUtil.getDepth(page);
      if (distance > maxDistance) {
        getCounter().increase(Counter.pagesTooDeep);
        return false;
      }
    }

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
