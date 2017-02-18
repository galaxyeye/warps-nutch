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
package org.apache.nutch.jobs.generate;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nutch.common.Params;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.filter.*;
import org.apache.nutch.jobs.NutchMapper;
import org.apache.nutch.jobs.generate.GenerateJob.SelectorEntry;
import org.apache.nutch.metadata.Mark;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.persist.gora.GoraWebPage;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.tools.NutchMetrics;
import org.apache.nutch.util.DateTimeUtil;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nutch.jobs.NutchCounter.Counter.rows;
import static org.apache.nutch.metadata.Nutch.*;

public class GenerateMapper extends NutchMapper<String, GoraWebPage, SelectorEntry, GoraWebPage> {
  public static final Logger LOG = GenerateJob.LOG;

  private enum Counter {
    malformedUrl, seeds, rowsDetail, rowsBanned,
    rowsDepth0, rowsDepth1, rowsDepth2, rowsDepth3, rowsDepthN,
    rowsBeforeStart, rowsNotInRange, rowsHostUnreachable,
    rowsNormalisedToNull, rowsUrlFiltered, oldUrlDate,
    tieba, bbs, news, blog,
    pagesGenerated, pagesTooDeep, pagesFetchLater, pagesInactive, seedsFetchLater
  }

  private NutchMetrics nutchMetrics;
  private final Set<String> unreachableHosts = new HashSet<>();
  private final Set<String> bannedUrls = new HashSet<>();

  private Configuration conf;
  private String batchId;
  private URLUtil.HostGroupMode hostGroupMode;
  private boolean filter;
  private boolean normalise;
  private URLFilters urlFilters;
  private URLNormalizers urlNormalizers;
  private ScoringFilters scoringFilters;
  private CrawlFilters crawlFilters;
  private FetchSchedule fetchSchedule;
  private Instant pseudoCurrTime;
  private float detailPageRate;
  private long maxDetailPageCount;
  private int maxDistance;
  private String[] keyRange;
  private boolean reGenerate = false;
  private boolean reGenerateSeeds = false;
  private int detailPages = 0;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);
    conf = context.getConfiguration();

    String crawlId = conf.get(PARAM_CRAWL_ID);
    batchId = conf.get(PARAM_BATCH_ID, ALL_BATCH_ID_STR);
    String fetchMode = conf.get(PARAM_FETCH_MODE);
    hostGroupMode = conf.getEnum(PARAM_FETCH_QUEUE_MODE, URLUtil.HostGroupMode.BY_HOST);
    reGenerate = conf.getBoolean(PARAM_GENERATE_REGENERATE, false);
    reGenerateSeeds = conf.getBoolean(PARAM_GENERATE_REGENERATE_SEEDS, false);

    nutchMetrics = NutchMetrics.getInstance(conf);
    boolean ignoreUnreachableHosts = conf.getBoolean(PARAM_GENERATE_IGNORE_UNREACHABLE_HOSTS, true);
    if (ignoreUnreachableHosts) {
      nutchMetrics.loadUnreachableHosts(unreachableHosts);
    }
    bannedUrls.addAll(loadBannedUrl());

    filter = conf.getBoolean(PARAM_GENERATE_FILTER, true);
    urlFilters = filter ? new URLFilters(conf) : null;
    normalise = conf.getBoolean(PARAM_GENERATE_NORMALISE, true);
    urlNormalizers = normalise ? new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT) : null;

    maxDistance = conf.getInt(PARAM_GENERATE_MAX_DISTANCE, -1);
    pseudoCurrTime = Instant.ofEpochMilli(conf.getLong(PARAM_GENERATE_CUR_TIME, startTime));
    long topN = conf.getLong(PARAM_GENERATE_TOP_N, 10000);
    detailPageRate = conf.getFloat(PARAM_GENERATE_DETAIL_PAGE_RATE, 0.80f);
    maxDetailPageCount = Math.round(topN * detailPageRate);

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
        "pseudoCurrTime", DateTimeUtil.format(pseudoCurrTime.truncatedTo(ChronoUnit.SECONDS)),
        "fetchSchedule", fetchSchedule.getClass().getName(),
        "scoringFilters", scoringFilters,
        "crawlFilters", crawlFilters,
        "keyRange", keyRange[0] + " - " + keyRange[1],
        "reGenerate", reGenerate,
        "reGenerateSeeds", reGenerateSeeds,
        "ignoreUnreachableHosts", ignoreUnreachableHosts,
        "unreachableHostsPath", nutchMetrics.getUnreachableHostsPath(),
        "unreachableHosts", unreachableHosts.size()
    ));
  }

  @Override
  public void map(String reversedUrl, GoraWebPage row, Context context) throws IOException, InterruptedException {
    getCounter().increase(rows);

    WebPage page = WebPage.wrap(reversedUrl, row, true);
    String url = page.url();

    if (!shouldFetch(url, reversedUrl, page)) {
      return;
    }

    int priority = page.sniffFetchPriority();
    float initSortScore = calculateInitSortScore(url, page);
    float sortScore = scoringFilters.generatorSortValue(url, page, initSortScore);

    output(new SelectorEntry(url, priority, sortScore), page, context);

    updateStatus(url, page);
  }

  /**
   * TODO : We may move some filters to hbase query filters directly
   * */
  private boolean shouldFetch(String url, String reversedUrl, WebPage page) {
    if (!checkHost(url)) {
      return false;
    }

    if (bannedUrls.contains(url)) {
      getCounter().increase(Counter.rowsBanned);
      return false;
    }

    if (reGenerateSeeds && page.isSeed()) {
      return true;
    }

    if (page.hasMark(Mark.GENERATE)) {
      getCounter().increase(Counter.pagesGenerated);

      /*
       * Fetch entries are generated, empty webpage entries are created in the database(HBase)
       * case 1. another fetcher job is fetching the generated batch. In this case, we should not generate it.
       * case 2. another fetcher job handled the generated batch, but failed, which means the pages are not fetched.
       *
       * There are three ways to fetch pages that are generated but not fetched nor fetching.
       * 1. Restart a crawl with ignoreGenerated set to be false
       * 2. Resume a FetchJob with resume set to be true
       * */
      if (!reGenerate) {
        // LOG.debug("Skipping {}; already generated", url);
        long generateTime = page.getGenerateTime();

        // Do not re-generate pages in one day if it's marked as "GENERATED"
        if (generateTime > 0 && (startTime - generateTime) < Duration.ofDays(1).toMillis()) {
          return false;
        }
      }
    } // if

    int distance = page.getDistance();
    // Filter on distance
    if (maxDistance > -1 && distance > maxDistance) {
      getCounter().increase(Counter.pagesTooDeep);
      return false;
    }

    // TODO : Url range filtering should be applied to HBase query filter
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
        getCounter().increase(Counter.rowsUrlFiltered);
        return false;
      }
    } catch (URLFilterException | MalformedURLException e) {
      LOG.warn("Filter failed, url: {} \n {}", url, e.getMessage());
      return false;
    }

    return checkFetchSchedule(url, page, distance);
  }

  /**
   * Fetch schedule, timing filter
   * */
  private boolean checkFetchSchedule(String url, WebPage page, int distance) {
    if (!fetchSchedule.shouldFetch(url, page, pseudoCurrTime)) {
      if (page.hasMark(Mark.INACTIVE)) {
        getCounter().increase(Counter.pagesInactive);
      }

      Instant fetchTime = page.getFetchTime();
      long days = ChronoUnit.DAYS.between(fetchTime, pseudoCurrTime);
      if (days <= 30) {
        getCounter().increase(Counter.pagesFetchLater);
      }

      if (distance == 0) {
        getCounter().increase(Counter.seedsFetchLater);
        nutchMetrics.debugFetchLaterSeeds(url, page);
      }

      return false;
    }

    return true;
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

  /*
   * Raise detail page's priority so them can be fetched sooner
   * Detail pages comes first, but we still need keep chances for pages with other category
   * */
  private float calculateInitSortScore(String url, WebPage page) {
    boolean isDetail = page.getPageCategory().isDetail() || crawlFilters.veryLikelyBeDetailUrl(url);
    boolean raise = isDetail && ++detailPages < maxDetailPageCount;

    float factor = raise ? 1.0f : 0.0f;
    int depth = page.getDepth();

    return (10000.0f - 100 * depth) + factor * 1000000.0f;
  }

  private List<String> loadBannedUrl() {
    List<String> urls = new ArrayList<>();

    boolean isDistributedFs = HadoopFSUtil.isDistributedFS(conf);
    try {
      if (isDistributedFs) {
        org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(PATH_BANNED_URLS);
        urls = IOUtils.readLines(FileSystem.get(conf).open(hdfsPath), Charset.defaultCharset());
      }
      else {
        urls = Files.readAllLines(Paths.get(PATH_BANNED_URLS));
      }

      LOG.info("Total " + urls.size() + " urls are banned");
      if (!urls.isEmpty()) {
        LOG.info("Banned urls : " + urls.stream().limit(10).collect(Collectors.joining(", ")));
      }
    }
    catch (NoSuchFileException e) {
      LOG.info("No banned urls");
    }
    catch (IOException e) {
      LOG.info("Failed to load url black list, isDistributedFs : " + isDistributedFs + ", " + e);
    }

    return urls;
  }

  private void output(SelectorEntry entry, WebPage page, Context context) throws IOException, InterruptedException {
    context.write(entry, page.get());
  }

  private void updateStatus(String url, WebPage page) throws IOException, InterruptedException {
    if (page.isSeed()) {
      getCounter().increase(Counter.seeds);
    }

    if ((page.getPageCategory().isDetail() || crawlFilters.veryLikelyBeDetailUrl(url))) {
      getCounter().increase(Counter.rowsDetail);
    }

    int depth = page.getDepth();
    Counter counter;

    if (depth == 0) {
      counter = Counter.rowsDepth0;
    }
    else if (depth == 1) {
      counter = Counter.rowsDepth1;
    }
    else if (depth == 2) {
      counter = Counter.rowsDepth2;
    }
    else if (depth == 3) {
      counter = Counter.rowsDepth3;
    }
    else {
      counter = Counter.rowsDepthN;
    }

    getCounter().increase(counter);
  }
}
