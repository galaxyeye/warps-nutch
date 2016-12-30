package org.apache.nutch.dbupdate;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.mapreduce.io.WebPageWritable;
import org.apache.nutch.persist.WebPage;
import org.apache.nutch.graph.WebEdge;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.nutch.metadata.Nutch.TCP_IP_STANDARDIZED_TIME;

/**
 * Created by vincent on 16-9-25.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class ReduceDatumBuilder {

  public static final Logger LOG = LoggerFactory.getLogger(ReduceDatumBuilder.class);

  private final NutchCounter counter;
  private final int retryMax;
  private final Duration maxFetchInterval;
  private final int maxLinks;
  private final FetchSchedule fetchSchedule;
  private final ScoringFilters scoringFilters;
  private final Params params;
  private final List<WebEdge> inlinkedScoreData = new ArrayList<>(200);

  public ReduceDatumBuilder(NutchCounter counter, Configuration conf) {
    this.counter = counter;

    retryMax = conf.getInt("db.fetch.retry.max", 3);
    maxFetchInterval = Duration.ofSeconds(conf.getLong("db.fetch.interval.max", Duration.ofDays(90).getSeconds()));
    // maxFetchInterval = ConfigUtils.getDuration(conf, "db.fetch.interval.max", Duration.ofDays(90));
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);
    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);

    params = Params.of(
        "retryMax", retryMax,
        "maxFetchInterval", maxFetchInterval,
        "maxLinks", maxLinks,
        "fetchSchedule", fetchSchedule.getClass().getSimpleName(),
        "scoringFilters", StringUtils.join(scoringFilters.getScoringFilterNames(), ",")
    );
  }

  public Params getParams() {
    return params;
  }

  public void reset() {
    inlinkedScoreData.clear();
  }

  public void calculateInlinks(List<WebEdge> edge) {
    inlinkedScoreData.clear();
    inlinkedScoreData.addAll(edge);
  }

  /**
   * In the mapper phrase, NutchWritable is sets to be a WebPage or a WebEdge,
   * the WrappedWebPageWritable is the webpage to be updated, and the score datum is calculated from the outlinks
   * */
  public WebPage calculateInlinks(String sourceUrl, Iterable<NutchWritable> values) {
    WebPage page = null;
    inlinkedScoreData.clear();

    for (NutchWritable nutchWritable : values) {
      Writable val = nutchWritable.get();
      if (val instanceof WebPageWritable) {
        page = ((WebPageWritable) val).getWebPage();
      } else {
        inlinkedScoreData.add((WebEdge) val);

        if (inlinkedScoreData.size() >= maxLinks) {
          LOG.info("Limit reached, skipping further inlinks for " + sourceUrl);
          break;
        }
      }
    } // for

    return page;
  }

  public void updateRow(String url, WebPage page) {
    calculateDistance(page);

//    updateScore(url, page);
//
//    updateMetadata(page);
//
//    updateStatusCounter(page);
  }

  public WebPage createNewRow(String url, int depth) {
    WebPage page = WebPage.newWebPage();

    page.setDistance(depth);

    fetchSchedule.initializeSchedule(url, page);
    page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);

    try {
      scoringFilters.initialScore(url, page);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      counter.increase(NutchCounter.Counter.errors);
    }

    return page;
  }

  /**
   * In Update JIT mode, there is only one inlink score data
   * */
  public void updateNewRow(String url, WebPage sourcePage, WebPage newPage) {
    newPage.setReferrer(sourcePage.getBaseUrl());

//    updateScore(url, newPage);
//
//    updateStatusCounter(newPage);
  }

  /**
   * Updated the old row if necessary
   * */
  public boolean updateExistOutPage(WebPage mainPage, WebPage oldPage, int newDepth, int oldDepth) {
    boolean changed = false;

    // TODO : Why we can get a empty metadata collection? All fields are not dirty?

    boolean detail = oldPage.veryLikeDetailPage();

    Instant publishTime = oldPage.getPublishTime();
    if (detail && publishTime.isAfter(TCP_IP_STANDARDIZED_TIME)) {
      mainPage.updateRefPublishTime(publishTime);
      changed = true;
    }

    // Vote the main page if not voted
    if (detail && oldPage.voteIfAbsent(mainPage)) {
      mainPage.increaseRefArticles(1);
      mainPage.increaseRefChars(oldPage.sniffTextLength());
      changed = true;
    }

    if (newDepth < oldDepth) {
      oldPage.setDistance(newDepth);
      oldPage.setReferrer(mainPage.getBaseUrl());
      changed = true;
    }

    return changed;
  }

  /**
   * Distance calculation.
   * Retrieve smallest distance from all inlinks distances
   * Calculate new distance for current page: smallest inlink distance plus 1.
   * If the new distance is smaller than old one (or if old did not exist yet),
   * write it to the page.
   * */
  private void calculateDistance(WebPage page) {
  }

}
