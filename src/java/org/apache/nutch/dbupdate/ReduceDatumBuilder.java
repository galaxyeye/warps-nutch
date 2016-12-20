package org.apache.nutch.dbupdate;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.*;
import org.apache.nutch.mapreduce.FetchJob;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.mapreduce.WebPageWritable;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.nutch.metadata.Nutch.*;
import static org.apache.nutch.storage.Mark.*;

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
  private final List<ScoreDatum> inlinkedScoreData = new ArrayList<>(200);

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

  public void process(String url, WebPage page, WebPage oldPage, boolean additionsAllowed) {
    //check if page is already in the db
    if (page == null && oldPage != null) {
      // if we return here inlinks will not be updated
      page = oldPage;
    } else if (page == null) {
      // Here we got a new webpage from outlink
      if (!additionsAllowed) {
        return;
      }

      page = createNewRow(url, MAX_DISTANCE);
    } else {
      // process the main page
      updateFetchSchedule(url, page);
    }

    updateRow(url, page);
  }

  public void reset() {
    inlinkedScoreData.clear();
  }

  /**
   *
   * */
  public void calculateInlinks(List<ScoreDatum> scoreDatum) {
    inlinkedScoreData.clear();
    inlinkedScoreData.addAll(scoreDatum);
  }

  /**
   * In the mapper phrase, NutchWritable is sets to be a WebPage or a ScoreDatum,
   * the WrappedWebPageWritable is the webpage to be updated, and the score datum is calculated from the outlinks
   * */
  public WebPage calculateInlinks(String sourceUrl, Iterable<NutchWritable> values) {
    WebPage page = null;
    inlinkedScoreData.clear();

    for (NutchWritable nutchWritable : values) {
      Writable val = nutchWritable.get();
      if (val instanceof WebPageWritable) {
        page = new WebPage(((WebPageWritable) val).getWebPage());
      } else {
        inlinkedScoreData.add((ScoreDatum) val);

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

    updateScore(url, page);

    updateMetadata(page);

    updateStatusCounter(page);
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

    updateScore(url, newPage);

    updateStatusCounter(newPage);
  }

  /**
   * Updated the old row if necessary
   * */
  public boolean updateExistOutPage(WebPage mainPage, WebPage oldPage, int newDepth, int oldDepth) {
    boolean changed = false;
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
    page.getInlinks().clear();

    int smallestDist = MAX_DISTANCE;
    for (ScoreDatum inlink : inlinkedScoreData) {
      int inlinkDist = inlink.getDistance();
      if (inlinkDist < smallestDist) {
        smallestDist = inlinkDist;
      }

      LOG.trace("Inlink : " + inlink.getDistance() + ", " + page.getBaseUrl() + " -> " + inlink.getUrl());

      page.getInlinks().put(new Utf8(inlink.getUrl()), new Utf8(inlink.getAnchor()));
    }

    if (smallestDist != MAX_DISTANCE) {
      int oldDistance = page.getDepth();
      int newDistance = smallestDist + 1;

      if (newDistance < oldDistance) {
        page.setDistance(newDistance);
      }
    }
  }

  private void updateScore(String url, WebPage page) {
    try {
      scoringFilters.updateScore(url, page, inlinkedScoreData);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      counter.increase(NutchCounter.Counter.errors);
    }

    // if page contains date string of today, and it's an index page
    // it should have a very high score
  }

  private void updateMetadata(WebPage page) {
    // Clear temporary metadata
    page.clearMetadata(FetchJob.REDIRECT_DISCOVERED);
    page.clearMetadata(Metadata.Name.GENERATE_TIME);

    page.putMarkIfNonNull(UPDATEDB, page.getMark(PARSE));

    page.removeMark(INJECT);
    page.removeMark(GENERATE);
    page.removeMark(FETCH);
    page.removeMark(PARSE);
  }

  public void updateFetchSchedule(String url, WebPage page) {
    byte status = page.getStatus().byteValue();

    switch (status) {
      case CrawlStatus.STATUS_FETCHED: // successful fetch
      case CrawlStatus.STATUS_REDIR_TEMP: // successful fetch, redirected
      case CrawlStatus.STATUS_REDIR_PERM:
      case CrawlStatus.STATUS_NOTMODIFIED: // successful fetch, not modified
        int modified = FetchSchedule.STATUS_UNKNOWN;
        if (status == CrawlStatus.STATUS_NOTMODIFIED) {
          modified = FetchSchedule.STATUS_NOTMODIFIED;
        }

        ByteBuffer prevSig = page.getPrevSignature();
        ByteBuffer signature = page.getSignature();
        if (prevSig != null && signature != null) {
          if (SignatureComparator.compare(prevSig, signature) != 0) {
            modified = FetchSchedule.STATUS_MODIFIED;
          } else {
            modified = FetchSchedule.STATUS_NOTMODIFIED;
          }
        }

        Instant prevFetchTime = page.getPrevFetchTime();
        Instant fetchTime = page.getFetchTime();

        Instant prevModifiedTime = page.getPrevModifiedTime();
        Instant modifiedTime = page.getModifiedTime();

        Instant latestModifiedTime = page.getHeaderLastModifiedTime(modifiedTime);
        if (latestModifiedTime.isAfter(modifiedTime)) {
          modifiedTime = latestModifiedTime;
          prevModifiedTime = modifiedTime;
        }
        else {
          LOG.trace("Bad last modified time : {} -> {}", modifiedTime, page.getHeader(HttpHeaders.LAST_MODIFIED, "unknown time"));
        }

        fetchSchedule.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, modified);

        Duration fetchInterval = page.getFetchInterval();
        if (fetchInterval.toDays() < NEVER_FETCH_INTERVAL_DAYS && fetchInterval.compareTo(maxFetchInterval) > 0) {
          LOG.info("Force refetch page " + url + ", fetch interval : " + fetchInterval);
          fetchSchedule.forceRefetch(url, page, false);
        }

        break;
      case CrawlStatus.STATUS_RETRY:
        fetchSchedule.setPageRetrySchedule(url, page, Instant.EPOCH, page.getPrevModifiedTime(), page.getFetchTime());
        if (page.getRetriesSinceFetch() < retryMax) {
          page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);
        } else {
          page.setStatus((int) CrawlStatus.STATUS_GONE);
        }
        break;
      case CrawlStatus.STATUS_GONE:
        fetchSchedule.setPageGoneSchedule(url, page, Instant.EPOCH, page.getPrevModifiedTime(), page.getFetchTime());
        break;
    }
  }

  private void updateStatusCounter(WebPage page) {
    byte status = page.getStatus().byteValue();

    switch (status) {
      case CrawlStatus.STATUS_FETCHED:
        counter.increase(NutchCounter.Counter.stFetched);
        break;
      case CrawlStatus.STATUS_REDIR_TEMP:
        counter.increase(NutchCounter.Counter.stRedirTemp);
        break;
      case CrawlStatus.STATUS_REDIR_PERM:
        counter.increase(NutchCounter.Counter.stRedirPerm);
        break;
      case CrawlStatus.STATUS_NOTMODIFIED:
        counter.increase(NutchCounter.Counter.stNotModified);
        break;
      case CrawlStatus.STATUS_RETRY:
        counter.increase(NutchCounter.Counter.stRetry);
        break;
      case CrawlStatus.STATUS_UNFETCHED:
        counter.increase(NutchCounter.Counter.stUnfetched);
        break;
      case CrawlStatus.STATUS_GONE:
        counter.increase(NutchCounter.Counter.stGone);
        break;
      default:
        break;
    }
  }
}
