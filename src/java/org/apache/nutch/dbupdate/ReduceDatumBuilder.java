package org.apache.nutch.dbupdate;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.*;
import org.apache.nutch.mapreduce.DbUpdateReducer;
import org.apache.nutch.mapreduce.FetchJob;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.mapreduce.WebPageWritable;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.nutch.metadata.Nutch.MAX_DISTANCE;
import static org.apache.nutch.metadata.Nutch.PARAM_GENERATE_TIME;

/**
 * Created by vincent on 16-9-25.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class ReduceDatumBuilder {

  public static final Logger LOG = LoggerFactory.getLogger(ReduceDatumBuilder.class);

  private final NutchCounter counter;
  private final int retryMax;
  private final int maxInterval;
  private final int maxLinks;
  private final FetchSchedule fetchSchedule;
  private final ScoringFilters scoringFilters;
  private final List<ScoreDatum> inlinkedScoreData = new ArrayList<>(200);
  private final Params params;

  public ReduceDatumBuilder(NutchCounter counter, Configuration conf) {
    this.counter = counter;

    retryMax = conf.getInt("db.fetch.retry.max", 3);
    maxInterval = conf.getInt("db.fetch.interval.max", 0);
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);
    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);

    params = Params.of(
        "retryMax", retryMax,
        "maxInterval", maxInterval,
        "maxLinks", maxLinks,
        "fetchSchedule", fetchSchedule.getClass().getSimpleName(),
        "scoringFilters", Stream.of(scoringFilters.getScoringFilterNames())
    );
  }

  public Params getParams() { return params; }

  public void process(String url, WebPage page, WebPage oldPage, boolean additionsAllowed) {
    //check if page is already in the db
    if(page == null && oldPage != null) {
      // if we return here inlinks will not be updated
      page = oldPage;
    }
    else if (page == null) {
      // Here we got a new webpage from outlink
      if (!additionsAllowed) {
        return;
      }

      page = createNewRow(url, MAX_DISTANCE);

      counter.increase(DbUpdateReducer.Counter.newRows);
    }
    else {
      // process the main page
      updateFetchSchedule(url, page);
    }

    updateRow(url, page);
  }

  /**
   * The mapper phrase can set NutchWritable to be a WebPageWritable or a ScoreDatum,
   * the WebPageWritable is the webpage to be updated, and the score datum is calculated from the outlinks
   * from that webpage
   * */
  public WebPage calculateInlinks(String sourceUrl, Iterable<NutchWritable> values) {
    WebPage page = null;
    inlinkedScoreData.clear();

    for (NutchWritable nutchWritable : values) {
      Writable val = nutchWritable.get();
      if (val instanceof WebPageWritable) {
        page = ((WebPageWritable) val).getWebPage();
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
    WebPage page = WebPage.newBuilder().build();

    fetchSchedule.initializeSchedule(url, page);
    page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);

    TableUtil.setDistance(page, depth);

    try {
      scoringFilters.initialScore(url, page);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      counter.increase(NutchCounter.Counter.errors);
    }

    return page;
  }

  public void updateNewRow(String url, WebPage sourcePage, WebPage newPage) {
    TableUtil.setReferrer(sourcePage, sourcePage.getBaseUrl().toString());

    updateScore(url, newPage);

    updateMetadata(newPage);

    updateStatusCounter(newPage);
  }

  /**
   * Distance calculation.
   * Retrieve smallest distance from all inlinks distances
   * Calculate new distance for current page: smallest inlink distance plus 1.
   * If the new distance is smaller than old one (or if old did not exist yet),
   * write it to the page.
   * */
  private void calculateDistance(WebPage page) {
    if (page.getInlinks() != null) {
      page.getInlinks().clear();
    }

    int smallestDist = MAX_DISTANCE;
    for (ScoreDatum inlink : inlinkedScoreData) {
      int inlinkDist = inlink.getDistance();
      if (inlinkDist < smallestDist) {
        smallestDist = inlinkDist;
      }

      LOG.debug("Inlink : " + inlink.getDistance() + ", " + page.getBaseUrl() + " -> " + inlink.getUrl());

      page.getInlinks().put(new Utf8(inlink.getUrl()), new Utf8(inlink.getAnchor()));
    }

    if (smallestDist != MAX_DISTANCE) {
      int oldDistance = TableUtil.getDepth(page);
      int newDistance = smallestDist + 1;

      if (newDistance < oldDistance) {
        TableUtil.setDistance(page, newDistance);
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
    TableUtil.clearMetadata(page, FetchJob.REDIRECT_DISCOVERED);
    TableUtil.clearMetadata(page, PARAM_GENERATE_TIME);

    // Clear markers
    Mark.INJECT_MARK.removeMarkIfExist(page);
    Mark.GENERATE_MARK.removeMarkIfExist(page);
    Mark.FETCH_MARK.removeMarkIfExist(page);

    Utf8 parseMark = Mark.PARSE_MARK.checkMark(page);
    if (parseMark != null) {
      Mark.PARSE_MARK.removeMark(page);
      // What about INDEX_MARK?
      Mark.UPDATEDB_MARK.putMark(page, parseMark);
    }
  }

  public void updateFetchSchedule(String url, WebPage page) {
    byte status = page.getStatus().byteValue();

    switch (status) {
      case CrawlStatus.STATUS_FETCHED: // successful fetch
      case CrawlStatus.STATUS_REDIR_TEMP: // successful fetch, redirected
      case CrawlStatus.STATUS_REDIR_PERM:
      case CrawlStatus.STATUS_NOTMODIFIED: // successful fetch, notmodified
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

        long fetchTime = page.getFetchTime();
        long prevFetchTime = page.getPrevFetchTime();
        long modifiedTime = page.getModifiedTime();
        long prevModifiedTime = page.getPrevModifiedTime();
        CharSequence lastModified = page.getHeaders().get(new Utf8(HttpHeaders.LAST_MODIFIED));

        if (lastModified != null) {
          try {
            modifiedTime = HttpDateFormat.toLong(lastModified.toString());
            prevModifiedTime = page.getModifiedTime();
          } catch (Exception e) {
            counter.increase(NutchCounter.Counter.errors);
          }
        }

        fetchSchedule.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, modified);

        // Vincent : no force re-fetch for opinion monitoring
//        if (!TableUtil.isNoMoreFetch(page) && maxInterval < page.getFetchInterval()) {
//          fetchSchedule.forceRefetch(url, page, false);
//        }

        break;
      case CrawlStatus.STATUS_RETRY:
        fetchSchedule.setPageRetrySchedule(url, page, 0L, page.getPrevModifiedTime(), page.getFetchTime());
        if (page.getRetriesSinceFetch() < retryMax) {
          page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);
        } else {
          page.setStatus((int) CrawlStatus.STATUS_GONE);
        }
        break;
      case CrawlStatus.STATUS_GONE:
        fetchSchedule.setPageGoneSchedule(url, page, 0L, page.getPrevModifiedTime(), page.getFetchTime());
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
        counter.increase(NutchCounter.Counter.stNotmodified);
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
