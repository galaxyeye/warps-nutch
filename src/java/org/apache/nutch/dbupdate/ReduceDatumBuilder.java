package org.apache.nutch.dbupdate;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.SignatureComparator;
import org.apache.nutch.mapreduce.DbUpdateReducer;
import org.apache.nutch.mapreduce.FetchJob;
import org.apache.nutch.mapreduce.NutchCounter;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Params;
import org.apache.nutch.util.TableUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vincent on 16-9-25.
 * Copyright @ 2013-2016 Warpspeed Information. All rights reserved
 */
public class ReduceDatumBuilder {

  private NutchCounter counter;
  private int retryMax;
  private int maxInterval;
  private FetchSchedule fetchSchedule;
  private ScoringFilters scoringFilters;
  private List<ScoreDatum> inlinkedScoreData = new ArrayList<>(200);
  private Params params;

  public ReduceDatumBuilder(NutchCounter counter, Configuration conf) {
    this.counter = counter;

    retryMax = conf.getInt("db.fetch.retry.max", 3);
    maxInterval = conf.getInt("db.fetch.interval.max", 0);
    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);

    params = Params.of(
        "retryMax", retryMax,
        "maxInterval", maxInterval,
        "fetchSchedule", fetchSchedule,
        "scoringFilters", scoringFilters
    );
  }

  public Params getParams() { return params; }

  public void updateRow(String url, WebPage page) {
    if (Mark.FETCH_MARK.hasMark(page)) {
      processStatus(url, page);
    }

    calculateDistance(page);

    calculateScore(url, page);

    updateMetadata(page);

    updateStatusCounter(page);
  }

  public WebPage createNewRow(String url) {
    WebPage page = WebPage.newBuilder().build();
    fetchSchedule.initializeSchedule(url, page);
    page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);

    try {
      scoringFilters.initialScore(url, page);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      counter.increase(DbUpdateReducer.Counter.errors);
    }

    return page;
  }

  public List<ScoreDatum> getInlinkedScoreData() {
    return this.inlinkedScoreData;
  }

  /**
   * Distance calculation.
   * Retrieve smallest distance from all inlinks distances
   * Calculate new distance for current page: smallest inlink distance plus 1.
   * If the new distance is smaller than old one (or if old did not exist yet),
   * write it to the page.
   * */
  private void calculateDistance(WebPage page) {
    if (page.getInlinks() == null) {
      return;
    }

    page.getInlinks().clear();

    int smallestDist = Integer.MAX_VALUE;
    for (ScoreDatum inlink : inlinkedScoreData) {
      int inlinkDist = inlink.getDistance();
      if (inlinkDist < smallestDist) {
        smallestDist = inlinkDist;
      }
      page.getInlinks().put(new Utf8(inlink.getUrl()), new Utf8(inlink.getAnchor()));
    }

    if (smallestDist != Integer.MAX_VALUE) {
      int oldDistance = Integer.MAX_VALUE;
      CharSequence oldDistUtf8 = page.getMarkers().get(Nutch.DISTANCE);
      if (oldDistUtf8 != null) {
        oldDistance = Integer.parseInt(oldDistUtf8.toString());
      }

      int newDistance = smallestDist + 1;
      if (newDistance < oldDistance) {
        page.getMarkers().put(Nutch.DISTANCE, new Utf8(Integer.toString(newDistance)));
      }
    }
  }

  private void calculateScore(String url, WebPage page) {
    try {
      scoringFilters.initialScore(url, page);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      counter.increase(DbUpdateReducer.Counter.errors);
    }
  }

  private void updateMetadata(WebPage page) {
    // Clear temporary metadata
    TableUtil.clearMetadata(page, FetchJob.REDIRECT_DISCOVERED);
    TableUtil.clearMetadata(page, Nutch.PARAM_GENERATE_TIME);

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

  private void processStatus(String url, WebPage page) {
    byte status = page.getStatus().byteValue();

    switch (status) {
      case CrawlStatus.STATUS_FETCHED: // succesful fetch
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
        CharSequence lastModified = page.getHeaders().get(new Utf8("Last-Modified"));

        if (lastModified != null) {
          try {
            modifiedTime = HttpDateFormat.toLong(lastModified.toString());
            prevModifiedTime = page.getModifiedTime();
          } catch (Exception e) {
            counter.increase(DbUpdateReducer.Counter.errors);
          }
        }

//      TableUtil.putMetadata(page, "Nutch-First-Fectch-Time", String.valueOf(fetchTime));
//      TableUtil.putMetadata(page, "Nutch-Last-Modified-Time", String.valueOf(modifiedTime));

        fetchSchedule.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, modified);
        if (maxInterval < page.getFetchInterval()) {
          fetchSchedule.forceRefetch(url, page, false);
        }
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
        counter.increase(NutchCounter.Counter.status_fetched);
        break;
      case CrawlStatus.STATUS_REDIR_TEMP:
        counter.increase(NutchCounter.Counter.status_redir_temp);
        break;
      case CrawlStatus.STATUS_REDIR_PERM:
        counter.increase(NutchCounter.Counter.status_redir_perm);
        break;
      case CrawlStatus.STATUS_NOTMODIFIED:
        counter.increase(NutchCounter.Counter.status_notmodified);
        break;
      case CrawlStatus.STATUS_RETRY:
        counter.increase(NutchCounter.Counter.status_retry);
        break;
      case CrawlStatus.STATUS_UNFETCHED:
        counter.increase(NutchCounter.Counter.status_unfetched);
        break;
      case CrawlStatus.STATUS_GONE:
        counter.increase(NutchCounter.Counter.status_gone);
        break;
      default:
        break;
    }
  }
}
