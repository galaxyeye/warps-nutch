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

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.fetcher.FetchJob;
import org.apache.nutch.mapreduce.NutchReducer;
import org.apache.nutch.mapreduce.WebPageWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DbUpdateReducer extends NutchReducer<UrlWithScore, NutchWritable, String, WebPage> {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdateReducer.class);

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

  public enum Counter { rows, newRows, errors };

  private int retryMax;
  private boolean additionsAllowed;
  private int maxInterval;
  private FetchSchedule fetchSchedule;
  private ScoringFilters scoringFilters;
  private List<ScoreDatum> inlinkedScoreData = new ArrayList<>();
  private int maxLinks;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    getCounter().register(Counter.class);

    String crawlId = conf.get(Nutch.CRAWL_ID_KEY);
    int UICrawlId = conf.getInt(Nutch.UI_CRAWL_ID, 0);
    String fetchMode = conf.get(Nutch.FETCH_MODE_KEY);
    retryMax = conf.getInt("db.fetch.retry.max", 3);
    additionsAllowed = conf.getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    maxInterval = conf.getInt("db.fetch.interval.max", 0);
    fetchSchedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
    maxLinks = conf.getInt("db.update.max.inlinks", 10000);

    LOG.info(StringUtil.formatParams(
        "className", this.getClass().getSimpleName(),
        "crawlId", crawlId,
        "UICrawlId", UICrawlId,
        "fetchMode", fetchMode,
        "retryMax", retryMax,
        "additionsAllowed", additionsAllowed,
        "maxInterval", maxInterval,
        "maxLinks", maxLinks
    )); 
  }

  @Override
  protected void reduce(UrlWithScore key, Iterable<NutchWritable> values, Context context) {
    try {
      doReduce(key, values, context);
    }
    catch(Throwable e) {
      LOG.error(StringUtil.stringifyException(e));
    }
  }

  protected void doReduce(UrlWithScore key, Iterable<NutchWritable> values, Context context) 
      throws IOException, InterruptedException {
    String keyUrl = key.getUrl().toString();
    String url = TableUtil.unreverseUrl(keyUrl);

    WebPage page = null;
    inlinkedScoreData.clear();

    for (NutchWritable nutchWritable : values) {
      Writable val = nutchWritable.get();
      if (val instanceof WebPageWritable) {
        page = ((WebPageWritable) val).getWebPage();
      } else {
        inlinkedScoreData.add((ScoreDatum) val);
        if (inlinkedScoreData.size() >= maxLinks) {
          LOG.info("Limit reached, skipping further inlinks for " + keyUrl);
          break;
        }
      }
    } // for

    if (page == null) {
      if (!additionsAllowed) {
        return;
      }

      page = buildNewPage(url);
    } else {
      processStatus(url, page);
    }

    if (page.getInlinks() != null) {
      page.getInlinks().clear();
    }

    // Distance calculation.
    // Retrieve smallest distance from all inlinks distances
    // Calculate new distance for current page: smallest inlink distance plus 1.
    // If the new distance is smaller than old one (or if old did not exist
    // yet),
    // write it to the page.
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

    try {
      scoringFilters.initialScore(url, page);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      getCounter().increase(Counter.errors);
    }

    // Clear temporary metadata
    TableUtil.clearMetadata(page, FetchJob.REDIRECT_DISCOVERED);
    TableUtil.clearMetadata(page, Nutch.GENERATOR_GENERATE_TIME);

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

    getCounter().increase(Counter.rows);
    getCounter().updateAffectedRows(url);

    context.write(keyUrl, page);
  }

  private WebPage buildNewPage(String url) { // new row
    WebPage page = WebPage.newBuilder().build();
    fetchSchedule.initializeSchedule(url, page);
    page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);

    try {
      scoringFilters.initialScore(url, page);
    } catch (ScoringFilterException e) {
      page.setScore(0.0f);
      getCounter().increase(Counter.errors);
    }

    getCounter().increase(Counter.newRows);

    return page;
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
          getCounter().increase(Counter.errors);
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
}
