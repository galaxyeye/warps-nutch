/**
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
 */

package org.apache.nutch.crawl.schedulers;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;

import static org.apache.nutch.metadata.Metadata.META_IS_SEED;
import static org.apache.nutch.metadata.Nutch.TCP_IP_STANDARDIZED_TIME;
import static org.apache.nutch.metadata.Nutch.YES_STRING;

/**
 * This class implements an adaptive re-fetch algorithm. This works as follows:
 * <ul>
 * <li>for pages that has changed since the last fetchTime, decrease their
 * fetchInterval by a factor of DEC_FACTOR (default value is 0.2f).</li>
 * <li>for pages that haven't changed since the last fetchTime, increase their
 * fetchInterval by a factor of INC_FACTOR (default value is 0.2f).<br>
 * If SYNC_DELTA property is true, then:
 * <ul>
 * <li>calculate a <code>delta = fetchTime - modifiedTime</code></li>
 * <li>try to synchronize with the time of change, by shifting the next
 * fetchTime by a fraction of the difference between the last modification time
 * and the last fetch time. I.e. the next fetch time will be set to
 * <code>fetchTime + fetchInterval - delta * SYNC_DELTA_RATE</code></li>
 * <li>if the adjusted fetch interval is bigger than the delta, then
 * <code>fetchInterval = delta</code>.</li>
 * </ul>
 * </li>
 * <li>the minimum value of fetchInterval may not be smaller than MIN_INTERVAL
 * (default is 1 minute).</li>
 * <li>the maximum value of fetchInterval may not be bigger than MAX_INTERVAL
 * (default is 365 days).</li>
 * </ul>
 * <p>
 * NOTE: values of DEC_FACTOR and INC_FACTOR higher than 0.4f may destabilize
 * the algorithm, so that the fetch interval either increases or decreases
 * infinitely, with little relevance to the page changes. Please use
 * {@link #main(String[])} method to test the values before applying them in a
 * production system.
 * </p>
 * 
 * @author Andrzej Bialecki
 */
public class AdaptiveFetchSchedule extends AbstractFetchSchedule {
  public static final Logger LOG = LoggerFactory.getLogger(AbstractFetchSchedule.class);

  protected float INC_RATE = 0.2f;

  protected float DEC_RATE = 0.2f;

  protected Duration MIN_INTERVAL = Duration.ofSeconds(1);

  protected Duration MAX_INTERVAL = Duration.ofDays(365);

  protected Duration SEED_MAX_INTERVAL = Duration.ofDays(1);

  protected boolean SYNC_DELTA = true;

  protected double SYNC_DELTA_RATE = 0.2f;

  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      return;
    }

    INC_RATE = conf.getFloat("db.fetch.schedule.adaptive.inc_rate", 0.2f);
    DEC_RATE = conf.getFloat("db.fetch.schedule.adaptive.dec_rate", 0.2f);
    MAX_INTERVAL = NutchConfiguration.getDuration(conf, "db.fetch.schedule.adaptive.min_interval", Duration.ofSeconds(60));
    MAX_INTERVAL = NutchConfiguration.getDuration(conf, "db.fetch.schedule.adaptive.max_interval", Duration.ofDays(365));
    SEED_MAX_INTERVAL = NutchConfiguration.getDuration(conf, "db.fetch.schedule.adaptive.seed_max_interval", Duration.ofDays(1));
    SYNC_DELTA = conf.getBoolean("db.fetch.schedule.adaptive.sync_delta", true);
    SYNC_DELTA_RATE = conf.getFloat("db.fetch.schedule.adaptive.sync_delta_rate", 0.2f);
  }

  @Override
  public void setFetchSchedule(String url, WebPage page, Instant prevFetchTime,
                               Instant prevModifiedTime, Instant fetchTime, Instant modifiedTime, int state) {
    super.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, state);
    if (modifiedTime.toEpochMilli() <= 0) {
      modifiedTime = fetchTime;
    }

    long adjustInterval = TableUtil.getFetchInterval(page).getSeconds();
    switch (state) {
      case FetchSchedule.STATUS_MODIFIED:
        adjustInterval *= (1.0f - DEC_RATE);
        break;
      case FetchSchedule.STATUS_NOTMODIFIED:
        adjustInterval *= (1.0f + INC_RATE);
        break;
      case FetchSchedule.STATUS_UNKNOWN:
        break;
    }

    if (SYNC_DELTA) {
      long delta = fetchTime.getEpochSecond() - modifiedTime.getEpochSecond();
      if (delta > adjustInterval) {
        adjustInterval = delta;
      }
      fetchTime = fetchTime.minusSeconds(Math.round(delta * SYNC_DELTA_RATE));
    }

    if (adjustInterval < MIN_INTERVAL.getSeconds()) adjustInterval = MIN_INTERVAL.getSeconds();
    if (adjustInterval > MAX_INTERVAL.getSeconds()) adjustInterval = MAX_INTERVAL.getSeconds();

    updateRefetchTime(page, Duration.ofSeconds(adjustInterval), fetchTime, prevModifiedTime, modifiedTime);
  }

  protected void updateRefetchTime(WebPage page, Duration interval, Instant fetchTime, Instant prevModifiedTime, Instant modifiedTime) {
    page.setFetchInterval((int) interval.getSeconds());
    page.setFetchTime(fetchTime.toEpochMilli() + interval.toMillis());
    page.setPrevModifiedTime(prevModifiedTime.toEpochMilli());
    page.setModifiedTime(modifiedTime.toEpochMilli());
  }

  public static void main(String[] args) throws Exception {
    FetchSchedule fs = new SeedFirstFetchSchedule();
    fs.setConf(NutchConfiguration.create());

    final Duration timeDelta = Duration.ofHours(2); // 2 hours
    // we trigger the update of the page every 30 days
    final Duration update = Duration.ofDays(1);

    // we start the time at 0, for simplicity
    Instant curTime = TCP_IP_STANDARDIZED_TIME;
    Instant lastModified = TCP_IP_STANDARDIZED_TIME;

    boolean changed = true;
    int miss = 0;
    int totalMiss = 0;
    int maxMiss = 0;
    int fetchCnt = 0;
    int changeCnt = 0;
    // initial fetchInterval is 10 days
    // WebPage p = new WebPage(1, 3600 * 24 * 30, 1.0f);
    WebPage p = WebPage.newBuilder().build();
    p.setStatus(0);
    p.setFetchInterval((int) Duration.ofDays(1).getSeconds()); // 3600 * 24 * 30
    p.setScore(1.0f);

    p.setFetchTime(TCP_IP_STANDARDIZED_TIME.toEpochMilli());
    TableUtil.putMetadata(p, META_IS_SEED, YES_STRING);

    LOG.info(p.toString());
    // let's move the timeline a couple of deltas
    for (int i = 0; i < 1000; i++) {
      if (lastModified.plus(update).isBefore(curTime)) {
        // System.out.println("i=" + i + ", lastModified=" + lastModified +
        // ", update=" + update + ", curTime=" + curTime);
        changed = true;
        changeCnt++;
        lastModified = curTime;
      }

      LOG.info("");
      LOG.info(i + ". " + (changed ? "[changed] " : "[no-change] ")
          + "Last Modified : " + getRoundNumber(p, p.getModifiedTime(), timeDelta) + ", "
          + "Next fetch : " + getRoundNumber(p, p.getFetchTime(), timeDelta) + ", "
          + "Interval : " + getRound(p, timeDelta) + " round, "
          + "Score : " + p.getScore() + ", "
          + "Missed " + miss);

      if (p.getFetchTime() <= curTime.toEpochMilli()) {
        fetchCnt++;
        fs.setFetchSchedule("http://www.example.com", p,
            Instant.ofEpochMilli(p.getFetchTime()),
            Instant.ofEpochMilli(p.getModifiedTime()),
            curTime,
            lastModified,
            changed ? FetchSchedule.STATUS_MODIFIED : FetchSchedule.STATUS_NOTMODIFIED);

        LOG.info("\tFetched & Adjusted: " + ", "
            + "Last Modified : " + getRoundNumber(p, p.getModifiedTime(), timeDelta) + ", "
            + "Next fetch : " + getRoundNumber(p, p.getFetchTime(), timeDelta) + ", "
            + "Score : " + p.getScore() + ", "
            + "Interval : " + getRound(p, timeDelta) + " round");

        TableUtil.updateReferredPublishTime(p, curTime);
        TableUtil.increaseFetchCount(p);
        TableUtil.increaseReferredArticles(p, 1);
        TableUtil.increaseReferredChars(p, 2500);
        setScore(p);

        if (!changed) miss++;
        if (miss > maxMiss) maxMiss = miss;

        changed = false;
        totalMiss += miss;
        miss = 0;
      }

      if (changed) {
        miss++;
      }

      // move time line
      curTime = curTime.plus(timeDelta);
    }
    LOG.info("Total missed: " + totalMiss + ", max miss: " + maxMiss);
    LOG.info("Page changed " + changeCnt + " times, fetched " + fetchCnt + " times.");
  }

  private static long getRoundNumber(WebPage p, long time, Duration timeDelta) {
    return (time - TCP_IP_STANDARDIZED_TIME.toEpochMilli()) / timeDelta.toMillis();
  }

  private static String getRound(WebPage p, Duration timeDelta) {
    final DecimalFormat df = new DecimalFormat("0.0");

    return df.format(1.0 * TableUtil.getFetchInterval(p).toMillis()  / timeDelta.toMillis());
  }

  private static void setScore(WebPage p) {
    float f1 = 1.0f;
    float f2 = 2.0f;

    long ra = TableUtil.getReferredArticles(p);
    long rc = TableUtil.getReferredChars(p);

    p.setScore(f1 * ra + f2 * ((rc - 1000) / 1000));
  }
}
