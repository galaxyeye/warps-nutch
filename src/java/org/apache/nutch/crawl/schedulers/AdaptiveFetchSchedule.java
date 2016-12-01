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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

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

  protected float INC_RATE;

  protected float DEC_RATE;

  protected long MAX_INTERVAL;

  protected long SEED_MAX_INTERVAL;

  protected int MIN_INTERVAL;

  protected boolean SYNC_DELTA;

  protected double SYNC_DELTA_RATE;

  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      return;
    }

    INC_RATE = conf.getFloat("db.fetch.schedule.adaptive.inc_rate", 0.2f);
    DEC_RATE = conf.getFloat("db.fetch.schedule.adaptive.dec_rate", 0.2f);
    MIN_INTERVAL = conf.getInt("db.fetch.schedule.adaptive.min_interval", 60);
    MAX_INTERVAL = conf.getLong("db.fetch.schedule.adaptive.max_interval", Duration.ofDays(365).getSeconds()); // 1 year
    SEED_MAX_INTERVAL = conf.getLong("db.fetch.schedule.adaptive.seed_max_interval", Duration.ofDays(1).getSeconds());
    SYNC_DELTA = conf.getBoolean("db.fetch.schedule.adaptive.sync_delta", true);
    SYNC_DELTA_RATE = conf.getFloat("db.fetch.schedule.adaptive.sync_delta_rate", 0.2f);
  }

  @Override
  public void setFetchSchedule(String url, WebPage page, long prevFetchTime,
                               long prevModifiedTime, long fetchTime, long modifiedTime, int state) {
    super.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, state);
    long refTime = fetchTime;
    if (modifiedTime <= 0) {
      modifiedTime = fetchTime;
    }

    long interval = page.getFetchInterval();
    switch (state) {
      case FetchSchedule.STATUS_MODIFIED:
        interval *= (1.0f - DEC_RATE);
        break;
      case FetchSchedule.STATUS_NOTMODIFIED:
        interval *= (1.0f + INC_RATE);
        break;
      case FetchSchedule.STATUS_UNKNOWN:
        break;
    }

    if (SYNC_DELTA) {
      // try to synchronize with the time of change
      // TODO: different from normal class (is delta in seconds)?
      int delta = (int) ((fetchTime - modifiedTime) / 1000L);
      if (delta > interval) {
        interval = delta;
      }
      refTime = fetchTime - Math.round(delta * SYNC_DELTA_RATE);
    }

    if (interval < MIN_INTERVAL) interval = MIN_INTERVAL;
    if (interval > MAX_INTERVAL) interval = MAX_INTERVAL;

    updateRefetchTime(page, interval, refTime, prevModifiedTime, modifiedTime);
  }

  protected void updateRefetchTime(WebPage page, long interval, long refTime, long prevModifiedTime, long modifiedTime) {
    page.setFetchInterval((int)interval);
    page.setFetchTime(refTime + interval * 1000L);
    page.setModifiedTime(modifiedTime);
    page.setPrevModifiedTime(prevModifiedTime);
  }

  public static void main(String[] args) throws Exception {
    FetchSchedule fs = new AdaptiveFetchSchedule();
    fs.setConf(NutchConfiguration.create());
    // we start the time at 0, for simplicity
    long curTime = 0;
    long delta = 1000L * 3600L * 24L; // 2 hours
    // we trigger the update of the page every 30 days
    long update = 1000L * 3600L * 24L * 30L; // 30 days
    boolean changed = true;
    long lastModified = 0;
    int miss = 0;
    int totalMiss = 0;
    int maxMiss = 0;
    int fetchCnt = 0;
    int changeCnt = 0;
    // initial fetchInterval is 10 days
    // WebPage p = new WebPage(1, 3600 * 24 * 30, 1.0f);
    WebPage p = WebPage.newBuilder().build();
    p.setStatus(0);
    p.setFetchInterval(3600 * 24 * 30);
    p.setScore(1.0f);
    p.setFetchTime(0L);

    LOG.info(p.toString());
    // let's move the timeline a couple of deltas
    for (int i = 0; i < 10000; i++) {
      if (lastModified + update < curTime) {
        // System.out.println("i=" + i + ", lastModified=" + lastModified +
        // ", update=" + update + ", curTime=" + curTime);
        changed = true;
        changeCnt++;
        lastModified = curTime;
      }

      LOG.info(i + ". " + changed + "\twill fetch at "
          + (p.getFetchTime() / delta) + "\tinterval "
          + (p.getFetchInterval() / Duration.ofDays(365).getSeconds()) + " days" + "\t missed "
          + miss);
      if (p.getFetchTime() <= curTime) {
        fetchCnt++;
        fs.setFetchSchedule("http://www.example.com", p, p.getFetchTime(), p.getModifiedTime(), curTime, lastModified,
            changed ? FetchSchedule.STATUS_MODIFIED
                : FetchSchedule.STATUS_NOTMODIFIED);

        LOG.info("\tfetched & adjusted: " + "\twill fetch at "
            + (p.getFetchTime() / delta) + "\tinterval "
            + (p.getFetchInterval() / Duration.ofDays(365).getSeconds()) + " days");

        if (!changed) miss++;
        if (miss > maxMiss) maxMiss = miss;
        changed = false;
        totalMiss += miss;
        miss = 0;
      }

      if (changed) {
        miss++;
      }
      curTime += delta;
    }
    LOG.info("Total missed: " + totalMiss + ", max miss: " + maxMiss);
    LOG.info("Page changed " + changeCnt + " times, fetched " + fetchCnt + " times.");
  }
}
